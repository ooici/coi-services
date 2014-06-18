#!/usr/bin/env python

"""The Discovery service supports finding resources and events by metadata attributes, potentially applying semantic reasoning"""

__author__ = 'Luke Campbell <LCampbell@ASAScience.com>, Michael Meisinger'

from collections import deque
import heapq

from pyon.datastore.datastore_query import QUERY_EXP_KEY, DQ
from pyon.public import PRED, CFG, RT, log, BadRequest, EventPublisher, get_sys_name, NotFound
from pyon.util.arg_check import validate_true, validate_is_instance

from ion.services.dm.utility.query_language import QueryLanguage
from ion.services.dm.presentation.ds_discovery import DatastoreDiscovery

from interface.services.dm.idiscovery_service import BaseDiscoveryService
from interface.objects import View, Catalog


class DiscoveryService(BaseDiscoveryService):
    MAX_SEARCH_RESULTS = CFG.get_safe('service.discovery.max_search_results', 250)

    def on_start(self):
        super(DiscoveryService, self).on_start()

        cfg_datastore = CFG.get_safe('container.datastore.default_server')
        if cfg_datastore != "postgresql":
            raise Exception("Discovery service does not support datastores other than postgresql")

        self.ds_discovery = DatastoreDiscovery(self)

   
    #===================================================================
    # Query Methods
    #===================================================================

    def parse(self, search_request='', id_only=True, search_args=None):
        """Parses a given string request and assembles the query, processes the query and returns the results of the query.
        See the query language definition: https://confluence.oceanobservatories.org/display/CIDev/Discovery+Service+Query+Format

        @param search_request    str
        @param id_only    bool
        @param search_args dict
        @retval results    list
        """
        log.info("Search DSL: %s", search_request)
        query_request = self._parse_query_string(search_request)
        return self._discovery_request(query_request, id_only=id_only, search_args=search_args, query_params=search_args)

    def _parse_query_string(self, query_string):
        """Given a query string in Discovery service DSL, parse and return query structure"""
        parser = QueryLanguage()
        query_request = parser.parse(query_string)
        return query_request

    def query(self, query=None, id_only=True, search_args=None):
        """Issue a query provided in structured dict format or internal datastore query format.
        Returns a list of resource or event objects or their IDs only.
        Search_args may contain parameterized values.
        See the query format definition: https://confluence.oceanobservatories.org/display/CIDev/Discovery+Service+Query+Format

        @param query    dict
        @param id_only    bool
        @param search_args dict
        @retval results    list
        """
        validate_true(query, 'Invalid query')

        return self._discovery_request(query, id_only, search_args=search_args, query_params=search_args)

    def query_view(self, view_id='', view_name='', ext_query=None, id_only=True, search_args=None):
        """Execute an existing query as defined within a View resource, providing additional arguments for
        parameterized values.
        If ext_query is provided, it will be combined with the query defined by the View.
        Search_args may contain parameterized values.
        Returns a list of resource or event objects or their IDs only.
        """
        if not view_id and not view_name:
            raise BadRequest("Must provide argument view_id or view_name")
        if view_id and view_name:
            raise BadRequest("Cannot provide both arguments view_id and view_name")
        if view_id:
            view_obj = self.clients.resource_registry.read(view_id)
        else:
            view_obj = self.ds_discovery.get_builtin_view(view_name)
            if not view_obj:
                view_objs, _ = self.clients.resource_registry.find_resources(restype=RT.View, name=view_name)
                if not view_objs:
                    raise NotFound("View with name '%s' not found" % view_name)
                view_obj = view_objs[0]

        if view_obj.type_ != RT.View:
            raise BadRequest("Argument view_id is not a View resource")
        view_query = view_obj.view_definition
        if not QUERY_EXP_KEY in view_query:
            raise BadRequest("Unknown View query format")

        # Get default query params and override them with provided args
        param_defaults = {param.name: param.default for param in view_obj.view_parameters}
        query_params = param_defaults
        if view_obj.param_values:
            query_params.update(view_obj.param_values)
        if search_args:
            query_params.update(search_args)

        # Merge ext_query into query
        if ext_query:
            if ext_query["where"] and view_query["where"]:
                view_query["where"] = [DQ.EXP_AND, [view_query["where"], ext_query["where"]]]
            else:
                view_query["where"] = view_query["where"] or ext_query["where"]
            if ext_query["order_by"]:
                # Override ordering if present
                view_query["where"] = ext_query["order_by"]

            # Other query settings
            view_qargs = view_query["query_args"]
            ext_qargs = ext_query["query_args"]
            view_qargs["id_only"] = ext_qargs.get("id_only", view_qargs["id_only"])
            view_qargs["limit"] = ext_qargs.get("limit", view_qargs["limit"])
            view_qargs["skip"] = ext_qargs.get("skip", view_qargs["skip"])

        return self._discovery_request(view_query, id_only=id_only,
                                       search_args=search_args, query_params=query_params)

    def _discovery_request(self, query=None, id_only=True, search_args=None, query_params=None):
        search_args = search_args or {}
        if not query:
            raise BadRequest('No request query provided')

        if QUERY_EXP_KEY in query and self.ds_discovery:
            query.setdefault("query_args", {})["id_only"] = id_only
            # Query in datastore query format (dict)
            log.debug("Executing datastore query: %s", query)

        elif "QUERYDSL" in query:
            # Query in DSL format
            if "query_str" not in query:
                raise BadRequest('No query_str provided')
            query = self._parse_query_string(query["query_str"])

        elif 'query' not in query:
            raise BadRequest('Unsupported request. %s' % query)

        # if count requested, run id_only query without limit/skip
        count = search_args.get("count", False)
        if count:
            # Only return the count of ID only search
            query.pop("limit", None)
            query.pop("skip", None)
            res = self.ds_discovery.execute_query(query, id_only=True, query_args=search_args, query_params=query_params)
            return [len(res)]

        # TODO: Not all queries are permissible by all users

        # Execute the query
        query_results = self.ds_discovery.execute_query(query, id_only=id_only,
                                                        query_args=search_args, query_params=query_params)

        # Strip out unwanted object attributes for size
        filtered_res = self._strip_query_results(query_results, id_only=id_only, search_args=search_args)

        return filtered_res

    def _strip_query_results(self, query_results, id_only, search_args):
        # Filter the results for smaller result size
        attr_filter = search_args.get("attribute_filter", [])
        if type(attr_filter) not in (list, tuple):
            raise BadRequest("Illegal argument type: attribute_filter")

        if not id_only and attr_filter:
            filtered_res = [dict(__noion__=True, **{k: v for k, v in obj.__dict__.iteritems() if k in attr_filter or k in {"_id", "type_"}}) for obj in query_results]
            return filtered_res
        return query_results


    #===================================================================
    # View Management
    #===================================================================

    def create_view(self, view=None):
        if view is None or not isinstance(view, View):
            raise BadRequest("Illegal argument: view")

        # view_objs, _ = self.clients.resource_registry.find_resources(restype=RT.View, name=view.name)
        # if view_objs:
        #     raise BadRequest("View with name '%s' already exists" % view.name)

        view_id, _ = self.clients.resource_registry.create(view)
        return view_id

    def read_view(self, view_id=''):
        view_res = self.clients.resource_registry.read(view_id)
        if not isinstance(view_res, View):
            raise BadRequest("Resource %s is not a View" % view_id)
        return view_res

    def update_view(self, view=None):
        if view is None or not isinstance(view, View):
            raise BadRequest("Illegal argument: view")
        self.clients.resource_registry.update(view)
        return True

    def delete_view(self, view_id=''):
        self.clients.resource_registry.delete(view_id)
        return True


    def create_catalog_view(self, view_name='', description='', fields=None, order=None, filters=''):
        """Creates a view which has the specified search fields, the order in which the search fields are presented
        to a query and a term filter.
        @param view_name Name of the view
        @param description Simple descriptive sentence
        @param fields Search fields
        @param order List of fields to determine order of precendence in which the results are presented
        @param filters Simple term filter
        """
        res, _ = self.clients.resource_registry.find_resources(name=view_name, id_only=True)
        if len(res) > 0:
            raise BadRequest('The view resource with name: %s, already exists.' % view_name)

        #======================
        # Arg Validations
        #======================
        validate_is_instance(fields,list, 'Specified fields must be a list.')
        validate_true(len(fields)>0, 'Specfied fields must be a list.')
        if order is not None:
            validate_is_instance(order,list, 'Specified order must be a list of fields')
            for field in order:
                if not field in fields:
                    raise BadRequest('The specified ordering field was not part of the search fields.')

        fields = set(fields) # Convert fields to a set for aggregation across the catalogs
        #======================================================================================================
        # Priorty Queue Index Matching
        #======================================================================================================

        pq = [] # Priority queue for matching
        catalog_id = None
        catalogs, _ = self.clients.resource_registry.find_resources(restype=RT.Catalog, id_only=False)
        for catalog in catalogs:
            if set(catalog.catalog_fields).issubset(fields):
                index_num = len(self.clients.catalog_management.list_indexes(catalog._id))
                heapq.heappush(pq, (index_num,catalog))
        if pq:
            weight, catalog = heapq.heappop(pq)
            if weight < 4:
                catalog_id = catalog._id


        if catalog_id is None:
            catalog_id = self.clients.catalog_management.create_catalog('%s_catalog'% view_name, keywords=list(fields))

        view_res = View(name=view_name, description=description)
        view_res.order = order
        view_res.filters = filters
        view_id, _ = self.clients.resource_registry.create(view_res)
        self.clients.resource_registry.create_association(subject=view_id, predicate=PRED.hasCatalog,object=catalog_id)
        return view_id

