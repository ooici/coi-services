#!/usr/bin/env python

"""The Discovery service supports finding resources and events by metadata attributes, potentially applying semantic reasoning"""

__author__ = 'Luke Campbell <LCampbell@ASAScience.com>, Michael Meisinger'

from collections import deque
import heapq

from pyon.util.containers import DotDict, get_safe
from pyon.util.arg_check import validate_true, validate_is_instance
from pyon.public import PRED, CFG, RT, log, BadRequest, EventPublisher, get_sys_name

from ion.services.dm.utility.query_language import QueryLanguage
from ion.services.dm.presentation.ds_discovery import DatastoreDiscovery

from interface.services.dm.idiscovery_service import BaseDiscoveryService
from interface.objects import View, Catalog


class DiscoveryService(BaseDiscoveryService):
    MAX_SEARCH_RESULTS=CFG.get_safe('service.discovery.max_search_results', 250)

    def on_start(self): # pragma no cover
        super(DiscoveryService,self).on_start()

        cfg_datastore = CFG.get_safe('container.datastore.default_server')
        if cfg_datastore != "postgresql":
            raise Exception("Discovery service does not support datastores other than postgresql")

        self.ds_discovery = DatastoreDiscovery(self)

   
    #===================================================================
    # Query Methods
    #===================================================================

    def query(self, query=None, id_only=True, search_args={}):
        """Issue a query against the indexes as specified in the query, applying filters and operators
        accordingly. The query format is a structured dict.
        See the query format definition: https://confluence.oceanobservatories.org/display/CIDev/Discovery+Service+Query+Format

        @param query    dict
        @param id_only    bool
        @param search_args dict
        @retval results    list
        """
        validate_true(query, 'Invalid query')

        return self.request(query, id_only, search_args=search_args)

    def parse(self, search_request='', id_only=True, search_args={}):
        """Parses a given string request and assembles the query, processes the query and returns the results of the query.
        This is the primary means of interfacing with the search features in discovery.
        See the query language definition: https://confluence.oceanobservatories.org/display/CIDev/Discovery+Service+Query+Format

        @param search_request    str
        @param id_only    bool
        @param search_args dict
        @retval results    list
        """
        log.info("Search DSL: %s", search_request)
        query_request = self._parse_query_string(search_request)
        return self.request(query_request, id_only=id_only, search_args=search_args)

    def _parse_query_string(self, query_string):
        """Given a query string in Discovery service DSL, parse and return query structure"""
        parser = QueryLanguage()
        query_request = parser.parse(query_string)
        return query_request

    def request(self, query=None, id_only=True, search_args={}):
        if not query:
            raise BadRequest('No request query provided')

        if "QUERYEXP" in query and self.ds_discovery:
            # Support for datastore queries
            pass

        elif 'query' not in query:
            raise BadRequest('Unsuported request. %s' % query)

        # if count requested, run id_only query without limit/skip
        count = search_args and search_args.get("count", False)
        if count:
            """Only return the count of ID only search"""
            query.pop("limit", None)
            query.pop("skip", None)
            res = self.ds_discovery.execute_query(query, id_only=True)
            return [len(res)]

        res = self.ds_discovery.execute_query(query, id_only=id_only)

        return res


    #===================================================================
    # Special Query Methods
    #===================================================================

    def query_association(self, resource_id='', depth=0, id_only=False):
        validate_true(resource_id, 'Unspecified resource')
        if depth:
            resource_ids = self.iterative_traverse(resource_id, depth-1)
        else:
            resource_ids = self.traverse(resource_id)
        if id_only:
            return resource_ids

        if not isinstance(resource_ids, list):
            resource_ids = list(resource_ids)
        resources = self.clients.resource_registry.read_mult(resource_ids)

        return resources

    def query_owner(self, resource_id='', depth=0, id_only=False):
        validate_true(resource_id, 'Unspecified resource')
        if depth:
            resource_ids = self.iterative_traverse(resource_id, depth-1)
        else:
            resource_ids = self.reverse_traverse(resource_id)
        if id_only:
            return resource_ids

        if not isinstance(resource_ids, list):
            resource_ids = list(resource_ids)
        resources = self.clients.resource_registry.read_mult(resource_ids)

        return resources

    def query_collection(self,collection_id='', id_only=False):
        validate_true(collection_id, 'Unspecified collection id')
        resource_ids = self.clients.index_management.list_collection_resources(collection_id, id_only=True)
        if id_only:
            return resource_ids
        
        resources = map(self.clients.resource_registry.read,resource_ids)
        return resources


    def traverse(self, resource_id=''):
        """Breadth-first traversal of the association graph for a specified resource.

        @param resource_id    str
        @retval resources    list
        """

        def edges(resource_ids=[]):
            if not isinstance(resource_ids, list):
                resource_ids = list(resource_ids)
            return self.clients.resource_registry.find_objects_mult(subjects=resource_ids,id_only=True)[0]

        visited_resources = deque(edges([resource_id]))
        traversal_queue = deque()
        done = False
        t = None
        while not done:
            t = traversal_queue or deque(visited_resources)
            traversal_queue = deque()
            for e in edges(t):
                if not e in visited_resources:
                    visited_resources.append(e)
                    traversal_queue.append(e)
            if not len(traversal_queue): done = True

        return list(visited_resources)

    def reverse_traverse(self, resource_id=''):
        """Breadth-first traversal of the association graph for a specified resource.

        @param resource_id    str
        @retval resources    list
        """

        def edges(resource_ids=[]):
            if not isinstance(resource_ids,list):
                resource_ids = list(resource_ids)
            return self.clients.resource_registry.find_subjects_mult(objects=resource_ids,id_only=True)[0]

        visited_resources = deque(edges([resource_id]))
        traversal_queue = deque()
        done = False
        t = None
        while not done:
            t = traversal_queue or deque(visited_resources)
            traversal_queue = deque()
            for e in edges(t):
                if not e in visited_resources:
                    visited_resources.append(e)
                    traversal_queue.append(e)
            if not len(traversal_queue): done = True



        return list(visited_resources)


    def iterative_traverse(self, resource_id='', limit=-1):
        '''
        Iterative breadth first traversal of the resource associations
        '''
        #--------------------------------------------------------------------------------
        # Retrieve edges for this resource
        #--------------------------------------------------------------------------------
        def edges(resource_ids=[]):
            if not isinstance(resource_ids, list):
                resource_ids = list(resource_ids)
            return self.clients.resource_registry.find_objects_mult(subjects=resource_ids,id_only=True)[0]

        gathered = deque()
        visited_resources = deque(edges([resource_id]))
        while limit>0:
            t = gathered or deque(visited_resources)
            for e in edges(t):
                if not e in visited_resources:
                    visited_resources.append(e)
                    gathered.append(e)
            if not len(gathered): break

            t = deque(gathered)
            gathered = deque()
            limit -= 1

        return list(visited_resources)

    def iterative_reverse_traverse(self, resource_id='', limit=-1):
        '''
        Iterative breadth first traversal of the resource associations
        '''
        #--------------------------------------------------------------------------------
        # Retrieve edges for this resource
        #--------------------------------------------------------------------------------
        def edges(resource_ids=[]):
            if not isinstance(resource_ids, list):
                resource_ids = list(resource_ids)
            return self.clients.resource_registry.find_subjects_mult(objects=resource_ids,id_only=True)[0]

        gathered = deque()
        visited_resources = deque(edges([resource_id]))
        while limit>0:
            t = gathered or deque(visited_resources)
            for e in edges(t):
                if not e in visited_resources:
                    visited_resources.append(e)
                    gathered.append(e)
            if not len(gathered): break

            t = deque(gathered)
            gathered = deque()
            limit -= 1

        return list(visited_resources)


    #===================================================================
    # View Management
    #===================================================================

    def create_view(self, view_name='', description='', fields=None, order=None, filters=''):
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
            if weight < self.heuristic_cutoff:
                catalog_id = catalog._id


        if catalog_id is None:
            catalog_id = self.clients.catalog_management.create_catalog('%s_catalog'% view_name, keywords=list(fields))

        view_res = View(name=view_name, description=description)
        view_res.order = order
        view_res.filters = filters
        view_id, _ = self.clients.resource_registry.create(view_res)
        self.clients.resource_registry.create_association(subject=view_id, predicate=PRED.hasCatalog,object=catalog_id)
        return view_id

    def read_view(self, view_id=''):
        return self.clients.resource_registry.read(view_id)

    def update_view(self, view=None):
        self.clients.resource_registry.update(view)
        return True

    def delete_view(self, view_id=''):
        _, assocs = self.clients.resource_registry.find_objects_mult(subjects=[view_id])
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)
        self.clients.resource_registry.delete(view_id)
        return True


