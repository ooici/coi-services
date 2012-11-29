#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/presentation/discovery_service.py
@description The Discovery service supports finding resources by metadata attributes, potentially applying semantic reasoning
'''


from interface.objects import View, Catalog, ElasticSearchIndex
from interface.services.dm.idiscovery_service import BaseDiscoveryService
from pyon.util.containers import DotDict, get_safe
from pyon.util.arg_check import validate_true, validate_is_instance
from pyon.public import PRED, CFG, RT, log
from pyon.core.exception import BadRequest
from pyon.event.event import EventPublisher
from pyon.core.bootstrap import get_obj_registry, get_sys_name
from pyon.core.object import IonObjectDeserializer
from ion.services.dm.inventory.index_management_service import IndexManagementService
from ion.processes.bootstrap.index_bootstrap import STD_INDEXES
from collections import deque
from ion.services.dm.utility.query_language import QueryLanguage

import dateutil.parser
import time
import elasticpy as ep
import heapq

SEARCH_BUFFER_SIZE=1024

class DiscoveryService(BaseDiscoveryService):

    """
    class docstring
    """

    def on_start(self): # pragma no cover
        super(DiscoveryService,self).on_start()

        self.use_es = CFG.get_safe('system.elasticsearch',False)

        self.elasticsearch_host = CFG.get_safe('server.elasticsearch.host','localhost')
        self.elasticsearch_port = CFG.get_safe('server.elasticsearch.port','9200')

        self.ep = EventPublisher(event_type = 'SearchBufferExceededEvent')
        self.heuristic_cutoff = 4

    
   
    @staticmethod
    def es_cleanup():
        es_host = CFG.get_safe('server.elasticsearch.host', 'localhost')
        es_port = CFG.get_safe('server.elasticsearch.port', '9200')
        es = ep.ElasticSearch(
            host=es_host,
            port=es_port,
            timeout=10
        )
        indexes = STD_INDEXES.keys()
        indexes.append('%s_resources_index' % get_sys_name().lower())
        indexes.append('%s_events_index' % get_sys_name().lower())

        for index in indexes:
            IndexManagementService._es_call(es.river_couchdb_delete,index)
            IndexManagementService._es_call(es.index_delete,index)

    #===================================================================
    # Views
    #===================================================================
        

    def create_view(self, view_name='', description='', fields=None, order=None, filters=''):
        """Creates a view which has the specified search fields, the order in which the search fields are presented
        to a query and a term filter.
        @param view_name Name of the view
        @param description Simple descriptive sentence
        @param fields Search fields
        @param order List of fields to determine order of precendence in which the results are presented
        @param filter Simple term filter

        @param view_name    str
        @param description    str
        @param fields    list
        @param order    list
        @param filters    str
        @retval view_id    str
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

    def list_catalogs(self, view_id=''):
        catalogs, _ = self.clients.resource_registry.find_objects(subject=view_id, object_type=RT.Catalog, predicate=PRED.hasCatalog, id_only=True)
        return catalogs


    #===================================================================
    # Helper Methods
    #===================================================================
    def _match_query_sources(self, source_name):
        index = self.clients.index_management.find_indexes(source_name)
        if index:
            return index
        _, resources = self.clients.resource_registry.find_resources(name=source_name, id_only=True)
        for res in resources:
            t = res['type']
            if t == 'View' or t == 'ElasticSearchIndex' or t == 'Catalog':
                return res['id']
        return None


    #===================================================================
    # Query Methods
    #===================================================================


    def query(self, query=None):
        validate_true(query,'Invalid query')

        return self.request(query)


    def query_couch(self, index_id='', key='', limit=0, offset=0, id_only=True):
        raise BadRequest('Not Implemented Yet')
#        cc = self.container
#
#        datastore_name = source.datastore_name
#        db = cc.datastore_manager.get_datastore(datastore_name)
#        view_name = source.view_name
#        opts = DotDict(include_docs=True)
#        opts.start_key = [query.query]
#        opts.end_key = [query.query,{}]
#        if query.results:
#            opts.limit = query.results
#        if query.offset:
#            opts.skip = query.offset
#
#        return db.query_view(view_name,opts=opts)

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


            


    def intersect(self, left=[], right=[]):
        """The intersection between two sets of resources.

        @param left    list
        @param right    list
        @retval result    list
        """
        return list(set(left).intersection(right))

    def union(self, left=[], right=[]):
        return list(set(left).union(right))

    def parse(self, search_request=''):
        parser = QueryLanguage()
        query_request = parser.parse(search_request)
        return self.request(query_request)

    def query_request(self, query=None, limit=0, id_only=False):
        validate_is_instance(query,dict, 'invalid query')

        #---------------------------------------------
        # Term Search
        #---------------------------------------------
        if QueryLanguage.query_is_term_search(query):
            source_id = self._match_query_sources(query['index']) or query['index']
            kwargs = dict(
                source_id= source_id,
                field    = query['field'],
                value    = query['value'],
                limit    = limit,
                id_only  = id_only
            )
            
            if query.get('limit'):
                kwargs['limit'] = query['limit']
            if query.get('order'):
                kwargs['order'] = query['order']
            if query.get('offset'):
                kwargs['offset'] = query['offset']

            return self.query_term(**kwargs)

        #---------------------------------------------
        # Fuzzy searching (phrases and such)
        #---------------------------------------------
        elif QueryLanguage.query_is_fuzzy_search(query):
            source_id = self._match_query_sources(query['index']) or query['index']
            kwargs = dict(
                source_id= source_id,
                fuzzy    = True,
                field    = query['field'],
                value    = query['fuzzy'],
                limit    = limit,
                id_only  = id_only
            )
            
            if query.get('limit'):
                kwargs['limit'] = query['limit']
            if query.get('order'):
                kwargs['order'] = query['order']
            if query.get('offset'):
                kwargs['offset'] = query['offset']

            return self.query_term(**kwargs)
        
        #---------------------------------------------
        # Association Search
        #---------------------------------------------
        elif QueryLanguage.query_is_association_search(query):
            kwargs = dict(
                resource_id = query['association'],
                id_only     = id_only
            )
            if query.get('depth'):
                kwargs['depth'] = query['depth']
            return self.query_association(**kwargs)

        elif QueryLanguage.query_is_owner_search(query):
            kwargs = dict(
                resource_id = query['owner'],
                id_only     = id_only
            )
            if query.get('depth'):
                kwargs['depth'] = query['depth']
            return self.query_owner(**kwargs)
        
        #---------------------------------------------
        # Range Search
        #---------------------------------------------
        elif QueryLanguage.query_is_range_search(query):
            source_id = self._match_query_sources(query['index']) or query['index']
            kwargs = dict(
                source_id  = source_id,
                field      = query['field'],
                limit      = limit,
                id_only    = id_only
            )
            if get_safe(query,'range.from') is not None:
                kwargs['from_value'] = query['range']['from']
            if get_safe(query,'range.to') is not None:
                kwargs['to_value'] = query['range']['to']
                
            if query.get('limit'):
                kwargs['limit'] = query['limit']
            if query.get('order'):
                kwargs['order'] = query['order']
            if query.get('offset'):
                kwargs['offset'] = query['offset']
            
            return self.query_range(**kwargs)
        
        #---------------------------------------------
        # Time Search
        #---------------------------------------------
        elif QueryLanguage.query_is_time_search(query):
            source_id = self._match_query_sources(query['index']) or query['index']
            kwargs = dict(
                source_id  = source_id,
                field      = query['field'],
                limit      = limit,
                id_only    = id_only
            )
            if get_safe(query,'range.from') is not None:
                kwargs['from_value'] = query['range']['from']
            if get_safe(query,'range.to') is not None:
                kwargs['to_value'] = query['range']['to']
            if query.get('limit'):
                kwargs['limit'] = query['limit']
            if query.get('order'):
                kwargs['order'] = query['order']
            if query.get('offset'):
                kwargs['offset'] = query['offset']
            
            return self.query_time(**kwargs)
        
        
        #---------------------------------------------
        # Collection Search
        #---------------------------------------------
        elif QueryLanguage.query_is_collection_search(query):
            return self.query_collection(
                collection_id = query['collection'],
                id_only       = id_only
            )
        
        #---------------------------------------------
        # Geo Distance Search
        #---------------------------------------------
        elif QueryLanguage.query_is_geo_distance_search(query):
            source_id = self._match_query_sources(query['index']) or query['index']
            kwargs = dict(
                source_id = source_id,
                field     = query['field'],
                origin    = [query['lon'], query['lat']],
                distance  = query['dist'],
                units     = query['units'],
                id_only   = id_only
            )
            if query.get('limit'):
                kwargs['limit'] = query['limit']
            if query.get('order'):
                kwargs['order'] = query['order']
            if query.get('offset'):
                kwargs['offset'] = query['offset']
            return self.query_geo_distance(**kwargs)
        
        #---------------------------------------------
        # Geo Bounding Box Search
        #---------------------------------------------
        elif QueryLanguage.query_is_geo_bbox_search(query):
            source_id = self._match_query_sources(query['index']) or query['index']
            kwargs = dict(
                source_id    = source_id,
                field        = query['field'],
                top_left     = query['top_left'],
                bottom_right = query['bottom_right'],
                id_only      = id_only,
            )
            if query.get('limit'):
                kwargs['limit'] = query['limit']
            if query.get('order'):
                kwargs['order'] = query['order']
            if query.get('offset'):
                kwargs['offset'] = query['offset']
            return self.query_geo_bbox(**kwargs)


        #@todo: query for couch
        raise BadRequest('improper query: %s' % query)

    def _multi(self, cb,source, *args, **kwargs):
        '''
        Manage the different collections of indexes for queries, views, catalogs
        Expand the resource into it's components and call the callback for each subcategory
        '''
        if isinstance(source, View):
            catalogs = self.list_catalogs(source._id)
            result_queue = list()
            for catalog in catalogs:
                result_queue.extend(cb(catalog, *args, **kwargs))
            if kwargs.has_key('limit') and kwargs['limit']:
                return result_queue[:kwargs['limit']]
            return result_queue

        if isinstance(source, Catalog):
            indexes = self.clients.catalog_management.list_indexes(source._id, id_only=True)
            result_queue = list()
            for index in indexes:
                result_queue.extend(cb(index, *args, **kwargs))
            if kwargs.has_key('limit') and kwargs['limit']:
                return result_queue[:kwargs['limit']]
            return result_queue
        return None

    def query_term(self, source_id='', field='', value='', fuzzy=False, order=None, limit=0, offset=0, id_only=False):
        '''
        Elasticsearch Query against an index
        > discovery.query_index('indexID', 'name', '*', order={'name':'asc'}, limit=20, id_only=False)
        '''
        if not self.use_es:
            raise BadRequest('Can not make queries without ElasticSearch, enable system.elasticsearch to make queries.')

        validate_true(source_id, 'Unspecified source_id')
        validate_true(field, 'Unspecified field')
        validate_true(value, 'Unspecified value')


        es = ep.ElasticSearch(host=self.elasticsearch_host, port=self.elasticsearch_port)

        source = self.clients.resource_registry.read(source_id)

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        # If source is a view, catalog or collection go through it and recursively call query_range on all the results in the indexes
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        iterate = self._multi(self.query_term, source, field=field, value=value, order=order, limit=limit, offset=offset, id_only=id_only)
        if iterate is not None:
            return iterate


        index = source
        validate_is_instance(index, ElasticSearchIndex, '%s does not refer to a valid index.' % index)
        if order: 
            validate_is_instance(order,dict, 'Order is incorrect.')
            es.sort(**order)

        if limit:
            es.size(limit)

        if offset:
            es.from_offset(offset)

        if field == '*':
            field = '_all'

        if fuzzy:
            query = ep.ElasticQuery.fuzzy_like_this(value, fields=[field])
        elif '*' in value:
            query = ep.ElasticQuery.wildcard(field=field, value=value)
        else:
            query = ep.ElasticQuery.field(field=field, query=value)

        response = IndexManagementService._es_call(es.search_index_advanced,index.index_name,query)

        IndexManagementService._check_response(response)

        return self._results_from_response(response, id_only)

    def query_range(self, source_id='', field='', from_value=None, to_value=None, order=None, limit=0, offset=0, id_only=False):
        
        if not self.use_es:
            raise BadRequest('Can not make queries without ElasticSearch, enable in res/config/pyon.yml')

        if from_value is not None:
            validate_true(isinstance(from_value,int) or isinstance(from_value,float), 'from_value is not a valid number')
        if to_value is not None:
            validate_true(isinstance(to_value,int) or isinstance(to_value,float), 'to_value is not a valid number')
        validate_true(source_id, 'source_id not specified')

        es = ep.ElasticSearch(host=self.elasticsearch_host, port=self.elasticsearch_port)


        source = self.clients.resource_registry.read(source_id)

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        # If source is a view, catalog or collection go through it and recursively call query_range on all the results in the indexes
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        iterate = self._multi(self.query_range, source, field=field, from_value=from_value, to_value=to_value, order=order, limit=limit, offset=offset, id_only=id_only)
        if iterate is not None:
            return iterate

        index = source
        validate_is_instance(index,ElasticSearchIndex,'%s does not refer to a valid index.' % source_id)
        if order:
            validate_is_instance(order,dict,'Order is incorrect.')
            es.sort(**order)

        if limit:
            es.size(limit)

        if field == '*':
            field = '_all'

        query = ep.ElasticQuery.range(
            field      = field,
            from_value = from_value,
            to_value   = to_value
        )
        response = IndexManagementService._es_call(es.search_index_advanced,index.index_name,query)

        IndexManagementService._check_response(response)

        return self._results_from_response(response, id_only)

    def query_time(self, source_id='', field='', from_value=None, to_value=None, order=None, limit=0, offset=0, id_only=False):
        if not self.use_es:
            raise BadRequest('Can not make queries without ElasticSearch, enable in res/config/pyon.yml')

        if from_value is not None:
            validate_is_instance(from_value,basestring,'"From" is not a valid string (%s)' % from_value)

        if to_value is not None:
            validate_is_instance(to_value,basestring,'"To" is not a valid string')

        es = ep.ElasticSearch(host=self.elasticsearch_host, port=self.elasticsearch_port)

        source = self.clients.resource_registry.read(source_id)

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        # If source is a view, catalog or collection go through it and recursively call query_time on all the results in the indexes
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        iterate = self._multi(self.query_time, source, field=field, from_value=from_value, to_value=to_value, order=order, limit=limit, offset=offset, id_only=id_only)
        if iterate is not None:
            return iterate

        index = source
        validate_is_instance(index,ElasticSearchIndex,'%s does not refer to a valid index.' % source_id)
        if order:
            validate_is_instance(order,dict,'Order is incorrect.')
            es.sort(**order)

        if limit:
            es.size(limit)

        if field == '*':
            field = '_all'

        if from_value is not None:
            from_value = time.mktime(dateutil.parser.parse(from_value).timetuple()) * 1000

        if to_value is not None:
            to_value = time.mktime(dateutil.parser.parse(to_value).timetuple()) * 1000

        query = ep.ElasticQuery.range(
            field      = field,
            from_value = from_value,
            to_value   = to_value
        )
        response = IndexManagementService._es_call(es.search_index_advanced,index.index_name,query)

        IndexManagementService._check_response(response)

        return self._results_from_response(response, id_only)





    def query_association(self,resource_id='', depth=0, id_only=False):
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

    def query_geo_distance(self, source_id='', field='', origin=None, distance='', units='mi',order=None, limit=0, offset=0, id_only=False):
        validate_true(isinstance(origin,(tuple,list)) , 'Origin is not a list or tuple.')
        validate_true(len(origin)==2, 'Origin is not of the right size: (2)')

        if not self.use_es:
            raise BadRequest('Can not make queries without ElasticSearch, enable in res/config/pyon.yml')

        es = ep.ElasticSearch(host=self.elasticsearch_host, port=self.elasticsearch_port)
        source = self.clients.resource_registry.read(source_id)

        iterate = self._multi(self.query_geo_distance, source=source, field=field, origin=origin, distance=distance) 
        if iterate is not None:
            return iterate

        index = source
        validate_is_instance(index,ElasticSearchIndex, '%s does not refer to a valid index.' % index)

        sorts = ep.ElasticSort()
        if order is not None and isinstance(order,dict):
            sort_field = order.keys()[0]
            value = order[sort_field]
            sorts.sort(sort_field,value)
            es.sorted(sorts)

        if limit:
            es.size(limit)

        if offset:
            es.from_offset(offset)

        if field == '*':
            field = '_all'


        sorts.geo_distance(field, origin, units)

        es.sorted(sorts)

        filter = ep.ElasticFilter.geo_distance(field,origin, '%s%s' %(distance,units))

        es.filtered(filter)

        query = ep.ElasticQuery.match_all()

        response = IndexManagementService._es_call(es.search_index_advanced,index.index_name,query)
        IndexManagementService._check_response(response)

        return self._results_from_response(response,id_only)


    def query_geo_bbox(self, source_id='', field='', top_left=None, bottom_right=None, order=None, limit=0, offset=0, id_only=False):
        validate_true(isinstance(top_left, (list,tuple)), 'Top Left is not a list or a tuple')
        validate_true(len(top_left)==2, 'Top Left is not of the right size: (2)')
        validate_true(isinstance(bottom_right, (list,tuple)), 'Bottom Right is not a list or a tuple')
        validate_true(len(bottom_right)==2, 'Bottom Right is not of the right size: (2)')

        if not self.use_es:
            raise BadRequest('Can not make queries without ElasticSearch, enable in res/config/pyon.yml')

        es = ep.ElasticSearch(host=self.elasticsearch_host, port=self.elasticsearch_port)
        source = self.clients.resource_registry.read(source_id)

        iterate = self._multi(self.query_geo_bbox, source=source, field=field, top_left=top_left, bottom_right=bottom_right, order=order, limit=limit, offset=offset, id_only=id_only)
        if iterate is not None:
            return iterate

        index = source
        validate_is_instance(index,ElasticSearchIndex, '%s does not refer to a valid index.' % index)

        sorts = ep.ElasticSort()
        if order is not None and isinstance(order,dict):
            sort_field = order.keys()[0]
            value = order[sort_field]
            sorts.sort(sort_field,value)
            es.sorted(sorts)

        if limit:
            es.size(limit)

        if offset:
            es.from_offset(offset)

        if field == '*':
            field = '_all'


        filter = ep.ElasticFilter.geo_bounding_box(field, top_left, bottom_right)

        es.filtered(filter)

        query = ep.ElasticQuery.match_all()

        response = IndexManagementService._es_call(es.search_index_advanced,index.index_name,query)
        IndexManagementService._check_response(response)

        return self._results_from_response(response,id_only)

        

    def es_complex_query(self,query,and_queries=None,or_queries=None):
        pass


    def es_map_query(self, query): 
        '''
        Maps an query request to an ElasticSearch query
        '''
        if not self.use_es:
            raise BadRequest('Can not make queries without ElasticSearch, enable in res/config/pyon.yml')
        if QueryLanguage.query_is_term_search(query):
            return ep.ElasticQuery.wildcard(field=query['field'],value=query['value'])
        if QueryLanguage.query_is_range_search(query):
            return ep.ElasticQuery.range(
                field      = query['field'],
                from_value = query['range']['from'],
                to_value   = query['range']['to']
            )
        

    def request(self, query=None, id_only=True):
        if not query:
            raise BadRequest('No request query provided')
        if not query.has_key('query'):
            raise BadRequest('Unsuported request. %s')

        #==============================
        # Check the form of the query
        #==============================
        #@todo: convert to IonObject
        if not (query.has_key('query') and query.has_key('and') and query.has_key('or')):
            raise BadRequest('Improper query request: %s' % query)

        query_queue = list()

        query = DotDict(query)
        #================================================
        # Tier-1 Query
        #================================================

        if not (query['or'] or query['and']): # Tier-1
            return self.query_request(query.query)

        #@todo: bulk requests against ES
        #@todo: filters

        #================================================
        # Tier-2 Query
        #================================================
        
        query_queue.append(self.query_request(query.query,limit=SEARCH_BUFFER_SIZE, id_only=True))
        
        #==================
        # Intersection
        #==================
        for q in query['and']:
            query_queue.append(self.query_request(q, limit=SEARCH_BUFFER_SIZE, id_only=True))
        while len(query_queue) > 1:
            tmp = self.intersect(query_queue.pop(), query_queue.pop())
            query_queue.append(tmp)
        
        #==================
        # Union
        #==================
        for q in query['or']:
            query_queue.append(self.query_request(q, limit=SEARCH_BUFFER_SIZE, id_only=True))
        while len(query_queue) > 1:
            tmp = self.union(query_queue.pop(), query_queue.pop())
            query_queue.append(tmp)
        
        if id_only:
            return query_queue[0]

        objects = self.clients.resource_registry.read_mult(query_queue[0])
        return objects




    def raise_search_buffer_exceeded(self):
        self.ep.publish_event(origin='Discovery Service', description='Search buffer was exceeded, results may not contain all the possible results.')

    def _results_from_response(self, response, id_only):
        deserializer = IonObjectDeserializer(obj_registry=get_obj_registry())
        if not (response.has_key('hits') and response['hits'].has_key('hits')):
            return []

        hits = response['hits']['hits']
       
        if len(hits) > 0:
            if len(hits) >= SEARCH_BUFFER_SIZE:
                log.warning("Query results exceeded search buffer limitations")
                self.raise_search_buffer_exceeded()
            if id_only:
                return [str(i['_id']) for i in hits]
            results = map(deserializer.deserialize,hits)
            return results
        
        else:
            return []




