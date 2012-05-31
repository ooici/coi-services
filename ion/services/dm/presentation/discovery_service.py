#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/presentation/discovery_service.py
@description The Discovery service supports finding resources by metadata attributes, potentially applying semantic reasoning
'''


from interface.objects import View, SearchQuery, Index, Catalog, ElasticSearchIndex, CouchDBIndex
from interface.services.dm.idiscovery_service import BaseDiscoveryService
from pyon.util.containers import DotDict
from pyon.util.arg_check import validate_true, validate_is_instance
from pyon.public import PRED, CFG, RT, log
from pyon.core.exception import BadRequest
from pyon.event.event import EventPublisher
from pyon.core.bootstrap import obj_registry
from pyon.core.object import IonObjectDeserializer
from ion.services.dm.inventory.index_management_service import IndexManagementService
from collections import deque
from pyparsing import CaselessLiteral, Regex, Optional, removeQuotes, MatchFirst, quotedString, ParseException

import elasticpy as ep
import heapq

SEARCH_BUFFER_SIZE=1024

class DiscoveryService(BaseDiscoveryService):

    """
    class docstring
    """

    def on_start(self): # pragma: no cover
        super(DiscoveryService,self).on_start()

        self.use_es = CFG.get_safe('system.elasticsearch',False)

        self.elasticsearch_host = CFG.get_safe('server.elasticsearch.host','localhost')
        self.elasticsearch_port = CFG.get_safe('server.elasticsearch.port','9200')

        self.ep = EventPublisher(event_type = 'SearchBufferExceededEvent')
        self.heuristic_cutoff = 4

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
            return self.clients.resource_registry.find_associations_mult(subjects=resource_ids,id_only=True)[0]
            
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
            return self.clients.resource_registry.find_associations_mult(subjects=resource_ids,id_only=True)[0]
            
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
        if self.query_is_term_search(query):
            source_id = self._match_query_sources(query['index']) or query['index']
            kwargs = dict(
                source_id= source_id,
                field    = query['field'].lower(),
                value    = query['value'].lower(),
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
        elif self.query_is_association_search(query):
            kwargs = dict(
                resource_id = query['association'],
                id_only     = id_only
            )
            if query.get('depth'):
                kwargs['depth'] = query['depth']
            return self.query_association(**kwargs)
        elif self.query_is_range_search(query):
            source_id = self._match_query_sources(query['index']) or query['index']
            kwargs = dict(
                source_id  = source_id,
                field      = query['field'],
                from_value = query['range']['from'],
                to_value   = query['range']['to'],
                limit      = limit,
                id_only    = id_only
            )
            
            if query.get('limit'):
                kwargs['limit'] = query['limit']
            if query.get('order'):
                kwargs['order'] = query['order']
            if query.get('offset'):
                kwargs['offset'] = query['offset']
            
            return self.query_range(**kwargs)
        elif self.query_is_collection_search(query):
            return self.query_collection(
                collection_id = query['collection'],
                id_only       = id_only
            )

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

    def query_term(self, source_id='', field='', value='', order=None, limit=0, offset=0, id_only=False):
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
        validate_is_instance(index, Index, '%s does not refer to a valid index.' % index)
        if order: 
            validate_is_instance(order,dict, 'Order is incorrect.')
            es.sort(**order)

        if limit:
            es.size(limit)

        if offset:
            es.from_offset(offset)

        if field == '*':
            field = '_all'

        query = ep.ElasticQuery().wildcard(field=field, value=value)
        response = IndexManagementService._es_call(es.search_index_advanced,index.index_name,query)

        IndexManagementService._check_response(response)

        return self._results_from_response(response, id_only)

    def query_range(self, source_id='', field='', from_value=None, to_value=None, order=None, limit=0, offset=0, id_only=False):
        
        if not self.use_es:
            raise BadRequest('Can not make queries without ElasticSearch, enable in res/config/pyon.yml')

        validate_true(not from_value is None, 'from_value not specified')
        validate_true(isinstance(from_value,int) or isinstance(from_value,float), 'from_value is not a valid number')
        validate_true(not to_value is None, 'to_value not specified')
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
        validate_is_instance(index,Index,'%s does not refer to a valid index.' % source_id)
        if order:
            validate_is_instance(order,dict,'Order is incorrect.')
            es.sort(**order)

        if limit:
            es.size(limit)

        if field == '*':
            value = '_all'

        query = ep.ElasticQuery().range(
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

        resources = self.clients.resource_registry.read_mult(resource_ids)

        return resources

    def query_collection(self, collection_id='', id_only=False):
        validate_true(collection_id, 'Unspecified collection id')
        resource_ids = self.clients.index_management.list_collection_resources(collection_id, id_only=True)
        if id_only:
            return resource_ids
        
        resources = map(self.clients.resource_registry.read,resource_ids)
        return resources

    def es_complex_query(self,query,and_queries=None,or_queries=None):
        pass


    def es_map_query(self, query): 
        '''
        Maps an query request to an ElasticSearch query
        '''
        if not self.use_es:
            raise BadRequest('Can not make queries without ElasticSearch, enable in res/config/pyon.yml')
        if query_is_term_search(query):
            return ep.ElasticQuery().wildcard(field=query['field'],value=query['value'])
        if query_is_range_search(query):
            return ep.ElasticQuery().range(
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


    #=========================================
    # Methods for checking the requests
    #=========================================
        
    def query_is_term_search(self, query=None):
        if not query:
            return False
        if not isinstance(query,dict):
            return False
        if query.has_key('index') and query.has_key('field') and query.has_key('value'):
            return True
        return False

    def query_is_range_search(self,query=None):
        if not query:
            return False
        if not isinstance(query,dict):
            return False
        if query.has_key('range') and isinstance(query['range'], dict) and query['range'].has_key('from') and query['range'].has_key('to') and query.has_key('index') and query.has_key('field'):
            return True
        return False
    def query_is_association_search(self,query=None): 
        if not query:
            return False
        if not isinstance(query,dict):
            return False
        if query.has_key('association'):
            return True
        return False

    def query_is_collection_search(self, query=None):
        if not query:
            return False
        if not isinstance(query,dict):
            return False
        if query.has_key('collection'):
            return True
        return False

    def raise_search_buffer_exceeded(self):
        self.ep.publish_event(origin='Discovery Service', description='Search buffer was exceeded, results may not contain all the possible results.')

    def _results_from_response(self, response, id_only):
        deserializer = IonObjectDeserializer(obj_registry=obj_registry)
        if not (response.has_key('hits') and response['hits'].has_key('hits')):
            return []

        hits = response['hits']['hits']
       
        if len(hits) > 0:
            if len(hits) >= SEARCH_BUFFER_SIZE:
                log.warning("Query results exceeded search buffer limitations")
                self.raise_search_buffer_exceeded()
            if id_only:
                return list([i['_id'] for i in hits])
            results = map(deserializer.deserialize,hits)
            return results
        
        else:
            return []





class QueryLanguage(object):
    '''
    Pyon Discovery Query DSL BNF

    SEARCH "model" IS "abc*" FROM "models" AND BELONGS TO "platformDeviceID"
    SEARCH "runtime" IS VALUES FROM 1. TO 100 FROM "devices" AND BELONGS TO "RSN resource ID"
    BELONGS TO "org resource ID"
    SEARCH "model" IS "sbc*" FROM "deices" ORDER BY "name" LIMIT 30 AND BELONGS TO "platformDeviceID"


             <sentence> ::= <query> [<query-filter>] [("AND" <sentence>)|("OR" <sentence>)]
                <query> ::= <search-query> | <association-query> | <collection-query>
    <association-query> ::= "BELONGS TO" <resource-id> [<limit-parameter>]
     <collection-query> ::= "IN" <collection-id>
         <search_query> ::= "SEARCH" <field> (<term-query> | <range-query>) "FROM" <index-name> [<query-parameter>]*
      <query-parameter> ::= <order-parameter> | <limit-parameter> | <offset-parameter>
     <offset-parameter> ::= "SKIP" <integer>
      <order-parameter> ::= "ORDER BY" <limited-string>
      <limit-parameter> ::= "LIMIT" <integer>
      <depth-parameter> ::= "DEPTH" <integer>
           <term-query> ::= "IS" <field-query>
          <field-query> ::= <wildcard-string>
          <range-query> ::= "VALUES FROM" <number> "TO" <number>
           <index-name> ::= <python-string>
        <collection-id> ::= <resource_id>
          <resource-id> ::= REGEX( "[a-zA-Z0-9]+" )
         <query-filter> ::= "FILTER" <python-string>
                <field> ::= <limited-string> | "*"
       <limited-string> ::= REGEX( "[a-zA-Z0-9_\.]+" )
      <wildcard-string> ::= <python-string>
        <python-string> ::= REGEX( "[^"]+" )
              <number>  ::= <integer> | <double>
              <double>  ::= 0-9 ('.' 0-9)
              <integer> ::= 0-9
    '''
    def __init__(self):


        self.json_query = {'query':{}, 'and': [], 'or': []}
        self.tokens = None
        #--------------------------------------------------------------------------------------
        # <integer> ::= 0-9
        # <double>  ::= 0-9 ('.' 0-9)
        # <number>  ::= <integer> | <double>
        #--------------------------------------------------------------------------------------
        integer = Regex(r'[0-9]+') # Word matches space for some reason
        double = Regex(r'[0-9]+.?[0-9]*')
        number = double | integer
        
        #--------------------------------------------------------------------------------------
        # <python-string>   ::= (String surrounded by double-quotes)
        # <wildcard-string> ::= <python-string>
        # <limited-string>  ::= '"' a..z A..Z 9..9 _ . '"' (alpha nums and ._ surrounded by double quotes)
        # <field>           ::= <limited-string> | "*"
        #--------------------------------------------------------------------------------------
        python_string = quotedString.setParseAction(removeQuotes)
        wildcard_string = python_string
        limited_string = Regex(r'("(?:[a-zA-Z0-9_\.])*"|\'(?:[a-zA-Z0-9_\.]*)\')').setParseAction(removeQuotes)
        field = limited_string ^ CaselessLiteral('"*"').setParseAction(removeQuotes)
        
        #--------------------------------------------------------------------------------------
        # <query-filter> ::= "FILTER" <python-string>
        # <index-name>   ::= <python-string>
        # <resource-id>  ::= '"' a..z A..Z 0..9 '"' (alpha nums surrounded by double quotes)
        # <collection-id> ::= <resource-id>
        #--------------------------------------------------------------------------------------
        query_filter = CaselessLiteral("FILTER") + python_string
        # Add the filter to the frame object
        query_filter.setParseAction(lambda x : self.frame.update({'filter' : x[1]}))
        index_name = MatchFirst(python_string)
        # Add the index to the frame object
        index_name.setParseAction(lambda x : self.frame.update({'index' : x[0]}))
        resource_id = Regex(r'("(?:[a-zA-Z0-9])*"|\'(?:[a-zA-Z0-9]*)\')').setParseAction(removeQuotes)
        collection_id = resource_id

        #--------------------------------------------------------------------------------------
        # <range-query>  ::= "VALUES FROM" <number> "TO" <number>
        #--------------------------------------------------------------------------------------
        range_query = CaselessLiteral("VALUES") + CaselessLiteral("FROM") + number + CaselessLiteral("TO") + number
        # Add the range to the frame object
        range_query.setParseAction(lambda x : self.frame.update({'range':{'from' : float(x[2]), 'to' : float(x[4])}}))

        #--------------------------------------------------------------------------------------
        # <field-query>  ::= <wildcard-string>
        # <term-query>   ::= "IS" <field-query>
        #--------------------------------------------------------------------------------------
        field_query = wildcard_string
        term_query = CaselessLiteral("IS") + field_query
        # Add the term to the frame object
        term_query.setParseAction(lambda x : self.frame.update({'value':x[1]}))

        #--------------------------------------------------------------------------------------
        # <limit-parameter>  ::= "LIMIT" <integer>
        # <depth-parameter>  ::= "DEPTH" <integer>
        # <order-parameter>  ::= "ORDER" "BY" <limited-string>
        # <offset-parameter> ::= "SKIP" <integer>
        # <query-parameter>  ::= <order-paramater> | <limit-parameter>
        #--------------------------------------------------------------------------------------
        limit_parameter = CaselessLiteral("LIMIT") + integer
        limit_parameter.setParseAction(lambda x: self.frame.update({'limit' : int(x[1])}))
        depth_parameter = CaselessLiteral("DEPTH") + integer
        depth_parameter.setParseAction(lambda x: self.frame.update({'depth' : int(x[1])}))
        order_parameter = CaselessLiteral("ORDER") + CaselessLiteral("BY") + limited_string
        order_parameter.setParseAction(lambda x: self.frame.update({'order' : {x[2] : 'asc'}}))
        offset_parameter = CaselessLiteral("SKIP") + integer
        offset_parameter.setParseAction(lambda x : self.frame.update({'offset' : int(x[1])}))
        query_parameter = limit_parameter | order_parameter | offset_parameter
        
        #--------------------------------------------------------------------------------------
        # <search-query>      ::= "SEARCH" <field> (<range-query> | <term-query>) "FROM" <index-name> [<query-parameter>]*
        # <collection-query>  ::= "IN <collection-id>"
        # <association-query> ::= "BELONGS TO" <resource-id> [ <depth-parameter> ]
        # <query>             ::= <search-query> | <association-query> | <collection-query>
        #--------------------------------------------------------------------------------------
        search_query = CaselessLiteral("SEARCH") + field + (range_query | term_query) + CaselessLiteral("FROM") + index_name + query_parameter*(0,None)
        # Add the field to the frame object
        search_query.setParseAction(lambda x : self.frame.update({'field' : x[1]}))
        collection_query = CaselessLiteral("IN") + collection_id
        collection_query.setParseAction(lambda x : self.frame.update({'collection': x[1]}))
        association_query = CaselessLiteral("BELONGS") + CaselessLiteral("TO") + resource_id + Optional(depth_parameter)
        # Add the association to the frame object
        association_query.setParseAction(lambda x : self.frame.update({'association':x[2]}))
        query = search_query | association_query | collection_query
        
        #--------------------------------------------------------------------------------------
        # <primary-query>  ::= <query> [<query-filter>] 
        # <atom>           ::= <query>
        # <intersection>   ::= "AND" <atom>
        # <union>          ::= "OR" <atom>
        # <sentence>       ::= <primary-query> [<intersection>]* [<union>]*
        #--------------------------------------------------------------------------------------
        primary_query = query + Optional(query_filter) 
        # Set the primary query on the json_query to the frame and clear the frame
        primary_query.setParseAction(lambda x : self.push_frame())
        atom = query
        intersection = CaselessLiteral("AND") + atom
        # Add an AND operation to the json_query and clear the frame
        intersection.setParseAction(lambda x : self.and_frame())
        union = CaselessLiteral("OR") + atom
        # Add an OR operation to the json_query and clear the frame
        union.setParseAction(lambda x : self.or_frame())

        self.sentence = primary_query + (intersection ^ union)*(0,None)
    
    def push_frame(self):
        self.json_query['query'] = self.frame
        self.frame = dict()

    def and_frame(self):
        if self.json_query.has_key('and'):
            self.json_query['and'].append(self.frame)
        else:
            self.json_query['and'] = [self.frame]
        self.frame = dict()

    def or_frame(self):
        if self.json_query.has_key('or'):
            self.json_query['or'].append(self.frame)
        else:
            self.json_query['or'] = [self.frame]
        self.frame = dict()


    def parse(self, s):
        '''
        Parses string s and returns a json_query object, self.tokens is set to the tokens
        '''
        self.json_query = {'query':{}, 'and': [], 'or': []}
        self.frame = {}
        try:
            self.tokens = self.sentence.parseString(s)
        except ParseException as e:
            raise BadRequest('%s' % e)

        return self.json_query
