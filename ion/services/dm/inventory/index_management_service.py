#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/inventory/index_management_service.py
@description Micro implementation of Index Management using ElasticSearch to index resources.

'''
from pyon.ion.resource import RT
from pyon.public import CFG, PRED
from gevent.event import AsyncResult
from gevent import Timeout
from pyon.util.async import spawn
from pyon.core.exception import NotFound, BadRequest, ServerError
from interface.objects import Index, SearchOptions, Collection, CouchDBIndex, ElasticSearchIndex
from interface.services.dm.iindex_management_service import BaseIndexManagementService
from pyon.util.arg_check import validate_true, validate_is_instance

import pyon.core.exception as exceptions


class IndexManagementService(BaseIndexManagementService):
    """
    Service will primarily manage the inventory and metadata for indexes, via index resource objects,
    that may be created and live in a separate technology. Indexes are used primarily for navigation/filtering of
    resources, not for query

    @see https://confluence.oceanobservatories.org/display/syseng/CIAD+DM+OV+Index+Management+Service
    """

    COUCHDB_INDEX       = 'couchdb_index'
    ELASTICSEARCH_INDEX = 'elasticsearch_index'

    '''
    couchdb_river indices  are generic bulk collection river indices which don't use a filter and simply indexes the entire datastore
    simple indices         are completely empty indexes in elastic search
    advanced indices       are similar to a blank slate but give the inital options for shards and replicas (ideal)
    '''

    def on_start(self):
        super(IndexManagementService,self).on_start()

        self.elasticsearch_host = CFG.server.elasticsearch.host
        self.elasticsearch_port = CFG.server.elasticsearch.port

    @staticmethod
    def _es_call(es, *args, **kwargs):
        res = AsyncResult()
        def async_call(es, *args, **kwargs):
            res.set(es(*args,**kwargs))
        spawn(async_call,es,*args,**kwargs)
        try:
            retval = res.get(timeout=10)
        except Timeout:
            raise exceptions.Timeout("Call to ElasticSearch timed out.")
        return retval

    @staticmethod
    def _check_response(response):
        validate_is_instance(response,dict,"Malformed response from ElasticSearch (%s)" % response, ServerError)
        if response.has_key('error'):
            raise ServerError("ElasticSearch error: %s" % response['error'])
        if response.has_key('ok') and not response['ok']:
            # Cant determine a better exception to throw, add a new one perhapse?
            raise NotFound("ElasticSearch Response: %s" % response)
        




    def create_index(self, name='', description='', content_type='', options=None, datastore_name='', view_name=''):
        
        res, _ = self.clients.resource_registry.find_resources(name=name)
        if len(res)>0:
            raise BadRequest('Resource with name %s already exists.' % name)

        if content_type == IndexManagementService.ELASTICSEARCH_INDEX:
            index = ElasticSearchIndex(name=name, description=description)
            index.content_type=IndexManagementService.ELASTICSEARCH_INDEX
            index.index_name = name 
            if options:
                index.options = options
            index_id, _ = self.clients.resource_registry.create(index)
            return index_id


        elif content_type == IndexManagementService.COUCHDB_INDEX:
            index = CouchDBIndex(name=name, description=description)
            index.datastore_name = datastore_name
            index.view_name = view_name
            index.content_type=IndexManagementService.COUCHDB_INDEX
            if options:
                index.options = options
            index_id, _ = self.clients.resource_registry.create(index)
            return index_id

        else:
            raise BadRequest('Unknown content_type or not specified')

    def update_index(self, index=None):
        if index is None:
            raise BadRequest("No index specified")
        validate_is_instance(index,Index,'The specified index is not of type interface.objects.Index')
        return self.clients.resource_registry.update(index)

    def read_index(self, index_id=''):
        """
        Retrieves the index resource from the registry.

        @param index_id    str
        @retval index    Index
        """


        index_resources = self.clients.resource_registry.read(index_id)
        return index_resources

    def delete_index(self, index_id=''):
        """
        Deletes and removes the index from the registry and from ElasticSearch

        @param index_id    str
        @retval success    bool
        """
        subjects, assocs = self.clients.resource_registry.find_subjects(object=index_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)
        self.clients.resource_registry.delete(index_id)
        return True

    def list_indexes(self, id_only=True):
        '''
        Obtains a list of indices based on the active resources
        @return List of indices
        '''
        indices = list()
        std_indexes, _           = self.clients.resource_registry.find_resources(restype=RT.Index, id_only=False)
        indices.extend(std_indexes)
        elasticsearch_indexes, _ = self.clients.resource_registry.find_resources(restype=RT.ElasticSearchIndex, id_only=False)
        indices.extend(elasticsearch_indexes)
        couchdb_indexes, _       = self.clients.resource_registry.find_resources(restype=RT.CouchDBIndex,id_only=False)
        indices.extend(couchdb_indexes)

        retval = dict()
        for index in indices:
            if id_only:
                retval.update({index.name:index._id})
            else:
                retval.update({index.name:index})
        return retval

    def find_indexes(self, index_name='', filters=None):
        validate_true(index_name,"No index name provided")
        indices = self.list_indexes()
        for name, index_id in indices.iteritems():
            if index_name in name:
                return index_id
        else:
            return None 


    def create_collection(self, name='', resources=None):
        res, _ = self.clients.resource_registry.find_resources(name=name)
        if len(res) > 0:
            raise BadRequest('Resource with name %s already exists.' % name)

        if resources is None:
            raise BadRequest('No resources provided to make collection.')

        validate_true(len(resources)>0, 'No resources provided to make collection.')


        collection = Collection(name=name)
        collection_id, _ = self.clients.resource_registry.create(collection)
        for resource in resources:
            self.clients.resource_registry.create_association(
               subject=collection_id, 
               predicate=PRED.hasElement, 
               object=resource
           )

        return collection_id

    def read_collection(self, collection_id=''):
        return self.clients.resource_registry.read(collection_id)

    def update_collection(self, collection=None):
        validate_is_instance(collection,Collection)

        return self.clients.resource_registry.update(collection)

    def delete_collection(self, collection_id=''):
        self.clients.resource_registry.delete(collection_id)
        return True

    def list_collection_resources(self, collection_id='', id_only=False):
        '''
        id_only added for convenience only
        '''
        collection = self.read_collection(collection_id)
        results = dict()


        return self.clients.resource_registry.find_objects(subject=collection_id, predicate=PRED.hasElement, id_only=id_only)[0]

    def find_collection(self, collection_name='', resource_ids=[]):
        if not resource_ids: resource_ids = []
        validate_true(collection_name or resource_ids, 'You must specify either a name or a list of resources.')
        results = set()
        
        if collection_name:
            colls = self.clients.resource_registry.find_resources(name=collection_name, restype=RT.Collection,id_only=True)
            results = results.union(colls[0])

        if resource_ids:
            for resource_id in resource_ids:
                assocs = self.clients.resource_registry.find_associations(object=resource_id, predicate=PRED.hasElement, id_only=False)
                collections = [assoc.s for assoc in assocs]
                results = results.union(collections)

        return list(results)



