#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file
@date 04/17/12 09:07
@description DESCRIPTION
'''

from mock import Mock

from nose.plugins.attrib import attr
from interface.objects import Index, Collection, SearchOptions, ElasticSearchIndex, CouchDBIndex, InformationResource, Resource, Association
from interface.services.dm.iindex_management_service import IndexManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.core.exception import BadRequest, NotFound
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from ion.services.dm.inventory.index_management_service import IndexManagementService
import unittest



@attr('UNIT',group='dm')
class IndexManagementUnitTest(PyonTestCase):
    def setUp(self):
        mock_clients = self._create_service_mock('index_management')
        self.index_management = IndexManagementService()
        self.index_management.clients = mock_clients

        self.rr_create = mock_clients.resource_registry.create
        self.rr_read   = mock_clients.resource_registry.read
        self.rr_update = mock_clients.resource_registry.update
        self.rr_delete = mock_clients.resource_registry.delete
        self.rr_find_resources = mock_clients.resource_registry.find_resources
        self.rr_find_assocs    = mock_clients.resource_registry.find_associations
        self.rr_find_subj      = mock_clients.resource_registry.find_subjects
        self.rr_find_obj       = mock_clients.resource_registry.find_objects
        self.rr_delete_assoc   = mock_clients.resource_registry.delete_association

        self.get_datastore = Mock()
        self.db_create = Mock()

        self.get_datastore.return_value = DotDict({'datastore_name':'test_datastore'})
        self.index_management.container = DotDict({
            'datastore_manager':DotDict({
                'get_datastore' : self.get_datastore
            })
        })
        self.index_management.elasticsearch_host = 'notarealhost'
        self.index_management.elasticsearch_port = 9000
        self.index_name = 'test_index'

    def test_create_index(self):
        '''
        test_create_index
        Unit test for basic creation of an index
        '''

        # Mocks
        self.rr_create.return_value = ('index_id','rev')
        self.rr_find_resources.return_value = ([],[])

        retval = self.index_management.create_index(name='mock', content_type=IndexManagementService.ELASTICSEARCH_INDEX, options='ugh')

        self.assertTrue(retval=='index_id','invalid return value: %s' % retval)
        self.assertTrue(self.rr_create.called)

        retval = self.index_management.create_index(name='argh', content_type=IndexManagementService.COUCHDB_INDEX)

        self.assertTrue(retval=='index_id','invalid return value: %s' % retval)

        with self.assertRaises(BadRequest):
            self.index_management.create_index(name='another', content_type='not_listed')

    def test_dup_index(self):
        # Mocks
        self.rr_find_resources.return_value = ([1],[1])

        # Execution
        with self.assertRaises(BadRequest):
            self.index_management.create_index('mock_index_id')


    def test_read_index(self):
        # mocks
        return_obj = dict(mock='mock')
        self.rr_read.return_value = return_obj

        # execution
        retval = self.index_management.read_index('mock_index_id')

        # assertions
        self.assertEquals(return_obj, retval, 'The resource should be returned.')


    def test_update_index(self):
        with self.assertRaises(BadRequest):
            self.index_management.update_index()
        with self.assertRaises(BadRequest):
            self.index_management.update_index('hi')
        self.index_management.update_index(Index())



    def test_delete_index(self):

        self.index_management.delete_index('index_id')
        self.rr_delete.assert_called_with('index_id')



    def test_list_indexes(self):
        # Mocks
        self.rr_find_resources.return_value = ([
                DotDict({'_id':'1','name':'1'}),
                DotDict({'_id':'2','name':'2'}),
                DotDict({'_id':'3','name':'3'}),
                DotDict({'_id':'4','name':'4'})
            ],[1,2,3,4])

        # Execution
        retval = self.index_management.list_indexes()

        # Assertions
        self.assertTrue(retval == {'1':'1','2':'2','3':'3','4':'4'}, 'Index mismatch')
    
    def test_find_indexes(self):
        self.index_management.list_indexes=Mock()
        self.index_management.list_indexes.return_value = {'index_name':'1'}
        retval = self.index_management.find_indexes('index_name')
        self.assertTrue(retval=='1')
        self.index_management.list_indexes.return_value = {}
        retval = self.index_management.find_indexes('index_name')
        self.assertTrue(retval==None)

    def test_create_collection(self):
        self.rr_create.return_value = 'collection_id', 'rev'

        self.rr_find_resources.return_value = ([0],[0])
        with self.assertRaises(BadRequest):
            self.index_management.create_collection('test',[0])

        self.rr_find_resources.return_value = ([],[])

        with self.assertRaises(BadRequest):
            self.index_management.create_collection('test',[])

        retval = self.index_management.create_collection('test',[0])
        self.assertTrue(retval=='collection_id')
            
    def test_read_collection(self):
        self.rr_read.return_value = 'retval'
        retval = self.index_management.read_collection('test')
        self.assertTrue(retval=='retval')

    def test_update_collection(self):
        with self.assertRaises(BadRequest):
            ind = Index()
            self.index_management.update_collection(ind)
        self.index_management.update_collection(Collection())
        self.assertTrue(self.rr_update.called)

    def test_delete_collection(self):
        self.rr_find_assocs.return_value = ['assoc']

        retval = self.index_management.delete_collection('collection_id')
        self.assertTrue(retval)
        self.rr_delete.assert_called_once_with('collection_id')
        self.rr_delete_assoc.assert_called_once_with('assoc')

    def test_list_collection_resources(self):
        self.rr_find_obj.return_value = (['test_id'],[''])


        result1 = self.index_management.list_collection_resources('collection_id', id_only=True)
        self.assertTrue(result1 == ['test_id'])
    
    def test_find_collection(self):
        self.rr_find_resources.return_value = (['test'],[])

        retval = self.index_management.find_collection(collection_name='hi')
        self.assertTrue(retval == ['test'] , '%s' % retval)

        fake_collection = Collection(resources=['test_res_id'])
        fake_assoc = Association(s='test_id')
        self.rr_find_assocs.return_value = [fake_assoc]

        retval = self.index_management.find_collection(resource_ids=['test_res_id'])
        self.assertTrue(retval == ['test_id'], '%s' % retval)

        with self.assertRaises(BadRequest):
            self.index_management.find_collection()



@attr('INT',group='dm')
class IndexManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.ims_cli = IndexManagementServiceClient()
        self.rr_cli  = ResourceRegistryServiceClient()
        self.index_name = 'test_index'

    def test_create_elasticsearch_index(self):
        index_name = self.index_name
        ims_cli    = self.ims_cli
        rr_cli     = self.rr_cli
        options = SearchOptions()
        options.attribute_match = ['test_field']
        index_id = ims_cli.create_index(
           name=index_name,
           content_type=IndexManagementService.ELASTICSEARCH_INDEX,
           options=options
       )
        
        index_result = self.rr_cli.read(index_id)
        self.assertIsInstance(index_result,ElasticSearchIndex)
        self.assertTrue(index_result.name == index_name)

        #======================================
        # Clean up
        #======================================
        rr_cli.delete(index_id)

    def test_create_couchdb_index(self):
        index_name = self.index_name
        ims_cli    = self.ims_cli
        rr_cli     = self.rr_cli
        options = SearchOptions()
        options.attribute_match = ['name']

        index_id = ims_cli.create_index(
            index_name, 
            content_type=IndexManagementService.COUCHDB_INDEX,
            options=options,
            datastore_name='fake',
            view_name='fake/by_fake'
        )
        index_result = self.rr_cli.read(index_id)
        self.assertIsInstance(index_result,CouchDBIndex)
        self.assertTrue(index_result.name==index_name)
        #======================================
        # Clean up
        #======================================
        rr_cli.delete(index_id)
        
    def test_read_index(self):
        ims_cli = self.ims_cli
        rr_cli  = self.rr_cli
        index_name  = self.index_name
        index_res   = Index(name=index_name)
        index_id, _ = rr_cli.create(index_res)

        index = ims_cli.read_index(index_id)
        self.assertIsInstance(index,Index)
        self.assertTrue(index.name==index_name)

        rr_cli.delete(index_id)

    def test_delete_index(self):
        ims_cli = self.ims_cli
        rr_cli  = self.rr_cli
        index_name  = self.index_name
        index_res   = Index(name=index_name)
        index_id, _ = rr_cli.create(index_res)

        ims_cli.delete_index(index_id)

        with self.assertRaises(NotFound):
            rr_cli.delete(index_id)

    def test_update_index(self):
        ims_cli = self.ims_cli
        rr_cli  = self.rr_cli
        index_name  = self.index_name
        index_res   = Index(name=index_name)
        index_id, _ = rr_cli.create(index_res)

        index = ims_cli.read_index(index_id)
        index.name = 'another'
        ims_cli.update_index(index)

        index = rr_cli.read(index_id)
        self.assertTrue(index.name == 'another')

    def test_find_indexes(self):
        ims_cli = self.ims_cli
        rr_cli  = self.rr_cli
        index_name  = self.index_name
        
        #======================================
        # Index Pool
        #======================================
        
        indexes = [
            Index(name='first'),
            Index(name='second'),
            Index(name='third')
        ]
        id_pool = list()
        for index in indexes:
            id_pool.append(rr_cli.create(index)[0])

        index_id = ims_cli.find_indexes(index_name='second')
        index = ims_cli.read_index(index_id)
        self.assertTrue(index.name=='second')

        
        #======================================
        # Clean up
        #======================================

        for index_id in id_pool:
            rr_cli.delete(index_id)

    def test_create_collection(self):
        ims_cli = self.ims_cli
        rr_cli  = self.rr_cli
        with self.assertRaises(BadRequest):
            ims_cli.create_collection('failing_collection')

        resources = [ Resource(), Resource(), Resource() ]
        resources = [ rr_cli.create(i)[0] for i in resources ] 
        

        collection_id = ims_cli.create_collection('working_collection',resources)

        collection = rr_cli.read(collection_id)
        collection_resources = ims_cli.list_collection_resources(collection_id, id_only=True)
        self.assertTrue(set(collection_resources) == set(resources), '%s != %s' % (set(collection_resources) , set(resources)))

    def test_read_collection(self):
        ims_cli = self.ims_cli
        rr_cli  = self.rr_cli
        collection = Collection(name='working_collection')
        collection_id, _  = rr_cli.create(collection)
        collection = ims_cli.read_collection(collection_id)
        self.assertTrue(collection.name == 'working_collection')

    def test_update_collection(self):
       ims_cli = self.ims_cli
       rr_cli  = self.rr_cli

       collection = Collection(name='useful_collection')
       collection_id, _ = rr_cli.create(collection)
       collection = rr_cli.read(collection_id)
       collection.name = 'nub'
       ims_cli.update_collection(collection)
       collection = rr_cli.read(collection_id)
       self.assertTrue(collection.name=='nub')

    def test_delete_collection(self):
        ims_cli = self.ims_cli
        rr_cli  = self.rr_cli
        res = Resource()
        res_id, rev = rr_cli.create(res)

        collection_id = ims_cli.create_collection(name='test_collection', resources=[res_id])

        ims_cli.delete_collection(collection_id)

        with self.assertRaises(NotFound):
            rr_cli.read(collection_id)

    def test_list_collection_resources(self):
        ims_cli = self.ims_cli
        rr_cli  = self.rr_cli
        #========================================
        # Resource Pool
        #========================================
        resources = [ InformationResource(name='bean_counter'), InformationResource(name='lunar_rock'), InformationResource('aperature'), InformationResource('lemons') ] 

        resources = [ rr_cli.create(i)[0] for i in resources ]

        collection = Collection(name='park_bench')

        collection_id = ims_cli.create_collection(name='park_bench', resources=resources)
        retval = ims_cli.list_collection_resources(collection_id, id_only=True)

        retval.sort()
        resources.sort()

        self.assertTrue(retval == resources, '%s != %s' %(retval , resources))

    def test_find_collection(self):
        res_id, _  = self.rr_cli.create(Resource(name='test_res'))
        collection_id = self.ims_cli.create_collection('test', [res_id])

        retval = self.ims_cli.find_collection(collection_name='test')
        self.assertTrue(retval[0] == collection_id)

        retval = self.ims_cli.find_collection(resource_ids=[res_id])
        self.assertTrue(retval[0] == collection_id)





