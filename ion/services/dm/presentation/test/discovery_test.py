#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file discovery_test
@date 05/07/12 08:17
@description Integration and Unit tests for Discovery Service
'''
from unittest.case import skipIf, skip
from pyon.public import PRED, CFG, RT
from pyon.core.exception import BadRequest, NotFound
from pyon.core.bootstrap import get_sys_name
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from interface.objects import View, Catalog, ElasticSearchIndex, InstrumentDevice, Site, PlatformDevice, BankAccount, DataProduct, Transform, ProcessDefinition, DataProcess, UserInfo, ContactInformation, Dataset
from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.dm.iindex_management_service import IndexManagementServiceClient
from interface.services.dm.icatalog_management_service import CatalogManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.dm.presentation.discovery_service import DiscoveryService
from ion.services.dm.inventory.index_management_service import IndexManagementService
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.processes.bootstrap.index_bootstrap import STD_INDEXES
from nose.plugins.attrib import attr
from mock import Mock, patch
from datetime import date, timedelta

import elasticpy as ep
import dateutil.parser
import time
import os
import unittest



use_es = CFG.get_safe('system.elasticsearch',False)


@attr('UNIT', group='dm')
class DiscoveryUnitTest(PyonTestCase):
    def setUp(self):
        super(DiscoveryUnitTest,self).setUp()
        mock_clients = self._create_service_mock('discovery')
        self.discovery = DiscoveryService()
        self.discovery.clients = mock_clients
        self.discovery.use_es = True
        
        self.rr_create = mock_clients.resource_registry.create
        self.rr_read = mock_clients.resource_registry.read
        self.rr_update = mock_clients.resource_registry.update
        self.rr_delete = mock_clients.resource_registry.delete
        self.rr_find_assoc = mock_clients.resource_registry.find_associations
        self.rr_find_res = mock_clients.resource_registry.find_resources
        self.rr_find_obj = mock_clients.resource_registry.find_objects
        self.rr_find_assocs_mult = mock_clients.resource_registry.find_objects_mult
        self.rr_create_assoc = mock_clients.resource_registry.create_association
        self.rr_delete_assoc = mock_clients.resource_registry.delete_association

        self.cms_create = mock_clients.catalog_management.create_catalog
        self.cms_list_indexes = mock_clients.catalog_management.list_indexes

    def test_create_view(self):
        # Mocks
        self.rr_find_res.return_value = ([],[])
        self.rr_create.return_value = ('res_id', 'rev_id')
        self.cms_create.return_value = 'catalog_id'

        retval = self.discovery.create_view('mock_view',fields=['name'])

        self.assertTrue(retval=='res_id', 'Improper resource creation')

    def test_create_exists(self):
        self.rr_find_res.return_value = ([1], [1])
        with self.assertRaises(BadRequest):
            self.discovery.create_view('doesnt matter', fields=['name'])

    def test_create_no_fields(self):
        self.rr_find_res.return_value = ([],[])
        self.rr_create.return_value = ('res_id', 'rev_id')
        self.cms_create.return_value = 'catalog_id'

        with self.assertRaises(BadRequest):
            self.discovery.create_view('mock_view')

    def test_create_order(self):
        self.rr_find_res.return_value = ([],[])
        self.rr_create.return_value = ('res_id', 'rev_id')
        self.cms_create.return_value = 'catalog_id'

        with self.assertRaises(BadRequest):
            self.discovery.create_view('movk_view',fields=['name'],order=['age'])


    def test_read_view(self):
        # Mocks
        self.rr_read.return_value = 'fake_resource'
        retval = self.discovery.read_view('mock_view_id')
        self.assertTrue(retval == 'fake_resource')

    def test_update_view(self):
        retval = self.discovery.update_view({})
        self.assertTrue(retval)
        self.rr_update.assert_called_once_with({})

    def test_delete_view(self):
        catalog_assoc = DotDict(_id=0)
        self.rr_find_assocs_mult.return_value = ([],[catalog_assoc])
        retval = self.discovery.delete_view('view_id')
        self.assertTrue(retval)
        self.rr_delete.assert_called_once_with('view_id')
        self.assertTrue(self.rr_delete_assoc.call_count == 1)


    def test_list_catalogs(self):
        self.rr_find_obj.return_value = (['test'],[])

        retval = self.discovery.list_catalogs('view_id')
        self.assertTrue(retval[0] == 'test')


    @patch('ion.services.dm.presentation.discovery_service.ep.ElasticSearch')
    def test_query_index(self, es_mock):
        mock_index = ElasticSearchIndex(content_type=IndexManagementService.ELASTICSEARCH_INDEX)
        self.rr_read.return_value = mock_index
        self.discovery.elasticsearch_host = 'fakehost'
        self.discovery.elasticsearch_port = 'fakeport'
        es_mock().search_index_advanced.return_value = {'hits':{'hits':[{'_id':'success'}]}}

        retval = self.discovery.query_term('mock_index', 'field', 'value', order={'name':'asc'}, limit=20, offset=20)

        self.assertTrue(retval[0]['_id']=='success', '%s' % retval)

    def test_query(self):
        self.discovery.request = lambda x : x
        retval = self.discovery.query('test')
        self.assertTrue(retval == 'test')

    def test_query_couch(self):
        pass
        

    @skip('Needs to be adjusted for new traversal')
    def test_traverse(self):
        edge_list = ['B','C','D']
        callers = []
        def pop_edge(*args, **kwargs):
            callers.append(args[0])
            if len(edge_list):
                val = edge_list.pop(0)
                return [val], 'blah'
            return [], 'blah'
        self.rr_find_obj.side_effect = pop_edge

        retval = self.discovery.traverse('A')
        retval.sort()
        self.assertTrue(retval == ['B','C','D'], '%s' % retval)

    def test_intersect(self):
        test_vals = [0,1,2,3]
        other = [2,1]
        retval = self.discovery.intersect(test_vals,other)
        retval.sort()
        self.assertTrue(retval == [1,2])

    def test_union(self):
        test_vals = [0,1,2,3]
        other = [4,5]
        retval = self.discovery.union(test_vals,other)
        retval.sort()
        self.assertTrue(retval == [0,1,2,3,4,5], '%s' % retval)
    @patch('ion.services.dm.presentation.discovery_service.QueryLanguage')
    def test_parse(self, mock_parser):
        mock_parser().parse.return_value = 'arg'
        self.discovery.request = Mock()
        self.discovery.request.return_value = 'correct_value'
        retval = self.discovery.parse('blah blah')
        self.discovery.request.assert_called_once_with('arg')
        self.assertTrue(retval=='correct_value', '%s' % retval)

    def test_query_request_term_search(self):
        query = DotDict()
        query.index = 'index_id'
        query.field = 'field'
        query.value = 'value'
        self.discovery.query_term = Mock()
        self.discovery.query_term.return_value = 'test'

        retval = self.discovery.query_request(query)
        self.assertTrue(retval == 'test', '%s' % retval)

    def test_query_request_association(self):
        query = DotDict()
        query.association = 'resource_id'

        self.discovery.query_association = Mock()
        self.discovery.query_association.return_value = 'test'

        retval = self.discovery.query_request(query)
        self.assertTrue(retval == 'test')

    def test_query_request_range(self):
        query = DotDict()
        query['range'] = {'from':0, 'to':90}
        query.index = 'index_id'
        query.field = 'field'

        self.discovery.query_range = Mock()
        self.discovery.query_range.return_value = 'test'

        retval = self.discovery.query_request(query)
        self.assertTrue(retval == 'test')

    def test_query_request_collection(self):
        query = DotDict()
        query.collection = 'test'

        self.discovery.query_collection = Mock()
        self.discovery.query_collection.return_value = 'test'

        retval = self.discovery.query_request(query)
        self.assertTrue(retval == 'test')

    def test_bad_query(self):
        query = DotDict()
        query.unknown = 'yup'

        with self.assertRaises(BadRequest):
            self.discovery.query_request(query)
    
    @patch('ion.services.dm.presentation.discovery_service.ep.ElasticSearch')
    def test_query_range(self, mock_es):
        mock_index = ElasticSearchIndex(name='index', index_name='index')
        self.discovery.elasticsearch_host = 'fakehost'
        self.discovery.elasticsearch_port = 'fakeport'
        self.rr_read.return_value = mock_index
        hits = [{'_id':'a'},{'_id':'b'}]
        mock_es().search_index_advanced.return_value = {'hits':{'hits':hits}}

        retval = self.discovery.query_range('index_id','field',0,100,id_only=False)

        mock_es().search_index_advanced.assert_called_once_with('index',ep.ElasticQuery.range(field='field', from_value=0, to_value=100))
        retval.sort()
        self.assertTrue(retval==hits, '%s' % retval)
        
        retval = self.discovery.query_range('index_id','field',0,100,id_only=True)
        retval.sort()
        self.assertTrue(retval==['a','b'])

    @patch('ion.services.dm.presentation.discovery_service.ep.ElasticSearch')
    def test_query_time(self, mock_es):
        mock_index = ElasticSearchIndex(name='index', index_name='index')
        self.discovery.elasticsearch_host = 'fakehost'
        self.discovery.elasticsearch_port = 'fakeport'
        self.rr_read.return_value = mock_index
        hits = [{'_id':'a'},{'_id':'b'}]
        mock_es().search_index_advanced.return_value = {'hits':{'hits':hits}}

        date1 = '2012-01-01'
        ts1 = time.mktime( dateutil.parser.parse(date1).timetuple()) * 1000
        date2 = '2012-02-01'
        ts2 = time.mktime( dateutil.parser.parse(date2).timetuple()) * 1000

        retval = self.discovery.query_time('index_id','field',date1,date2,id_only=False)

        mock_es().search_index_advanced.assert_called_once_with('index',ep.ElasticQuery.range(field='field', from_value=ts1, to_value=ts2))
        retval.sort()
        self.assertTrue(retval==hits, '%s' % retval)
        
        retval = self.discovery.query_time('index_id','field',date1,date2,id_only=True)
        retval.sort()
        self.assertTrue(retval==['a','b'])

    @skip('Needs to be adjusted for changes in association traversal')
    def test_query_association(self):
        self.discovery.traverse = Mock()
        self.discovery.traverse.return_value = ['a','b','c']

        retval = self.discovery.query_association('blah',id_only=True)
        retval.sort()
        self.assertTrue(retval == ['a','b','c'])

        self.rr_read.return_value = 'test'

        retval = self.discovery.query_association('blah',id_only=False)
        self.assertTrue(retval == (['test']*3))

    def test_es_map_query(self):
        pass

    def test_tier1_request(self):
        self.discovery.query_request = Mock()
        self.discovery.query_request.return_value = 'test'

        query = {'and':[], 'or':[], 'query':{}}

        retval = self.discovery.request(query)

        self.assertTrue(retval=='test')
        self.discovery.query_request.assert_called_once_with({})

    def test_tier2_request(self):
        result_list = [[0,1,2],[1,2],[0,1,2],[1,2,3,4]]
        def query_request(*args, **kwargs):
            if len(result_list):
                return result_list.pop(0)
            return None

        self.discovery.query_request = Mock()
        self.discovery.query_request.side_effect = query_request
        #========================
        # Intersection
        #========================

        request = {'and':[{}], 'or':[], 'query':{}}

        retval = self.discovery.request(request)
        retval.sort()

        self.assertTrue(retval == [1,2])

        #========================
        # Union
        #========================

        request = {'and':[], 'or':[{}], 'query':{}}

        retval = self.discovery.request(request)
        retval.sort()

        self.assertTrue(retval == [0,1,2,3,4])

    def test_bad_requests(self):
        #================================
        # Battery of broken requests
        #================================

        bad_requests = [
            {},
            {'and':[]},
            {'query':{}},
            {'or':[]},
            {'and':{},'or':[],'query':[]},

        ]
        for req in bad_requests:
            with self.assertRaises(BadRequest):
                self.discovery.request(req)

    @patch('ion.services.dm.presentation.discovery_service.ep.ElasticSearch')
    def test_view_request(self, mock_es):
        self.call_count = 0
        def cb(*args, **kwargs):
            self.call_count +=1
            return [1]
        self.discovery.elasticsearch_host = ''
        self.discovery.elasticsearch_port = ''

        v = View()
        setattr(v, '_id', 'a')
        self.discovery.list_catalogs = Mock()
        self.discovery.list_catalogs.return_value = [1,2]

        retval = self.discovery._multi(cb,v)
        self.assertTrue(retval == [1,1])

        c = Catalog()
        setattr(c, '_id', 'c')
        self.cms_list_indexes.return_value = [1,2]

        retval = self.discovery._multi(cb,c)
        self.assertTrue(retval == [1,1])


        retval = self.discovery._multi(cb,v, limit=1)
        self.assertTrue(retval == [1])

        retval = self.discovery._multi(cb,c, limit=1)
        self.assertTrue(retval == [1])
        
        self.discovery._multi = Mock()
        self.rr_read.return_value = v
        self.discovery._multi.return_value = 'test'
        retval = self.discovery.query_term('blah', 'field', 'value')
        self.assertTrue(retval == 'test')

    @patch('ion.services.dm.presentation.discovery_service.ep.ElasticSearch')
    def test_query_geo_distance(self, mock_es):
        self.rr_read.return_value = ElasticSearchIndex(name='test')
        self.discovery.elasticsearch_host = ''
        self.discovery.elasticsearch_port = ''
        self.discovery._multi = Mock()
        self.discovery._multi.return_value = None
        response = {'ok':True, 'status':200, 'hits':{'hits':['hi']}}
        mock_es().search_index_advanced.return_value = response

        retval = self.discovery.query_geo_distance('abc13', 'blah', [0,0], 20)

        self.assertTrue(retval == ['hi'])

    @patch('ion.services.dm.presentation.discovery_service.ep.ElasticSearch')
    def test_query_geo_bbox(self, mock_es):
        self.rr_read.return_value = ElasticSearchIndex(name='test')
        self.discovery.elasticsearch_host = ''
        self.discovery.elasticsearch_port = ''
        self.discovery._multi = Mock()
        self.discovery._multi.return_value = None
        response = {'ok':True, 'status':200, 'hits':{'hits':['hi']}}
        mock_es().search_index_advanced.return_value = response

        retval = self.discovery.query_geo_bbox('abc123', 'blah', [0,10], [10,0])

        self.assertTrue(retval == ['hi'])


        
@attr('INT', group='dm')
@attr('LOCOINT')
@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
class DiscoveryIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(DiscoveryIntTest, self).setUp()
        config = DotDict()
        config.bootstrap.use_es = True

        self._start_container()
        self.addCleanup(DiscoveryIntTest.es_cleanup)
        self.container.start_rel_from_url('res/deploy/r2deploy.yml', config)

        self.discovery               = DiscoveryServiceClient()
        self.catalog                 = CatalogManagementServiceClient()
        self.ims                     = IndexManagementServiceClient()
        self.rr                      = ResourceRegistryServiceClient()
        self.dataset_management      = DatasetManagementServiceClient()
        self.pubsub_management       = PubsubManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()

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


    def poll(self, tries, callback, *args, **kwargs):
        '''
        Polling wrapper for queries
        Elasticsearch may not index and cache the changes right away so we may need 
        a couple of tries and a little time to go by before the results show.
        '''
        for i in xrange(tries):
            retval = callback(*args, **kwargs)
            if retval:
                return retval
            time.sleep(0.2)
        return None


    def test_traversal(self):
        dp        = DataProcess()
        transform = Transform()
        pd        = ProcessDefinition()

        dp_id, _        = self.rr.create(dp)
        transform_id, _ = self.rr.create(transform)
        pd_id, _        = self.rr.create(pd)

        self.rr.create_association(subject=dp_id, object=transform_id, predicate=PRED.hasTransform)
        self.rr.create_association(subject=transform_id, object=pd_id, predicate=PRED.hasProcessDefinition)

        results = self.discovery.traverse(dp_id)
        results.sort()
        correct = [pd_id, transform_id]
        correct.sort()
        self.assertTrue(results == correct, '%s' % results)

    def test_iterative_traversal(self):
        dp        = DataProcess()
        transform = Transform()
        pd        = ProcessDefinition()

        dp_id, _        = self.rr.create(dp)
        transform_id, _ = self.rr.create(transform)
        pd_id, _        = self.rr.create(pd)

        self.rr.create_association(subject=dp_id, object=transform_id, predicate=PRED.hasTransform)
        self.rr.create_association(subject=transform_id, object=pd_id, predicate=PRED.hasProcessDefinition)

        results = self.discovery.iterative_traverse(dp_id)
        results.sort()
        correct = [transform_id]
        self.assertTrue(results == correct)

        results = self.discovery.iterative_traverse(dp_id, 1)
        results.sort()
        correct = [transform_id, pd_id]
        correct.sort()
        self.assertTrue(results == correct)
    
    @skipIf(not use_es, 'No ElasticSearch')
    def test_view_crud(self):
        view_id = self.discovery.create_view('big_view',fields=['name'])
        catalog_id = self.discovery.list_catalogs(view_id)[0]
        index_ids = self.catalog.list_indexes(catalog_id)
        self.assertTrue(len(index_ids))

        view = self.discovery.read_view(view_id)
        self.assertIsInstance(view,View)
        self.assertTrue(view.name == 'big_view')

        view.name = 'not_so_big_view'

        self.discovery.update_view(view)

        view = self.discovery.read_view(view_id)
        self.assertTrue(view.name == 'not_so_big_view')

        self.discovery.delete_view(view_id)
        with self.assertRaises(NotFound):
            self.discovery.read_view(view_id)

    def test_view_best_match(self):
        #---------------------------------------------------------------
        # Matches the best catalog available OR creates a new one
        #---------------------------------------------------------------
        catalog_id = self.catalog.create_catalog('dev', keywords=['name','model'])
        view_id    = self.discovery.create_view('exact_view', fields=['name','model'])
        catalog_ids = self.discovery.list_catalogs(view_id)
        self.assertTrue(catalog_ids == [catalog_id])

        view_id = self.discovery.create_view('another_view', fields=['name','model'])
        catalog_ids = self.discovery.list_catalogs(view_id)
        self.assertTrue(catalog_ids == [catalog_id])

        view_id = self.discovery.create_view('big_view', fields=['name'])
        catalog_ids = self.discovery.list_catalogs(view_id)
        self.assertTrue(catalog_ids != [catalog_id])

    @skipIf(not use_es, 'No ElasticSearch')
    def test_basic_searching(self):

        #- - - - - - - - - - - - - - - - - 
        # set up the fake resources
        #- - - - - - - - - - - - - - - - - 

        instrument_pool = [
            InstrumentDevice(name='sonobuoy1', firmware_version='1'),
            InstrumentDevice(name='sonobuoy2', firmware_version='2'),
            InstrumentDevice(name='sonobuoy3', firmware_version='3')
        ]
        for instrument in instrument_pool:
            self.rr.create(instrument)

        view_id = self.discovery.create_view('devices', fields=['firmware_version'])

        search_string = "search 'firmware_version' is '2' from '%s'"%view_id
        results = self.poll(5, self.discovery.parse,search_string)
        result  = results[0]['_source']
        self.assertIsInstance(result, InstrumentDevice)
        self.assertTrue(result.name == 'sonobuoy2')
        self.assertTrue(result.firmware_version == '2')


    @skipIf(not use_es, 'No ElasticSearch')
    def test_associative_searching(self):

        dp_id,_ = self.rr.create(DataProduct('test_foo'))
        ds_id,_ = self.rr.create(Dataset('test_bar', registered=True))
        self.rr.create_association(subject=dp_id, object=ds_id, predicate='hasDataset')

        search_string = "search 'type_' is 'Dataset' from 'resources_index' and belongs to '%s'" % dp_id

        results = self.poll(5, self.discovery.parse,search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(ds_id in results)

    def test_iterative_associative_searching(self):
        #--------------------------------------------------------------------------------
        # Tests the ability to limit the iterations
        #--------------------------------------------------------------------------------
        dp        = DataProcess()
        transform = Transform()
        pd        = ProcessDefinition()

        dp_id, _        = self.rr.create(dp)
        transform_id, _ = self.rr.create(transform)
        pd_id, _        = self.rr.create(pd)

        self.rr.create_association(subject=dp_id, object=transform_id, predicate=PRED.hasTransform)
        self.rr.create_association(subject=transform_id, object=pd_id, predicate=PRED.hasProcessDefinition)

        search_string = "belongs to '%s' depth 1" % dp_id
        results = self.poll(5, self.discovery.parse,search_string)
        results = list([i._id for i in results])
        correct = [transform_id]
        self.assertTrue(results == correct, '%s' % results)

        search_string = "belongs to '%s' depth 2" % dp_id
        results = self.poll(5, self.discovery.parse,search_string)
        results = list([i._id for i in results])
        results.sort()
        correct = [transform_id, pd_id]
        correct.sort()
        self.assertTrue(results == correct)


    @skipIf(not use_es, 'No ElasticSearch')
    def test_ranged_value_searching(self):
        discovery = self.discovery
        rr        = self.rr
        
        view_id = discovery.create_view('bank_view', fields=['cash_balance'])
        bank_id, _ = rr.create(BankAccount(name='broke', cash_balance=10))

        search_string = "search 'cash_balance' values from 0 to 100 from '%s'" % view_id

        results = self.poll(5, discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == bank_id)
        
        bank_id, _ = rr.create(BankAccount(name='broke', cash_balance=90))

        search_string = "search 'cash_balance' values from 80 from '%s'" % view_id

        results = self.poll(5, discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == bank_id)

    @skipIf(not use_es, 'No ElasticSearch')
    def test_collections_searching(self):

        site_id, _ = self.rr.create(Site(name='black_mesa'))
        view_id    = self.discovery.create_view('big', fields=['name'])

        # Add the site to a new collection
        collection_id = self.ims.create_collection('resource_collection', [site_id])

        search_string = "search 'name' is '*' from '%s' and in '%s'" %(view_id, collection_id)

        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0] == site_id, '%s' % results)

    @skipIf(not use_es, 'No ElasticSearch')
    def test_search_by_name(self):
        inst_dev = InstrumentDevice(name='test_dev',serial_number='ABC123')

        dev_id, _ = self.rr.create(inst_dev)
        self.discovery.create_view('devs',fields=['name','serial_number'])

        search_string = "search 'serial_number' is 'abc*' from 'devs'"
        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dev_id)

    @skipIf(not use_es, 'No ElasticSearch')
    def test_search_by_name_index(self):
        inst_dev = InstrumentDevice(name='test_dev',serial_number='ABC123')

        dev_id, _ = self.rr.create(inst_dev)
        search_string = "search 'serial_number' is 'abc*' from 'resources_index'"
        
        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dev_id)

        bank_acc = BankAccount(name='blah', cash_balance=10)
        res_id , _ = self.rr.create(bank_acc)

        search_string = "search 'cash_balance' values from 0 to 100 from 'resources_index'"

        results = self.poll(9, self.discovery.parse,search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == res_id)

    #@skipIf(not use_es, 'No ElasticSearch')
    @skip('Skip until time to refactor, data_format is removed from DataProduct resource')
    def test_data_product_search(self):

        # Create the dataproduct
        dp = DataProduct(name='test_product')
        dp.data_format.name = 'test_signal'
        dp.data_format.description = 'test signal'
        dp.data_format.character_set = 'utf8'
        dp.data_format.nominal_sampling_rate_maximum = '44000'
        dp.data_format.nominal_sampling_rate_minimum = '44000'
        dp.CDM_data_type = 'basic'
        dp_id, _ = self.rr.create(dp)

        search_string = "search 'data_format.name' is 'test_signal' from 'data_products_index'"
        results = self.poll(9, self.discovery.parse, search_string)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)

        search_string = "search 'CDM_data_type' is 'basic' from 'data_products_index'"
        results = self.poll(9, self.discovery.parse, search_string)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        
        search_string = "search 'data_format.character_set' is 'utf8' from 'data_products_index'"
        results = self.poll(9, self.discovery.parse, search_string)

        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)

    @skipIf(not use_es, 'No ElasticSearch')
    def test_events_search(self):
        # Create a resource to force a new event

        dp = DataProcess()
        dp_id, rev = self.rr.create(dp)

        search_string = "SEARCH 'origin' IS '%s' FROM 'events_index'" % dp_id

        results = self.poll(9, self.discovery.parse,search_string)
        origin_type = results[0]['_source'].origin_type
        origin_id = results[0]['_source'].origin

        self.assertTrue(origin_type == RT.DataProcess)
        self.assertTrue(origin_id == dp_id)

    @skipIf(not use_es, 'No ElasticSearch')
    def test_geo_distance_search(self):

        pd = PlatformDevice(name='test_dev')

        pd_id, _ = self.rr.create(pd)

        search_string = "search 'index_location' geo distance 20 km from lat 0 lon 0 from 'devices_index'"

        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')

        self.assertTrue(results[0]['_id'] == pd_id)
        self.assertTrue(results[0]['_source'].name == 'test_dev')
   
    @skipIf(not use_es, 'No ElasticSearch')
    def test_geo_bbox_search(self):

        pd = PlatformDevice(name='test_dev')
        pd.index_location.lat = 5
        pd.index_location.lon = 5

        pd_id, _ = self.rr.create(pd)

        search_string = "search 'index_location' geo box top-left lat 10 lon 0 bottom-right lat 0 lon 10 from 'devices_index'"

        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')

        self.assertTrue(results[0]['_id'] == pd_id)
        self.assertTrue(results[0]['_source'].name == 'test_dev')


    @skipIf(not use_es, 'No ElasticSearch')
    def test_time_search(self):
        today     = date.today()
        yesterday = today - timedelta(days=1)
        tomorrow  = today + timedelta(days=1)

        data_product = DataProduct()
        dp_id, _ = self.rr.create(data_product)
        
        search_string = "search 'ts_created' time from '%s' to '%s' from 'data_products_index'" % (yesterday, tomorrow)

        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results,'Results not found')

        self.assertTrue(results[0]['_id'] == dp_id)
        
        search_string = "search 'ts_created' time from '%s' from 'data_products_index'" % yesterday

        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results,'Results not found')

        self.assertTrue(results[0]['_id'] == dp_id)

        
    @skipIf(not use_es, 'No ElasticSearch')
    def test_user_search(self):
        user = UserInfo()
        user.name = 'test'
        user.contact.phones.append('5551212')

        user_id, _ = self.rr.create(user)

        search_string = 'search "name" is "test" from "users_index"'

        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')

        self.assertTrue(results[0]['_id'] == user_id)
        self.assertTrue(results[0]['_source'].name == 'test')

        search_string = 'search "contact.phones" is "5551212" from "users_index"'
        results = self.poll(9, self.discovery.parse,search_string)

        self.assertIsNotNone(results, 'Results not found')

        self.assertTrue(results[0]['_id'] == user_id)
        self.assertTrue(results[0]['_source'].name == 'test')


    @skipIf(not use_es, 'No ElasticSearch')
    def test_subobject_search(self):
        contact = ContactInformation()
        contact.email = 'test@gmail.com'
        contact.individual_name_family = 'Tester'
        contact.individual_names_given = 'Intern'

        dp = DataProduct(name='example')
        dp.contacts.append(contact)

        dp_id,_ = self.rr.create(dp)

        #--------------------------------------------------------------------------------
        # Example using the full field name
        #--------------------------------------------------------------------------------
        search_string = 'search "contacts.email" is "test@gmail.com" from "data_products"'
        results = self.poll(9, self.discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')

        #--------------------------------------------------------------------------------
        # Example using a sub-object's field name (ambiguous searching)
        #--------------------------------------------------------------------------------
        search_string = 'search "individual_names_given" is "Intern" from "data_products"'
        results = self.poll(9, self.discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')

    @skipIf(not use_es, 'No ElasticSearch')
    def test_descriptive_phrase_search(self):
        dp = DataProduct(name='example', description='This is simply a description for this data product')
        dp_id, _ = self.rr.create(dp)

        search_string = 'search "description" like "description for" from "data_products"'
        results = self.poll(9, self.discovery.parse, search_string)
        self.assertIsNotNone(results, 'Results not found')
        self.assertTrue(results[0]['_id'] == dp_id)
        self.assertEquals(results[0]['_source'].name, 'example')


    @skipIf(not use_es, 'No ElasticSearch')
    def test_ownership_searching(self):
        # Create two data products so that there is competition to the search, one is parsed 
        # (with conductivity as a parameter) and the other is raw
        dp = DataProduct(name='example dataproduct')
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        stream_def_id = self.pubsub_management.create_stream_definition('ctd parsed', parameter_dictionary_id=pdict_id)
        tdom, sdom = time_series_domain()
        dp.spatial_domain = sdom.dump()
        dp.temporal_domain = tdom.dump()
        dp_id = self.data_product_management.create_data_product(dp, stream_definition_id=stream_def_id, exchange_point='xp1')

        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_raw_param_dict')
        stream_def_id = self.pubsub_management.create_stream_definition('ctd raw', parameter_dictionary_id=pdict_id)
        dp = DataProduct(name='WRONG')
        dp.spatial_domain = sdom.dump()
        dp.temporal_domain = tdom.dump()
        self.data_product_management.create_data_product(dp, stream_definition_id=stream_def_id, exchange_point='xp1')

        parameter_search = 'search "name" is "conductivity" from "resources_index"'
        results = self.poll(9, self.discovery.parse, parameter_search)
        param_id = results[0]['_id']

        data_product_search = 'search "name" is "*" from "data_products_index" and has "%s"' % param_id
        results = self.poll(9, self.discovery.parse, data_product_search)
        print results
        self.assertEquals(results[0], dp_id)

        

