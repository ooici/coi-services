#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/ingestion/test/ingestion_management_test.py
@date Mon Jul  2 13:50:40 EDT 2012
@description Testing for Ingestion Management Service
'''
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from ion.services.dm.ingestion.ingestion_management_service import IngestionManagementService
from interface.objects import IngestionQueue
from mock import Mock
from nose.plugins.attrib import attr

@attr('UNIT', group='dm')
class IngestionManagementUnitTest(PyonTestCase):
    def setUp(self):
        
        mock_clients = self._create_service_mock('ingestion_management')

        self.ingestion_management = IngestionManagementService()

        self.ingestion_management.clients = mock_clients

        self.rr_create = mock_clients.resource_registry.create
        self.rr_read = mock_clients.resource_registry.read
        self.rr_update = mock_clients.resource_registry.update
        self.rr_delete = mock_clients.resource_registry.delete
        self.rr_find_objs = mock_clients.resource_registry.find_objects
        self.rr_find_assocs = mock_clients.resource_registry.find_associations
        self.rr_create_assoc = mock_clients.resource_registry.create_association
        self.rr_del_assoc =  mock_clients.resource_registry.delete_association
        self.pubsub_create_sub = mock_clients.pubsub_management.create_subscription
        self.pubsub_del_sub = mock_clients.pubsub_management.delete_subscription
        self.pubsub_act_sub = mock_clients.pubsub_management.activate_subscription
        self.pubsub_deact_sub = mock_clients.pubsub_management.deactivate_subscription
        self.dataset_create = mock_clients.dataset_management.create_dataset

    def test_read_ingestion(self):
        testval = 'test'
        self.rr_read.return_value = testval
        retval = self.ingestion_management.read_ingestion_configuration('testconfig')
        self.assertTrue(retval == testval)
        
    def test_update_ingestion(self):
        testval = {0:0}
        self.ingestion_management.update_ingestion_configuration(testval)
        self.rr_update.assert_called_once_with(testval)

    def test_create_ingestion(self):
        testval = 'config_id'
        self.rr_create.return_value = (testval, '0')

        queue = IngestionQueue()
        retval = self.ingestion_management.create_ingestion_configuration(name='blah', exchange_point_id='test', queues=[queue])

        self.assertTrue(retval == testval)
        self.assertTrue(self.rr_create.call_count == 1)

    def test_delete_ingestion(self):
        testval = DotDict()
        testval.o = 'sub_id'
        self.rr_find_assocs.return_value = [testval]

        self.ingestion_management.delete_ingestion_configuration('test')

        self.rr_del_assoc.assert_called_once_with(testval)
        self.pubsub_del_sub.assert_called_once_with(testval.o)

        self.rr_delete.assert_called_once_with('test')

    def test_persist_data_stream(self):
        ingestval = DotDict()
        ingestval.queues = None

        queueval = DotDict()
        queueval.name = 'test'
        queueval.datastore_name = 'test'

        testval = 'dataset_id'

        self.ingestion_management.read_ingestion_configuration = Mock()
        self.ingestion_management.read_ingestion_configuration.return_value = ingestval
        self.ingestion_management._determine_queue = Mock()
        self.ingestion_management._determine_queue.return_value = queueval

        self.ingestion_management._new_dataset = Mock()
        self.ingestion_management._new_dataset.return_value = testval

        retval = self.ingestion_management.persist_data_stream('stream_id')


        self.assertTrue(self.pubsub_act_sub.call_count)
        self.assertTrue(self.pubsub_create_sub.call_count)
        self.assertTrue(self.rr_create_assoc.call_count)

        self.assertTrue(retval == testval)

    def test_unpersist_data_stream(self):

        self.rr_find_objs.return_value = [('sub'),('assoc')]
        self.rr_find_assocs.return_value = ['assoc']

        self.ingestion_management.unpersist_data_stream('stream_id','ingestion_id')

        self.assertTrue(self.pubsub_deact_sub.call_count)
        self.assertTrue(self.rr_del_assoc.call_count)
        self.assertTrue(self.pubsub_del_sub.call_count)

    def test_determine_queue(self):
        pass #unimplemented

    def test_new_dataset(self):
        testval = 'dataset_id'
        self.dataset_create.return_value = testval

        retval = self.ingestion_management._new_dataset('stream_id', 'datastore_name')
        self.assertTrue(retval == testval)


