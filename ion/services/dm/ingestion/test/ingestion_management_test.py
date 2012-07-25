#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/ingestion/test/ingestion_management_test.py
@date Mon Jul  2 13:50:40 EDT 2012
@description Testing for Ingestion Management Service
'''
from pyon.public import PRED
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.containers import DotDict
from pyon.core.exception import NotFound
from ion.services.dm.ingestion.ingestion_management_service import IngestionManagementService
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import IngestionQueue, Subscription, DataSet
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
        self.rr_find_res = mock_clients.resource_registry.find_resources
        self.rr_create_assoc = mock_clients.resource_registry.create_association
        self.rr_find_subjects = mock_clients.resource_registry.find_subjects
        self.rr_del_assoc =  mock_clients.resource_registry.delete_association
        self.pubsub_create_sub = mock_clients.pubsub_management.create_subscription
        self.pubsub_read = mock_clients.pubsub_management.read_stream 
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
        ingestval.exchange_point = 'test'

        queueval = DotDict()
        queueval.name = 'test'
        queueval.datastore_name = 'test'

        testval = 'dataset_id'

        self.ingestion_management.read_ingestion_configuration = Mock()
        self.ingestion_management.read_ingestion_configuration.return_value = ingestval
        self.ingestion_management._determine_queue = Mock()
        self.ingestion_management._determine_queue.return_value = queueval
        self.pubsub_read.return_value = DotDict({'persisted':False})

        self.ingestion_management._existing_dataset = Mock()

        retval = self.ingestion_management.persist_data_stream('stream_id', 'config_id', 'dataset_id')

        self.assertTrue(self.pubsub_act_sub.call_count)
        self.assertTrue(self.pubsub_create_sub.call_count)
        self.assertTrue(self.rr_create_assoc.call_count)

        self.assertTrue(retval == testval)

    def test_unpersist_data_stream(self):

        self.rr_find_objs.return_value = [('sub'),('assoc')]
        self.rr_find_assocs.return_value = ['assoc']
        self.rr_find_subjects.return_value = ([],[])

        self.ingestion_management.unpersist_data_stream('stream_id','ingestion_id')

        self.assertTrue(self.pubsub_deact_sub.call_count)
        self.assertTrue(self.rr_del_assoc.call_count)
        self.assertTrue(self.pubsub_del_sub.call_count)


    def test_determine_queue(self):
        pass #unimplemented

    def test_list_ingestion(self):
        testval = (['resource'], ['other'])
        self.rr_find_res.return_value = testval
        retval = self.ingestion_management.list_ingestion_configurations(id_only=True)
        self.assertTrue(retval == testval[0])
        self.assertTrue(self.rr_find_res.call_count)

    def test_is_persisted(self):
        stream = DotDict()
        stream.persisted=True
        self.pubsub_read.return_value = stream

        retval = self.ingestion_management.is_persisted('stream_id')

        self.assertEquals(retval,True)


@attr('INT', group='dm')
class IngestionManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.ingestion_management = IngestionManagementServiceClient()
        self.resource_registry    = ResourceRegistryServiceClient()
        self.pubsub_management    = PubsubManagementServiceClient()
        self.ingest_name = 'basic'
        self.exchange    = 'testdata'

    @staticmethod
    def clean_subscriptions():
        ingestion_management = IngestionManagementServiceClient()
        pubsub = PubsubManagementServiceClient()
        rr     = ResourceRegistryServiceClient()
        ingestion_config_ids = ingestion_management.list_ingestion_configurations(id_only=True)
        for ic in ingestion_config_ids:

            assocs = rr.find_associations(subject=ic, predicate=PRED.hasSubscription, id_only=False)
            for assoc in assocs:
                rr.delete_association(assoc)
                try:
                    pubsub.deactivate_subscription(assoc.o)
                except:
                    pass
                pubsub.delete_subscription(assoc.o)


    def create_ingest_config(self):
        self.queue = IngestionQueue(name='test', type='testdata')

        # Create the ingestion config
        ingestion_config_id = self.ingestion_management.create_ingestion_configuration(name=self.ingest_name, exchange_point_id=self.exchange, queues=[self.queue])
        return ingestion_config_id



    def test_ingestion_config_crud(self):
        ingestion_config_id = self.create_ingest_config()

        ingestion_config = self.ingestion_management.read_ingestion_configuration(ingestion_config_id)
        self.assertTrue(ingestion_config.name == self.ingest_name)
        self.assertTrue(ingestion_config.queues[0].name == 'test')
        self.assertTrue(ingestion_config.queues[0].type == 'testdata')

        ingestion_config.name = 'another'

        self.ingestion_management.update_ingestion_configuration(ingestion_config)

        # Create an association just to make sure that it will delete them
        sub = Subscription()
        sub_id, _ = self.resource_registry.create(sub)
        assoc_id, _ = self.resource_registry.create_association(subject=ingestion_config_id, predicate=PRED.hasSubscription,object=sub_id)

        self.ingestion_management.delete_ingestion_configuration(ingestion_config_id)

        with self.assertRaises(NotFound):
            self.resource_registry.read(assoc_id)

    def test_list_ingestion(self):

        # Create the ingest_config
        config_id = self.create_ingest_config()

        retval = self.ingestion_management.list_ingestion_configurations(id_only=True)
        # Nice thing about this is that it breaks if r2dm adds an ingest_config
        self.assertTrue(config_id in retval)

    def test_persist_data(self):
        config_id = self.create_ingest_config()
        stream_id = self.pubsub_management.create_stream()
        dataset_id = self.ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=config_id)
        assoc = self.resource_registry.find_associations(subject=config_id, predicate=PRED.hasSubscription)

        sub = self.resource_registry.read(assoc[0].o)

        self.assertTrue(sub.is_active)

        dataset = self.resource_registry.read(dataset_id)
        self.assertIsInstance(dataset,DataSet)

