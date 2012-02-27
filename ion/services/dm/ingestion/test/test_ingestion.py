#!/usr/bin/env python

'''
@file ion/services/dm/ingestion/test/test_ingestion.py
@author Swarbhanu Chatterjee
@test ion.services.dm.ingestion.ingestion_management_service test suite to cover all ingestion mgmt service code
'''

import gevent
from gevent.timeout import Timeout
from mock import Mock
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.dm.ingestion.ingestion_management_service import IngestionManagementService
from nose.plugins.attrib import attr
from pyon.core.exception import NotFound, BadRequest
from pyon.public import StreamPublisherRegistrar, CFG
from interface.objects import HdfStorage, CouchStorage, StreamGranuleContainer
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.public import RT, PRED, log, IonObject

from pyon.datastore.datastore import DataStore
from prototype.sci_data.ctd_stream import ctd_stream_packet, ctd_stream_definition
from interface.objects import BlogPost, BlogComment, ExchangeQuery, DatasetIngestionConfiguration
from pyon.ion.process import StandaloneProcess

import random
import time
import unittest
import os

import unittest

@attr('UNIT', group='dm')
class IngestionTest(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('ingestion_management')
        self.ingestion_service = IngestionManagementService()
        self.ingestion_service.clients = mock_clients
        self.ingestion_service.process_definition_id = '1914'

        # save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_read = mock_clients.resource_registry.read
        self.mock_create_association = mock_clients.resource_registry.create_association
        self.mock_delete_association = mock_clients.resource_registry.delete_association
        self.mock_find_resources = mock_clients.resource_registry.find_resources
        self.mock_find_subjects = mock_clients.resource_registry.find_subjects
        self.mock_find_objects = mock_clients.resource_registry.find_objects
        self.mock_find_associations = mock_clients.resource_registry.find_associations
        self.mock_transform_activate = mock_clients.transform_management.activate_transform
        self.mock_transform_deactivate = mock_clients.transform_management.deactivate_transform
        self.mock_transform_delete = mock_clients.transform_management.delete_transform
        self.mock_pubsub_create_subscription = mock_clients.pubsub_management.create_subscription
        self.ingestion_service._launch_transforms = Mock()
        self.ingestion_service.process_definition_id = Mock()
        self.mock_launch_transforms = self.ingestion_service._launch_transforms

        self.ingestion_configuration_id = Mock()
        self.ingestion_configuration = Mock()

        # Exchange point
        self.exchange_point_id = "exchange_point_id"
        self.exchange_name = 'ingestion_queue'

        # Couch storage
        self.couch_storage = CouchStorage()

        # hfd_storage
        self.hdf_storage = HdfStorage()

        # number of workers
        self.number_of_workers = 2




    def test_create_ingestion_configuration(self):

        subscription_id = Mock()
        ingestion_configuration_id = Mock()
        ingestion_configuration = Mock()
        ingestion_configuration_id = Mock()
        query = ExchangeQuery()

        #--------------------------------------------------------------------------------
        # Fixing return values
        #--------------------------------------------------------------------------------

        self.mock_find_associations.return_value = ['association']

        self.mock_pubsub_create_subscription.return_value = subscription_id
        self.mock_create.return_value = ingestion_configuration_id, None

        #--------------------------------------------------------------------------------
        # Calling the delete ingestion configuration method
        #--------------------------------------------------------------------------------

        ingestion_configuration_id_out = self.ingestion_service.create_ingestion_configuration(
            exchange_point_id=self.exchange_point_id,
            couch_storage=self.couch_storage,
            hdf_storage=self.hdf_storage,
            number_of_workers=self.number_of_workers)


        #--------------------------------------------------------------------------------
        # Assertions
        #--------------------------------------------------------------------------------

        # assert that the function for creating subscription was called...
        # note: A query is created inside the create_ingestion_configuration method and since we cannot know the memory location for that object
        # we cannot check if that query is used as argument during the create_subscription call.
        # In mock, one either checks all the arguments that are passed in, or one doesnt check the arguments at all.
        # Therefore, here we are only checking if the method, create_subscription, was called.

        self.assertTrue(self.mock_pubsub_create_subscription.called )

        # check that the resource registry create function was called
        self.assertTrue(self.mock_create.called)

        # check that the _launch_transform method was called
        self.assertTrue(self.mock_launch_transforms.called )

        # check that a value was returned by the create_ingestion_configuration method
        self.assertTrue(ingestion_configuration_id_out)

    def test_read_and_update_ingestion_configuration(self):
        # reading
        self.mock_read.return_value = self.ingestion_configuration
        ingestion_configuration_obj = self.ingestion_service.read_ingestion_configuration(self.ingestion_configuration_id)

        # updating
        self.mock_update.return_value = [self.ingestion_configuration_id, 2]
        ingestion_configuration_obj.name = "UpdatedSampleIngestionConfiguration"
        self.ingestion_service.update_ingestion_configuration(ingestion_configuration_obj)

        # checking that things are alright
        self.mock_update.assert_called_once_with(ingestion_configuration_obj)

    def test_read_ingestion_configuration(self):
        # reading
        self.mock_read.return_value = self.ingestion_configuration
        ingestion_configuration_obj = self.ingestion_service.read_ingestion_configuration(self.ingestion_configuration_id)

        # checking things are alright
        assert ingestion_configuration_obj is self.mock_read.return_value
        self.mock_read.assert_called_once_with(self.ingestion_configuration_id, '')

    def test_read_ingestion_configuration_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.ingestion_service.read_ingestion_configuration('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Ingestion configuration notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_delete_ingestion_configuration(self):

        ingestion_configuration_id = Mock()
        transform1 = Mock()

        #--------------------------------------------------------------------------------
        # Fixing return values
        #--------------------------------------------------------------------------------

        self.mock_find_objects.return_value = [transform1]
        self.mock_find_associations.return_value = ['association']

        #--------------------------------------------------------------------------------
        # Calling the delete ingestion configuration method
        #--------------------------------------------------------------------------------

        self.ingestion_service.delete_ingestion_configuration(ingestion_configuration_id)

        #--------------------------------------------------------------------------------
        # Assertions
        #--------------------------------------------------------------------------------

        self.mock_find_objects.assert_called_once_with(ingestion_configuration_id, PRED.hasTransform, RT.Transform , True)
        self.mock_transform_delete.assert_called_with(transform1)
        self.mock_find_associations.assert_called_once_with(ingestion_configuration_id, PRED.hasTransform, '', False)
        self.mock_delete_association.assert_called_once_with('association')
        self.mock_delete.assert_called_once_with(ingestion_configuration_id)

        #@todo add some logic to check for state of the resources and ingestion service!

    def test_delete_ingestion_configuration_not_found(self):


        #--------------------------------------------------------------------------------
        # Fixing return values
        #--------------------------------------------------------------------------------

        self.mock_find_objects.return_value = []
        self.mock_find_associations.return_value = ['association']

        #--------------------------------------------------------------------------------
        # Calling the delete ingestion configuration method
        #--------------------------------------------------------------------------------

        with self.assertRaises(NotFound) as cm:
            self.ingestion_service.delete_ingestion_configuration('notfound')

        #--------------------------------------------------------------------------------
        # Assertions
        #--------------------------------------------------------------------------------

        ex = cm.exception
        self.assertEqual(ex.message, 'No transforms associated with this ingestion configuration!')
        self.mock_find_objects.assert_called_once_with('notfound','hasTransform', 'Transform', True)

    def test_activate_ingestion_configuration(self):
        """
        Test that the ingestion configuration is activated
        """

        ingestion_configuration_id = Mock()
        transform1 = Mock()

        #--------------------------------------------------------------------------------
        # Fixing return values
        #--------------------------------------------------------------------------------

        self.mock_find_objects.return_value = [transform1], None

        #--------------------------------------------------------------------------------
        # Calling the delete ingestion configuration method
        #--------------------------------------------------------------------------------

        #@todo add some logic to check for state of the resources and ingestion service!
        self.ingestion_service.activate_ingestion_configuration(ingestion_configuration_id)

        #--------------------------------------------------------------------------------
        # Assertions
        #--------------------------------------------------------------------------------

        self.mock_find_objects.assert_called_once_with(ingestion_configuration_id, PRED.hasTransform, RT.Transform , True)
        self.mock_transform_activate.assert_called_once_with(transform1)

    def test_deactivate_ingestion_configuration(self):
        """
        Test that the ingestion configuration is deactivated
        """

        ingestion_configuration_id = Mock()
        transform1 = Mock()

        #--------------------------------------------------------------------------------
        # Fixing return values
        #--------------------------------------------------------------------------------

        self.mock_find_objects.return_value = [transform1], None

        #--------------------------------------------------------------------------------
        # Calling the delete ingestion configuration method
        #--------------------------------------------------------------------------------

        #@todo add some logic to check for state of the resources and ingestion service!
        self.ingestion_service.deactivate_ingestion_configuration(ingestion_configuration_id)

        #--------------------------------------------------------------------------------
        # Assertions
        #--------------------------------------------------------------------------------

        self.mock_find_objects.assert_called_once_with(ingestion_configuration_id, PRED.hasTransform, RT.Transform , True)
        self.mock_transform_deactivate.assert_called_once_with(transform1)

    def test_activate_ingestion_configuration_not_found(self):
        """
        Test that non existent ingestion configuration does not cause crash when attempting to activate
        """
        #--------------------------------------------------------------------------------
        # Fixing return values
        #--------------------------------------------------------------------------------

        self.mock_find_objects.return_value = [], None

        #--------------------------------------------------------------------------------
        # Calling the delete ingestion configuration method
        #--------------------------------------------------------------------------------

        #@todo add some logic to check for state of the resources and ingestion service!
        with self.assertRaises(NotFound) as cm:
            self.ingestion_service.activate_ingestion_configuration('wrong_configuration_id')

        #--------------------------------------------------------------------------------
        # Assertions
        #--------------------------------------------------------------------------------

        ex = cm.exception
        self.mock_find_objects.assert_called_once_with('wrong_configuration_id', PRED.hasTransform, RT.Transform , True)
        self.assertEqual(ex.message, 'The ingestion configuration wrong_configuration_id does not exist')

    def test_deactivate_ingestion_configuration_not_found(self):
        """
        Test that non existent ingestion configuration does not cause crash when attempting to activate
        """
        #--------------------------------------------------------------------------------
        # Fixing return values
        #--------------------------------------------------------------------------------

        self.mock_find_objects.return_value = [], None

        #--------------------------------------------------------------------------------
        # Calling the delete ingestion configuration method
        #--------------------------------------------------------------------------------

        #@todo add some logic to check for state of the resources and ingestion service!
        with self.assertRaises(NotFound) as cm:
            self.ingestion_service.deactivate_ingestion_configuration('wrong_configuration_id')

        #--------------------------------------------------------------------------------
        # Assertions
        #--------------------------------------------------------------------------------

        ex = cm.exception
        self.mock_find_objects.assert_called_once_with('wrong_configuration_id', PRED.hasTransform, RT.Transform , True)
        self.assertEqual(ex.message, 'The ingestion configuration wrong_configuration_id does not exist')


@attr('INT', group='dm')
class IngestionManagementServiceIntTest(IonIntegrationTestCase):

    def setUp(self):
        # set up the container for testing

        #------------------------------------------------------------------------
        # Container
        #----------------------------------------------------------------------
        self._start_container()

        self.cc = ContainerAgentClient(node=self.container.node,name=self.container.name)

        self.cc.start_rel_from_url('res/deploy/r2dm.yml')

        #------------------------------------------------------------------------
        # Service clients
        #----------------------------------------------------------------------
        self.pubsub_cli = PubsubManagementServiceClient(node=self.cc.node)
        self.tms_cli = TransformManagementServiceClient(node=self.cc.node)
        self.ingestion_cli = IngestionManagementServiceClient(node=self.cc.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.cc.node)
        self.dataset_cli = DatasetManagementServiceClient(node=self.cc.node)

        #------------------------------------------------------------------------
        # Configuration parameters
        #----------------------------------------------------------------------
        self.exchange_point_id = 'science_data'
        self.number_of_workers = 2
        self.hdf_storage = HdfStorage(relative_path='ingest')
        self.couch_storage = CouchStorage(datastore_name='test_datastore')
        self.XP = 'science_data'
        self.exchange_name = 'ingestion_queue'


        #------------------------------------------------------------------------
        # Stream publisher for testing round robin handling
        #----------------------------------------------------------------------

        ctd_stream_def = ctd_stream_definition()

        stream_def_id = self.pubsub_cli.create_stream_definition(container=ctd_stream_def, name='Junk definition')


        self.input_stream_id = self.pubsub_cli.create_stream(
            name='input_stream',
            original=True,
            stream_definition_id=stream_def_id)

        self.input_dataset_id = self.dataset_cli.create_dataset(
            stream_id=self.input_stream_id,
            datastore_name=self.couch_storage.datastore_name,
            view_name='datasets/stream_join_granule'
        )


        pid = self.container.spawn_process(name='dummy_process_for_test',
            module='pyon.ion.process',
            cls='SimpleProcess',
            config={})
        dummy_process = self.container.proc_manager.procs[pid]

        # Normally the user does not see or create the publisher, this is part of the containers business.
        # For the test we need to set it up explicitly
        self.publisher_registrar = StreamPublisherRegistrar(process=dummy_process, node=self.cc.node)
        self.ctd_stream1_publisher = self.publisher_registrar.create_publisher(stream_id=self.input_stream_id)


        self.db = self.container.datastore_manager.get_datastore('dm_datastore', DataStore.DS_PROFILE.EXAMPLES, CFG)

        #------------------------------------------------------------------------
        # Create ingestion configuration and activate it
        #----------------------------------------------------------------------
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(
            exchange_point_id=self.exchange_point_id,
            couch_storage=self.couch_storage,
            hdf_storage=self.hdf_storage,
            number_of_workers=self.number_of_workers
        )

        self.ingestion_cli.activate_ingestion_configuration(ingestion_configuration_id)

        self.ingestion_configuration_id = ingestion_configuration_id


    def tearDown(self):
        """
        Cleanup. Delete Subscription, Stream, Process Definition
        """
        self._stop_container()

    def test_create_ingestion_configuration(self):
        """
        Tests whether an ingestion configuration is created successfully and an ingestion_configuration_id
        is generated.
        """


        #------------------------------------------------------------------------
        # Make assertions
        #----------------------------------------------------------------------
        # checking that an ingestion_configuration_id gets successfully generated
        self.assertIsNotNone(self.ingestion_configuration_id, "Could not generate ingestion_configuration_id.")

        # read the ingestion configuration object and see if it contains what it is supposed to....
        ingestion_configuration = self.ingestion_cli.read_ingestion_configuration(self.ingestion_configuration_id)

        self.assertEquals(ingestion_configuration.number_of_workers, self.number_of_workers)
        self.assertEquals(ingestion_configuration.hdf_storage.relative_path, self.hdf_storage.relative_path)
        self.assertEquals(ingestion_configuration.couch_storage.datastore_name, self.couch_storage.datastore_name)

    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_ingestion_workers_creation(self):
        """
        test_ingestion_workers
        1. Test whether the ingestion workers are launched correctly.
        2. Test the associations between the ingestion configuration object and the transforms.
	    3. Test the number of worker processes created by getting the process object from the container
        """


        #------------------------------------------------------------------------
        # Check that the two ingestion workers are running
        #------------------------------------------------------------------------

        print ("self.rr_cli.find_associations(ingestion_configuration_id, PRED.hasTransform) : %s" % self.rr_cli.find_associations(self.ingestion_configuration_id, PRED.hasTransform))
        print ("type : %s" % type(self.rr_cli.find_associations(self.ingestion_configuration_id, PRED.hasTransform)))


        transforms = [self.rr_cli.read(assoc.o)
                      for assoc in self.rr_cli.find_associations(self.ingestion_configuration_id, PRED.hasTransform)]

        for transform in transforms:
            self.assertTrue(self.container.proc_manager.procs[transform.process_id])

    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_ingestion_workers_working_round_robin(self):
        """
        test_ingestion_workers_working_round_robin
        Test that the ingestion workers are handling messages in round robin
        """



        #------------------------------------------------------------------------
        # Get the ingestion process instances:
        #------------------------------------------------------------------------

        transforms = [self.rr_cli.read(assoc.o)
                      for assoc in self.rr_cli.find_associations(self.ingestion_configuration_id, PRED.hasTransform)]

        proc_1 = self.container.proc_manager.procs[transforms[0].process_id]
        log.info("PROCESS 1: %s" % str(proc_1))

        proc_2 = self.container.proc_manager.procs[transforms[1].process_id]
        log.info("PROCESS 2: %s" % str(proc_2))

        #------------------------------------------------------------------------
        # Set up the gevent events
        #------------------------------------------------------------------------

        results = gevent.queue.Queue()
        def message_received_1(message):
            # Heads
            results.put(1)

        proc_1.process = message_received_1

        def message_received_2(message):
            # Tails
            results.put(2)


        proc_2.process = message_received_2

        #------------------------------------------------------------------------
        # Publish messages and test for round robin handling
        #------------------------------------------------------------------------

        # Flip the coin
        msg = {'num':'1'}
        self.ctd_stream1_publisher.publish(msg)


        # Flip the coin
        self.ctd_stream1_publisher.publish(msg)


        self.assertTrue((results.get(timeout=4) + results.get(timeout=4))==3,
            "The ingestion workers are not properly consuming from the broker")


        #------------------------------------------------------------------------
        # Flip the coin two more times
        #------------------------------------------------------------------------

        # Flip the coin
        self.ctd_stream1_publisher.publish(msg)



        # Flip the coin
        self.ctd_stream1_publisher.publish(msg)


        # Check heads and tails
        self.assertTrue((results.get(timeout=4) + results.get(timeout=4)) == 3,
            "The ingestion workers are not properly consuming from the broker")


    @unittest.skip("todo")
    def test_deactivate_ingestion_configuration(self):
        """
        Test the deactivation of the ingestion configuration
        """

        # now deactivate the ingestion configuration
        ret = self.ingestion_cli.deactivate_ingestion_configuration(self.ingestion_configuration_id)
        self.assertTrue(ret)

        # @TODO Test deactivate and reactivate....

    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_create_dataset_config_and_event_subscriber(self):
        """
        Test the creation of a dataset ingestion configuration and the call-back method of the event subscriber
        """


        #------------------------------------------------------------------------
        # Get the ingestion process instances:
        #------------------------------------------------------------------------

        transforms = [self.rr_cli.read(assoc.o)
                      for assoc in self.rr_cli.find_associations(self.ingestion_configuration_id, PRED.hasTransform)]

        proc_1 = self.container.proc_manager.procs[transforms[0].process_id]
        log.info("PROCESS 1: %s" % str(proc_1))

        proc_2 = self.container.proc_manager.procs[transforms[1].process_id]
        log.info("PROCESS 2: %s" % str(proc_2))

        #------------------------------------------------------------------------
        # Set up the gevent events
        #------------------------------------------------------------------------

        # Over ride the call back for the event subscriber
        ar_1 = gevent.event.AsyncResult()
        def message_received_1(message, headers):
            ar_1.set(message)

        proc_1.event_subscriber._callback = message_received_1

        # Over ride the call back for the event subscriber
        proc_2 = self.container.proc_manager.procs[transforms[1].process_id]
        log.info("PROCESS 2: %s" % str(proc_2))

        ar_2 = gevent.event.AsyncResult()
        def message_received_2(message, headers):
            ar_2.set(message)

        proc_2.event_subscriber._callback = message_received_2


        # Create a dataset ingestion config which sends an event

        dataset_config_id = self.ingestion_cli.create_dataset_configuration(
            dataset_id = self.input_dataset_id,
            archive_data = True,
            archive_metadata = False,
            ingestion_configuration_id = self.ingestion_configuration_id
            )


        dataset_config = self.rr_cli.read(dataset_config_id)

        #--------------------------------------------------------------------------------------------------------
        # Do assertions!
        #--------------------------------------------------------------------------------------------------------

        self.assertEquals(dataset_config.configuration.stream_id, self.input_stream_id)
        self.assertEquals(dataset_config.configuration.archive_data, True)
        self.assertEquals(dataset_config.configuration.archive_metadata, False)


        self.assertEqual(ar_1.get(timeout=10).configuration.stream_id,self.input_stream_id)
        self.assertEqual(ar_2.get(timeout=10).configuration.stream_id,self.input_stream_id)


    def test_create_dataset_configuration_not_found(self):
        """
        Test that trying to create a dataset configuration for a dataset that does not exist results in the raising of an
        a NotFound Assertion Error.
        """


        #--------------------------------------------------------------------------------------------------------
        # Do assertions!
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(NotFound):
            dataset_config_id = self.ingestion_cli.create_dataset_configuration(
                dataset_id = 'non_existent_dataset' ,
                archive_data = True,
                archive_metadata=True)

    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_dataset_config_dict_in_ingestion_worker(self):
        """
        Test that when a dataset config is created, each ingestion worker updates its dataset configs dict containing the
        config that has just been created
        """


        #------------------------------------------------------------------------
        # Get the ingestion process instances:
        #------------------------------------------------------------------------

        transforms = [self.rr_cli.read(assoc.o)
                      for assoc in self.rr_cli.find_associations(self.ingestion_configuration_id, PRED.hasTransform)]

        proc_1 = self.container.proc_manager.procs[transforms[0].process_id]
        log.info("PROCESS 1: %s" % str(proc_1))

        proc_2 = self.container.proc_manager.procs[transforms[1].process_id]
        log.info("PROCESS 2: %s" % str(proc_2))

        ar_1 = gevent.event.AsyncResult()
        def got_event_1(msg, headers):
            ar_1.set(msg)

        ar_2 = gevent.event.AsyncResult()
        def got_event_2(msg, headers):
            ar_2.set(msg)

        proc_1.dataset_configs_event_test_hook = got_event_1

        proc_2.dataset_configs_event_test_hook = got_event_2

        #------------------------------------------------------------------------
        # Create the dataset config
        #------------------------------------------------------------------------

        dataset_config_id = self.ingestion_cli.create_dataset_configuration(
            dataset_id = self.input_dataset_id,
            archive_data = True,
            archive_metadata = False,
            ingestion_configuration_id = self.ingestion_configuration_id
        )
        #--------------------------------------------------------------------------------------------------------
        # Do assertions and checks!
        #--------------------------------------------------------------------------------------------------------

        ar_1.get(timeout=5)
        ar_2.get(timeout=5)


        self.assertIn(self.input_stream_id, proc_1.dataset_configs)
        self.assertIn(self.input_stream_id, proc_2.dataset_configs)


    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_update_dataset_config(self):
        """
        Test updating a dataset config
        """

        #------------------------------------------------------------------------
        # Get the ingestion process instances:
        #------------------------------------------------------------------------

        transforms = [self.rr_cli.read(assoc.o)
                      for assoc in self.rr_cli.find_associations(self.ingestion_configuration_id, PRED.hasTransform)]

        proc_1 = self.container.proc_manager.procs[transforms[0].process_id]
        log.info("PROCESS 1: %s" % str(proc_1))

        proc_2 = self.container.proc_manager.procs[transforms[1].process_id]
        log.info("PROCESS 2: %s" % str(proc_2))

        #--------------------------------------------------------------------------------------------------------
        # Create a dataset config
        #--------------------------------------------------------------------------------------------------------

        dataset_config_id = self.ingestion_cli.create_dataset_configuration(
            dataset_id = self.input_dataset_id,
            archive_data = True,
            archive_metadata = False,
            ingestion_configuration_id = self.ingestion_configuration_id
        )

        dataset_config = self.rr_cli.read(dataset_config_id)

        old_description = dataset_config.description


        # check that the dataset_configs dict in the ingestion workers have been updated
        ar_1 = gevent.event.AsyncResult()
        def got_event_1(msg, headers):
            ar_1.set(msg)

        ar_2 = gevent.event.AsyncResult()
        def got_event_2(msg, headers):
            ar_2.set(msg)

        proc_1.dataset_configs_event_test_hook = got_event_1

        proc_2.dataset_configs_event_test_hook = got_event_2


        #--------------------------------------------------------------------------------------------------------
        # Change the dataset config and update it
        #--------------------------------------------------------------------------------------------------------

        dataset_config.configuration.archive_metadata = True
        # now update the dataset_config
        self.ingestion_cli.update_dataset_config( dataset_config)

        ar_1.get(timeout=5)
        ar_2.get(timeout=5)

        self.assertEquals(proc_1.dataset_configs[self.input_stream_id].configuration.archive_metadata, True)
        self.assertEquals(proc_2.dataset_configs[self.input_stream_id].configuration.archive_metadata, True)

        #--------------------------------------------------------------------------------------------------------
        # Read the updated config using resource registry to check that it has indeed been updated
        #--------------------------------------------------------------------------------------------------------

        new_dataset_config = self.rr_cli.read(dataset_config_id)

        #--------------------------------------------------------------------------------------------------------
        # Do assertions and checks!
        #--------------------------------------------------------------------------------------------------------

        self.assertEquals(new_dataset_config.configuration.archive_metadata, True)

    def test_update_dataset_config_not_found(self):
        """
        Test updating a dataset config that does not exist
        Assert that the operation fails
        """


        #--------------------------------------------------------------------------------------------------------
        # Assert that a non existent dataset config cannot be updated
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(BadRequest):
            dataset_config = DatasetIngestionConfiguration()
            dataset_config.description = 'updated right now'
            self.ingestion_cli.update_dataset_config(dataset_ingestion_configuration = dataset_config)

    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_read_dataset_config(self):
        """
        Test reading a dataset config
        """


        #------------------------------------------------------------------------
        # Get the ingestion process instances:
        #------------------------------------------------------------------------

        transforms = [self.rr_cli.read(assoc.o)
                      for assoc in self.rr_cli.find_associations(self.ingestion_configuration_id, PRED.hasTransform)]

        proc_1 = self.container.proc_manager.procs[transforms[0].process_id]
        log.info("PROCESS 1: %s" % str(proc_1))

        proc_2 = self.container.proc_manager.procs[transforms[1].process_id]
        log.info("PROCESS 2: %s" % str(proc_2))

        #--------------------------------------------------------------------------------------------------------
        # Create a dataset config
        #--------------------------------------------------------------------------------------------------------

        dataset_config_id = self.ingestion_cli.create_dataset_configuration(
            dataset_id = self.input_dataset_id,
            archive_data = True,
            archive_metadata = False,
            ingestion_configuration_id = self.ingestion_configuration_id
        )

        #--------------------------------------------------------------------------------------------------------
        # Read the dataset config
        #--------------------------------------------------------------------------------------------------------

        dataset_config = self.ingestion_cli.read_dataset_config(dataset_config_id)

        #--------------------------------------------------------------------------------------------------------
        # Do assertions and checks!
        #--------------------------------------------------------------------------------------------------------

        self.assertEquals(dataset_config.configuration.stream_id, self.input_stream_id)


    def test_read_dataset_config_not_found(self):
        """
        Test reading a dataset config that does not exist
        Assert that the operation fails
        """


        #--------------------------------------------------------------------------------------------------------
        # Assert that reading not existent dataset config raises an exception
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(NotFound):
            dataset_config = self.ingestion_cli.read_dataset_config('abracadabra')

    def test_delete_dataset_config(self):
        """
        Test deleting a dataset config
        """


        #--------------------------------------------------------------------------------------------------------
        # Create a dataset config
        #--------------------------------------------------------------------------------------------------------

        dataset_config_id = self.ingestion_cli.create_dataset_configuration(
            dataset_id = self.input_dataset_id,
            archive_data = True,
            archive_metadata = False,
            ingestion_configuration_id = self.ingestion_configuration_id
        )

        #--------------------------------------------------------------------------------------------------------
        # Delete the dataset config
        #--------------------------------------------------------------------------------------------------------

        self.ingestion_cli.delete_dataset_config(dataset_config_id)

        #--------------------------------------------------------------------------------------------------------
        # Assert that trying to read the dataset config now raises an exception
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(NotFound):
            dataset_config = self.rr_cli.read(dataset_config_id)

    def test_delete_dataset_config_not_found(self):
        """
        Test delting a stream that does not exist
        Assert that the operation fails
        """

        #--------------------------------------------------------------------------------------------------------
        # Delete a dataset config that does not exists
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(NotFound):
            self.ingestion_cli.delete_dataset_config('non_existent_stream_id')

    def test_ingestion_workers_writes_to_couch(self):
        """
        Test that the ingestion workers are writing messages to couch
        """


        #------------------------------------------------------------------------
        # Publish messages
        #----------------------------------------------------------------------

        post = BlogPost( post_id = '1234', title = 'The beautiful life',author = {'name' : 'Jacques', 'email' : 'jacques@cluseaou.com'}, updated = 'too early', content ='summer', stream_id=self.input_stream_id )

        self.ctd_stream1_publisher.publish(post)

        comment = BlogComment(ref_id = '1234',author = {'name': 'Roger', 'email' : 'roger@rabbit.com'}, updated = 'too late',content = 'when summer comes', stream_id=self.input_stream_id)

        self.ctd_stream1_publisher.publish(comment)


        #------------------------------------------------------------------------
        # List the posts and the comments that should have been written to couch
        #----------------------------------------------------------------------

        objs = self.db.list_objects()

        # the list of ion_objects... in our case BlogPost and BlogComment
        ion_objs = []

        for obj in objs:

            # read the document returned by list
            result = self.db.read_doc(objs[0])

            # convert the persistence dict to an ion_object
            ion_obj = self.db._persistence_dict_to_ion_object(result)

            if isinstance(ion_obj, BlogPost):
                log.debug("ION OBJECT: %s\n" % ion_obj)
                log.debug("POST: %s\n" % post)

                # since the retrieved document has an extra attribute, rev_id, which the orginal post did not have
                # it is easier to compare the attributes than the whole objects
                self.assertTrue(ion_obj.post_id == post.post_id), "The post is not to be found in couch storage"
                self.assertTrue(ion_obj.author == post.author), "The post is not to be found in couch storage"
                self.assertTrue(ion_obj.title == post.title), "The post is not to be found in couch storage"
                self.assertTrue(ion_obj.updated == post.updated), "The post is not to be found in couch storage"
                self.assertTrue(ion_obj.content == post.content), "The post is not to be found in couch storage"

            elif isinstance(ion_obj, BlogComment):
                log.debug("ION OBJECT: %s\n" % ion_obj)
                log.debug("COMMENT: %s\n" % comment)

                # since the retrieved document has an extra attribute, rev_id, which the orginal post did not have
                # it is easier to compare the attributes than the whole objects
                self.assertTrue(ion_obj.author == comment.author), "The comment is not to be found in couch storage"
                self.assertTrue(ion_obj.content == comment.content), "The comment is not to be found in couch storage"
                self.assertTrue(ion_obj.ref_id == comment.ref_id), "The comment is not to be found in couch storage"
                self.assertTrue(ion_obj.updated == comment.updated), "The comment is not to be found in couch storage"


    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_receive_dataset_config_event(self):
        """
        Test that the dataset config is being used properly
        """



        #------------------------------------------------------------------------
        # Test that the dataset config is implemented
        #----------------------------------------------------------------------

        #@todo after we have implemented how we handle stream depending on how dataset config gets evaluated, test the implementation

        #------------------------------------------------------------------------
        # Get the ingestion process instances:
        #------------------------------------------------------------------------

        transforms = [self.rr_cli.read(assoc.o)
                      for assoc in self.rr_cli.find_associations(self.ingestion_configuration_id, PRED.hasTransform)]

        proc_1 = self.container.proc_manager.procs[transforms[0].process_id]
        log.info("PROCESS 1: %s" % str(proc_1))

        proc_2 = self.container.proc_manager.procs[transforms[1].process_id]
        log.info("PROCESS 2: %s" % str(proc_2))

        #------------------------------------------------------------------------
        # Set up the gevent events
        #------------------------------------------------------------------------

        queue = gevent.queue.Queue()

        def dataset_config_hook(msg,headers):
            queue.put(True)


        proc_1.dataset_configs_event_test_hook = dataset_config_hook

        #--------------------------------------------------------------------------------------------------------
        # Create a dataset config
        #--------------------------------------------------------------------------------------------------------

        dataset_config_id = self.ingestion_cli.create_dataset_configuration(
            dataset_id = self.input_dataset_id,
            archive_data = True,
            archive_metadata = False,
            ingestion_configuration_id = self.ingestion_configuration_id
        )

        self.assertTrue(queue.get(timeout=10))

    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_dataset_config_implementation_for_science_data(self):
        """
        Test that the dataset config is being used properly for science data.
        Test that create and update dataset config functions properly and their implementation is correct
        """


        #--------------------------------------------------------------------------------------------------------
        # Get the ingestion process instances:
        #--------------------------------------------------------------------------------------------------------

        transforms = [self.rr_cli.read(assoc.o)
                      for assoc in self.rr_cli.find_associations(self.ingestion_configuration_id, PRED.hasTransform)]

        proc_1 = self.container.proc_manager.procs[transforms[0].process_id]
        log.info("PROCESS 1: %s" % str(proc_1))

        proc_2 = self.container.proc_manager.procs[transforms[1].process_id]
        log.info("PROCESS 2: %s" % str(proc_2))

        #--------------------------------------------------------------------------------------------------------
        # Create a dataset config
        #--------------------------------------------------------------------------------------------------------

        dataset_config_id = self.ingestion_cli.create_dataset_configuration(
            dataset_id = self.input_dataset_id,
            archive_data = True,
            archive_metadata = True,
            ingestion_configuration_id = self.ingestion_configuration_id
        )

        #--------------------------------------------------------------------------------------------------------
        # Set up the gevent event AsyncResult queue
        #--------------------------------------------------------------------------------------------------------

        queue=gevent.queue.Queue()

        def call_to_persist(packet):
            queue.put(packet)

        #--------------------------------------------------------------------------------------------------------
        # Grab the ingestion worker processes
        #--------------------------------------------------------------------------------------------------------

        # when persist_immutable() is called, then call_to_persist() is called instead....
        proc_1.persist_immutable = call_to_persist
        proc_2.persist_immutable = call_to_persist

        #--------------------------------------------------------------------------------------------------------
        # Create a packet and publish it
        #--------------------------------------------------------------------------------------------------------

        ctd_packet = self._create_packet(self.input_stream_id)

        self.ctd_stream1_publisher.publish(ctd_packet)

        #--------------------------------------------------------------------------------------------------------
        # Assert that the packets were handled according to the dataset config
        #--------------------------------------------------------------------------------------------------------

        # test that the ingestion worker tries to persist the ctd_packet in accordance to the dataset config
        self.assertEquals(queue.get(timeout=10).stream_resource_id, ctd_packet.stream_resource_id)

        #--------------------------------------------------------------------------------------------------------
        # Now change the dataset config for the same stream
        #--------------------------------------------------------------------------------------------------------

        dataset_config = self.ingestion_cli.read_dataset_config(dataset_config_id)

        dataset_config.configuration.archive_metadata = False

        self.ingestion_cli.update_dataset_config(dataset_config)

        #--------------------------------------------------------------------------------------------------------
        # Create a new packet and publish it
        #--------------------------------------------------------------------------------------------------------

        ctd_packet = self._create_packet(self.input_stream_id)

        self.ctd_stream1_publisher.publish(ctd_packet)

        #--------------------------------------------------------------------------------------------------------
        # Assert that the packets were handled according to the new dataset config...
        # This time, the packet should not be persisted since archive_metadata is False
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(gevent.queue.Empty):
            queue.get(timeout=0.25)

        #--------------------------------------------------------------------------------------------------------
        # Now just do this thing one more time, with an updated dataset config...
        #
        # Change the dataset config for the same stream for the third time, updating archive_metadata back to True
        #--------------------------------------------------------------------------------------------------------

        dataset_config = self.ingestion_cli.read_dataset_config(dataset_config_id)
        dataset_config.configuration.archive_metadata = True
        self.ingestion_cli.update_dataset_config(dataset_config)

        #--------------------------------------------------------------------------------------------------------
        # Create a new packet and publish it
        #--------------------------------------------------------------------------------------------------------

        ctd_packet = self._create_packet(self.input_stream_id)
        self.ctd_stream1_publisher.publish(ctd_packet)

        #--------------------------------------------------------------------------------------------------------
        # Assert that the packets were handled according to the new dataset config
        #--------------------------------------------------------------------------------------------------------

        self.assertEquals(queue.get(timeout=10).stream_resource_id, ctd_packet.stream_resource_id)


    def _create_packet(self, stream_id):
        
        length = random.randint(1,20)

        c = [random.uniform(0.0,75.0)  for i in xrange(length)]

        t = [random.uniform(-1.7, 21.0) for i in xrange(length)]

        p = [random.lognormvariate(1,2) for i in xrange(length)]

        lat = [random.uniform(-90.0, 90.0) for i in xrange(length)]

        lon = [random.uniform(0.0, 360.0) for i in xrange(length)]

        tvar = [ i for i in xrange(1,length+1)]

        ctd_packet = ctd_stream_packet(stream_id=stream_id,
            c=c, t=t, p=p, lat=lat, lon=lon, time=tvar, create_hdf=True)

        return ctd_packet

