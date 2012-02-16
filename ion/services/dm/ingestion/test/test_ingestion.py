#!/usr/bin/env python

'''
@file ion/services/dm/ingestion/test/test_ingestion.py
@author Swarbhanu Chatterjee
@test ion.services.dm.ingestion.ingestion_management_service test suite to cover all ingestion mgmt service code
'''

import gevent
from mock import Mock
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.dm.ingestion.ingestion_management_service import IngestionManagementService, IngestionManagementServiceException
from nose.plugins.attrib import attr

from pyon.core.exception import NotFound
from pyon.public import log, StreamPublisherRegistrar, CFG
from interface.objects import HdfStorage, CouchStorage, StreamPolicy, ProcessDefinition
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from pyon.datastore.datastore import DataStore

from interface.objects import BlogPost, BlogComment


import unittest

@attr('UNIT', group='dm')
class IngestionTest(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('ingestion_management')
        self.ingestion_service = IngestionManagementService()
        self.ingestion_service.clients = mock_clients
        self.ingestion_service.process_definition = ProcessDefinition()
        self.ingestion_service.process_definition.executable['module'] = 'ion.processes.data.ingestion.ingestion_worker'
        self.ingestion_service.process_definition.executable['class']= 'IngestionWorker'
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

        # Ingestion Configuration
#        self.ingestion_configuration_id = "ingestion_configuration_id"
        self.ingestion_configuration_id = Mock()
        self.ingestion_configuration = Mock()
        self.ingestion_configuration._id = self.ingestion_configuration_id
        self.ingestion_configuration._rev = "Sample_ingestion_configuration_rev"

        # Exchange point
        self.exchange_point_id = "exchange_point_id"

        # Couch storage
        self.couch_storage = CouchStorage()

        # hfd_storage
        self.hdf_storage = HdfStorage()

        # number of workers
        self.number_of_workers = 2

        # default policy
        self.default_policy = StreamPolicy()



    def test_create_ingestion_configuration(self):

        self.mock_create.return_value = [self.ingestion_configuration_id, 1]


        ingestion_configuration_id = self.ingestion_service.create_ingestion_configuration(self.exchange_point_id, \
                                self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        self.assertEqual(ingestion_configuration_id, self.ingestion_configuration_id)

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

    @unittest.skip("Nothing to test")
    def test_delete_ingestion_configuration(self):

        self.mock_create.return_value = [self.ingestion_configuration_id, 1]

        self.mock_find_objects.return_value = ['transform_id']

        ingestion_configuration_id = self.ingestion_service.create_ingestion_configuration(self.exchange_point_id,\
            self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        log.debug("ingestion_configuration_id: %s" % ingestion_configuration_id)

        self.ingestion_service.delete_ingestion_configuration(ingestion_configuration_id)
        #@todo add some logic to check for state of the resources and ingestion service!

        # check that everything is alright
#        self.mock_read.assert_called_once_with(self.ingestion_configuration_id, '')
#        self.mock_delete.assert_called_once_with(self.ingestion_configuration_id)

    @unittest.skip("Nothing to test")
    def test_delete_ingestion_configuration_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.ingestion_service.delete_ingestion_configuration('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Ingestion configuration notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
        self.assertEqual(self.mock_delete.call_count, 0)

    @unittest.skip("Nothing to test")
    def test_activate_deactivate_ingestion_configuration(self):
        """
        Test that the ingestion configuration is activated
        """

        #@todo add some logic to check for state of the resources and ingestion service!
        self.ingestion_service.activate_ingestion_configuration(self.ingestion_configuration_id)

        self.ingestion_service.deactivate_ingestion_configuration(self.ingestion_configuration_id)



    @unittest.skip("Nothing to test")
    def test_activate_ingestion_configuration_not_found(self):
        """
        Test that non existent ingestion configuration does not cause crash when attempting to activate
        """
        ingestion_service = IngestionManagementService()

        with self.assertRaises(NotFound) as cm:
            ingestion_service.activate_ingestion_configuration('wrong')
        ex = cm.exception
        self.assertEqual(ex.message, 'Ingestion configuration wrong does not exist')

    @unittest.skip("Nothing to test")
    def test_deactivate_ingestion_configuration_not_found(self):
        """
        Test that non existent ingestion configuration does not cause crash when attempting to activate
        """
        ingestion_service = IngestionManagementService()

        with self.assertRaises(NotFound) as cm:
            ingestion_service.deactivate_ingestion_configuration('wrong')
        ex = cm.exception
        self.assertEqual(ex.message, 'Ingestion configuration wrong does not exist')


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

        #------------------------------------------------------------------------
        # Configuration parameters
        #----------------------------------------------------------------------
        self.exchange_point_id = 'science_data'
        self.number_of_workers = 2
        self.hdf_storage = HdfStorage(file_system='mysystem')
        self.couch_storage = CouchStorage(datastore_name='test_datastore')
        self.default_policy = StreamPolicy(archive_metadata=False)
        self.XP = 'science_data'
        self.exchange_name = 'ingestion_queue'


        #------------------------------------------------------------------------
        # Stream publisher for testing round robin handling
        #----------------------------------------------------------------------

        self.input_stream_id = self.pubsub_cli.create_stream(name='input_stream',original=True)


        pid = self.container.spawn_process(name='dummy_process_for_test',
            module='pyon.ion.process',
            cls='SimpleProcess',
            config={})
        dummy_process = self.container.proc_manager.procs[pid]

        # Normally the user does not see or create the publisher, this is part of the containers business.
        # For the test we need to set it up explicitly
        publisher_registrar = StreamPublisherRegistrar(process=dummy_process, node=self.cc.node)
        self.ctd_stream1_publisher = publisher_registrar.create_publisher(stream_id=self.input_stream_id)


        self.db = self.container.datastore_manager.get_datastore('dm_datastore', DataStore.DS_PROFILE.EXAMPLES, CFG)

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
        # Create ingestion configuration
        #----------------------------------------------------------------------
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        #------------------------------------------------------------------------
        # Make assertions
        #----------------------------------------------------------------------
        # checking that an ingestion_configuration_id gets successfully generated
        self.assertIsNotNone(ingestion_configuration_id, "Could not generate ingestion_configuration_id.")

        # read the ingestion configuration object and see if it contains what it is supposed to....
        ingestion_configuration = self.ingestion_cli.read_ingestion_configuration(ingestion_configuration_id)

        self.assertEquals(ingestion_configuration.number_of_workers, self.number_of_workers)
        self.assertEquals(ingestion_configuration.hdf_storage.file_system, self.hdf_storage.file_system)
        self.assertEquals(ingestion_configuration.couch_storage.datastore_name, self.couch_storage.datastore_name)
        self.assertEquals(ingestion_configuration.default_policy.archive_metadata, self.default_policy.archive_metadata)

        #------------------------------------------------------------------------
        # Cleanup
        #----------------------------------------------------------------------

        self.ingestion_cli.delete_ingestion_configuration(ingestion_configuration_id)


    def test_ingestion_workers(self):
        """
        test_ingestion_workers
        1. Test whether the ingestion workers are launched correctly.
        2. Test the associations between the ingestion configuration object and the transforms.
	    3. Test the number of worker processes created by getting the process object from the container
        """

        #------------------------------------------------------------------------
        # Create ingestion configuration
        #----------------------------------------------------------------------
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        #------------------------------------------------------------------------
        # Check that the two ingestion workers are running
        #----------------------------------------------------------------------
        name_1 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 1)
        name_2 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 2)

        self.assertTrue(self.container.proc_manager.procs_by_name.has_key(name_1))
        self.assertTrue(self.container.proc_manager.procs_by_name.has_key(name_2))


        #------------------------------------------------------------------------
        # Cleanup
        #----------------------------------------------------------------------
        self.ingestion_cli.delete_ingestion_configuration(ingestion_configuration_id)


    def test_ingestion_workers_in_round_robin(self):
        """
        Test that the ingestion workers are handling messages in round robin
        """

        #------------------------------------------------------------------------
        # Create ingestion configuration and activate it
        #----------------------------------------------------------------------
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, \
            self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)
        self.ingestion_cli.activate_ingestion_configuration(ingestion_configuration_id)


        name_1 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 1)
        name_2 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 2)
        #Get the ingestion process instances:
        proc_1 = self.container.proc_manager.procs_by_name.get(name_1)
        log.info("PROCESS 1: %s" % str(proc_1))

        ar_1 = gevent.event.AsyncResult()
        def message_received_1(message):
            ar_1.set(message)

        proc_1.process = message_received_1


        proc_2 = self.container.proc_manager.procs_by_name.get(name_2)
        log.info("PROCESS 2: %s" % str(proc_2))


        ar_2 = gevent.event.AsyncResult()
        def message_received_2(message):
            ar_2.set(message)


        proc_2.process = message_received_2

        #------------------------------------------------------------------------
        # Publish messages and test for round robin handling
        #----------------------------------------------------------------------

        num = 1
        msg = dict(num=str(num))

        self.ctd_stream1_publisher.publish(msg)

        self.assertEqual(ar_1.get(timeout=10),msg)

        num += 1
        msg = dict(num=str(num))

        self.ctd_stream1_publisher.publish(msg)

        self.assertEqual(ar_2.get(timeout=10),msg)


        # Reset the async results and try again...
        ar_1 = gevent.event.AsyncResult()
        ar_2 = gevent.event.AsyncResult()


        num += 1
        msg = dict(num=str(num))

        self.ctd_stream1_publisher.publish(msg)

        self.assertEqual(ar_1.get(timeout=10),msg)


        num += 1
        msg = dict(num=str(num))
        self.ctd_stream1_publisher.publish(msg)

        self.assertEqual(ar_2.get(timeout=10),msg)


        #------------------------------------------------------------------------
        # Cleanup
        #----------------------------------------------------------------------
        self.ingestion_cli.deactivate_ingestion_configuration(ingestion_configuration_id)
        self.ingestion_cli.delete_ingestion_configuration(ingestion_configuration_id)


    def test_activate_ingestion_configuration(self):
        """
        Test the activation of the ingestion configuration
        """
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        # activate an ingestion configuration
        ret = self.ingestion_cli.activate_ingestion_configuration(ingestion_configuration_id)

        self.assertTrue(ret)
        # pubsub has tested the activation of subscriptions
        # @TODO when these are proper life cycle state changes, test the state transition of the resources...


        #------------------------------------------------------------------------
        # Cleanup
        #----------------------------------------------------------------------
        self.ingestion_cli.deactivate_ingestion_configuration(ingestion_configuration_id)
        self.ingestion_cli.delete_ingestion_configuration(ingestion_configuration_id)


    def test_deactivate_ingestion_configuration(self):
        """
        Test the deactivation of the ingestion configuration
        """
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        # activate an ingestion configuration
        ret = self.ingestion_cli.activate_ingestion_configuration(ingestion_configuration_id)
        self.assertTrue(ret)

        # now deactivate the ingestion configuration
        ret = self.ingestion_cli.deactivate_ingestion_configuration(ingestion_configuration_id)
        self.assertTrue(ret)

        # @TODO when these are proper life cycle state changes, test the state transition of the resources...

        # pubsub has tested the deactivation of subscriptions

        #------------------------------------------------------------------------
        # Cleanup
        #----------------------------------------------------------------------
        self.ingestion_cli.delete_ingestion_configuration(ingestion_configuration_id)

    def test_create_stream_policy(self):
        """
        Test creating a stream policy
        """


        # Create the ingestion workers
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)


        # get the worker processes
        name_1 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 1)
        name_2 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 2)
        #Get the ingestion process instances:
        proc_1 = self.container.proc_manager.procs_by_name.get(name_1)
        log.info("PROCESS 1: %s" % str(proc_1))

        # Over ride the call back for the event subscriber
        ar_1 = gevent.event.AsyncResult()
        def message_received_1(message, headers):
            ar_1.set(message)

        proc_1.event_subscriber._callback = message_received_1

        # Over ride the call back for the event subscriber
        proc_2 = self.container.proc_manager.procs_by_name.get(name_2)
        log.info("PROCESS 2: %s" % str(proc_2))

        ar_2 = gevent.event.AsyncResult()
        def message_received_2(message, headers):
            ar_2.set(message)

        proc_2.event_subscriber._callback = message_received_2


        # Create a stream policy which sends an event

        stream_policy_id = self.ingestion_cli.create_stream_policy( stream_id = self.input_stream_id , archive_data = True, archive_metadata=False)
        stream_policy = self.rr_cli.read(stream_policy_id)

        #--------------------------------------------------------------------------------------------------------
        # Do assertions!
        #--------------------------------------------------------------------------------------------------------

        self.assertEquals(stream_policy.policy.stream_id, self.input_stream_id)
        self.assertEquals(stream_policy.policy.archive_data, True)
#        self.assertEquals(stream_policy.policy.archive_metadata, False)


        self.assertEqual(ar_1.get(timeout=10).stream_id,self.input_stream_id)
        self.assertEqual(ar_2.get(timeout=10).stream_id,self.input_stream_id)


    def test_create_stream_policy_stream_not_found(self):
        # try to create a stream policy for a stream that does not exist
        # Assert that the operation fails

        #--------------------------------------------------------------------------------------------------------
        # Create the ingestion workers
        #--------------------------------------------------------------------------------------------------------

        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        #--------------------------------------------------------------------------------------------------------
        # Do assertions!
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(Exception):
            stream_policy_id = self.ingestion_cli.create_stream_policy( stream_id = 'non_existent_stream' , archive_data = True, archive_metadata=True)


    def test_event_subscriber(self):
        """
        Test that an event subscriber is created for each ingestion worker that subscribes to events
        Assert that the subscriber receives messages
        """

        #--------------------------------------------------------------------------------------------------------
        # Create the ingestion workers
        #--------------------------------------------------------------------------------------------------------

        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)


        # the worker processes
        name_1 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 1)
        name_2 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 2)

        proc_1 = self.container.proc_manager.procs_by_name.get(name_1)
        proc_2 = self.container.proc_manager.procs_by_name.get(name_2)

        stream_policy_id = self.ingestion_cli.create_stream_policy( stream_id = self.input_stream_id , archive_data = True, archive_metadata=False)

        #--------------------------------------------------------------------------------------------------------
        # Do assertions and checks!
        #--------------------------------------------------------------------------------------------------------

        self.assertEquals(proc_1.stream_policies.get(self.input_stream_id).stream_id, self.input_stream_id)
        self.assertEquals(proc_2.stream_policies.get(self.input_stream_id).stream_id, self.input_stream_id)


    def test_update_stream_policy(self):
        """
        Test updating a stream policy
        """

        #--------------------------------------------------------------------------------------------------------
        # Create the ingestion workers
        #--------------------------------------------------------------------------------------------------------

        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        # the worker processes
        name_1 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 1)
        name_2 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 2)

        proc_1 = self.container.proc_manager.procs_by_name.get(name_1)
        proc_2 = self.container.proc_manager.procs_by_name.get(name_2)

        #--------------------------------------------------------------------------------------------------------
        # Create a stream policy
        #--------------------------------------------------------------------------------------------------------

        stream_policy_id = self.ingestion_cli.create_stream_policy( stream_id = self.input_stream_id , archive_data = True, archive_metadata=False)
        stream_policy = self.rr_cli.read(stream_policy_id)

        old_description = stream_policy.description

        #--------------------------------------------------------------------------------------------------------
        # Change the stream policy and update it
        #--------------------------------------------------------------------------------------------------------

        stream_policy.description = 'updated right now'
        # now update the stream polic
        self.ingestion_cli.update_stream_policy( stream_policy)

        #--------------------------------------------------------------------------------------------------------
        # Read the updated policy
        #--------------------------------------------------------------------------------------------------------

        new_stream_policy = self.rr_cli.read(stream_policy_id)

        #--------------------------------------------------------------------------------------------------------
        # Do assertions and checks!
        #--------------------------------------------------------------------------------------------------------

        self.assertEquals(new_stream_policy.description, 'updated right now')
        self.assertNotEquals(old_description, new_stream_policy.description)

    def test_update_stream_policy_not_found(self):
        """
        Test updating a stream policy that does not exist
        Assert that the operation fails
        """

        #--------------------------------------------------------------------------------------------------------
        # Create the ingestion workers
        #--------------------------------------------------------------------------------------------------------

        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        # the worker processes
        name_1 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 1)
        name_2 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 2)

        proc_1 = self.container.proc_manager.procs_by_name.get(name_1)
        proc_2 = self.container.proc_manager.procs_by_name.get(name_2)


        #--------------------------------------------------------------------------------------------------------
        # Assert that a non existent stream policy cannot be updated
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(Exception):
            stream_policy.description = 'updated right now'
            self.ingestion_cli.update_stream_policy(stream_policy = 'bad_stream')

    def test_read_stream_policy(self):
        """
        Test reading a stream policy
        """

        #--------------------------------------------------------------------------------------------------------
        # Create the ingestion workers
        #--------------------------------------------------------------------------------------------------------

        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        # the worker processes
        name_1 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 1)
        name_2 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 2)

        proc_1 = self.container.proc_manager.procs_by_name.get(name_1)
        proc_2 = self.container.proc_manager.procs_by_name.get(name_2)

        #--------------------------------------------------------------------------------------------------------
        # Create a stream policy
        #--------------------------------------------------------------------------------------------------------

        stream_policy_id = self.ingestion_cli.create_stream_policy( stream_id = self.input_stream_id , archive_data = True, archive_metadata=False)

        #--------------------------------------------------------------------------------------------------------
        # Read the stream policy
        #--------------------------------------------------------------------------------------------------------

        stream_policy = self.rr_cli.read(stream_policy_id)

        #--------------------------------------------------------------------------------------------------------
        # Do assertions and checks!
        #--------------------------------------------------------------------------------------------------------

        self.assertEquals(stream_policy.policy.stream_id, self.input_stream_id)


    def test_read_stream_policy_not_found(self):
        """
        Test reading a stream policy that does not exist
        Assert that the operation fails
        """

        #--------------------------------------------------------------------------------------------------------
        # Create the ingestion workers
        #--------------------------------------------------------------------------------------------------------

        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        # the worker processes
        name_1 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 1)
        name_2 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 2)

        proc_1 = self.container.proc_manager.procs_by_name.get(name_1)
        proc_2 = self.container.proc_manager.procs_by_name.get(name_2)

        #--------------------------------------------------------------------------------------------------------
        # Assert that reading not existent stream policy raises an exception
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(Exception):
            stream_policy = self.rr_cli.read('abracadabra')


    def test_delete_stream_policy(self):
        """
        Test deleting a strema policy
        """

        #--------------------------------------------------------------------------------------------------------
        # Create the ingestion workers
        #--------------------------------------------------------------------------------------------------------

        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        # the worker processes
        name_1 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 1)
        name_2 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 2)

        proc_1 = self.container.proc_manager.procs_by_name.get(name_1)
        proc_2 = self.container.proc_manager.procs_by_name.get(name_2)

        #--------------------------------------------------------------------------------------------------------
        # Create a stream policy
        #--------------------------------------------------------------------------------------------------------

        stream_policy_id = self.ingestion_cli.create_stream_policy( stream_id = self.input_stream_id , archive_data = True, archive_metadata=False)

        #--------------------------------------------------------------------------------------------------------
        # Delete the stream policy
        #--------------------------------------------------------------------------------------------------------

        self.ingestion_cli.delete_stream_policy(stream_policy_id)

        #--------------------------------------------------------------------------------------------------------
        # Assert that trying to read the stream policy now raises an exception
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(Exception):
            stream_policy = self.rr_cli.read(stream_policy_id)

    def test_delete_stream_policy_not_found(self):
        """
        Test delting a stream that does not exist
        Assert that the operation fails
        """

        #--------------------------------------------------------------------------------------------------------
        # Create the ingestion workers
        #--------------------------------------------------------------------------------------------------------

        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)

        # the worker processes
        name_1 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 1)
        name_2 = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, 2)

        proc_1 = self.container.proc_manager.procs_by_name.get(name_1)
        proc_2 = self.container.proc_manager.procs_by_name.get(name_2)

        #--------------------------------------------------------------------------------------------------------
        # Delete a stream policy that does not exists
        #--------------------------------------------------------------------------------------------------------

        with self.assertRaises(Exception):
            self.ingestion_cli.delete_stream_policy('non_existent_stream_id')

    def test_ingestion_workers_writes_to_couch(self):
        """
        Test that the ingestion workers are writing messages to couch
        """

        #------------------------------------------------------------------------
        # Create ingestion configuration and activate it
        #----------------------------------------------------------------------
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id,\
            self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)
        self.ingestion_cli.activate_ingestion_configuration(ingestion_configuration_id)

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

        #------------------------------------------------------------------------
        # Cleanup
        #----------------------------------------------------------------------

        self.ingestion_cli.deactivate_ingestion_configuration(ingestion_configuration_id)
        self.ingestion_cli.delete_ingestion_configuration(ingestion_configuration_id)

    @unittest.skip("todo: after stream policy has been implemented")
    def test_default_policy(self):
        """
        Test that the default policy is being used properly
        """
        """
        Test that the ingestion workers are writing messages to couch
        """

        #------------------------------------------------------------------------
        # Create ingestion configuration and activate it
        #----------------------------------------------------------------------
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id,\
            self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)
        self.ingestion_cli.activate_ingestion_configuration(ingestion_configuration_id)

        #------------------------------------------------------------------------
        # Publish messages
        #----------------------------------------------------------------------

        post = BlogPost( post_id = '1234', title = 'The beautiful life',author = {'name' : 'Jacques', 'email' : 'jacques@cluseaou.com'}, updated = 'too early', content ='summer', stream_id=self.input_stream_id )

        self.ctd_stream1_publisher.publish(post)

        comment = BlogComment(ref_id = '1234',author = {'name': 'Roger', 'email' : 'roger@rabbit.com'}, updated = 'too late',content = 'when summer comes', stream_id=self.input_stream_id)

        self.ctd_stream1_publisher.publish(comment)

        #------------------------------------------------------------------------
        # Test that the policy is
        #----------------------------------------------------------------------

        #@todo after we have implemented how we handle stream depending on how policy gets evaluated, test the implementation


        #------------------------------------------------------------------------
        # Cleanup
        #----------------------------------------------------------------------

        self.ingestion_cli.deactivate_ingestion_configuration(ingestion_configuration_id)
        self.ingestion_cli.delete_ingestion_configuration(ingestion_configuration_id)
