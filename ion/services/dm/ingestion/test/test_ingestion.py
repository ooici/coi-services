#!/usr/bin/env python

'''
@file ion/services/dm/ingestion/test/test_ingestion.py
@author Swarbhanu Chatterjee
@test ion.services.dm.ingestion.ingestion_management_service Unit test suite to cover all ingestion mgmt service code
'''

from mock import Mock, sentinel, patch
from pyon.util.unit_test import PyonTestCase
from ion.services.dm.ingestion.ingestion_management_service import IngestionManagementService, IngestionManagementServiceException
from nose.plugins.attrib import attr
from pyon.core.exception import NotFound
from pyon.public import log, AT
import unittest
from pyon.public import CFG, IonObject, log, RT, AT, LCS
from pyon.public import Container
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import Container
from interface.services.icontainer_agent import ContainerAgentClient
from interface.objects import ProcessDefinition, StreamQuery
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient


@attr('UNIT', group='dm')
class IngestionTest(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('ingestion_management')
        self.ingestion_service = IngestionManagementService()
        self.ingestion_service.clients = mock_clients

        # save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_read = mock_clients.resource_registry.read
        self.mock_create_association = mock_clients.resource_registry.create_association
        self.mock_delete_association = mock_clients.resource_registry.delete_association
        self.mock_find_resources = mock_clients.resource_registry.find_resources
        self.mock_find_subjects = mock_clients.resource_registry.find_subjects

        # Ingestion Configuration
        self.ingestion_configuration_id = "ingestion_configuration_id"
        self.ingestion_configuration = Mock()
        self.ingestion_configuration._id = self.ingestion_configuration_id
        self.ingestion_configuration._rev = "Sample_ingestion_configuration_rev"

        # Exchange point
        self.exchange_point_id = "exchange_point_id"

        # Couch storage
        self.couch_storage = {'filesystem':"SampleFileSystem", 'root_path':"SampleRootPath"}

        # hfd_storage
        self.hfd_storage = {'server':"SampleServer", 'database':"SampleDatabase"}

        # number of workers
        self.number_of_workers = 2

        # default policy
        self.default_policy = "SampleDefaultPolicy" # todo: later use Mock(specset = 'StreamIngestionPolicy')

    def test_create_ingestion_configuration(self):

        self.mock_create.return_value = [self.ingestion_configuration_id, 1]


        ingestion_configuration_id = self.ingestion_service.create_ingestion_configuration(self.exchange_point_id, \
                                self.couch_storage, self.hfd_storage, self.number_of_workers, self.default_policy)

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

    def test_delete_ingestion_configuration(self):
        self.mock_create.return_value = [self.ingestion_configuration_id, 1]

        ingestion_configuration_id = self.ingestion_service.create_ingestion_configuration(self.exchange_point_id,\
            self.couch_storage, self.hfd_storage, self.number_of_workers, self.default_policy)

        self.mock_read.return_value = self.ingestion_configuration

        # now delete it
        self.ingestion_service.delete_ingestion_configuration(ingestion_configuration_id)

        # check that everything is alright
        self.mock_read.assert_called_once_with(self.ingestion_configuration_id, '')
        self.mock_delete.assert_called_once_with(self.ingestion_configuration)

    def test_delete_ingestion_configuration_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.ingestion_service.delete_ingestion_configuration('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Ingestion configuration notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
        self.assertEqual(self.mock_delete.call_count, 0)


@attr('INT', group='dm')
class IngestionManagementServiceIntTest(IonIntegrationTestCase):

    def setUp(self):
        # set up the container
        self._start_container()

        self.cc = ContainerAgentClient(node=self.container.node,name=self.container.name)

        self.cc.start_rel_from_url('res/deploy/r2deploy.yml')

        self.pubsub_cli = PubsubManagementServiceClient(node=self.cc.node)
        self.tms_cli = TransformManagementServiceClient(node=self.cc.node)
        self.ingestion_cli = IngestionManagementServiceClient(node=self.cc.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.cc.node)

        self.input_stream_id = self.pubsub_cli.create_stream(name='input_stream',original=True)
        self.input_subscription_id = self.pubsub_cli.create_subscription(query=StreamQuery(stream_ids=[self.input_stream_id]),exchange_name='transform_input',name='input_subscription')

        self.process_definition = ProcessDefinition(name='basic_ingestion_definition')
        self.process_definition.executable = {'module': 'ion.services.dm.ingestion.ingestion_example',
                                              'class':'TransformExample'}
        self.process_definition_id, _= self.rr_cli.create(self.process_definition)

        # for now we havent finalized on what the exchange_point_id should be
        self.exchange_point_id = 'an exchange_point_id'
        self.number_of_workers = 2
        self.hfd_storage = {'filesystem' : 'a filesystem'}
        self.couch_storage = {'couchstorage' : 'a couchstorage' }
        self.default_policy = 'a default_policy'
        self.ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, self.couch_storage, self.hfd_storage, \
                    self.number_of_workers, self.default_policy)


    def tearDown(self):
        self.ingestion_cli.deactivate_ingestion_configuration(self.ingestion_configuration_id)
        self.ingestion_cli.delete_ingestion_configuration(self.ingestion_configuration_id)
        self._stop_container()


    def test_create_ingestion_configuration(self):
        """
        Tests whether an ingestion configuration is created successfully and an ingestion_configuration_id
        is generated.
        """
        configuration = {'program_args':{'arg1':'value'}}

        # for less typing storing the instance's ingestion_configuration parameters
        number_of_workers = self.ingestion_configuration.number_of_workers
        hfd_storage = self.ingestion_configuration.hfd_storage
        couch_storage = self.ingestion_configuration.couch_storage
        default_policy = self.ingestion_configuration.default_policy
        exchange_point_id = self.exchange_point_id

        # checking that an ingestion configuration can be successfully created
        with self.assertRaises(IngestionManagementServiceException):
            ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(exchange_point_id, \
                couch_storage,hfd_storage, number_of_workers, default_policy)

        # checking that an ingestion_configuration_id gets successfully generated
        self.assertIsNotNone(ingestion_configuration_id, "Could not generate ingestion_configuration_id.")

        ingestion_configuration = self.ingestion_cli.read_ingestion_configuration(ingestion_configuration_id)

        # checking that the ingestion_configuration contains what it is supposed to...
        self.assertEquals(ingestion_configuration.number_of_workers, number_of_workers)
        self.assertEquals(ingestion_configuration.hdf_storage, hfd_storage)
        self.assrtEquals(ingestion_configuration.couch_storage, couch_storage)
        self.assertEquals(ingestion_configuration.default_policy, default_policy)
        self.assertEquals(ingestion_configuration.exchange_point_id, exchange_point_id)

        # clean up the specific ingestion_configuration created here
        self.ingestion_cli.delete_ingestion_configuration(ingestion_configuration_id)


    def test_create_transform_no_procdef(self):
        with self.assertRaises(NotFound):
            self.tms_cli.create_transform(name='test',in_subscription_id=self.input_subscription_id)

    def test_create_transform_bad_procdef(self):
        with self.assertRaises(NotFound):
            self.tms_cli.create_transform(name='test',
                in_subscription_id=self.input_subscription_id,
                process_definition_id='bad')

    def test_create_transform_no_config(self):
        transform_id = self.tms_cli.create_transform(
            name='test_transform',
            in_subscription_id=self.input_subscription_id,
            out_streams={'output':self.output_stream_id},
            process_definition_id=self.process_definition_id,
        )
