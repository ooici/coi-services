
"""
@author Luke Campbell
@file ion/services/dm/test/test_ctd_integration.py
@description Provides a full fledged integration from ingestion to replay using scidata
"""
import time
from interface.objects import CouchStorage, ProcessDefinition, StreamQuery, StreamPolicy
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
import os

@attr('INT',group='dm')
class CTDIntegrationTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.pubsub_management_service = PubsubManagementServiceClient(node=self.container.node)
        self.ingestion_management_service = IngestionManagementServiceClient(node=self.container.node)
        self.dataset_management_service = DatasetManagementServiceClient(node=self.container.node)
        self.data_retriever_service = DataRetrieverServiceClient(node=self.container.node)
        self.transform_management_service = TransformManagementServiceClient(node=self.container.node)
        self.resource_registry_service = ResourceRegistryServiceClient(node=self.container.node)
        self.process_dispatcher = ProcessDispatcherServiceClient(node=self.container.node)

        # We keep track of our own processes
        self.process_list = []
        self.datasets = []

        # store name
        self.datastore_name = 'test_dm_integration'

    def test_dm_integration(self):
        '''
        Test full DM Services Integration
        '''

        #---------------------------
        # Set up ingestion
        #---------------------------
        # Configure ingestion using eight workers, ingesting to test_dm_integration datastore with the SCIDATA profile
        ingestion_configuration_id = self.ingestion_management_service.create_ingestion_configuration(
            exchange_point_id='science_data',
            couch_storage=CouchStorage(datastore_name=self.datastore_name,datastore_profile='SCIDATA'),
            default_policy=StreamPolicy(archive_metadata=False, archive_data=False),
            number_of_workers=8
        )
        #
        self.ingestion_management_service.activate_ingestion_configuration(
            ingestion_configuration_id=ingestion_configuration_id)

        #---------------------------
        # Set up the producers (CTD Simulators)
        #---------------------------
        # Launch five simulated CTD producers
        for iteration in xrange(5):
            # Make a stream to output on
            stream_id = self.pubsub_management_service.create_stream()
            stream_policy_id = self.ingestion_management_service.create_stream_policy(
                stream_id=stream_id,
                archive_data=False,
                archive_metadata=True
            )

            pid = self.container.spawn_process(
                name='CTD_%d' % iteration,
                module='ion.processes.data.ctd_stream_publisher',
                cls='SimpleCtdPublisher',
                config={'process':{'stream_id':stream_id,'datastore_name':self.datastore_name}}
            )
            # Keep track, we'll kill 'em later.
            #---------------------------
            # Set up the datasets
            #---------------------------

            dataset_id = self.dataset_management_service.create_dataset(
                stream_id=stream_id,
                datastore_name=self.datastore_name,
                view_name='datasets/stream_join_granule'
            )
            # Keep track of the datasets
            self.datasets.append(dataset_id)
            self.process_list.append(pid)
        # Get about 4 seconds of data
        time.sleep(4)

        #---------------------------
        # Stop producing data
        #---------------------------

        for process in self.process_list:
            self.container.proc_manager.terminate_process(process)

        #----------------------------------------------
        # The replay and the transform, a love story.
        #----------------------------------------------
        # Happy Valentines to the clever coder who catches the above!

        transform_definition = ProcessDefinition()
        transform_definition.executable = {
            'module':'ion.processes.data.transforms.transform_example',
            'class':'TransformCapture'
        }
        transform_definition_id = self.process_dispatcher.create_process_definition(process_definition=transform_definition)

        dataset_id = self.datasets.pop() # Just need one for now
        replay_id, stream_id = self.data_retriever_service.define_replay(dataset_id=dataset_id)

        #--------------------------------------------
        # I'm Selling magazine subscriptions here!
        #--------------------------------------------

        subscription = self.pubsub_management_service.create_subscription(query=StreamQuery(stream_ids=[stream_id]),
            exchange_name='transform_capture_point')

        #--------------------------------------------
        # Start the transform (capture)
        #--------------------------------------------
        transform_id = self.transform_management_service.create_transform(
            name='capture_transform',
            in_subscription_id=subscription,
            process_definition_id=transform_definition_id
        )

        self.transform_management_service.activate_transform(transform_id=transform_id)

        #--------------------------------------------
        # BEGIN REPLAY!
        #--------------------------------------------

        self.data_retriever_service.start_replay(replay_id=replay_id)

        #--------------------------------------------
        # Lets get some boundaries
        #--------------------------------------------

        bounds = self.dataset_management_service.get_dataset_bounds(dataset_id=dataset_id)
        self.assertTrue('latitude_bounds' in bounds, 'dataset_id: %s' % dataset_id)
        self.assertTrue('longitude_bounds' in bounds)
        self.assertTrue('pressure_bounds' in bounds)

        #--------------------------------------------
        # Make sure the transform capture worked
        #--------------------------------------------

        stats = os.stat('/tmp/transform_output')
        self.assertTrue(stats.st_blksize > 0)

        # BEAUTIFUL!

        os.unlink('/tmp/transform_output')

