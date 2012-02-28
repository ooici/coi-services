
"""
@author Luke Campbell
@file ion/services/dm/test/test_ctd_integration.py
@description Provides a full fledged integration from ingestion to replay using scidata
"""

from pyon.util.file_sys import FS, FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from interface.objects import CouchStorage, ProcessDefinition, StreamQuery
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from nose.plugins.attrib import attr
from prototype.sci_data.ctd_stream import ctd_stream_definition
from pyon.public import log
import os
import time

@attr('INT',group='dm')
class CTDIntegrationTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')


    def test_dm_integration(self):
        '''
        test_dm_integration
        Test full DM Services Integration
        '''
        cc = self.container
        assertions = self.assertTrue

        #-----------------------------
        # Copy below here
        #-----------------------------
        pubsub_management_service = PubsubManagementServiceClient(node=cc.node)
        ingestion_management_service = IngestionManagementServiceClient(node=cc.node)
        dataset_management_service = DatasetManagementServiceClient(node=cc.node)
        data_retriever_service = DataRetrieverServiceClient(node=cc.node)
        transform_management_service = TransformManagementServiceClient(node=cc.node)
        process_dispatcher = ProcessDispatcherServiceClient(node=cc.node)

        process_list = []
        datasets = []

        datastore_name = 'test_dm_integration'


        #---------------------------
        # Set up ingestion
        #---------------------------
        # Configure ingestion using eight workers, ingesting to test_dm_integration datastore with the SCIDATA profile
        log.debug('Calling create_ingestion_configuration')
        ingestion_configuration_id = ingestion_management_service.create_ingestion_configuration(
            exchange_point_id='science_data',
            couch_storage=CouchStorage(datastore_name=datastore_name,datastore_profile='SCIDATA'),
            number_of_workers=8
        )
        #
        ingestion_management_service.activate_ingestion_configuration(
            ingestion_configuration_id=ingestion_configuration_id)

        ctd_stream_def = ctd_stream_definition()

        stream_def_id = pubsub_management_service.create_stream_definition(container=ctd_stream_def, name='Junk definition')


        #---------------------------
        # Set up the producers (CTD Simulators)
        #---------------------------
        # Launch five simulated CTD producers
        for iteration in xrange(5):
            # Make a stream to output on

            stream_id = pubsub_management_service.create_stream(stream_definition_id=stream_def_id)

            #---------------------------
            # Set up the datasets
            #---------------------------
            dataset_id = dataset_management_service.create_dataset(
                stream_id=stream_id,
                datastore_name=datastore_name,
                view_name='datasets/stream_join_granule'
            )
            # Keep track of the datasets
            datasets.append(dataset_id)

            stream_policy_id = ingestion_management_service.create_dataset_configuration(
                dataset_id = dataset_id,
                archive_data = True,
                archive_metadata = True,
                ingestion_configuration_id = ingestion_configuration_id
            )


            producer_definition = ProcessDefinition()
            producer_definition.executable = {
                'module':'ion.processes.data.ctd_stream_publisher',
                'class':'SimpleCtdPublisher'
            }
            configuration = {
                'process':{
                    'stream_id':stream_id,
                }
            }
            procdef_id = process_dispatcher.create_process_definition(process_definition=producer_definition)
            log.debug('LUKE_DEBUG: procdef_id: %s', procdef_id)
            pid = process_dispatcher.schedule_process(process_definition_id=procdef_id, configuration=configuration)


            # Keep track, we'll kill 'em later.
            process_list.append(pid)
        # Get about 4 seconds of data
        time.sleep(4)

        #---------------------------
        # Stop producing data
        #---------------------------

        for process in process_list:
            process_dispatcher.cancel_process(process)

        #----------------------------------------------
        # The replay and the transform, a love story.
        #----------------------------------------------
        # Happy Valentines to the clever coder who catches the above!

        transform_definition = ProcessDefinition()
        transform_definition.executable = {
            'module':'ion.processes.data.transforms.transform_example',
            'class':'TransformCapture'
        }
        transform_definition_id = process_dispatcher.create_process_definition(process_definition=transform_definition)

        dataset_id = datasets.pop() # Just need one for now
        replay_id, stream_id = data_retriever_service.define_replay(dataset_id=dataset_id)

        #--------------------------------------------
        # I'm Selling magazine subscriptions here!
        #--------------------------------------------

        subscription = pubsub_management_service.create_subscription(query=StreamQuery(stream_ids=[stream_id]),
            exchange_name='transform_capture_point')

        #--------------------------------------------
        # Start the transform (capture)
        #--------------------------------------------
        transform_id = transform_management_service.create_transform(
            name='capture_transform',
            in_subscription_id=subscription,
            process_definition_id=transform_definition_id
        )

        transform_management_service.activate_transform(transform_id=transform_id)

        #--------------------------------------------
        # BEGIN REPLAY!
        #--------------------------------------------

        data_retriever_service.start_replay(replay_id=replay_id)

        #--------------------------------------------
        # Lets get some boundaries
        #--------------------------------------------

        bounds = dataset_management_service.get_dataset_bounds(dataset_id=dataset_id)
        assertions('latitude_bounds' in bounds, 'dataset_id: %s' % dataset_id)
        assertions('longitude_bounds' in bounds)
        assertions('pressure_bounds' in bounds)

        #--------------------------------------------
        # Make sure the transform capture worked
        #--------------------------------------------

        time.sleep(3) # Give the other processes up to 3 seconds to catch up


        stats = os.stat(FileSystem.get_url(FS.TEMP,'transform_output'))
        assertions(stats.st_blksize > 0)

        # BEAUTIFUL!

        FileSystem.unlink(FileSystem.get_url(FS.TEMP,'transform_output'))

