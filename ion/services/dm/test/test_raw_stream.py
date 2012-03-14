
"""
@author Luke Campbell
@file ion/services/dm/test/test_ctd_integration.py
@description Provides a full fledged integration from ingestion to replay using scidata
"""

from pyon.util.file_sys import FS, FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import StreamSubscriberRegistrar
from interface.objects import CouchStorage, ProcessDefinition, StreamQuery
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from nose.plugins.attrib import attr
from prototype.sci_data.stream_defs import SBE37_RAW_stream_definition
from pyon.public import log
import os
import time
from prototype.sci_data.stream_parser import PointSupplementStreamParser



import gevent


from ion.processes.data.transforms.ctd.ctd_L2_salinity import SalinityTransform

@attr('INT',group='dm')
class RawStreamIntegration(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')


    def test_raw_stream_integration(self):
        cc = self.container
        assertions = self.assertTrue


        #-----------------------------
        # Copy below here to run as a script (don't forget the imports of course!)
        #-----------------------------


        # Create some service clients...
        pubsub_management_service = PubsubManagementServiceClient(node=cc.node)
        ingestion_management_service = IngestionManagementServiceClient(node=cc.node)
        dataset_management_service = DatasetManagementServiceClient(node=cc.node)
        process_dispatcher = ProcessDispatcherServiceClient(node=cc.node)

        # declare some handy variables

        datastore_name = 'test_dm_integration'



        ###
        ### In the beginning there was one stream definitions...
        ###
        # create a stream definition for the data from the ctd simulator
        raw_ctd_stream_def = SBE37_RAW_stream_definition()
        raw_ctd_stream_def_id = pubsub_management_service.create_stream_definition(container=raw_ctd_stream_def, name='Simulated RAW CTD data')



        ###
        ### And two process definitions...
        ###
        # one for the ctd simulator...
        producer_definition = ProcessDefinition()
        producer_definition.executable = {
            'module':'ion.processes.data.raw_stream_publisher',
            'class':'RawStreamPublisher'
        }

        raw_ctd_sim_procdef_id = process_dispatcher.create_process_definition(process_definition=producer_definition)




        #---------------------------
        # Set up ingestion - this is an operator concern - not done by SA in a deployed system
        #---------------------------
        # Configure ingestion using eight workers, ingesting to test_dm_integration datastore with the SCIDATA profile
        log.debug('Calling create_ingestion_configuration')
        ingestion_configuration_id = ingestion_management_service.create_ingestion_configuration(
            exchange_point_id='science_data',
            couch_storage=CouchStorage(datastore_name=datastore_name,datastore_profile='SCIDATA'),
            number_of_workers=1
        )
        #
        ingestion_management_service.activate_ingestion_configuration(
            ingestion_configuration_id=ingestion_configuration_id)



        #---------------------------
        # Set up the producer (CTD Simulator)
        #---------------------------

        # Create the stream
        raw_ctd_stream_id = pubsub_management_service.create_stream(stream_definition_id=raw_ctd_stream_def_id)


        # Set up the datasets
        raw_ctd_dataset_id = dataset_management_service.create_dataset(
            stream_id=raw_ctd_stream_id,
            datastore_name=datastore_name,
            view_name='datasets/stream_join_granule'
        )

        # Configure ingestion of this dataset
        raw_ctd_dataset_config_id = ingestion_management_service.create_dataset_configuration(
            dataset_id = raw_ctd_dataset_id,
            archive_data = True,
            archive_metadata = True,
            ingestion_configuration_id = ingestion_configuration_id, # you need to know the ingestion configuration id!
        )
        # Hold onto ctd_dataset_config_id if you want to stop/start ingestion of that dataset by the ingestion service



        # Start the ctd simulator to produce some data
        configuration = {
            'process':{
                'stream_id':raw_ctd_stream_id,
                }
        }
        raw_sim_pid = process_dispatcher.schedule_process(process_definition_id=raw_ctd_sim_procdef_id, configuration=configuration)


        ###
        ### Make a subscriber in the test to listen for salinity data
        ###
        raw_subscription_id = pubsub_management_service.create_subscription(
            query=StreamQuery([raw_ctd_stream_id,]),
            exchange_name = 'raw_test',
            name = "test raw subscription",
        )

        # this is okay - even in cei mode!
        pid = cc.spawn_process(name='dummy_process_for_test',
            module='pyon.ion.process',
            cls='SimpleProcess',
            config={})
        dummy_process = cc.proc_manager.procs[pid]

        subscriber_registrar = StreamSubscriberRegistrar(process=dummy_process, node=cc.node)

        result = gevent.event.AsyncResult()
        results = []
        def message_received(message, headers):
            # Heads
            log.warn('Raw data received!')
            results.append(message)
            if len(results) >3:
                result.set(True)

        subscriber = subscriber_registrar.create_subscriber(exchange_name='raw_test', callback=message_received)
        subscriber.start()

        # after the queue has been created it is safe to activate the subscription
        pubsub_management_service.activate_subscription(subscription_id=raw_subscription_id)


        # Assert that we have received data
        assertions(result.get(timeout=10))

        # stop the flow parse the messages...
        process_dispatcher.cancel_process(raw_sim_pid) # kill the ctd simulator process - that is enough data

        gevent.sleep(1)

        for message in results:

            sha1 = message.identifiables['stream_encoding'].sha1

            data = message.identifiables['data_stream'].values

            filename = FileSystem.get_url(FS.CACHE, sha1, ".raw")

            with open(filename, 'r') as f:

                assertions(data == f.read())




