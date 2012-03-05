
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
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
from pyon.public import log
import os
import time
from prototype.sci_data.stream_parser import PointSupplementStreamParser

import gevent


from ion.processes.data.transforms.ion_seawater import SalinityTransform

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
        # Copy below here to run as a script (don't forget the imports of course!)
        #-----------------------------


        # Create some service clients...
        pubsub_management_service = PubsubManagementServiceClient(node=cc.node)
        ingestion_management_service = IngestionManagementServiceClient(node=cc.node)
        dataset_management_service = DatasetManagementServiceClient(node=cc.node)
        data_retriever_service = DataRetrieverServiceClient(node=cc.node)
        transform_management_service = TransformManagementServiceClient(node=cc.node)
        process_dispatcher = ProcessDispatcherServiceClient(node=cc.node)

        # declare some handy variables

        datastore_name = 'test_dm_integration'



        ###
        ### In the beginning there were two stream definitions...
        ###
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = pubsub_management_service.create_stream_definition(container=ctd_stream_def, name='Simulated CTD data')

        # create a stream definition for the data from the salinity Transform
        sal_stream_def_id = pubsub_management_service.create_stream_definition(container=SalinityTransform.outgoing_stream_def, name='Scalar Salinity data stream')



        ###
        ### And two process definitions...
        ###
        # one for the ctd simulator...
        producer_definition = ProcessDefinition()
        producer_definition.executable = {
            'module':'ion.processes.data.ctd_stream_publisher',
            'class':'SimpleCtdPublisher'
        }

        ctd_sim_procdef_id = process_dispatcher.create_process_definition(process_definition=producer_definition)

        # one for the salinity transform
        producer_definition = ProcessDefinition()
        producer_definition.executable = {
            'module':'ion.processes.data.transforms.ion_seawater',
            'class':'SalinityTransform'
        }

        salinity_transform_procdef_id = process_dispatcher.create_process_definition(process_definition=producer_definition)



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
        ctd_stream_id = pubsub_management_service.create_stream(stream_definition_id=ctd_stream_def_id)


        # Set up the datasets
        ctd_dataset_id = dataset_management_service.create_dataset(
            stream_id=ctd_stream_id,
            datastore_name=datastore_name,
            view_name='datasets/stream_join_granule'
        )

        # Configure ingestion of this dataset
        ctd_dataset_config_id = ingestion_management_service.create_dataset_configuration(
            dataset_id = ctd_dataset_id,
            archive_data = True,
            archive_metadata = True,
            ingestion_configuration_id = ingestion_configuration_id, # you need to know the ingestion configuration id!
        )
        # Hold onto ctd_dataset_config_id if you want to stop/start ingestion of that dataset by the ingestion service

        #---------------------------
        # Set up the salinity transform
        #---------------------------


        # Create the stream
        sal_stream_id = pubsub_management_service.create_stream(stream_definition_id=sal_stream_def_id)


        # Set up the datasets
        sal_dataset_id = dataset_management_service.create_dataset(
            stream_id=sal_stream_id,
            datastore_name=datastore_name,
            view_name='datasets/stream_join_granule'
        )

        # Configure ingestion of the salinity as a dataset
        sal_dataset_config_id = ingestion_management_service.create_dataset_configuration(
            dataset_id = sal_dataset_id,
            archive_data = True,
            archive_metadata = True,
            ingestion_configuration_id = ingestion_configuration_id, # you need to know the ingestion configuration id!
        )
        # Hold onto sal_dataset_config_id if you want to stop/start ingestion of that dataset by the ingestion service



        # Create a subscription as input to the transform
        sal_transform_input_subscription_id = pubsub_management_service.create_subscription(
            query = StreamQuery(stream_ids=[ctd_stream_id,]),
            exchange_name='salinity_transform_input') # how do we make these names??? i.e. Should they be anonymous?

        # create the salinity transform
        sal_transform_id = transform_management_service.create_transform(
            name='example salinity transform',
            in_subscription_id=sal_transform_input_subscription_id,
            out_streams={'output':sal_stream_id,},
            process_definition_id = salinity_transform_procdef_id,
            # no configuration needed at this time...
            )
        # start the transform - for a test case it makes sense to do it before starting the producer but it is not required
        transform_management_service.activate_transform(transform_id=sal_transform_id)



        # Start the ctd simulator to produce some data
        configuration = {
            'process':{
                'stream_id':ctd_stream_id,
            }
        }
        ctd_sim_pid = process_dispatcher.schedule_process(process_definition_id=ctd_sim_procdef_id, configuration=configuration)


        ###
        ### Make a subscriber in the test to listen for salinity data
        ###
        salinity_subscription_id = pubsub_management_service.create_subscription(
            query=StreamQuery([sal_stream_id,]),
            exchange_name = 'salinity_test',
            name = "test salinity subscription",
            )

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
            log.warn('Salinity data received!')
            results.append(message)
            if len(results) >3:
                result.set(True)

        subscriber = subscriber_registrar.create_subscriber(exchange_name='salinity_test', callback=message_received)
        subscriber.start()

        # after the queue has been created it is safe to activate the subscription
        pubsub_management_service.activate_subscription(subscription_id=salinity_subscription_id)


        # Assert that we have received data
        assertions(result.get(timeout=10))

        # stop the flow parse the messages...
        process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data



        for message in results:

            psd = PointSupplementStreamParser(stream_definition=SalinityTransform.outgoing_stream_def, stream_granule=message)

            # Test the handy info method for the names of fields in the stream def
            assertions('salinity' in psd.list_field_names())


            # you have to know the name of the coverage in stream def
            salinity = psd.get_values('salinity')

            import numpy

            assertions(isinstance(salinity, numpy.ndarray))

            assertions(numpy.nanmin(salinity) > 0.0) # salinity should always be greater than 0




