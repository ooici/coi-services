"""
@author Swarbhanu Chatterjee
@file ion/services/dm/test/test_replay_integration.py
@description Provides a full fledged integration from ingestion to replay using scidata
"""
from interface.objects import CouchStorage, ProcessDefinition, StreamQuery, StreamPolicy
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import StreamPublisherRegistrar
from nose.plugins.attrib import attr
from prototype.sci_data.ctd_stream import ctd_stream_packet, ctd_stream_definition
import random

@attr('INT',group='dm')
class ReplayIntegrationTest(IonIntegrationTestCase):
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
        self.datastore_name = 'test_replay_integration'

        pid = self.container.spawn_process(name='dummy_process_for_test',
            module='pyon.ion.process',
            cls='SimpleProcess',
            config={})
        dummy_process = self.container.proc_manager.procs[pid]

        # Normally the user does not see or create the publisher, this is part of the containers business.
        # For the test we need to set it up explicitly
        self.publisher_registrar = StreamPublisherRegistrar(process=dummy_process, node=self.container.node)

    def test_replay_integration(self):
        '''
        Test full DM Services Integration
        '''

        #------------------------------------------------------------------------------------------------------
        # Set up ingestion
        #------------------------------------------------------------------------------------------------------

        # Configure ingestion using eight workers, ingesting to test_dm_integration datastore with the SCIDATA profile
        ingestion_configuration_id = self.ingestion_management_service.create_ingestion_configuration(
            exchange_point_id='science_data',
            couch_storage=CouchStorage(datastore_name=self.datastore_name, datastore_profile='SCIDATA'),
            default_policy=StreamPolicy(archive_metadata=False, archive_data=False),
            number_of_workers=8
        )

        self.ingestion_management_service.activate_ingestion_configuration(
            ingestion_configuration_id=ingestion_configuration_id)

        #------------------------------------------------------------------------------------------------------
        # Set up the producers (CTD Simulators)
        #------------------------------------------------------------------------------------------------------

        ctd_stream_def = ctd_stream_definition()

        stream_id = self.pubsub_management_service.create_stream(stream_definition=ctd_stream_def)

        stream_policy_id = self.ingestion_management_service.create_stream_policy(
            stream_id=stream_id,
            archive_data=True,
            archive_metadata=False
        )

        #------------------------------------------------------------------------------------------------------
        # launch a ctd_publisher and set up AsyncResult()
        #------------------------------------------------------------------------------------------------------

        publisher = self.publisher_registrar.create_publisher(stream_id=stream_id)


        #------------------------------------------------------------------------
        # Create a packet and publish it
        #------------------------------------------------------------------------

        ctd_packet = self._create_packet(stream_id)

        publisher.publish(ctd_packet)


        #------------------------------------------------------------------------------------------------------
        # Set up the datasets
        #------------------------------------------------------------------------------------------------------

        dataset_id = self.dataset_management_service.create_dataset(
            stream_id=stream_id,
            datastore_name=self.datastore_name,
            view_name='datasets/stream_join_granule'
        )


    def _create_packet(self, stream_id):

        length = random.randint(1,20)

        c = [random.uniform(0.0,75.0)  for i in xrange(length)]

        t = [random.uniform(-1.7, 21.0) for i in xrange(length)]

        p = [random.lognormvariate(1,2) for i in xrange(length)]

        lat = [random.uniform(-90.0, 90.0) for i in xrange(length)]

        lon = [random.uniform(0.0, 360.0) for i in xrange(length)]

        tvar = [ i for i in xrange(1,length+1)]

        ctd_packet = ctd_stream_packet(stream_id=stream_id,
            c=c, t=t, p=p, lat=lat, lon=lon, time=tvar)

        return ctd_packet