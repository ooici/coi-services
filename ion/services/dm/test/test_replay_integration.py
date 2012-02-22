"""
@author Swarbhanu Chatterjee
@file ion/services/dm/test/test_replay_integration.py
@description Provides a full fledged integration from ingestion to replay using scidata
"""
from interface.objects import CouchStorage, StreamQuery, StreamPolicy, HdfStorage
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import StreamPublisherRegistrar, StreamSubscriberRegistrar
from nose.plugins.attrib import attr
from prototype.sci_data.ctd_stream import ctd_stream_packet, ctd_stream_definition
from pyon.public import RT, PRED, log, IonObject


import random

import gevent

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

        # datastore name
        self.datastore_name = 'test_replay_integration'

        pid = self.container.spawn_process(name='dummy_process_for_test',
            module='pyon.ion.process',
            cls='SimpleProcess',
            config={})

        dummy_process = self.container.proc_manager.procs[pid]

        # Normally the user does not see or create the publisher, this is part of the containers business.
        # For the test we need to set it up explicitly
        self.publisher_registrar = StreamPublisherRegistrar(process=dummy_process, node=self.container.node)
        self.subscriber_registrar = StreamSubscriberRegistrar(process=self.container, node=self.container.node)

        # Create an async result
        self.ar = gevent.event.AsyncResult()
        self.ar2 = gevent.event.AsyncResult()


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
            hdf_storage=HdfStorage(),
            number_of_workers=1,
            default_policy=StreamPolicy(archive_metadata=False, archive_data=False)
        )

        self.ingestion_management_service.activate_ingestion_configuration(
            ingestion_configuration_id=ingestion_configuration_id)

        transforms = [self.resource_registry_service.read(assoc.o)
                      for assoc in self.resource_registry_service.find_associations(ingestion_configuration_id, PRED.hasTransform)]

        proc_1 = self.container.proc_manager.procs[transforms[0].process_id]
        log.info("PROCESS 1: %s" % str(proc_1))

        def ingestion_worker_received(message, headers):
            self.ar.set(message)

        proc_1.ingest_process_test_hook = ingestion_worker_received

        #------------------------------------------------------------------------------------------------------
        # Set up the producers (CTD Simulators)
        #------------------------------------------------------------------------------------------------------

        ctd_stream_def = ctd_stream_definition()

        stream_id = self.pubsub_management_service.create_stream(stream_definition=ctd_stream_def)

        stream_policy_id = self.ingestion_management_service.create_stream_policy(
            stream_id=stream_id,
            archive_data=True,
            archive_metadata=True
        )

        #------------------------------------------------------------------------------------------------------
        # Launch a ctd_publisher
        #------------------------------------------------------------------------------------------------------

        publisher = self.publisher_registrar.create_publisher(stream_id=stream_id)

        #------------------------------------------------------------------------
        # Create a packet and publish it
        #------------------------------------------------------------------------

        ctd_packet = self._create_packet(stream_id)
        published_hdfstring = ctd_packet.identifiables['ctd_data'].values

        publisher.publish(ctd_packet)

        #------------------------------------------------------------------------------------------------------
        # Catch what the ingestion worker gets! Assert it is the same packet that was published!
        #------------------------------------------------------------------------------------------------------

        packet = self.ar.get(timeout=2)

        self.assertEquals(packet.identifiables['stream_encoding'].sha1, ctd_packet.identifiables['stream_encoding'].sha1)

        #------------------------------------------------------------------------------------------------------
        # Create subscriber to listen to the replays
        #------------------------------------------------------------------------------------------------------

        self._set_up_replay(stream_id)

        retrieved_hdf_string  = self.ar2.get(timeout=2)

        self.assertEquals(retrieved_hdf_string, published_hdfstring)


    def _create_packet(self, stream_id):
        """
        Create a ctd_packet for scientific data
        """

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

    def _set_up_replay(self, stream_id):
        """
        Set up replay and start it
        """
        dataset_id = self.dataset_management_service.create_dataset(
            stream_id=stream_id,
            datastore_name=self.datastore_name,
            view_name='datasets/stream_join_granule'
        )

        replay_id, replay_stream_id = self.data_retriever_service.define_replay(dataset_id)

        query = StreamQuery(stream_ids=[replay_stream_id])

        self._create_subscriber(subscription_name='replay_capture_point', subscription_query=query)

        self.data_retriever_service.start_replay(replay_id)


    def _subscriber_call_back(self, message, headers):
        """
        Checks that what replay was sending out was of correct format
        """
        retrieved_hdf_string = message.identifiables['ctd_data'].values

        self.ar2.set(retrieved_hdf_string)


    def _create_subscriber(self, subscription_name, subscription_query):
        """
        Create subscriber to listen to the replay
        """
        subscription_id = self.pubsub_management_service.create_subscription(query = subscription_query, exchange_name=subscription_name ,name = subscription_name)

        # It is not required or even generally a good idea to use the subscription resource name as the queue name, but it makes things simple here
        # Normally the container creates and starts subscribers for you when a transform process is spawned
        subscriber = self.subscriber_registrar.create_subscriber(exchange_name=subscription_name, callback=self._subscriber_call_back)
        subscriber.start()

        self.pubsub_management_service.activate_subscription(subscription_id)

        return subscription_id