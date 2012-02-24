"""
@author Swarbhanu Chatterjee
@file ion/services/dm/test/test_replay_integration.py
@description Provides a full fledged integration from ingestion to replay using scidata
"""
from interface.objects import CouchStorage, StreamQuery, HdfStorage
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

import unittest
import os


import random

import gevent

#------------------------------------------------------------------------------------------------------
# Create an async result
#------------------------------------------------------------------------------------------------------

ar = gevent.event.AsyncResult()
ar2 = gevent.event.AsyncResult()

def _create_packet( stream_id):
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



def _subscriber_call_back(message, headers):
    """
    Checks that what replay was sending out was of correct format
    """
    retrieved_hdf_string = message.identifiables['ctd_data'].values

    ar2.set(retrieved_hdf_string)



@attr('INT',group='dm')
class ReplayIntegrationTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_replay_integration(self):
        '''
        Test full DM Services Integration
        '''

        cc = self.container

        ### Every thing below here can be run as a script:


        pubsub_management_service = PubsubManagementServiceClient(node=cc.node)
        ingestion_management_service = IngestionManagementServiceClient(node=cc.node)
        dataset_management_service = DatasetManagementServiceClient(node=cc.node)
        data_retriever_service = DataRetrieverServiceClient(node=cc.node)
        resource_registry_service = ResourceRegistryServiceClient(node=cc.node)

        #------------------------------------------------------------------------------------------------------
        # Datastore name
        #------------------------------------------------------------------------------------------------------

        datastore_name = 'test_replay_integration'

        #------------------------------------------------------------------------------------------------------
        # Spawn process
        #------------------------------------------------------------------------------------------------------

        pid = cc.spawn_process(name='dummy_process_for_test',
            module='pyon.ion.process',
            cls='SimpleProcess',
            config={})

        dummy_process = cc.proc_manager.procs[pid]

        #------------------------------------------------------------------------------------------------------
        # Set up subscriber
        #------------------------------------------------------------------------------------------------------

        # Normally the user does not see or create the publisher, this is part of the containers business.
        # For the test we need to set it up explicitly
        publisher_registrar = StreamPublisherRegistrar(process=dummy_process, node=cc.node)
        subscriber_registrar = StreamSubscriberRegistrar(process=cc, node=cc.node)


        #------------------------------------------------------------------------------------------------------
        # Set up ingestion
        #------------------------------------------------------------------------------------------------------

        # Configure ingestion using eight workers, ingesting to test_dm_integration datastore with the SCIDATA profile
        ingestion_configuration_id = ingestion_management_service.create_ingestion_configuration(
            exchange_point_id='science_data',
            couch_storage=CouchStorage(datastore_name=datastore_name, datastore_profile='SCIDATA'),
            hdf_storage=HdfStorage(),
            number_of_workers=1,
        )

        ingestion_management_service.activate_ingestion_configuration(
            ingestion_configuration_id=ingestion_configuration_id)

        #------------------------------------------------------------------------------------------------------
        # Grab the transforms acting as ingestion workers
        #------------------------------------------------------------------------------------------------------

        transforms = [resource_registry_service.read(assoc.o)
                      for assoc in resource_registry_service.find_associations(ingestion_configuration_id, PRED.hasTransform)]

        proc_1 = cc.proc_manager.procs[transforms[0].process_id]
        log.info("PROCESS 1: %s" % str(proc_1))

        #------------------------------------------------------------------------------------------------------
        # Set up the test hooks for the gevent event AsyncResult object
        #------------------------------------------------------------------------------------------------------

        def ingestion_worker_received(message, headers):
            ar.set(message)

        proc_1.ingest_process_test_hook = ingestion_worker_received

        #------------------------------------------------------------------------------------------------------
        # Set up the producers (CTD Simulators)
        #------------------------------------------------------------------------------------------------------

        ctd_stream_def = ctd_stream_definition()

        stream_def_id = pubsub_management_service.create_stream_definition(container=ctd_stream_def, name='Junk definition')


        stream_id = pubsub_management_service.create_stream(stream_definition_id=stream_def_id)

        #------------------------------------------------------------------------------------------------------
        # Set up the dataset config
        #------------------------------------------------------------------------------------------------------


        dataset_id = dataset_management_service.create_dataset(
            stream_id=stream_id,
            datastore_name=datastore_name,
            view_name='datasets/stream_join_granule'
        )

        dataset_config_id = ingestion_management_service.create_dataset_configuration(
            dataset_id = dataset_id,
            archive_data = True,
            archive_metadata = True,
            ingestion_configuration_id = ingestion_configuration_id
        )

        #------------------------------------------------------------------------------------------------------
        # Launch a ctd_publisher
        #------------------------------------------------------------------------------------------------------

        publisher = publisher_registrar.create_publisher(stream_id=stream_id)

        #------------------------------------------------------------------------
        # Create a packet and publish it
        #------------------------------------------------------------------------

        ctd_packet = _create_packet(stream_id)
        published_hdfstring = ctd_packet.identifiables['ctd_data'].values

        publisher.publish(ctd_packet)

        #------------------------------------------------------------------------------------------------------
        # Catch what the ingestion worker gets! Assert it is the same packet that was published!
        #------------------------------------------------------------------------------------------------------

        packet = ar.get(timeout=2)

        #------------------------------------------------------------------------------------------------------
        # Create subscriber to listen to the replays
        #------------------------------------------------------------------------------------------------------

        replay_id, replay_stream_id = data_retriever_service.define_replay(dataset_id)

        query = StreamQuery(stream_ids=[replay_stream_id])

        subscription_id = pubsub_management_service.create_subscription(query = query, exchange_name='replay_capture_point' ,name = 'replay_capture_point')

        # It is not required or even generally a good idea to use the subscription resource name as the queue name, but it makes things simple here
        # Normally the container creates and starts subscribers for you when a transform process is spawned
        subscriber = subscriber_registrar.create_subscriber(exchange_name='replay_capture_point', callback=_subscriber_call_back)
        subscriber.start()

        pubsub_management_service.activate_subscription(subscription_id)

        #------------------------------------------------------------------------------------------------------
        # Start the replay
        #------------------------------------------------------------------------------------------------------

        data_retriever_service.start_replay(replay_id)

        #------------------------------------------------------------------------------------------------------
        # Get the hdf string from the captured stream in the replay
        #------------------------------------------------------------------------------------------------------

        retrieved_hdf_string  = ar2.get(timeout=2)


        ### Non scriptable portion of the test

        #------------------------------------------------------------------------------------------------------
        # Assert that it matches the message we sent
        #------------------------------------------------------------------------------------------------------

        self.assertEquals(packet.identifiables['stream_encoding'].sha1, ctd_packet.identifiables['stream_encoding'].sha1)


        self.assertEquals(retrieved_hdf_string, published_hdfstring)




