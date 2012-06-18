#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file collaboration_test
@date 06/18/12 09:46
@description DESCRIPTION
'''
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.bootstrap import get_sys_name
from pyon.net.endpoint import Subscriber
from nose.plugins.attrib import attr
from gevent.event import AsyncResult
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, CouchStorage

import gevent
@attr('INT',group='dm')
class DMCollaborationIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        config = DotDict()
        config.bootstrap.processes.ingestion.module = 'ion.processes.data.ingestion.ingestion_worker_a'
        config.bootstrap.processes.replay.module    = 'ion.processes.data.replay.replay_process_a'
        self.container.start_rel_from_url('res/deploy/r2dm.yml', config)


        self.datastore_name = 'test_datasets'
        self.pubsub_management    = PubsubManagementServiceClient()
        self.ingestion_management = IngestionManagementServiceClient()
        self.dataset_management   = DatasetManagementServiceClient()
        self.process_dispatcher   = ProcessDispatcherServiceClient()
        self.data_retriever       = DataRetrieverServiceClient()

    def subscriber_action(self, msg, header):
        if not hasattr(self,'received'):
            self.received = 0
        if not hasattr(self, 'async_done'):
            self.async_done = AsyncResult()
        self.received += 1
        if self.received >= 2:
            self.async_done.set(True)


    def test_ingest_to_replay(self):

        self.async_done = AsyncResult()
        sysname = get_sys_name()


        datastore = self.container.datastore_manager.get_datastore(self.datastore_name,'SCIDATA')


        producer_definition = ProcessDefinition(name='Example Data Producer')
        producer_definition.executable = {
            'module':'ion.processes.data.example_data_producer',
            'class' :'ExampleDataProducer'
        }

        process_definition_id = self.process_dispatcher.create_process_definition(process_definition=producer_definition)
        
        ingestion_configuration_id = self.ingestion_management.create_ingestion_configuration(
            exchange_point_id = 'science_data',
            couch_storage=CouchStorage(datastore_name=self.datastore_name,datastore_profile='SCIDATA'),
            number_of_workers=1
        )

        self.ingestion_management.activate_ingestion_configuration(
                ingestion_configuration_id=ingestion_configuration_id)

        stream_id = self.pubsub_management.create_stream(name='data stream')
        
        dataset_id = self.dataset_management.create_dataset(
            stream_id = stream_id, 
            datastore_name = self.datastore_name,
        )

        self.ingestion_management.create_dataset_configuration(
            dataset_id = dataset_id,
            archive_data = True,
            archive_metadata = True,
            ingestion_configuration_id = ingestion_configuration_id
        )

        configuration = {
            'process': {
                'stream_id' : stream_id
            }
        }

        self.process_dispatcher.schedule_process(process_definition_id, configuration=configuration)

        replay_id, stream_id = self.data_retriever.define_replay(dataset_id = dataset_id)

        subscriber = Subscriber(name=('%s.science_data' % sysname, 'test_queue'), callback=self.subscriber_action, binding='%s.data' % stream_id)
        gevent.spawn(subscriber.listen)

        done = False
        while not done:
            results = datastore.query_view('manifest/by_dataset')
            if len(results) >= 2:
                done = True

        self.data_retriever.start_replay(replay_id)

        self.async_done.get(timeout=10)

