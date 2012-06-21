
"""
@author Luke Campbell
@file ion/services/dm/test/test_ctd_integration.py
@description Provides a full fledged integration from ingestion to replay using scidata
"""


from pyon.util.int_test import IonIntegrationTestCase
from interface.objects import CouchStorage, ProcessDefinition
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from nose.plugins.attrib import attr
from mock import patch
from pyon.public import log, CFG
from pyon.util.containers import DotDict
from pyon.core.bootstrap import get_sys_name
from pyon.datastore.datastore import DataStore
from ion.processes.data.ingestion.ingestion_worker_a import IngestionWorker



@attr('INT',group='dm')

class RawStreamIntegration(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

        config = DotDict()
        config.bootstrap.processes.ingestion.module = 'ion.processes.data.ingestion.ingestion_worker_a'

        self.container.start_rel_from_url('res/deploy/r2dm.yml', config)


        xs_dot_xp = CFG.core_xps.science_data

        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = '.'.join([get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)

        #    def _spawn_service_process(self, process_id, name, module, cls, config):
        #    def spawn_process(self, name=None, module=None, cls=None, config=None, process_id=None):

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
        datastore = cc.datastore_manager.get_datastore(datastore_name,profile=DataStore.DS_PROFILE.SCIDATA)


        ###
        ### And two process definitions...
        ###
        # one for the ctd simulator...
        producer_definition = ProcessDefinition(name='Example Data Producer')
        producer_definition.executable = {
            'module':'ion.processes.data.example_data_producer',
            'class':'ExampleDataProducer'
        }

        producer_procdef_id = process_dispatcher.create_process_definition(process_definition=producer_definition)




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
        stream_id = pubsub_management_service.create_stream(name='A data stream')


        # Set up the datasets
        dataset_id = dataset_management_service.create_dataset(
            stream_id=stream_id,
            datastore_name=datastore_name,
            view_name='Undefined!'
        )

        # Configure ingestion of this dataset
        dataset_ingest_config_id = ingestion_management_service.create_dataset_configuration(
            dataset_id = dataset_id,
            archive_data = True,
            archive_metadata = True,
            ingestion_configuration_id = ingestion_configuration_id, # you need to know the ingestion configuration id!
        )
        # Hold onto dataset_ingest_config_id if you want to stop/start ingestion of that dataset by the ingestion service



        # Start the ctd simulator to produce some data
        configuration = {
            'process':{
                'stream_id':stream_id,
                }
        }
        producer_pid = process_dispatcher.schedule_process(process_definition_id= producer_procdef_id, configuration=configuration)

        found = False
        processes = cc.proc_manager.procs.values()
        for proc in processes:
            if isinstance(proc, IngestionWorker):
                found = True
                break
        self.assertTrue(found, '%s' % cc.proc_manager.procs)


        done = False
        while not done:
            results = datastore.query_view('manifest/by_dataset')
            if len(results) >= 5:
                done = True


        #pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.stream_granule_logger',cls='StreamGranuleLogger',config={'process':{'stream_id':stream_id}})



