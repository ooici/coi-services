#!/usr/bin/env python

from ion.core.bootstrap_process import BootstrapPlugin
from pyon.util.containers import DotDict

from interface.objects import ProcessDefinition
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceProcessClient
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService


class BootstrapProcessDispatcher(BootstrapPlugin):
    """
    Bootstrap process for process dispatcher.
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        pds_client = ProcessDispatcherServiceProcessClient(process=process)

        ingestion_module    = config.get_safe('bootstrap.processes.ingestion.module','ion.processes.data.ingestion.science_granule_ingestion_worker')
        ingestion_class     = config.get_safe('bootstrap.processes.ingestion.class' ,'ScienceGranuleIngestionWorker')
        ingestion_datastore = config.get_safe('bootstrap.processes.ingestion.datastore_name', 'datasets')
        ingestion_queue     = config.get_safe('bootstrap.processes.ingestion.queue' , 'science_granule_ingestion')
        ingestion_workers   = config.get_safe('bootstrap.processes.ingestion.workers', 2)
        
        bin_module    = config.get_safe('bootstrap.processes.bin.module','ion.processes.data.ingestion.blob_ingestion_worker')
        bin_class     = config.get_safe('bootstrap.processes.bin.class' ,'BlobIngestionWorker')
        bin_datastore = config.get_safe('bootstrap.processes.bin.datastore_name', 'filesystem')
        bin_queue     = config.get_safe('bootstrap.processes.bin.queue' , 'binary_ingestion')
        bin_workers   = config.get_safe('bootstrap.processes.bin.workers', 2)

        replay_module       = config.get_safe('bootstrap.processes.replay.module', 'ion.processes.data.replay.replay_process')
        replay_class        = config.get_safe('bootstrap.processes.replay.class' , 'ReplayProcess')

        binary_replay_module = config.get_safe('bootstrap.processes.bin_replay.module', 'ion.processes.data.replay.binary_replay')
        binary_replay_class  = config.get_safe('bootstrap.processes.bin_replay.class', 'BinaryReplayProcess')

        process_definition = ProcessDefinition(
            name='ingestion_worker_process',
            description='Worker transform process for ingestion of datasets')
        process_definition.executable['module']= ingestion_module
        process_definition.executable['class'] = ingestion_class
        ingestion_procdef_id = pds_client.create_process_definition(process_definition=process_definition)

        process_definition = ProcessDefinition(
            name='binary_ingestion_worker_process',
            description='Binary Ingestion Worker Process')
        process_definition.executable['module']= bin_module
        process_definition.executable['class'] = bin_class
        bin_procdef_id = pds_client.create_process_definition(process_definition=process_definition)
        #--------------------------------------------------------------------------------
        # Simulate a HA ingestion worker by creating two of them
        #--------------------------------------------------------------------------------
        config = DotDict()
        config.process.datastore_name = ingestion_datastore
        config.process.queue_name     = ingestion_queue

        for i in xrange(ingestion_workers):
            pds_client.schedule_process(process_definition_id=ingestion_procdef_id, configuration=config)


        config = DotDict()
        config.process.datastore_name = bin_datastore
        config.process.queue_name     = bin_queue

        for i in xrange(bin_workers):
            pds_client.schedule_process(process_definition_id=bin_procdef_id, configuration=config)

        process_definition = ProcessDefinition(name=DataRetrieverService.REPLAY_TYPES[DataRetrieverService.SCIENCE_REPLAY], description='Process for the replay of datasets')
        process_definition.executable['module']= replay_module
        process_definition.executable['class'] = replay_class
        pds_client.create_process_definition(process_definition=process_definition)

        process_definition = ProcessDefinition(name=DataRetrieverService.REPLAY_TYPES[DataRetrieverService.BINARY_REPLAY], description='Process for the replay of binary file data.')
        process_definition.executable['module']= binary_replay_module
        process_definition.executable['class'] = binary_replay_class
        pds_client.create_process_definition(process_definition=process_definition)
    
    def on_restart(self, process, config, **kwargs):
        pass
