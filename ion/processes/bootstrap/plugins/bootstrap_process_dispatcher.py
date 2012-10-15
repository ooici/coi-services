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

        # ingestion
        ingestion_module    = config.get_safe('bootstrap.processes.ingestion.module','ion.processes.data.ingestion.science_granule_ingestion_worker')
        ingestion_class     = config.get_safe('bootstrap.processes.ingestion.class' ,'ScienceGranuleIngestionWorker')
        ingestion_datastore = config.get_safe('bootstrap.processes.ingestion.datastore_name', 'datasets')
        ingestion_queue     = config.get_safe('bootstrap.processes.ingestion.queue' , 'science_granule_ingestion')
        ingestion_workers   = config.get_safe('bootstrap.processes.ingestion.workers', 1)
        

        # user notifications
        notification_module    = config.get_safe('bootstrap.processes.user_notification.module','ion.processes.data.transforms.notification_worker')
        notification_class     = config.get_safe('bootstrap.processes.user_notification.class' ,'NotificationWorker')
        notification_workers = config.get_safe('bootstrap.processes.user_notification.workers', 1)

        replay_module       = config.get_safe('bootstrap.processes.replay.module', 'ion.processes.data.replay.replay_process')
        replay_class        = config.get_safe('bootstrap.processes.replay.class' , 'ReplayProcess')

        #--------------------------------------------------------------------------------
        # Create ingestion workers
        #--------------------------------------------------------------------------------

        process_definition = ProcessDefinition(
            name='ingestion_worker_process',
            description='Worker transform process for ingestion of datasets')
        process_definition.executable['module']= ingestion_module
        process_definition.executable['class'] = ingestion_class
        ingestion_procdef_id = pds_client.create_process_definition(process_definition=process_definition)

        #--------------------------------------------------------------------------------
        # Simulate a HA ingestion worker by creating two of them
        #--------------------------------------------------------------------------------
        config = DotDict()
        config.process.datastore_name = ingestion_datastore
        config.process.queue_name     = ingestion_queue

        for i in xrange(ingestion_workers):
            pds_client.schedule_process(process_definition_id=ingestion_procdef_id, configuration=config)

        #--------------------------------------------------------------------------------
        # Create notification workers
        #--------------------------------------------------------------------------------

        # set up the process definition
        process_definition_uns = ProcessDefinition(
            name='notification_worker_process',
            description='Worker transform process for user notifications')
        process_definition_uns.executable['module']= notification_module
        process_definition_uns.executable['class'] = notification_class
        uns_procdef_id = pds_client.create_process_definition(process_definition=process_definition_uns)

        config = DotDict()
        config.process.type = 'simple'

        for i in xrange(notification_workers):
            config.process.name = 'notification_worker_%s' % i
            pds_client.schedule_process(process_definition_id=uns_procdef_id, configuration=config)

        #--------------------------------------------------------------------------------
        # Create replay process definition
        #--------------------------------------------------------------------------------

        process_definition = ProcessDefinition(name=DataRetrieverService.REPLAY_PROCESS, description='Process for the replay of datasets')
        process_definition.executable['module']= replay_module
        process_definition.executable['class'] = replay_class
        pds_client.create_process_definition(process_definition=process_definition)

    
    def on_restart(self, process, config, **kwargs):
        pass
