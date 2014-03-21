#!/usr/bin/env python

from ion.core.bootstrap_process import BootstrapPlugin
from pyon.util.containers import DotDict
from pyon.public import RT

from interface.objects import ProcessDefinition
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceProcessClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService


class BootstrapProcessDispatcher(BootstrapPlugin):
    """
    Bootstrap process for process dispatcher.
    """

    def on_initial_bootstrap(self, process, config, **kwargs):
        self.pds_client = ProcessDispatcherServiceProcessClient(process=process)
        self.resource_registry = ResourceRegistryServiceProcessClient(process=process)
        self.ingestion_worker(process,config)
        self.replay_defs(process,config)
        self.notification_worker(process,config)
        self.registration_worker(process,config)
        self.pydap_server(process,config)

    def pydap_server(self, process, config):
        pydap_module = config.get_safe('bootstrap.processes.pydap.module', 'ion.processes.data.externalization.lightweight_pydap')
        pydap_class  = config.get_safe('bootstrap.processes.pydap.class', 'LightweightPyDAP')

        use_pydap = config.get_safe('bootstrap.launch_pydap', False)


        process_definition = ProcessDefinition(
                name = 'pydap_server',
                description = 'Lightweight WSGI Server for PyDAP')
        process_definition.executable['module'] = pydap_module
        process_definition.executable['class'] = pydap_class

        self._create_and_launch(process_definition,use_pydap)



    def registration_worker(self, process, config):
        res, meta = self.resource_registry.find_resources(name='registration_worker', restype=RT.ProcessDefinition)
        if len(res):
            return

        registration_module = config.get_safe('bootstrap.processes.registration.module', 'ion.processes.data.registration.registration_process')
        registration_class  = config.get_safe('bootstrap.processes.registration.class', 'RegistrationProcess')
        use_pydap = True


        process_definition = ProcessDefinition(
                name='registration_worker',
                description='For registering datasets with ERDDAP')
        process_definition.executable['module'] = registration_module
        process_definition.executable['class']  = registration_class


        self._create_and_launch(process_definition, use_pydap)

    def _create_and_launch(self, process_definition, conditional=True):
        proc_def_id = self.pds_client.create_process_definition(process_definition=process_definition)

        if conditional:

            process_res_id = self.pds_client.create_process(process_definition_id=proc_def_id)
            self.pds_client.schedule_process(process_definition_id=proc_def_id, process_id=process_res_id)

    def ingestion_worker(self, process, config):
        # ingestion
        ingestion_module    = config.get_safe('bootstrap.processes.ingestion.module','ion.processes.data.ingestion.science_granule_ingestion_worker')
        ingestion_class     = config.get_safe('bootstrap.processes.ingestion.class' ,'ScienceGranuleIngestionWorker')
        ingestion_datastore = config.get_safe('bootstrap.processes.ingestion.datastore_name', 'datasets')
        ingestion_queue     = config.get_safe('bootstrap.processes.ingestion.queue' , 'science_granule_ingestion')
        ingestion_workers   = config.get_safe('bootstrap.processes.ingestion.workers', 1)
        #--------------------------------------------------------------------------------
        # Create ingestion workers
        #--------------------------------------------------------------------------------

        process_definition = ProcessDefinition(
            name='ingestion_worker_process',
            description='Worker transform process for ingestion of datasets')
        process_definition.executable['module']= ingestion_module
        process_definition.executable['class'] = ingestion_class
        ingestion_procdef_id = self.pds_client.create_process_definition(process_definition=process_definition)

        #--------------------------------------------------------------------------------
        # Simulate a HA ingestion worker by creating two of them
        #--------------------------------------------------------------------------------
#        config = DotDict()
#        config.process.datastore_name = ingestion_datastore
#        config.process.queue_name     = ingestion_queue
#
#        for i in xrange(ingestion_workers):
#            self.pds_client.schedule_process(process_definition_id=ingestion_procdef_id, configuration=config)


    def notification_worker(self, process, config):
        # user notifications
        notification_module    = config.get_safe('bootstrap.processes.user_notification.module','ion.processes.data.transforms.notification_worker')
        notification_class     = config.get_safe('bootstrap.processes.user_notification.class' ,'NotificationWorker')
        notification_workers = config.get_safe('bootstrap.processes.user_notification.workers', 1)

        #--------------------------------------------------------------------------------
        # Create notification workers
        #--------------------------------------------------------------------------------

        # set up the process definition
        process_definition_uns = ProcessDefinition(
            name='notification_worker_process',
            description='Worker transform process for user notifications')
        process_definition_uns.executable['module']= notification_module
        process_definition_uns.executable['class'] = notification_class
        uns_procdef_id = self.pds_client.create_process_definition(process_definition=process_definition_uns)

        config = DotDict()
        config.process.type = 'simple'

        for i in xrange(notification_workers):
            config.process.name = 'notification_worker_%s' % i
            config.process.queue_name = 'notification_worker_queue'
            self.pds_client.schedule_process(process_definition_id=uns_procdef_id, configuration=config)


    def replay_defs(self, process, config):
        replay_module       = config.get_safe('bootstrap.processes.replay.module', 'ion.processes.data.replay.replay_process')
        replay_class        = config.get_safe('bootstrap.processes.replay.class' , 'ReplayProcess')
        #--------------------------------------------------------------------------------
        # Create replay process definition
        #--------------------------------------------------------------------------------

        process_definition = ProcessDefinition(name=DataRetrieverService.REPLAY_PROCESS, description='Process for the replay of datasets')
        process_definition.executable['module']= replay_module
        process_definition.executable['class'] = replay_class
        self.pds_client.create_process_definition(process_definition=process_definition)

    
    def on_restart(self, process, config, **kwargs):
        pass
