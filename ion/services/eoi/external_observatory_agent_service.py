#!/usr/bin/env python

"""
@package 
@file ion/services/eoi/external_observatory_agent_service
@author Christopher Mueller
@brief 
"""

__author__ = 'Christopher Mueller'
__licence__ = 'Apache 2.0'

from mock import Mock
from pyon.agent.agent import ResourceAgentClient
from pyon.net.endpoint import ProcessRPCClient
from pyon.public import log
from pyon.core.exception import IonException
from interface.services.eoi.iexternal_observatory_agent_service import BaseExternalObservatoryAgentService
from interface.messages import external_observatory_agent_execute_in, external_observatory_agent_get_capabilities_in

class ExternalObservatoryAgentService(BaseExternalObservatoryAgentService):

    _worker_clients = {}

    def __init__(self):
        BaseExternalObservatoryAgentService.__init__(self)

        #TODO: de-mock
#        self.clients.process_dispatcher_service = DotDict()
#        self.clients.process_dispatcher_service["create_process_definition"] = Mock()
#        self.clients.process_dispatcher_service.create_process_definition.return_value = 'process_definition_id'
#        self.clients.process_dispatcher_service["schedule_process"] = Mock()
#        self.clients.process_dispatcher_service.schedule_process.return_value = True
#        self.clients.process_dispatcher_service["cancel_process"] = Mock()
#        self.clients.process_dispatcher_service.cancel_process.return_value = True
#        self.clients.process_dispatcher_service["delete_process_definition"] = Mock()
#        self.clients.process_dispatcher_service.delete_process_definition.return_value = True


    def _spawn_worker(self, resource_id=''):
        '''
        Steps:
         1. check (with RR? with CEI??) to see if there is already a worker process for the resource_id (ExternalDataset Resource)
         2. retrieve the information necessary for the ExternalDataAgent to publish data (StreamPublisher?  DataProducer??  minimum of stream_id)
         3. spawn the worker process: currently via a "hack", eventually via CEI
         4. add the pid as the worker process for the resource_id
        '''
        log.debug("EOAS: spawn_worker")
        proc_name = resource_id+'_worker'
        log.debug("Process Name: %s" % proc_name)

        # If a worker already exists for this process, return that
        if resource_id in self._worker_clients:
            (rpc_cli, proc_name, pid, queue_id) = self._worker_clients[resource_id]
            log.debug("Found worker process for resource_id=%s ==> proc_name: %s\tproc_id: %s\tqueue_id: %s" % (resource_id, proc_name, pid, queue_id))
            return rpc_cli, proc_name, pid, queue_id

#        config = {'agent':{'dataset_id': resource_id}}
        config = {}
        config['process']={'name':proc_name,'type':'agent'}
        config['process']['eoa']={'dataset_id':resource_id}

        pid = self.container.spawn_process(name=proc_name, module='eoi.agent.external_observatory_agent', cls='ExternalObservatoryAgent', config=config)
        queue_id = "%s.%s" % (self.container.id, pid)
        log.debug("Spawned worker process for resource_id=%s ==> proc_name: %s\tproc_id: %s\tqueue_id: %s" % (resource_id, proc_name, pid, queue_id))

        cli=ResourceAgentClient(resource_id, name=pid, process=self)

#        self._worker_clients[resource_id] = (ProcessRPCClient(name=queue_id, process=self), proc_name, pid, queue_id)
        self._worker_clients[resource_id] = (cli, proc_name, pid, queue_id)

        return self._worker_clients[resource_id]

    def get_worker(self, resource_id=''):
        return self._spawn_worker(resource_id=resource_id)

    def execute(self, resource_id="", command=None):
        if resource_id:
            cli, proc_name, pid, queue_id = self.get_worker(resource_id)
            if cli is not None:
                log.debug("Using ResourceAgentClient: res_id=%s" % cli.resource_id)
                return cli.execute(command=command)
            else:
                raise IonException("No worker for resource_id=%s" % resource_id)
        else:
            raise IonException("Resource ID cannot be empty or None")

    def get_capabilities(self, resource_id="", capability_types=[]):
        if resource_id:
            cli, proc_name, pid, queue_id = self.get_worker(resource_id)
            if cli is not None:
                log.debug("Using ResourceAgentClient: res_id=%s" % cli.resource_id)
                return cli.get_capabilities(capability_types=capability_types)
            else:
                raise IonException("No worker for resource_id=%s" % resource_id)
        else:
            raise IonException("Resource ID cannot be empty or None")

    def set_param(self, resource_id="", name='', value=''):
        raise IonException

    def get_param(self, resource_id="", name=''):
        raise IonException

    def execute_agent(self, resource_id="", command={}):
        raise IonException

    def set_agent_param(self, resource_id="", name='', value=''):
        raise IonException

    def get_agent_param(self, resource_id="", name=''):
        raise IonException