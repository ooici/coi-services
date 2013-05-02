#!/usr/bin/env python

"""
Ask container to start an external dataset agent and put it into streaming mode
so it will read and relay data from the file into a data product.

Must have resources already defined in the system (ie- preload):
    ExternalDataset (implies also ExternalDatasetModel, ParameterDictionary, ParameterDefinitions)
    ExternalDatasetAgentInstance (implies ExternalDatasetAgent)
    DataProduct associated with ExternalDataset (ie- DataProductLink, implies StreamDefinition)

invoke with command like this:
    bin/pycc -x ion.processes.data.import_dataset.ImportDataset dataset='External CTD Dataset'
"""

__author__ = 'Michael Meisinger, Ian Katz, Thomas Lennan'


from pyon.core.bootstrap import get_service_registry
from pyon.public import ImmediateProcess
from interface.objects import AgentCommand
from pyon.public import RT, log, PRED
from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
import numpy as np
from nose.plugins.attrib import attr
from gevent.event import Event
from ion.services.sa.acquisition.test.test_bulk_data_ingestion import FakeProcess
from ion.core.includes.mi import DriverEvent

class ImportDataset(ImmediateProcess):
    def on_start(self):
        name = self.CFG.get("dataset")

        rr = self.container.resource_registry

        objects,_ = rr.find_resources(RT.ExternalDataset)
        filtered_objs = [obj for obj in objects if obj.name == name]
        external_dataset = filtered_objs[0]

        instance_id = rr.read_object(subject=external_dataset._id, predicate=PRED.hasAgentInstance, object_type=RT.ExternalDatasetAgentInstance, id_only=True)
        dams = get_service_registry().services['data_acquisition_management'].client(process=self)
        dams.start_external_dataset_agent_instance(instance_id)

        # OLD METHOD of starting agent, now this code has been implemented and tested in DAMS
        #obj,_ = rr.find_objects(subject=external_dataset._id, predicate=PRED.hasAgentInstance, object_type=RT.ExternalDatasetAgentInstance)
        #agent_instance = obj[0]
        #obj,_ = rr.find_objects(object_type=RT.ExternalDatasetAgent, predicate=PRED.hasAgentDefinition, subject=agent_instance._id)
        #agent = obj[0]
        #stream_definition_id = agent_instance.dataset_driver_config['dh_cfg']['stream_def']
        #stream_definition = rr.read(stream_definition_id)
        #data_producer_id = agent_instance.dataset_driver_config['dh_cfg']['data_producer_id']
        #data_producer = rr.read(data_producer_id) #subject="", predicate="", object_type="", assoc="", id_only=False)
        #data_product = rr.read_object(object_type=RT.DataProduct, predicate=PRED.hasOutputProduct, subject=external_dataset._id)
        #ids,_ = rr.find_objects(data_product._id, PRED.hasStream, RT.Stream, id_only=True)
        #stream_id = ids[0]
        #route = pubsub.read_stream_route(stream_id)
        #agent_instance.dataset_agent_config['driver_config']['dh_cfg']['stream_id'] = stream_id
        #agent_instance.dataset_agent_config['driver_config']['stream_id'] = stream_id
        #agent_instance.dataset_agent_config['driver_config']['dh_cfg']['stream_route'] = route
        #
        #log.info('launching agent process: %s.%s', agent.handler_module, agent.handler_class)
        #self.container.spawn_process(name=agent_instance.name,
        #    module=agent.handler_module, cls=agent.handler_class,
        #    config=agent_instance.dataset_agent_config)
        #
        # should i wait for process (above) to start
        # before launching client (below)?
        #
        log.info('activating agent...')
        client = ResourceAgentClient(external_dataset._id, process=FakeProcess())
        client.execute_agent(AgentCommand(command=ResourceAgentEvent.INITIALIZE))
        client.execute_agent(AgentCommand(command=ResourceAgentEvent.GO_ACTIVE))
        client.execute_agent(AgentCommand(command=ResourceAgentEvent.RUN))
        client.execute_resource(command=AgentCommand(command=DriverEvent.START_AUTOSAMPLE))
        log.info('now reading external dataset: %s', name)
