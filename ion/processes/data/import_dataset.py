#!/usr/bin/env python

"""
Ask container to start an external dataset agent and put it into streaming mode
so it will read and relay data from the file into a data product.

If using an external dataset, must have resources already defined in the system (ie- preload):
    ExternalDataset (implies also ExternalDatasetModel, ParameterDictionary, ParameterDefinitions)
    ExternalDatasetAgentInstance (implies ExternalDatasetAgent)
    DataProduct associated with ExternalDataset (ie- DataProductLink, implies StreamDefinition)

If using an instrument, must have resources already defined in the system:
    InstrumentDevice (implies also InstrumentModel, ParameterDictionary, ParameterDefinitions)
    ExternalDatasetAgentInstance (implies ExternalDatasetAgent)
    DataProduct (ie- DataProductLink, implies StreamDefinition)

invoke with command like this:
    bin/pycc -x ion.processes.data.import_dataset.ImportDataset dataset='External CTD Dataset'
    bin/pycc -x ion.process.data.import_dataset.ImportDataset instrument_device='CTDPF'
"""

__author__ = 'Michael Meisinger, Ian Katz, Thomas Lennan'


from pyon.core.bootstrap import get_service_registry
from pyon.core.exception import ConfigNotFound
from pyon.public import ImmediateProcess
from interface.objects import AgentCommand
from pyon.public import RT, log, PRED
from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from ion.services.sa.acquisition.test.test_bulk_data_ingestion import FakeProcess
from ion.core.includes.mi import DriverEvent

class ImportDataset(ImmediateProcess):
    def on_start(self):
        rr = self.container.resource_registry

        name = self.CFG.get("dataset")
        if name: # We are looking for an external dataset!
            log.trace("Looking for an external dataset with name %s", name)
            objects,_ = rr.find_resources(RT.ExternalDataset)
            filtered_objs = [obj for obj in objects if obj.name == name]
            if (filtered_objs) == []:
                raise ConfigNotFound("No appropriate ExternalDataset objects loaded")
            external_dataset = filtered_objs[0]
    
            instance_id = rr.read_object(subject=external_dataset._id,
                                         predicate=PRED.hasAgentInstance,
                                         object_type=RT.ExternalDatasetAgentInstance,
                                         id_only=True)
            dams = get_service_registry().services['data_acquisition_management'].client(process=self)
            dams.start_external_dataset_agent_instance(instance_id)
            client = ResourceAgentClient(external_dataset._id, process=FakeProcess())
        else:
            name = self.CFG.get("instrument")
            
        if name: # We are looking for an instrument device!
            log.trace("Looking for an instrument device with name %s", name)
            objects,_ = rr.find_resources(RT.InstrumentDevice)
            filtered_objs = [obj for obj in objects if obj.name == name]
            if (filtered_objs) == []:
                raise ConfigNotFound("No appropriate InstrumentDevice objects loaded")
            instrument_device = filtered_objs[0]
            log.trace("Found instrument device: %s", instrument_device)
    
            instance_id = rr.read_object(subject=instrument_device._id,
                                         predicate=PRED.hasAgentInstance,
                                         object_type=RT.ExternalDatasetAgentInstance,
                                         id_only=True)
            dams = get_service_registry().services['data_acquisition_management'].client(process=self)
            proc_id = dams.start_external_dataset_agent_instance(instance_id)
            client = ResourceAgentClient(instrument_device._id, process=FakeProcess())
        else:
            raise ConfigNotFound("Could not find dataset or instrument_device value name")

        log.info('Activating agent...')
        client.execute_agent(AgentCommand(command=ResourceAgentEvent.INITIALIZE))
        client.execute_agent(AgentCommand(command=ResourceAgentEvent.GO_ACTIVE))
        client.execute_agent(AgentCommand(command=ResourceAgentEvent.RUN))
        client.execute_resource(command=AgentCommand(command=DriverEvent.START_AUTOSAMPLE))
        log.info('Now reading: %s', name)
