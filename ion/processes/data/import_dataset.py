#!/usr/bin/env python

"""
Control an external dataset agent.
Supports the following ops:
- start: via DAMS service call, start agent instance and subsequently put it into streaming mode (optionally)
- stop: via DAMS service call, stop agent instance

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
    bin/pycc -x ion.processes.data.import_dataset.ImportDataset instrument='CTDPF'
    bin/pycc -x ion.processes.data.import_dataset.ImportDataset device_name='uuid' op=start activate=False
    bin/pycc -x ion.processes.data.import_dataset.ImportDataset agent_name='uuid' op=stop
"""

__author__ = 'Michael Meisinger, Ian Katz'

from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from pyon.public import RT, log, PRED, ImmediateProcess, BadRequest, NotFound

from ion.core.includes.mi import DriverEvent

from interface.objects import AgentCommand
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceProcessClient


class DataAgentControl(ImmediateProcess):
    def on_start(self):
        rr = self.container.resource_registry

        op = self.CFG.get("op", "start")
        dataset_name = self.CFG.get("dataset", None)
        device_name = self.CFG.get("device_name", None) or self.CFG.get("instrument", None)
        agent_name = self.CFG.get("agent_name", None)
        resource_name = dataset_name or device_name or agent_name

        if not resource_name:
            raise BadRequest("Must provide ExternalDataset, Device or ExternalDatasetAgentInstance resource name or id")

        resource = None
        try:
            log.debug("Looking for a resource with id %s", dataset_name)
            resource = rr.read(resource_name)
        except NotFound:
            pass

        if not resource:
            if dataset_name:
                log.debug("Looking for an ExternalDataset with name %s", dataset_name)
                objects,_ = rr.find_resources(RT.ExternalDataset, name=dataset_name, id_only=False)
            elif device_name:
                log.debug("Looking for an InstrumentDevice with name %s", device_name)
                objects,_ = rr.find_resources(RT.InstrumentDevice, name=device_name, id_only=False)
                if not objects:
                    log.debug("Looking for a PlatformDevice with name %s", device_name)
                    objects,_ = rr.find_resources(RT.PlatformDevice, name=device_name, id_only=False)
            elif agent_name:
                log.debug("Looking for an ExternalDatasetAgentInstance with name %s", agent_name)
                objects,_ = rr.find_resources(RT.ExternalDatasetAgentInstance, name=agent_name, id_only=False)

            if not objects:
                raise BadRequest("Could not find resource with name %s", resource_name)
            elif len(objects) > 1:
                log.warn("More than one resource found with name %s. Using id=%s", dataset_name, objects[0])
            resource = objects[0]

        if resource.type_ in (RT.ExternalDataset, RT.InstrumentDevice, RT.PlatformDevice):
            resource_id = resource._id
            eda_instance_id = rr.read_object(subject=resource_id,
                                         predicate=PRED.hasAgentInstance,
                                         object_type=RT.ExternalDatasetAgentInstance,
                                         id_only=True)

        elif resource.type_ == RT.ExternalDatasetAgentInstance:
            eda_instance_id = resource._id
            resource_id = rr.read_subject(object=eda_instance_id,
                                         predicate=PRED.hasAgentInstance,
                                         id_only=True)

        else:
            raise BadRequest("Unexpected resource type: %s", resource.type_)

        if op == "start" or op == "load":
            self._start_agent(eda_instance_id, resource_id)
        elif op == "stop":
            self._stop_agent(eda_instance_id, resource_id)
        else:
            raise BadRequest("Operation %s unknown", op)


    def _start_agent(self, eda_instance_id, resource_id):
        log.info('Starting agent...')
        dams = DataAcquisitionManagementServiceProcessClient(process=self)
        dams.start_external_dataset_agent_instance(eda_instance_id)
        log.info('Agent started!')

        activate = self.CFG.get("activate", True)
        if activate:
            log.info('Activating agent...')
            client = ResourceAgentClient(resource_id, process=self)
            client.execute_agent(AgentCommand(command=ResourceAgentEvent.INITIALIZE))
            client.execute_agent(AgentCommand(command=ResourceAgentEvent.GO_ACTIVE))
            client.execute_agent(AgentCommand(command=ResourceAgentEvent.RUN))
            client.execute_resource(command=AgentCommand(command=DriverEvent.START_AUTOSAMPLE))

            log.info('Agent active!')

    def _stop_agent(self, eda_instance_id, resource_id):
        try:
            client = ResourceAgentClient(resource_id, process=self)
        except NotFound:
            log.warn("Agent for resource %s seems not running", resource_id)

        log.info('Stopping agent...')
        dams = DataAcquisitionManagementServiceProcessClient(process=self)
        try:
            dams.stop_external_dataset_agent_instance(eda_instance_id)
        except NotFound:
            log.warn("Agent for resource %s not found", resource_id)

        log.info('Agent stopped!')


ImportDataset = DataAgentControl