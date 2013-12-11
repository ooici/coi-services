#!/usr/bin/env python

"""
Control agents including external dataset.
@see https://confluence.oceanobservatories.org/display/CIDev/R2+Agent+Use+Guide

Supports the following ops:
- start: via DAMS service call, start agent instance and subsequently put it into streaming mode (optionally)
- stop: via DAMS service call, stop agent instance
- configure_instance: using a config csv lookup file, update the AgentInstance driver_config
- activate_persistence: activate persistence for the data products of the devices
- suspend_persistence: suspend persistence for the data products of the devices

Resource ids (uuid) can be used instead of names.

Invoke via command line like this:
    bin/pycc -x ion.agents.agentctrl.AgentControl instrument='CTDPF'
    bin/pycc -x ion.agents.agentctrl.AgentControl device_name='uuid' op=start activate=False
    bin/pycc -x ion.agents.agentctrl.AgentControl agent_name='uuid' op=stop
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=start recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=config_instance cfg=file.csv recurse=True
    and others (see Confluence page)
TODO:
- Reset agent
- Support for instrument and platform agents
- Force terminate agents
"""

__author__ = 'Michael Meisinger, Ian Katz'

import csv

from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from pyon.public import RT, log, PRED, ImmediateProcess, BadRequest, NotFound

from ion.core.includes.mi import DriverEvent
from ion.util.parse_utils import parse_dict

from interface.objects import AgentCommand
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceProcessClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceProcessClient


class AgentControl(ImmediateProcess):
    def on_start(self):
        log.info("======================== ION AgentControl ========================")
        self.rr = self.container.resource_registry

        self.op = self.CFG.get("op", "start")
        if self.op not in {"start", "stop", "config_instance", "activate_persistence", "suspend_persistence"}:
            raise BadRequest("Operation %s unknown", self.op)

        dataset_name = self.CFG.get("dataset", None)
        device_name = self.CFG.get("device_name", None) or self.CFG.get("instrument", None) or self.CFG.get("platform", None)
        agent_name = self.CFG.get("agent_name", None)
        resource_name = dataset_name or device_name or agent_name

        self.recurse = self.CFG.get("recurse", False)
        self.cfg_mappings = {}

        if not resource_name:
            raise BadRequest("Must provide ExternalDataset, Device or AgentInstance resource name or id")

        resource = None
        try:
            log.debug("Looking for a resource with id %s", dataset_name)
            resource = self.rr.read(resource_name)
        except NotFound:
            pass

        if not resource:
            if dataset_name:
                log.debug("Looking for an ExternalDataset with name %s", dataset_name)
                objects,_ = self.rr.find_resources(RT.ExternalDataset, name=dataset_name, id_only=False)
            elif device_name:
                log.debug("Looking for an InstrumentDevice with name %s", device_name)
                objects,_ = self.rr.find_resources(RT.InstrumentDevice, name=device_name, id_only=False)
                if not objects:
                    log.debug("Looking for a PlatformDevice with name %s", device_name)
                    objects,_ = self.rr.find_resources(RT.PlatformDevice, name=device_name, id_only=False)
            elif agent_name:
                log.debug("Looking for an ExternalDatasetAgentInstance with name %s", agent_name)
                objects,_ = self.rr.find_resources(RT.ExternalDatasetAgentInstance, name=agent_name, id_only=False)

            if not objects:
                raise BadRequest("Could not find resource with name %s", resource_name)
            elif len(objects) > 1:
                log.warn("More than one resource found with name %s. Using id=%s", dataset_name, objects[0])
            resource = objects[0]

        resource_id, eda_instance_id = None, None
        if resource.type_ in (RT.ExternalDataset, RT.InstrumentDevice, RT.PlatformDevice):
            resource_id = resource._id
        elif resource.type_ == RT.ExternalDatasetAgentInstance:
            eda_instance_id = resource._id
        else:
            log.warn("Unexpected resource type: %s", resource.type_)

        self._execute_op(eda_instance_id, resource_id)

        log.info("======================== ION AgentControl completed ========================")

    def _execute_op(self, agent_instance_id, resource_id):
        try:
            if resource_id is None and agent_instance_id:
                resource_id = self.rr.read_subject(object=agent_instance_id,
                                                   predicate=PRED.hasAgentInstance,
                                                   id_only=True)

            res_obj = self.rr.read(resource_id)

            if agent_instance_id is None:
                aids, _ = self.rr.find_objects(subject=resource_id,
                                               predicate=PRED.hasAgentInstance,
                                               object_type=RT.ExternalDatasetAgentInstance,
                                               id_only=True)
                if aids:
                    agent_instance_id = aids[0]

            log.info("--- Executing %s on '%s' id=%s agent=%s ---", self.op, res_obj.name, resource_id, agent_instance_id)

            if self.op == "start" or self.op == "load":
                self._start_agent(agent_instance_id, resource_id)
            elif self.op == "stop":
                self._stop_agent(agent_instance_id, resource_id)
            elif self.op == "activate_persistence":
                self._activate_persistence(agent_instance_id, resource_id)
            elif self.op == "suspend_persistence":
                self._suspend_persistence(agent_instance_id, resource_id)
            elif self.op == "config_instance":
                self._config_instance(agent_instance_id, resource_id)

        except Exception:
            log.exception("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)

        if self.recurse:
            child_devices, _ = self.rr.find_objects(resource_id, PRED.hasDevice, id_only=False)
            if child_devices:
                log.debug("recurse==True. Executing %s on %s child devices", self.op, len(child_devices))
            for ch_obj in child_devices:
                ch_id = ch_obj._id
                agent_instance_id = None
                try:
                    self._execute_op(None, ch_id)
                except Exception:
                    log.exception("Could not %s agent %s for child device %s", self.op, agent_instance_id, ch_id)

    def _start_agent(self, agent_instance_id, resource_id):
        if not agent_instance_id or not resource_id:
            log.warn("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)
            return

        log.info('Starting agent...')
        dams = DataAcquisitionManagementServiceProcessClient(process=self)
        dams.start_external_dataset_agent_instance(agent_instance_id)
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

    def _stop_agent(self, agent_instance_id, resource_id):
        if not agent_instance_id or not resource_id:
            log.warn("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)
            return

        try:
            client = ResourceAgentClient(resource_id, process=self)
        except NotFound:
            log.warn("Agent for resource %s seems not running", resource_id)

        log.info('Stopping agent...')
        dams = DataAcquisitionManagementServiceProcessClient(process=self)
        try:
            dams.stop_external_dataset_agent_instance(agent_instance_id)
        except NotFound:
            log.warn("Agent for resource %s not found", resource_id)

        log.info('Agent stopped!')

    def _config_instance(self, agent_instance_id, resource_id):
        if not agent_instance_id:
            log.warn("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)
            return

        cfg_file = self.CFG.get("cfg", None)
        if not cfg_file:
            raise BadRequest("No cfg argument provided")

        if not self.cfg_mappings:
            self.cfg_mappings = {}

            with open(cfg_file, "rU") as f:
                reader = csv.DictReader(f, delimiter=',')
                for row in reader:
                    self.cfg_mappings[row["ID"]] = dict(harvester_cfg=parse_dict(row["Harvester Config"]),
                                                        parser_cfg=parse_dict(row["Parser Config"]),
                                                        max_records=parse_dict(row["Records Per Granule"]))
        edai = self.rr.read(agent_instance_id)
        alt_ids = [aid[4:-5] for aid in edai.alt_ids if aid.startswith("PRE:")]
        if not alt_ids:
            log.warn("Could not determine reference designator for EDAI %s", agent_instance_id)
            return

        dev_rd = alt_ids[0]
        dev_cfg = self.cfg_mappings.get(dev_rd, None)
        if not dev_cfg:
            return
        log.info("Setting config for device RD %s '%s': %s", dev_rd, edai.name, dev_cfg)

        if dev_cfg["harvester_cfg"]:
            edai.driver_config.setdefault("startup_config", {})["harvester"] = dev_cfg["harvester_cfg"]
        if dev_cfg["parser_cfg"]:
            edai.driver_config.setdefault("startup_config", {})["parser"] = dev_cfg["parser_cfg"]
        if dev_cfg["max_records"]:
            edai.driver_config["max_records"] = int(dev_cfg["max_records"])

        self.rr.update(edai)

    def _activate_persistence(self, agent_instance_id, resource_id):
        if not agent_instance_id or not resource_id:
            log.warn("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)
            return

        dpms = DataProductManagementServiceProcessClient(process=self)
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, id_only=False)
        for dp in dp_objs:
            try:
                log.info("Activating persistence for '%s'", dp.name)
                dpms.activate_data_product_persistence(dp._id)
            except Exception:
                log.exception("Could not activate persistence")

    def _suspend_persistence(self, agent_instance_id, resource_id):
        if not agent_instance_id or not resource_id:
            log.warn("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)
            return

        dpms = DataProductManagementServiceProcessClient(process=self)
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, id_only=False)
        for dp in dp_objs:
            try:
                log.info("Suspending persistence for '%s'", dp.name)
                dpms.suspend_data_product_persistence(dp._id)
            except Exception:
                log.exception("Could not suspend persistence")

ImportDataset = AgentControl
