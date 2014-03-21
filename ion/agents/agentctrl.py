#!/usr/bin/env python

"""
Control agents and related resources.
@see https://confluence.oceanobservatories.org/display/CIDev/R2+Agent+Use+Guide

Supports the following ops:
- start: via DAMS service call, start agent instance and subsequently put it into streaming mode (optionally)
         can provide start and stop date for instrument agent reachback recover (optionally)
- stop: via DAMS service call, stop agent instance
- configure_instance: using a config csv lookup file, update the AgentInstance driver_config
- activate_persistence: activate persistence for the data products of the devices
- suspend_persistence: suspend persistence for the data products of the devices
- recover_data: requires start and stop dates, issues reachback command for instrument agents.  Running this command
                against other agents will get a warning.
- set_calibration: add or replace calibration information for devices and their data products
- clear_saved_state: clear out the saved_agent_state in EDAI resources
- clear_status: clear out the device status
- create_dataset: create Dataset resource and coverage, but don't activate ingestion worker
- delete_dataset: remove Dataset resource and coverage
- delete_all_data: remove all device related DataProduct, StreamDefinition, Dataset, resources and coverages

Supports the following arguments:
- instrument, platform, device_name: name of device. Resource ids (uuid) can be used instead of names.
- agent_name: name of agent instance. Resource ids (uuid) can be used instead of names.
- recurse: given platform device, execute op on plaform and all child platforms and instruments
- fail_fast: if True, exit after the first exception. Otherwise log errors only
- recover_start, recover_end: floating point strings representing seconds since 1900-01-01 00:00:00 (NTP64 Epoch)

Invoke via command line like this:
    bin/pycc -x ion.agents.agentctrl.AgentControl instrument='CTDPF'
    bin/pycc -x ion.agents.agentctrl.AgentControl device_name='uuid' op=start activate=False
    bin/pycc -x ion.agents.agentctrl.AgentControl agent_name='uuid' op=stop
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=start recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=config_instance cfg=file.csv recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=set_calibration cfg=file.csv recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl instrument='CTDPF' op=recover_data recover_start=0.0 recover_end=1.0
    and others (see Confluence page)

TODO:
- Ability to apply to all platforms (maybe a facility/site), not just one
- Force terminate agents and clean up
- Identify devices in CFG files other than by ID or RD
- Share agent definition or resource in facility
- Change owner of resource
- Change contact info, metadata of resource based on spreadsheet
"""

__author__ = 'Michael Meisinger, Ian Katz, Bill French, Luke Campbell'

import csv

from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from pyon.ion.event import EventPublisher
from pyon.public import RT, log, PRED, OT, ImmediateProcess, BadRequest, NotFound

from ion.core.includes.mi import DriverEvent
from ion.util.parse_utils import parse_dict

from interface.objects import AgentCommand
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceProcessClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient


class AgentControl(ImmediateProcess):
    def on_start(self):
        log.info("======================== OOINet AgentControl ========================")
        self.rr = self.container.resource_registry

        self.op = self.CFG.get("op", "start")
        if not self.op or self.op.startswith("_") or not hasattr(self, self.op):
            if self.op not in {"start", "stop", "load"}:
                raise BadRequest("Operation %s unknown", self.op)

        dataset_name = self.CFG.get("dataset", None)
        device_name = self.CFG.get("device_name", None) or self.CFG.get("instrument", None) or self.CFG.get("platform", None)
        agent_name = self.CFG.get("agent_name", None)
        resource_name = dataset_name or device_name or agent_name

        self.recurse = self.CFG.get("recurse", False)
        self.fail_fast = self.CFG.get("fail_fast", False)
        self.cfg_mappings = {}

        self._recover_data_status = {'ignored': [], 'success': [], 'fail': []}

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

        log.info("======================== OOINet AgentControl completed ========================")

    def _execute_op(self, agent_instance_id, resource_id):
        try:
            if resource_id is None and agent_instance_id:
                resource_id = self.rr.read_subject(object=agent_instance_id,
                                                   predicate=PRED.hasAgentInstance,
                                                   id_only=True)

            res_obj = self.rr.read(resource_id)

            if agent_instance_id is None:
                agent_instance_id = self._get_agent_instance_id(resource_id)

            log.info("--- Executing %s on '%s' id=%s agent=%s ---", self.op, res_obj.name, resource_id, agent_instance_id)

            if self.op == "start" or self.op == "load" or self.op == "start_agent":
                self.start_agent(agent_instance_id, resource_id)
            elif self.op == "stop" or self.op == "stop_agent":
                self.stop_agent(agent_instance_id, resource_id)
            elif self.op == "activate_persistence":
                self.activate_persistence(agent_instance_id, resource_id)
            elif self.op == "suspend_persistence":
                self.suspend_persistence(agent_instance_id, resource_id)
            elif self.op == "config_instance":
                self.config_instance(agent_instance_id, resource_id)
            elif self.op == "clear_saved_state":
                self.clear_saved_state(agent_instance_id, resource_id)
            elif self.op == "clear_status":
                self.clear_status(agent_instance_id, resource_id)
            elif self.op == "set_calibration":
                self.set_calibration(agent_instance_id, resource_id)
            elif self.op == "recover_data":
                self.recover_data(agent_instance_id, resource_id)
            elif self.op == "create_dataset":
                self.create_dataset(agent_instance_id, resource_id)
            elif self.op == "delete_dataset":
                self.delete_dataset(agent_instance_id, resource_id)
            elif self.op == "delete_all_data":
                self.delete_all_data(agent_instance_id, resource_id)

        except Exception:
            log.exception("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)
            if self.fail_fast:
                raise

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
                    if self.fail_fast:
                        raise

        if self.op == "recover_data":
            self._recover_data_report()

    def _get_agent_instance_id(self, resource_id):
        dsaids, _ = self.rr.find_objects(subject=resource_id,
                                         predicate=PRED.hasAgentInstance,
                                         object_type=RT.ExternalDatasetAgentInstance,
                                         id_only=True)

        iaids, _ = self.rr.find_objects(subject=resource_id,
                                        predicate=PRED.hasAgentInstance,
                                        object_type=RT.InstrumentAgentInstance,
                                        id_only=True)

        paids, _ = self.rr.find_objects(subject=resource_id,
                                        predicate=PRED.hasAgentInstance,
                                        object_type=RT.PlatformAgentInstance,
                                        id_only=True)

        aids = dsaids + iaids + paids
        if len(aids) > 1:
            log.error("Multiple agent instances found")
            raise BadRequest("Failed to identify agent instance")

        if len(aids) == 0:
            log.error("Agent instance not found")
            raise BadRequest("Failed to identify agent instance")

        log.info("Found agent instance ID: %s", aids[0])
        return aids[0]

    def start_agent(self, agent_instance_id, resource_id):
        if not agent_instance_id or not resource_id:
            log.warn("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)
            return

        res_obj = self.rr.read(resource_id)

        log.info('Starting agent...')
        if res_obj.type_ == RT.ExternalDatasetAgentInstance or res_obj == RT.ExternalDataset:
            dams = DataAcquisitionManagementServiceProcessClient(process=self)
            dams.start_external_dataset_agent_instance(agent_instance_id)
        elif res_obj.type_ == RT.InstrumentDevice:
            ims = InstrumentManagementServiceClient()
            ims.start_instrument_agent_instance(agent_instance_id)
        elif res_obj.type_ == RT.PlatformDevice:
            ims = InstrumentManagementServiceClient()
            ims.start_platform_agent_instance(agent_instance_id)
        else:
            BadRequest("Attempt to start unsupported agent type: %s", res_obj.type_)
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

    def stop_agent(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)

        if not agent_instance_id or not resource_id:
            log.warn("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)
            return

        try:
            client = ResourceAgentClient(resource_id, process=self)
        except NotFound:
            log.warn("Agent for resource %s seems not running", resource_id)

        log.info('Stopping agent...')

        if res_obj.type_ == RT.ExternalDatasetAgentInstance or res_obj == RT.ExternalDataset:
            dams = DataAcquisitionManagementServiceProcessClient(process=self)
            try:
                dams.stop_external_dataset_agent_instance(agent_instance_id)
            except NotFound:
                log.warn("Agent for resource %s not found", resource_id)
        elif res_obj.type_ == RT.InstrumentDevice:
            ims = InstrumentManagementServiceClient()
            try:
                ims.stop_instrument_agent_instance(agent_instance_id)
            except NotFound:
                log.warn("Agent for resource %s not found", resource_id)
        elif res_obj.type_ == RT.PlatformDevice:
            ims = InstrumentManagementServiceClient()
            try:
                ims.stop_platform_agent_instance(agent_instance_id)
            except NotFound:
                log.warn("Agent for resource %s not found", resource_id)
        else:
            BadRequest("Attempt to start unsupported agent type: %s", res_obj.type_)

        log.info('Agent stopped!')

    def config_instance(self, agent_instance_id, resource_id):
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

    def set_calibration(self, agent_instance_id, resource_id):
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
                    device_id = row["ID"]
                    param_name = row["Name"]
                    self.cfg_mappings.setdefault(device_id, {})[param_name] = {k:v for k, v in row.iteritems() if k not in {"ID", "Name", "Value"}}
                    self.cfg_mappings[device_id][param_name]["value"] = row["Value"]  # Make sure it exists

        res_obj = self.rr.read(resource_id)

        # Device has no reference designator - but use preload ID as reference designator
        alt_ids = [aid[4:] for aid in res_obj.alt_ids if aid.startswith("PRE:ID")]
        device_rd = alt_ids[0] if alt_ids else None

        dev_cfg = self.cfg_mappings.get(resource_id, None) or self.cfg_mappings.get(device_rd, None)
        if not dev_cfg:
            return
        log.info("Setting calibration for device %s (RD %s) '%s': %s", resource_id, device_rd, res_obj.name, dev_cfg)

        # Find parsed data product from device id
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, RT.DataProduct, id_only=False)
        dp_objs_filtered = [dp for dp in dp_objs if dp.processing_level_code == "Parsed"]
        for dp_obj in dp_objs_filtered:
            self._set_calibration_for_data_product(dp_obj, dev_cfg)

        log.info("Calibration set for device %s (RD %s) '%s'", resource_id, device_rd, res_obj.name)

    def _set_calibration_for_data_product(self, dp_obj, dev_cfg):
        from ion.util.direct_coverage_utils import DirectCoverageAccess
        from coverage_model import SparseConstantType

        log.debug(" Setting calibration for data product '%s'", dp_obj.name)
        dataset_ids, _ = self.rr.find_objects(dp_obj, PRED.hasDataset, id_only=True)
        publisher = EventPublisher(OT.InformationContentModifiedEvent)
        for dataset_id in dataset_ids:
            # Synchronize with ingestion
            with DirectCoverageAccess() as dca:
                cov = dca.get_editable_coverage(dataset_id)
                # Iterate over the calibrations
                for cal_name, contents in dev_cfg.iteritems():
                    if cal_name in cov.list_parameters() and isinstance(cov.get_parameter_context(cal_name).param_type, SparseConstantType):
                        value = float(contents['value'])
                        cov.set_parameter_values(cal_name, value)
                    else:
                        log.warn("Calibration %s not found in dataset", cal_name)
                publisher.publish_event(origin=dataset_id, description="Calibrations Updated")
        publisher.close()
        log.info(" Calibration set for data product '%s' in %s coverages", dp_obj.name, len(dataset_ids))

    def activate_persistence(self, agent_instance_id, resource_id):
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

    def suspend_persistence(self, agent_instance_id, resource_id):
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

    def recover_data(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)

        if res_obj.type_ != RT.InstrumentDevice:
            log.warn("Ignoring resource because it is not an instrument: %s - %s", res_obj.name, res_obj.type_)
            self._recover_data_status['ignored'].append("%s (%s)" % (res_obj.name, res_obj.type_))
            return

        self.recover_start = self.CFG.get("recover_start", None)
        self.recover_end = self.CFG.get("recover_end", None)

        if self.recover_end is None:
            raise BadRequest("Missing recover_end parameter")
        if self.recover_start is None:
            raise BadRequest("Missing recover_start parameter")

        try:
            ia_client = ResourceAgentClient(resource_id, process=self)
            log.info('Got ia client %s.', str(ia_client))
            ia_client.execute_resource(command=AgentCommand(command=DriverEvent.GAP_RECOVERY, args=[self.recover_start, self.recover_end]))

            self._recover_data_status['success'].append(res_obj.name)
        except Exception as e:
            log.warn("Failed to start recovery process for %s", res_obj.name)
            log.warn("Exception: %s", e)
            self._recover_data_status['fail'].append("%s (%s)" % (res_obj.name, e))

    def _recover_data_report(self):
        print "==================== Recover Data Report: ===================="

        print "\nSuccessfully started recovery for:"
        if self._recover_data_status['success']:
            for name in self._recover_data_status['success']:
                print "   %s" % name
        else:
            print "    None"

        print "\nIgnored resources:"
        if self._recover_data_status['ignored']:
            for name in self._recover_data_status['ignored']:
                print "   %s" % name
        else:
            print "    None"

        print "\nFailed to start recovery for:"
        if self._recover_data_status['fail']:
            for name in self._recover_data_status['fail']:
                print "   %s" % name
        else:
            print "    None"

    def clear_status(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        from ion.processes.event.device_state import STATE_PREFIX
        try:
            self.container.object_store.delete_doc(STATE_PREFIX+resource_id)
            # TODO: Maybe we only want to reset any alerts?
            log.info("Status cleared for device %s '%s'", resource_id, res_obj.name)
        except NotFound:
            pass

    def clear_saved_state(self, agent_instance_id, resource_id):
        if not agent_instance_id:
            log.warn("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)
            return

        res_obj = self.rr.read(resource_id)
        ai = self.rr.read(agent_instance_id)
        existed = bool(ai.saved_agent_state)
        ai.saved_agent_state = {}
        self.rr.update(ai)
        if existed:
            log.info("Saved state cleared for device %s '%s'", resource_id, res_obj.name)

    def create_dataset(self, agent_instance_id, resource_id):
        # Find hasOutputProduct DataProducts
        # Execute call to create Dataset and coverage
        pass

    def delete_dataset(self, agent_instance_id, resource_id):

        res_obj = self.rr.read(resource_id)
        dpms = DataProductManagementServiceProcessClient(process=self)

        # Find data products from device id
        count_ds = 0
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, RT.DataProduct, id_only=False)
        for dp_obj in dp_objs:
            if dpms.is_persisted(dp_obj._id):
                raise BadRequest("DataProduct %s '%s' is currently persisted", dp_obj._id, dp_obj.name)

            ds_objs, _ = self.rr.find_objects(dp_obj._id, PRED.hasDataset, RT.Dataset, id_only=False)
            for ds_obj in ds_objs:
                # Delete coverage
                # TODO: Luke, please add delete coverage code here

                # Delete Dataset and associations
                self.rr.delete(ds_obj._id)
                count_ds += 1

        log.info("Datasets and coverages deleted for device %s '%s': %s", resource_id, res_obj.name, count_ds)

    def delete_all_data(self, agent_instance_id, resource_id):
        # Delete Dataset and coverage for all original DataProducts
        self.delete_dataset(agent_instance_id, resource_id)

        res_obj = self.rr.read(resource_id)

        # Find parsed data product from device id
        count_dp, count_sd, count_st = 0, 0, 0
        dp_objs, _ = self.rr.find_subjects(RT.DataProduct, PRED.hasSource, resource_id, id_only=False)
        for dp_obj in dp_objs:
            # Find and delete Stream
            st_objs, _ = self.rr.find_objects(dp_obj._id, PRED.hasStream, RT.Stream, id_only=False)
            for st_obj in st_objs:
                self.rr.delete(st_obj._id)
                count_st += 1

            # Find and delete StreamDefinition
            sd_objs, _ = self.rr.find_objects(dp_obj._id, PRED.hasStreamDefinition, RT.StreamDefinition, id_only=False)
            for sd_obj in sd_objs:
                self.rr.delete(sd_obj._id)
                count_sd += 1

            # Delete DataProduct
            self.rr.delete(dp_obj._id)
            count_dp += 1

        log.info("Data resources deleted for device %s '%s': %s DataProduct, %s StreamDefinition, %s Stream",
                 resource_id, res_obj.name, count_dp, count_sd, count_st)


ImportDataset = AgentControl
