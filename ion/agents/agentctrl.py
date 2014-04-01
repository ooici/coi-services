#!/usr/bin/env python

"""
Control agents and related resources.
@see https://confluence.oceanobservatories.org/display/CIDev/R2+Agent+Use+Guide

Supports the following ops:
- start_agent (alias start, load): Start agent instance and subsequently put it into streaming mode (optionally)
        can provide start and stop date for instrument agent reachback recover (optionally)
- stop_agent (alias stop): Stop agent instance
- configure_instance: using a config csv lookup file, update the AgentInstance driver_config
- set_attributes: using a config csv lookup file, update the given resource attributes
- activate_persistence: activate persistence for the data products of the devices
- suspend_persistence: suspend persistence for the data products of the devices
- cleanup_persistence: Deletes remnant persistent records about activated persistence in the system
- cleanup_agent: deletes remnant persistent records about running agents in the system
- recover_data: requires start and stop dates, issues reachback command for instrument agents.  Running this command
                against other agents will get a warning.
- set_calibration: add or replace calibration information for devices and their data products
- clear_saved_state: clear out the saved_agent_state in EDAI resources
- clear_status: clear out the device status
- create_dataset: create Dataset resource and coverage, but don't activate ingestion worker
- delete_dataset: remove Dataset resource and coverage
- delete_all_data: remove all device related DataProduct, StreamDefinition, Dataset, resources and coverages
- delete_all_device: remove all device related resources and all from delete_all_data
- delete_site: remove site resources
- activate_deployment: If device has Deployments, activate the current one
- deactivate_deployment: If device has an active Deployment, deactivate it
- clone_device: For a given device, create another device with similar associations and data products.
        If a config csv lookup file was given, set new attributes accordingly
- clone_deployment: For a given deployment, create another deployment with similar associations
- list_persistence: Prints a report of currently active persistence
- list_agents: Prints a report of currently active agents
- show_dataset: Prints a report about a dataset (coverage)

Supports the following arguments:
- instrument, platform, device_name: name of device. Resource ids (uuid) can be used instead of names.
- agent_name: name of agent instance. Resource ids (uuid) can be used instead of names.
- preload_id: preload id or list of preload ids of devices
- recurse: given platform device, execute op on platform and all child platforms and instruments
- fail_fast: if True, exit after the first exception. Otherwise log errors only
- recover_start, recover_end: floating point strings representing seconds since 1900-01-01 00:00:00 (NTP64 Epoch)
- force: if True, ignore some warning conditions and move on or clear up
- autoclean: if True, try to clean up resources directly after failed operations
- verbose: if True, log more messages for detailed steps
- dryrun: if True, log attempted actions but don't execute them (use verbose=True to see many details)
- clone_id: if set, used to suffix a cloned preload id, e.g. CP02PMUI-WP001_PD -> CP02PMUI-WP001_PD_CLONE1

Invoke via command line like this:
    bin/pycc -x ion.agents.agentctrl.AgentControl instrument='CTDPF'
    bin/pycc -x ion.agents.agentctrl.AgentControl device_name='uuid' op=start activate=False
    bin/pycc -x ion.agents.agentctrl.AgentControl agent_name='uuid' op=stop
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=start recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=config_instance cfg=file.csv recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=set_calibration cfg=file.csv recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl instrument='CTDPF' op=recover_data recover_start=0.0 recover_end=1.0
    bin/pycc -x ion.agents.agentctrl.AgentControl preload_id='CP02PMUI-WF001_PD' op=start
    and others (see above and Confluence page)

TODO:
- Force terminate agents and clean up
- Share agent definition or resource in facility
- Change owner of resource
- Change contact info, metadata of resource based on spreadsheet
"""

__author__ = 'Michael Meisinger, Ian Katz, Bill French, Luke Campbell'

import csv
import os
import shutil
import time

from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from pyon.public import RT, log, PRED, OT, ImmediateProcess, BadRequest, NotFound, LCS, EventPublisher, dict_merge, IonObject

from ion.core.includes.mi import DriverEvent
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.sa.observatory.observatory_util import ObservatoryUtil
from ion.util.parse_utils import parse_dict, get_typed_value

from interface.objects import AgentCommand, Site, TemporalBounds, AgentInstance, Device
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceProcessClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient


class AgentControl(ImmediateProcess):
    def on_start(self):
        log.info("======================== OOINet AgentControl ========================")
        self.rr = self.container.resource_registry

        self.op = self.CFG.get("op", "start")
        # Map op aliases to internal names
        self.op = dict(start="start_agent", load="start_agent", stop="stop_agent").get(self.op, self.op)
        if not self.op or self.op.startswith("_") or not hasattr(self, self.op):
            raise BadRequest("Operation %s unknown", self.op)
        log.info("OPERATION: %s", self.op)

        dataset_name = self.CFG.get("dataset", None)
        device_name = self.CFG.get("device_name", None) or self.CFG.get("instrument", None) or self.CFG.get("platform", None)
        agent_name = self.CFG.get("agent_name", None)
        resource_name = dataset_name or device_name or agent_name

        self.recurse = self.CFG.get("recurse", False)
        self.fail_fast = self.CFG.get("fail_fast", False)
        self.force = self.CFG.get("force", False)
        self.autoclean = self.CFG.get("autoclean", False)
        self.verbose = self.CFG.get("verbose", False)
        self.dryrun = self.CFG.get("dryrun", False)
        self.timeout = self.CFG.get("timeout", 120)
        self.cfg_mappings = {}
        self.system_actor = None
        self.errors = []
        self._recover_data_status = {'ignored': [], 'success': [], 'fail': []}

        preload_id = self.CFG.get("preload_id", None)
        if preload_id:
            preload_ids = preload_id.split(",")
            for pid in preload_ids:
                res_obj = self._get_resource_by_alt_id("PRE", pid)
                if res_obj:
                    log.info("Found preload id=%s as %s '%s' id=%s", pid, res_obj.type_, res_obj.name, res_obj._id)
                    self._execute(res_obj)

        elif self.op in {"list_persistence", "list_agents"}:
            # None-device operation
            log.info("--- Executing %s  ---", self.op)
            if hasattr(self, self.op):
                opfunc = getattr(self, self.op)
                opfunc()

        else:
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
                    log.debug("Looking for an AgentInstance with name %s", agent_name)
                    objects,_ = self.rr.find_resources(RT.ExternalDatasetAgentInstance, name=agent_name, id_only=False)
                    if not objects:
                        objects,_ = self.rr.find_resources(RT.InstrumentAgentInstance, name=agent_name, id_only=False)
                    if not objects:
                        objects,_ = self.rr.find_resources(RT.PlatformAgentInstance, name=agent_name, id_only=False)

                if not objects:
                    raise BadRequest("Could not find resource with name %s", resource_name)
                elif len(objects) > 1:
                    log.warn("More than one resource found with name %s. Using id=%s", dataset_name, objects[0])
                resource = objects[0]

            self._execute(resource)

        if self.errors:
            log.warn("ERROR: There were %s errors:\n%s", len(self.errors),
                     "\n".join("dev_id=%(resource_id)s: %(msg)s" % err for err in self.errors))
        else:
            log.info("Completed, NO errors.")

        log.info("======================== ION AgentControl completed ========================")

    def _execute(self, resource):
        """Executes script for one resource"""
        resource_id, ai_id = None, None
        if resource.type_ == RT.ExternalDataset or isinstance(resource, Device):
            resource_id = resource._id
        elif isinstance(resource, AgentInstance):
            ai_id = resource._id
        elif isinstance(resource, Site):
            resource_id = resource._id
        else:
            log.warn("Unexpected resource type: %s", resource.type_)
            resource_id = resource._id

        # This does all the work and will recurse if desired
        self._execute_op(ai_id, resource_id)

    def _execute_op(self, agent_instance_id, resource_id):
        """Executes op on one resource/agent including recursive applications."""
        child_res = None  # Need to save this for the case the device is deleted during the op
        try:
            if resource_id is None and agent_instance_id:
                resource_id = self.rr.read_subject(object=agent_instance_id,
                                                   predicate=PRED.hasAgentInstance,
                                                   id_only=True)

            res_obj = self.rr.read(resource_id)
            if isinstance(res_obj, Device):
                child_res, _ = self.rr.find_objects(resource_id, PRED.hasDevice, id_only=False)
            elif isinstance(res_obj, Site):
                child_res, _ = self.rr.find_objects(resource_id, PRED.hasSite, id_only=False)

            if agent_instance_id is None and isinstance(res_obj, Device):
                agent_instance_id = self._get_agent_instance_id(resource_id)

            log.info("--- Executing %s on '%s' id=%s agent=%s ---", self.op, res_obj.name, resource_id, agent_instance_id)

            if hasattr(self, self.op):
                opfunc = getattr(self, self.op)
                opfunc(agent_instance_id=agent_instance_id, resource_id=resource_id)

        except Exception:
            self._log_error(agent_instance_id, resource_id, logexc=True,
                            msg="Failed op=%s on device=%s agent=%s" % (self.op, resource_id, agent_instance_id))
            if self.fail_fast:
                raise

        if self.recurse:
            if child_res is None:
                if isinstance(res_obj, Device):
                    child_res, _ = self.rr.find_objects(resource_id, PRED.hasDevice, id_only=False)
                elif isinstance(res_obj, Site):
                    child_res, _ = self.rr.find_objects(resource_id, PRED.hasSite, id_only=False)
            if child_res:
                log.debug("recurse==True. Executing op=%s on %s child devices", self.op, len(child_res))
            for ch_obj in child_res:
                ch_id = ch_obj._id
                agent_instance_id = None
                try:
                    self._execute_op(None, ch_id)
                except Exception:
                    self._log_error(agent_instance_id, resource_id, logexc=True,
                                    msg="Failed op=%s on child device=%s agent=%s" % (self.op, ch_id, agent_instance_id))
                    if self.fail_fast:
                        raise

        # Post function if existing (signature _opname_post()):
        post_func = "_%s_post" % self.op
        if hasattr(self, post_func):
            opfunc = getattr(self, post_func)
            opfunc(resource_id=resource_id, agent_instance_id=agent_instance_id)

    def _get_agent_instance_id(self, resource_id):
        aids, _ = self.rr.find_objects(subject=resource_id,
                                       predicate=PRED.hasAgentInstance,
                                       id_only=True)

        if len(aids) > 1:
            raise BadRequest("Multiple agent instances found")
        if aids:
            log.debug("Found agent instance ID: %s", aids[0])
            return aids[0]

        log.debug("Agent instance not found for device=%s", resource_id)
        return None

    def _get_resource_by_alt_id(self, alt_id_ns, alt_id):
        res_objs, _ = self.rr.find_resources_ext(alt_id_ns=alt_id_ns, alt_id=alt_id, id_only=False)
        if res_objs:
            return res_objs[0]

    def _get_system_actor_headers(self):
        if self.system_actor is None:
            system_actor, _ = self.rr.find_resources(RT.ActorIdentity, name=self.CFG.system.system_actor, id_only=False)
            self.system_actor = system_actor[0]._id if system_actor else 'anonymous'
        return {'ion-actor-id': self.system_actor,
                'ion-actor-roles': {'ION': ['ION_MANAGER', 'ORG_MANAGER']},
                'expiry':'0'}

    def _log_error(self, agent_instance_id, resource_id, msg, logexc=False):
        self.errors.append(dict(resource_id=resource_id, agent_instance_id=agent_instance_id, msg=msg))
        if logexc:
            log.exception(msg)

    # -------------------------------------------------------------------------
    # Control commands

    def start_agent(self, agent_instance_id, resource_id):
        if not agent_instance_id or not resource_id:
            log.warn("Could not op=%s agent %s for device %s", self.op, agent_instance_id, resource_id)
            return

        res_obj = self.rr.read(resource_id)
        ai_obj = self.rr.read(agent_instance_id)

        try:
            client = ResourceAgentClient(resource_id, process=self)
            if self.force:
                log.warn("Agent for resource %s seems running - continuing", resource_id)
                if self.autoclean:
                    self.cleanup_agent(agent_instance_id, resource_id)
            else:
                log.warn("Agent for resource %s seems running", resource_id)
                return
        except NotFound:
            pass  # This is expected

        log.info('Starting agent...')
        if ai_obj.type_ == RT.ExternalDatasetAgentInstance:
            dams = DataAcquisitionManagementServiceProcessClient(process=self)
            if not self.dryrun:
                dams.start_external_dataset_agent_instance(agent_instance_id, headers=self._get_system_actor_headers(),
                                                           timeout=self.timeout)
        elif ai_obj.type_ == RT.InstrumentAgentInstance:
            ims = InstrumentManagementServiceProcessClient(process=self)
            if not self.dryrun:
                ims.start_instrument_agent_instance(agent_instance_id, headers=self._get_system_actor_headers(),
                                                    timeout=self.timeout)
        elif ai_obj.type_ == RT.PlatformAgentInstance:
            ims = InstrumentManagementServiceProcessClient(process=self)
            if not self.dryrun:
                ims.start_platform_agent_instance(agent_instance_id, headers=self._get_system_actor_headers(),
                                                  timeout=self.timeout)
        else:
            BadRequest("Attempt to start unsupported agent type: %s", ai_obj.type_)
        log.info('Agent started!')

        activate = self.CFG.get("activate", True)
        if activate:
            log.info('Activating agent...')
            if not self.dryrun:
                client = ResourceAgentClient(resource_id, process=self)
                client.execute_agent(AgentCommand(command=ResourceAgentEvent.INITIALIZE),
                                     headers=self._get_system_actor_headers(), timeout=self.timeout)
                client.execute_agent(AgentCommand(command=ResourceAgentEvent.GO_ACTIVE),
                                     headers=self._get_system_actor_headers(), timeout=self.timeout)
                client.execute_agent(AgentCommand(command=ResourceAgentEvent.RUN),
                                     headers=self._get_system_actor_headers(), timeout=self.timeout)
                client.execute_resource(command=AgentCommand(command=DriverEvent.START_AUTOSAMPLE),
                                        headers=self._get_system_actor_headers(), timeout=self.timeout)

            log.info('Agent in auto-sample mode!')

    def stop_agent(self, agent_instance_id, resource_id):
        if not agent_instance_id or not resource_id:
            return

        res_obj = self.rr.read(resource_id)
        ai_obj = self.rr.read(agent_instance_id)

        try:
            client = ResourceAgentClient(resource_id, process=self)
        except NotFound:
            if self.force:
                log.warn("Agent for resource %s seems not running - continuing", resource_id)
            else:
                log.info("Agent for resource %s seems not running", resource_id)
                return

        log.info('Stopping agent...')

        if ai_obj.type_ == RT.ExternalDatasetAgentInstance:
            dams = DataAcquisitionManagementServiceProcessClient(process=self)
            try:
                if not self.dryrun:
                    dams.stop_external_dataset_agent_instance(agent_instance_id,
                                                              headers=self._get_system_actor_headers(), timeout=self.timeout)
                log.info('Agent stopped!')
            except NotFound:
                log.warn("Agent for resource %s not found", resource_id)
                if self.autoclean:
                    self.cleanup_agent(agent_instance_id, resource_id)
            except Exception:
                if self.autoclean:
                    self.cleanup_agent(agent_instance_id, resource_id)
                raise
        elif ai_obj.type_ == RT.InstrumentAgentInstance:
            ims = InstrumentManagementServiceProcessClient(process=self)
            try:
                if not self.dryrun:
                    ims.stop_instrument_agent_instance(agent_instance_id,
                                                       headers=self._get_system_actor_headers(), timeout=self.timeout)
                log.info('Agent stopped!')
            except NotFound:
                log.warn("Agent for resource %s not found", resource_id)
                if self.autoclean:
                    self.cleanup_agent(agent_instance_id, resource_id)
            except Exception:
                if self.autoclean:
                    self.cleanup_agent(agent_instance_id, resource_id)
                raise
        elif ai_obj.type_ == RT.PlatformAgentInstance:
            ims = InstrumentManagementServiceProcessClient(process=self)
            try:
                if not self.dryrun:
                    ims.stop_platform_agent_instance(agent_instance_id,
                                                     headers=self._get_system_actor_headers(), timeout=self.timeout)
                log.info('Agent stopped!')
            except NotFound:
                log.warn("Agent for resource %s not found", resource_id)
                if self.autoclean:
                    self.cleanup_agent(agent_instance_id, resource_id)
            except Exception:
                if self.autoclean:
                    self.cleanup_agent(agent_instance_id, resource_id)
                raise
        else:
            BadRequest("Attempt to stop unsupported agent type: %s", ai_obj.type_)

    def _get_alt_id(self, res_obj, alt_ns):
        """Return a resource object's alt id for a given namespace, or None"""
        # TODO: Move into IonObjectBase
        alt_ns = alt_ns or "_"
        alt_ids = [aid[len(alt_ns) + 1:] for aid in res_obj.alt_ids if aid.startswith(alt_ns + ":")]
        if alt_ids:
            return alt_ids[0]

    def _set_alt_id(self, res_obj, alt_id):
        """Set or replace alt_id in given resource. Alt id must be namespaced, e.g. PRE:CP02PMUI-WP001"""
        # TODO: Move into IonObjectBase
        alt_id_parts = alt_id.split(":", 1)[0]
        if len(alt_id_parts) < 2 or not alt_id_parts[0]:
            raise BadRequest("Illegal alt_id: %s" % alt_id)
        alt_ns = alt_id_parts[0] + ":"
        for i, aid in enumerate(res_obj.alt_ids):
            if aid.startswith(alt_ns):
                res_obj.alt_ids[i] = alt_id
                return
        res_obj.alt_ids.append(alt_id)

    def _get_resource_cfg(self, res_obj, cfg_dict):
        """Given a (device) resource object, try to find an associated config entry from the CSV file"""
        # Find config by resource UUID
        cfg_id = res_obj._id
        dev_cfg = cfg_dict.get(res_obj._id, None)
        if not dev_cfg and getattr(res_obj, "ooi_property_number", None):
            # Find config by resource OOI property number, e.g. 12345-54321
            cfg_id = res_obj.ooi_property_number
            dev_cfg = cfg_dict.get(cfg_id, None)
        if not dev_cfg:
            pre_id = self._get_alt_id(res_obj, "PRE")
            if pre_id:
                # Find config by resource's preload ID
                cfg_id = pre_id
                dev_cfg = cfg_dict.get(cfg_id, None)
                if not dev_cfg:
                    # Find config by resource's preload ID reference designator
                    # Note: this is bad because devices are not assigned to the same RD over time
                    cfg_id = pre_id.rsplit("_", 1)[0]
                    dev_cfg = cfg_dict.get(cfg_id, None)
        if not dev_cfg and getattr(res_obj, "serial_number", None):
            if res_obj.type_ == RT.InstrumentDevice:
                # Find config by instrument series + serial number, e.g. CDTBPN:123123
                model_obj = self.rr.read_object(res_obj._id, PRED.hasModel, id_only=False)
                cfg_id = "%s:%s" % (model_obj.series_id, res_obj.serial_number)
                dev_cfg = cfg_dict.get(cfg_id, None)
            elif res_obj.type_ == RT.PlatformDevice:
                # Find config by platform serial number, e.g. OOI-231
                cfg_id = res_obj.serial_number
                dev_cfg = cfg_dict.get(cfg_id, None)

        return cfg_id, dev_cfg

    def _add_attribute_row(self, row, cfg_dict):
        # Value per row format
        device_id = row["ID"]
        attr_name = row["Attribute"]
        param_name = row["Name"]
        param_value = row["Value"]
        param_type = row.get("Type", "str") or "str"
        if not param_name:
            log.warn("Row %s value %s has no name", device_id, attr_name, param_value)
            return

        target_dict = cfg_dict.setdefault(device_id, {})
        nested_attrs = attr_name.split(".") if attr_name else []
        for na in nested_attrs:
            if ":" in na:
                # Index in a list, 0-based e.g. field_name:0
                nan, naidx = na.rsplit(":", 1)
                target_list = target_dict.setdefault(nan, [])
                naidx = int(naidx)
                if len(target_list) < naidx+1:
                    # Expand list length
                    target_list[len(target_list):naidx] = [{}] * (naidx + 1 - len(target_list))
                target_dict = target_list[naidx]
            else:
                target_dict = target_dict.setdefault(na, {})
        target_dict[param_name] = get_typed_value(param_value, targettype=param_type)

    def _update_attributes(self, res_obj, res_cfg):
        """Changes resource attributes of given object based on content of the given config dict"""
        if not res_cfg:
            return
        for attr_name, attr_cfg in res_cfg.iteritems():
            if hasattr(res_obj, attr_name):
                if attr_name in {"_id", "_rev", "type_", "alt_ids", "lcstate", "availability"}:
                    log.warn("Attribute %s cannot be modified", attr_name)
                    continue
                if type(getattr(res_obj, attr_name)) != type(attr_cfg):
                    log.warn("Attribute %s incompatible type: %s, expected %s", attr_name, type(attr_cfg), type(getattr(res_obj, attr_name)))
                    continue
                if isinstance(attr_cfg, dict) and "_replace" in attr_cfg:
                    attr_cfg.pop("_replace")
                    if self.verbose:
                        log.debug("Change resource %s attribute %s: OLD=%s NEW=%s", res_obj._id, attr_name, getattr(res_obj, attr_name), attr_cfg)
                    setattr(res_obj, attr_name, attr_cfg)
                elif isinstance(attr_cfg, dict):
                    attr_val = getattr(res_obj, attr_name)
                    if self.verbose:
                        log.debug("Change resource %s attribute %s: OLD=%s NEW=%s", res_obj._id, attr_name, getattr(res_obj, attr_name), dict_merge(attr_val, attr_cfg))
                    dict_merge(attr_val, attr_cfg, inplace=True)
                else:
                    if self.verbose:
                        log.debug("Change resource %s attribute %s: OLD=%s NEW=%s", res_obj._id, attr_name, getattr(res_obj, attr_name), attr_cfg)
                    setattr(res_obj, attr_name, attr_cfg)

    def config_instance(self, agent_instance_id, resource_id):
        """Configure an agent instance"""
        if not agent_instance_id:
            return

        cfg_file = self.CFG.get("cfg", None)
        if not cfg_file:
            raise BadRequest("No cfg argument provided")

        if not self.cfg_mappings:
            self.cfg_mappings = {}

            with open(cfg_file, "rU") as f:
                reader = csv.DictReader(f, delimiter=',')
                for row in reader:
                    if "Attribute" in row:
                        self._add_attribute_row(row, self.cfg_mappings)
                    else:
                        # Parse_dict format, one row per AI resource
                        self.cfg_mappings[row["ID"]] = dict(
                            harvester_cfg=parse_dict(row.get("Harvester Config", "")),
                            parser_cfg=parse_dict(row.get("Parser Config", "")),
                            max_records=row.get("Records Per Granule", ""),
                            startup_config=parse_dict(row.get("Startup Config", "")),
                            port_agent_config=parse_dict(row.get("Port Agent Config", "")),
                            driver_config=parse_dict(row.get("Driver Config", "")),
                            alerts=[parse_dict(row.get("Alerts", ""))])

        res_obj = self.rr.read(resource_id)
        ai = self.rr.read(agent_instance_id)

        cfg_id, dev_cfg = self._get_resource_cfg(res_obj, self.cfg_mappings)

        if not dev_cfg:
            log.warn("Could not determine AI config for device %s", resource_id)
            return

        log.info("Setting config for device %s '%s': %s", cfg_id, ai.name, dev_cfg)

        if ai.type_ == RT.ExternalDatasetAgentInstance:
            # Special case treatment for EDAI to make entry easier
            if dev_cfg["harvester_cfg"]:
                if self.verbose:
                    log.debug("Change AI %s attribute driver_config.startup_config.harvester: OLD=%s NEW=%s", ai._id,
                              ai.driver_config.get("startup_config", {}).get("harvester", None), dev_cfg["harvester_cfg"])
                ai.driver_config.setdefault("startup_config", {})["harvester"] = dev_cfg["harvester_cfg"]
            if dev_cfg["parser_cfg"]:
                if self.verbose:
                    log.debug("Change AI %s attribute driver_config.startup_config.parser: OLD=%s NEW=%s", ai._id,
                              ai.driver_config.get("startup_config", {}).get("parser", None), dev_cfg["parser_cfg"])
                ai.driver_config.setdefault("startup_config", {})["parser"] = dev_cfg["parser_cfg"]
            if dev_cfg["max_records"]:
                if self.verbose:
                    log.debug("Change AI %s attribute driver_config.max_records: OLD=%s NEW=%s", ai._id,
                              ai.driver_config.get("max_records", None), dev_cfg["max_records"])
                ai.driver_config["max_records"] = int(dev_cfg["max_records"])

        self._update_attributes(ai, dev_cfg)

        if not self.dryrun:
            self.rr.update(ai)

    def set_attributes(self, agent_instance_id, resource_id):
        cfg_file = self.CFG.get("cfg", None)
        if not cfg_file:
            raise BadRequest("No cfg argument provided")

        if not self.cfg_mappings:
            self.cfg_mappings = {}

            with open(cfg_file, "rU") as f:
                reader = csv.DictReader(f, delimiter=',')
                for row in reader:
                    self._add_attribute_row(row, self.cfg_mappings)

        res_obj = self.rr.read(resource_id)
        cfg_id, dev_cfg = self._get_resource_cfg(res_obj, self.cfg_mappings)

        if not dev_cfg:
            log.warn("Could not determine attributes for resource %s", resource_id)
            return

        log.info("Setting attributes for resource %s '%s': %s", cfg_id, res_obj.name, dev_cfg)

        self._update_attributes(res_obj, dev_cfg)

        if not self.dryrun:
            self.rr.update(res_obj)

    def set_calibration(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        if res_obj.type_ != RT.InstrumentDevice:
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

        # Device has no reference designator - but use preload ID as reference designator
        cfg_id, dev_cfg = self._get_resource_cfg(res_obj, self.cfg_mappings)
        if not dev_cfg:
            log.warn("Instrument %s not found in calibrations file" % resource_id)
            return

        log.info("Setting calibration for device %s '%s': %s", resource_id, res_obj.name, dev_cfg)

        # Find parsed data product from device id
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, RT.DataProduct, id_only=False)
        dp_objs_filtered = [dp for dp in dp_objs if dp.processing_level_code == "Parsed"]
        for dp_obj in dp_objs_filtered:
            self._set_calibration_for_data_product(dp_obj, dev_cfg)

    def _set_calibration_for_data_product(self, dp_obj, dev_cfg):
        from ion.util.direct_coverage_utils import DirectCoverageAccess
        from coverage_model import SparseConstantType

        log.debug("Setting calibration for data product '%s'", dp_obj.name)
        dataset_ids, _ = self.rr.find_objects(dp_obj, PRED.hasDataset, id_only=True)
        publisher = EventPublisher(OT.InformationContentModifiedEvent)
        if not dataset_ids:
            data_product_management = DataProductManagementServiceProcessClient(process=self)
            log.debug(" Creating dataset for data product %s", dp_obj.name)
            if not self.dryrun:
                data_product_management.create_dataset_for_data_product(dp_obj._id, headers=self._get_system_actor_headers())
                dataset_ids, _ = self.rr.find_objects(dp_obj, PRED.hasDataset, id_only=True)
            if not dataset_ids:
                raise NotFound('No datasets were found for this data product, ensure that it was created')
        for dataset_id in dataset_ids:
            # Synchronize with ingestion
            with DirectCoverageAccess() as dca:
                cov = dca.get_editable_coverage(dataset_id)
                # Iterate over the calibrations
                for cal_name, contents in dev_cfg.iteritems():
                    if cal_name in cov.list_parameters() and isinstance(cov.get_parameter_context(cal_name).param_type, SparseConstantType):
                        value = float(contents['value'])
                        log.info(' Updating Calibrations for %s in %s', cal_name, dataset_id)
                        if not self.dryrun:
                            cov.set_parameter_values(cal_name, value)
                    else:
                        log.warn(" Calibration %s not found in dataset", cal_name)
                if not self.dryrun:
                    publisher.publish_event(origin=dataset_id, description="Calibrations Updated")
        publisher.close()
        log.info("Calibration set for data product '%s' in %s coverages", dp_obj.name, len(dataset_ids))

    def activate_persistence(self, agent_instance_id, resource_id):
        dpms = DataProductManagementServiceProcessClient(process=self)
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, id_only=False)
        for dp in dp_objs:
            try:
                if dpms.is_persisted(dp._id, headers=self._get_system_actor_headers()):
                    if self.force:
                        log.warn("DataProduct %s '%s' is currently persisted - continuing", dp._id, dp.name)
                    else:
                        log.warn("DataProduct %s '%s' is currently persisted", dp._id, dp.name)
                        continue
                log.info("Activating persistence for '%s'", dp.name)
                if not self.dryrun:
                    dpms.activate_data_product_persistence(dp._id, headers=self._get_system_actor_headers(), timeout=self.timeout)
            except Exception:
                self._log_error(agent_instance_id, resource_id, logexc=True,
                                msg="Could not activate persistence for dp_id=%s" % (dp._id))

    def suspend_persistence(self, agent_instance_id, resource_id):
        dpms = DataProductManagementServiceProcessClient(process=self)
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, id_only=False)
        for dp in dp_objs:
            try:
                log.info("Suspending persistence for '%s'", dp.name)
                if not self.dryrun:
                    dpms.suspend_data_product_persistence(dp._id, headers=self._get_system_actor_headers(), timeout=self.timeout)
            except Exception:
                self._log_error(agent_instance_id, resource_id, logexc=True,
                                msg="Could not suspend persistence for dp_id=%s" % (dp._id))
                if self.autoclean:
                    self._cleanup_persistence(dp)

    def cleanup_persistence(self, agent_instance_id, resource_id):
        dpms = DataProductManagementServiceProcessClient(process=self)
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, id_only=False)
        for dp in dp_objs:
            self._cleanup_persistence(dp)

    def _cleanup_persistence(self, data_product_obj):
        """Deletes remnant persistent records about activated persistence in the system"""
        # Cleanup ExchangeName, Subscription,
        try:
            log.info("Cleaning up persistence for '%s'", data_product_obj.name)
            count_sub, count_xn, count_ds = 0, 0, 0
            st_objs, _ = self.rr.find_objects(data_product_obj._id, PRED.hasStream, id_only=False)
            for st_obj in st_objs:
                if self.verbose:
                    log.debug("Current Stream %s persisted: %s", st_obj._id, st_obj.persisted)
                st_obj.persisted = False
                if not self.dryrun:
                    self.rr.update(st_obj)

                assocs = self.rr.find_associations(PRED.hasStream, object=st_obj._id, id_only=False)
                for assoc in assocs:
                    if assoc.st == RT.Dataset:
                        if self.verbose:
                            log.debug("Delete Stream association: %s", assoc)
                        if not self.dryrun:
                            self.rr.delete_association(assoc)
                            count_ds += 1

                # Delete Subscription, ExchangeName, Process
                ingcfg_id = data_product_obj.dataset_configuration_id
                if ingcfg_id:
                    sub_objs, _ = self.rr.find_objects(ingcfg_id, predicate=PRED.hasSubscription, id_only=False)
                    for sub_obj in sub_objs:
                        if self.rr.find_associations(subject=sub_obj._id, object=st_obj._id):
                            # TODO: Could attempt Exchange _unbind cleanup. Not needed for now

                            xn_ids, _ = self.rr.find_subjects(object=sub_obj._id, predicate=PRED.hasSubscription,
                                                               subject_type=RT.ExchangeName, id_only=True)
                            if xn_ids:
                                if self.verbose:
                                    log.debug("Delete ExchangeNames: %s", xn_ids)
                                if not self.dryrun:
                                    self.rr.rr_store.delete_mult(xn_ids)
                                    count_xn += len(xn_ids)

                            # Delete Subscription with all associations
                            if self.verbose:
                                log.debug("Delete Subscription: %s", sub_obj._id)
                            if not self.dryrun:
                                self.rr.delete(sub_obj._id)
                                count_sub += 1

            log.info("Persistence cleaned up for data product %s '%s': %s Subscription, %s ExchangeName, %s Stream assoc",
                     data_product_obj._id, data_product_obj.name, count_sub, count_xn, count_ds)

        except Exception:
            self._log_error(None, None, logexc=True,
                            msg="Could not cleanup persistence for dp_id=%s" % (data_product_obj._id))

    def cleanup_agent(self, agent_instance_id, resource_id):
        """Deletes remnant persistent records about running agents in the system"""
        # Cleanup directory entry
        agent_procs = self.container.directory.find_by_value('/Agents', 'resource_id', resource_id)
        if agent_procs:
            for ap in agent_procs:
                if self.verbose:
                    current_entry = self.container.directory.lookup("/Agents/%s" % ap.key)
                    log.debug("Current directory entry for %s: %s", "/Agents/%s" % ap.key, current_entry)
                if not self.dryrun:
                    self.container.directory.unregister_safe("/Agents", ap.key)
        if agent_procs:
            log.debug("Cleaned up agent directory for device %s", resource_id)

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
            ia_client.execute_resource(command=AgentCommand(command=DriverEvent.GAP_RECOVERY, args=[self.recover_start, self.recover_end]),
                                       headers=self._get_system_actor_headers())

            self._recover_data_status['success'].append(res_obj.name)
        except Exception as e:
            log.warn("Failed to start recovery process for %s", res_obj.name)
            log.warn("Exception: %s", e)
            self._recover_data_status['fail'].append("%s (%s)" % (res_obj.name, e))

    def _recover_data_post(self, resource_id, agent_instance_id):
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
            if not self.dryrun:
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
        if self.verbose:
            log.debug("Current saved state: %s", ai.saved_agent_state)
        ai.saved_agent_state = {}
        if not self.dryrun:
            self.rr.update(ai)
        if existed:
            log.info("Saved state cleared for device %s '%s'", resource_id, res_obj.name)

    def create_dataset(self, agent_instance_id, resource_id):
        # Find hasOutputProduct DataProducts
        # Execute call to create Dataset and coverage
        res_obj = self.rr.read(resource_id)
        dpms = DataProductManagementServiceProcessClient(process=self)

        # Find data products from device id
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, RT.DataProduct, id_only=False)
        for dp_obj in dp_objs:
            if self.verbose:
                log.debug("Create dataset for data product %s", dp_obj._id)
            if not self.dryrun:
                dpms.create_dataset_for_data_product(dp_obj._id, headers=self._get_system_actor_headers())

        log.info("Checked datasets for device %s '%s': %s", resource_id, res_obj.name, len(dp_objs))

    def delete_dataset(self, agent_instance_id, resource_id):
        """Deletes dataset and coverage files for all of a device's data products"""
        res_obj = self.rr.read(resource_id)
        dpms = DataProductManagementServiceProcessClient(process=self)

        # Find data products from device id
        count_ds = 0
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, RT.DataProduct, id_only=False)
        for dp_obj in dp_objs:
            if dpms.is_persisted(dp_obj._id, headers=self._get_system_actor_headers()):
                if self.force:
                    log.warn("DataProduct %s '%s' is currently persisted - continuing", dp_obj._id, dp_obj.name)
                else:
                    raise BadRequest("DataProduct %s '%s' is currently persisted. Use force=True to ignore", dp_obj._id, dp_obj.name)

            ds_objs, _ = self.rr.find_objects(dp_obj._id, PRED.hasDataset, RT.Dataset, id_only=False)
            for ds_obj in ds_objs:
                # Delete coverage
                cov_path = DatasetManagementService._get_coverage_path(ds_obj._id)
                if os.path.exists(cov_path):
                    log.info("Removing coverage tree at %s", cov_path)
                    if self.verbose:
                        try:
                            proc = os.popen('du -h "%s"' % cov_path)
                            log.debug(proc.read())
                        except Exception:
                            pass
                    if not self.dryrun:
                        shutil.rmtree(cov_path)
                else:
                    log.warn("Coverage path does not exist %s" % cov_path)

                # Delete Dataset and associations
                if self.verbose:
                    log.debug("Delete Dataset: %s", ds_obj._id)
                if not self.dryrun:
                    self.rr.delete(ds_obj._id)
                    count_ds += 1

        log.info("Datasets and coverages deleted for device %s '%s': %s", resource_id, res_obj.name, count_ds)

    def delete_all_data(self, agent_instance_id, resource_id):
        # Delete Dataset and coverage for all original DataProducts
        self.delete_dataset(agent_instance_id, resource_id)

        res_obj = self.rr.read(resource_id)

        # Find parsed data product from device id
        dpms = DataProductManagementServiceProcessClient(process=self)
        count_dp, count_sd, count_st = 0, 0, 0
        dp_objs, _ = self.rr.find_subjects(RT.DataProduct, PRED.hasSource, resource_id, id_only=False)
        for dp_obj in dp_objs:
            if dpms.is_persisted(dp_obj._id, headers=self._get_system_actor_headers()):
                if self.force:
                    log.warn("DataProduct %s '%s' is currently persisted - continuing", dp_obj._id, dp_obj.name)
                else:
                    raise BadRequest("DataProduct %s '%s' is currently persisted. Use force=True to ignore", dp_obj._id, dp_obj.name)

            # Find and delete Stream
            st_objs, _ = self.rr.find_objects(dp_obj._id, PRED.hasStream, RT.Stream, id_only=False)
            for st_obj in st_objs:
                if self.verbose:
                    log.debug("Delete Stream: %s", st_obj._id)
                if not self.dryrun:
                    self.rr.delete(st_obj._id)
                    count_st += 1

            # Find and delete StreamDefinition
            sd_objs, _ = self.rr.find_objects(dp_obj._id, PRED.hasStreamDefinition, RT.StreamDefinition, id_only=False)
            for sd_obj in sd_objs:
                if self.verbose:
                    log.debug("Delete StreamDefinition: %s", sd_obj._id)
                if not self.dryrun:
                    self.rr.delete(sd_obj._id)
                    count_sd += 1

            # Delete DataProduct
            if self.verbose:
                log.debug("Delete DataProduct: %s", dp_obj._id)
            if not self.dryrun:
                self.rr.delete(dp_obj._id)
                count_dp += 1

        log.info("Data resources deleted for device %s '%s': %s DataProduct, %s StreamDefinition, %s Stream",
                 resource_id, res_obj.name, count_dp, count_sd, count_st)

    def delete_all_device(self, agent_instance_id, resource_id):
        """Deletes all resources related to a device"""
        res_obj = self.rr.read(resource_id)
        if not isinstance(res_obj, Device):
            raise BadRequest("Resource is not a device but: %s" % res_obj.type_)

        # Check if agent is running
        try:
            client = ResourceAgentClient(resource_id, process=self)
            if self.force:
                log.warn("Agent for %s '%s' seems active - continuing", resource_id, res_obj.name)
            else:
                raise BadRequest("Agent for %s '%s' seems active. Use force=True to ignore" % (resource_id, res_obj.name))
        except NotFound:
            pass  # That's what we expect

        # Attempt to delete dataset, then all data resources. Fail if this fails.
        self.delete_all_data(agent_instance_id, resource_id)

        count_dev, count_dp, count_dep, count_ai = 0, 0, 0, 0

        # Delete DataProducers
        dp_ids, _ = self.rr.find_objects(resource_id, PRED.hasDataProducer, RT.DataProducer, id_only=True)
        if dp_ids:
            if self.verbose:
                log.debug("Delete DataProducers: %s", dp_ids)
            if not self.dryrun:
                self.rr.rr_store.delete_mult(dp_ids)
                count_dp += len(dp_ids)

        # Delete Deployment
        dep_ids, _ = self.rr.find_objects(resource_id, PRED.hasDeployment, RT.Deployment, id_only=True)
        if dep_ids:
            if self.verbose:
                log.debug("Delete Deployments: %s", dep_ids)
            if not self.dryrun:
                self.rr.rr_store.delete_mult(dep_ids)
                count_dep += len(dep_ids)

        # Delete ExternalDatasetAgentInstance, InstrumentAgentInstance, PlatformAgentInstance
        ai_ids, _ = self.rr.find_objects(resource_id, PRED.hasAgentInstance, id_only=True)
        if ai_ids:
            if self.verbose:
                log.debug("Delete AgentInstances: %s", ai_ids)
            if not self.dryrun:
                self.rr.rr_store.delete_mult(ai_ids)
                count_ai += len(ai_ids)

        # Delete Device
        if self.verbose:
            log.debug("Delete Device: %s", resource_id)
        if not self.dryrun:
            self.rr.delete(resource_id)
            count_dev += 1

        log.info("Device resources deleted for device %s '%s': %s Device, %s DataProducer, %s Deployment, %s AgentInstance",
                 resource_id, res_obj.name, count_dev, count_dp, count_dep, count_ai)

    def delete_site(self, agent_instance_id, resource_id):
        """Deletes site resources"""
        if not resource_id:
            log.warn("No resource id for op=%s", self.op)
            return

        res_obj = self.rr.read(resource_id)
        if not isinstance(res_obj, Site):
            raise BadRequest("Resource is not a site but: %s" % res_obj.type_)

        if self.verbose:
            log.debug("Delete Site resource: %s", res_obj._id)
        if not self.dryrun:
            self.rr.delete(res_obj._id)
        log.info("Site resources deleted for site %s '%s': 1 Site", resource_id, res_obj.name)

    def activate_deployment(self, agent_instance_id, resource_id):
        # For current device, find all deployments. Activate the one that
        dep_objs, _ = self.rr.find_objects(resource_id, PRED.hasDeployment, RT.Deployment, id_only=False)
        if dep_objs:
            current_dep = self._get_current_deployment(dep_objs, only_deployed=False)
            if current_dep:
                obs_ms = ObservatoryManagementServiceProcessClient(process=self)
                if not self.dryrun:
                    obs_ms.activate_deployment(current_dep._id, headers=self._get_system_actor_headers())

    def deactivate_deployment(self, agent_instance_id, resource_id):
        dep_objs, _ = self.rr.find_objects(resource_id, PRED.hasDeployment, RT.Deployment, id_only=False)
        if dep_objs:
            current_dep = self._get_current_deployment(dep_objs, only_deployed=True)
            if current_dep:
                obs_ms = ObservatoryManagementServiceProcessClient(process=self)
                if not self.dryrun:
                    obs_ms.deactivate_deployment(current_dep._id, headers=self._get_system_actor_headers())

    def _get_current_deployment(self, dep_objs, only_deployed=False):
        """Return the most likely current deployment from given list of Deployment resources
        # TODO: For a better way to find current active deployment wait for R3 M193 and then refactor
        # Procedure:
        # 1 Eliminate all RETIRED lcstate
        # 2 If only_deployed==True, eliminate all that are not DEPLOYED lcstate
        # 3 Eliminate all with illegal or missing TemporalBounds
        # 4 Eliminate past deployments (end date before now)
        # 5 Eliminate known future deployments (start date after now)
        # 6 Sort the remaining list by start date and return first (ambiguous!!)
        """
        now = time.time()
        filtered_dep = []
        for dep_obj in dep_objs:
            if dep_obj.lcstate == LCS.RETIRED:
                continue
            if only_deployed and dep_obj.lcstate != LCS.DEPLOYED:
                continue
            temp_const = [c for c in dep_obj.constraint_list if isinstance(c, TemporalBounds)]
            if not temp_const:
                continue
            temp_const = temp_const[0]
            if not temp_const.start_datetime or not temp_const.end_datetime:
                continue
            start_time = int(temp_const.start_datetime)
            if start_time > now:
                continue
            end_time = int(temp_const.end_datetime)
            if end_time > now:
                filtered_dep.append((start_time, end_time, dep_obj))

        if not filtered_dep:
            return None
        if len(filtered_dep) == 1:
            return filtered_dep[0][2]

        log.warn("Cannot determine current deployment unambiguously - choosing earliest start date")
        filtered_dep = sorted(filtered_dep, key=lambda x: x[0])
        return filtered_dep[0][2]

    def clone_device(self, agent_instance_id, resource_id):
        """Clones the given device with its associations and makes a few modifications"""
        cfg_file = self.CFG.get("cfg", None)
        if cfg_file and not self.cfg_mappings:
            self.cfg_mappings = {}

            with open(cfg_file, "rU") as f:
                reader = csv.DictReader(f, delimiter=',')
                for row in reader:
                    self._add_attribute_row(row, self.cfg_mappings)

        if not hasattr(self, "clone_map"):
            self.clone_map = {}   # Holds a map of old to new devices

        clone_id = self.CFG.get("clone_id", None)
        if not clone_id:
            raise BadRequest("Must provide clone_id argument")

        ims = InstrumentManagementServiceProcessClient(process=self)
        orgms = OrgManagementServiceProcessClient(process=self)
        dams = DataAcquisitionManagementServiceProcessClient(process=self)

        # ---------------------------------------------------------------------
        # (1) Clone device and associations
        res_obj = self.rr.read(resource_id)
        if not isinstance(res_obj, Device):
            raise BadRequest("Given resource %s is not a Device: %s" % (resource_id, res_obj.type_))

        dev_cfg_id, dev_cfg = self._get_resource_cfg(res_obj, self.cfg_mappings)
        dev_pre_id = self._get_alt_id(res_obj, "PRE")

        # Duplicate device using service call. Append name, desc, clear out serial
        dev_dict = res_obj.__dict__.copy()
        dev_type = dev_dict.pop("type_")
        for attr in ["_id", "_rev", "lcstate", "availability", "alt_ids"]:
            dev_dict.pop(attr)

        newdev_obj = IonObject(dev_type, **dev_dict)
        self._set_alt_id(newdev_obj, "CLONE:%s" % resource_id)
        if dev_pre_id:
            self._set_alt_id(newdev_obj, dev_pre_id + "_" + clone_id)
        newdev_obj.name = res_obj.name + " (cloned)"
        newdev_obj.description = "Cloned from %s" % resource_id
        newdev_obj.serial_number = ""
        newdev_obj.ooi_property_number = ""
        if newdev_obj.temporal_bounds:
            newdev_obj.temporal_bounds.start_datetime = str(time.time())
        else:
            newdev_obj.temporal_bounds = IonObject(OT.TemporalBounds)
            newdev_obj.temporal_bounds.start_datetime = str(time.time())
            newdev_obj.temporal_bounds.end_datetime = "2650838400"  # 2054-01-01

        # Apply any attribute updates to the clone, but found by the original resource
        self._update_attributes(newdev_obj, dev_cfg)

        # Create resource
        if res_obj.type_ == RT.InstrumentDevice:
            newdev_id = ims.create_instrument_device(newdev_obj, headers=self._get_system_actor_headers())
        else:
            newdev_id = ims.create_platform_device(newdev_obj, headers=self._get_system_actor_headers())
        self.clone_map[resource_id] = newdev_id
        log.debug("Cloned device %s as new device %s '%s'", resource_id, newdev_id, newdev_obj.name)
        if self.verbose:
            newdev_obj1 = self.rr.read(newdev_id)
            log.debug("Device details: %s", newdev_obj1)

        # Association to model
        model_id = self.rr.read_object(resource_id, PRED.hasModel, id_only=True)
        self.rr.create_association(newdev_id, PRED.hasModel, model_id)

        # Mirror associations to parent device
        try:
            parent_id = self.rr.read_subject(resource_id, PRED.hasDevice, id_only=True)
            if parent_id and parent_id in self.clone_map:
                # This cloned device is a child as part of a recurse
                self.rr.create_association(self.clone_map[parent_id], PRED.hasDevice, newdev_id)
        except NotFound:
            pass  # No parent found

        # Share in Orgs
        org_ids, _ = self.rr.find_subjects(RT.Org, PRED.hasResource, resource_id, id_only=True)
        for org_id in org_ids:
            if self.verbose:
                log.debug("Share cloned device %s in org %s", newdev_id, org_id)
            orgms.share_resource(org_id, newdev_id, headers=self._get_system_actor_headers())

        # ---------------------------------------------------------------------
        # (2) Clone agent instance and associations
        if agent_instance_id:
            ai_obj = self.rr.read(agent_instance_id)
            ai_cfg_id, ai_cfg = self._get_resource_cfg(res_obj, self.cfg_mappings)
            ai_pre_id = self._get_alt_id(ai_obj, "PRE")

            # Duplicate device using service call. Append name, desc, clear out serial
            ai_dict = ai_obj.__dict__.copy()
            ai_type = ai_dict.pop("type_")
            for attr in ["_id", "_rev", "lcstate", "availability", "alt_ids"]:
                ai_dict.pop(attr)

            newai_obj = IonObject(ai_type, **ai_dict)
            self._set_alt_id(newai_obj, "CLONE:%s" % agent_instance_id)
            if ai_pre_id:
                self._set_alt_id(newai_obj, ai_pre_id + "_" + clone_id)
            newai_obj.name = ai_obj.name + " (cloned)"
            newai_obj.description = "Cloned from %s" % agent_instance_id

            # Apply any attribute updates to the clone, but found by the original resource
            self._update_attributes(newai_obj, ai_cfg)

            adef_id = self.rr.read_object(agent_instance_id, PRED.hasAgentDefinition, id_only=True)

            # Create agent instance, association to agent definition and to device
            if ai_obj.type_ == RT.InstrumentAgentInstance:
                newai_id = ims.create_instrument_agent_instance(newai_obj,
                                                                instrument_agent_id=adef_id,
                                                                instrument_device_id=newdev_id,
                                                                headers=self._get_system_actor_headers())
            elif ai_obj.type_ == RT.PlatformAgentInstance:
                newai_id = ims.create_platform_agent_instance(newai_obj,
                                                              platform_agent_id=adef_id,
                                                              platform_device_id=newdev_id,
                                                              headers=self._get_system_actor_headers())
            else:
                newai_id = dams.create_external_dataset_agent_instance(newai_obj,
                                                                       external_dataset_agent_id=adef_id,
                                                                       external_dataset_id=None,
                                                                       headers=self._get_system_actor_headers())
                # Need to create association to device
                self.rr.create_association(newdev_id, PRED.hasAgentInstance, newai_id)
            log.debug("Cloned agent instance %s as new agent instance %s '%s'", agent_instance_id, newai_id, newai_obj.name)
            if self.verbose:
                newai_obj1 = self.rr.read(newai_id)
                log.debug("Agent instance details: %s", newai_obj1)

            # Share in Orgs
            org_ids, _ = self.rr.find_subjects(RT.Org, PRED.hasResource, agent_instance_id, id_only=True)
            for org_id in org_ids:
                if self.verbose:
                    log.debug("Share cloned agent instance %s in org %s", newai_id, org_id)
                orgms.share_resource(org_id, newai_id, headers=self._get_system_actor_headers())

        # To duplicate data products run incremental preload for this

    def clone_deployment(self, agent_instance_id, resource_id):
        clone_id = self.CFG.get("clone_id", None)
        if not clone_id:
            raise BadRequest("Must provide clone_id argument")

        res_obj = self.rr.read(resource_id)
        if not res_obj.type_ == RT.Deployment:
            raise BadRequest("Given resource %s is not a Deployment: %s" % (resource_id, res_obj.type_))
        dep_pre_id = self._get_alt_id(res_obj, "PRE")

        oms = ObservatoryManagementServiceProcessClient(process=self)
        orgms = OrgManagementServiceProcessClient(process=self)

        # Clone the Deployment resource and associations
        dep_dict = res_obj.__dict__.copy()
        for attr in ["_id", "_rev", "type_", "lcstate", "availability", "alt_ids"]:
            dep_dict.pop(attr)

        newdep_obj = IonObject(RT.Deployment, **dep_dict)
        self._set_alt_id(newdep_obj, "CLONE:%s" % resource_id)
        if dep_pre_id:
            self._set_alt_id(newdep_obj, dep_pre_id + "_" + clone_id)
        newdep_obj.name = res_obj.name + " (cloned)"
        newdep_obj.description = "Cloned from %s" % resource_id
        for const in newdep_obj.constraint_list:
            if const.type_ == OT.TemporalBounds:
                const.start_datetime = str(time.time())

        site_id = self.rr.read_subject(resource_id, PRED.hasDeployment, id_only=True)
        device_obj = self.rr.read_subject(resource_id, PRED.hasDeployment, id_only=False)

        # Determine device tree from current deployment device
        ou = ObservatoryUtil()
        dev_child_devices = ou.get_child_devices(device_obj._id)
        self.clone_map = {}
        def build_map(dev_id, dev_child_devices):
            dev_obj = self.rr.read(dev_id)
            dev_pre_id = self._get_alt_id(dev_obj, "PRE")
            if not dev_pre_id:
                raise BadRequest("Cannot find PRE id of device %s" % dev_id)
            newdev_obj = self._get_resource_by_alt_id("PRE", dev_pre_id + "_" + clone_id)
            if not newdev_obj:
                raise BadRequest("Cannot find cloned device for device %s" % dev_id)
            self.clone_map[dev_id] = newdev_obj._id
            for _, ch_id, _ in dev_child_devices.get(dev_id, []):
                build_map(ch_id, dev_child_devices)
        build_map(device_obj._id, dev_child_devices)

        newdev_id = self.clone_map[device_obj._id]

        # Revise port assignments
        new_port_assignments = {}
        for dev_id, dev_port in device_obj.port_assignments.iteritems():
            if dev_id in self.clone_map:
                new_port_assignments[self.clone_map[dev_id]] = dev_port
                if hasattr(dev_port, "parent_id"):
                    if dev_port.parent_id in self.clone_map:
                        dev_port.parent_id = self.clone_map[dev_port.parent_id]
                    else:
                        log.warn("Could not find parent device id %s in cloned device tree", dev_port.parent_id)
            else:
                log.warn("Could not find device id %s in cloned device tree", dev_id)

        # Create Deployment resource with associations
        newdep_id = oms.create_deployment(newdep_obj,
                                          site_id=site_id,
                                          device_id=newdev_id,
                                          headers=self._get_system_actor_headers())

        # Orgs
        org_ids, _ = self.rr.find_subjects(RT.Org, PRED.hasResource, resource_id, id_only=True)
        for org_id in org_ids:
            if self.verbose:
                log.debug("Share cloned deployment %s in org %s", newdep_id, org_id)
            orgms.share_resource(org_id, newdep_id, headers=self._get_system_actor_headers())

    def list_persistence(self):
        # Show ingestion streams, workers (active or not) etc
        pass

    def list_agents(self):
        # Show running agent instances (active or not)
        pass

    def show_dataset(self, agent_instance_id, resource_id):
        # Show details for a dataset (coverage)
        pass

ImportDataset = AgentControl
