#!/usr/bin/env python

"""
Control agents and related resources.
@see https://confluence.oceanobservatories.org/display/CIDev/R2+Agent+Use+Guide

See help below for available options and arguments.

Invoke via command line like this:
    bin/pycc -x ion.agents.agentctrl.AgentControl instrument='CTDPF' op=start
    bin/pycc -x ion.agents.agentctrl.AgentControl resource_id='uuid' op=start activate=False
    bin/pycc -x ion.agents.agentctrl.AgentControl agent_name='uuid' op=stop
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=start recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=config_instance cfg=file.csv recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl instrument='CTDPF' op=recover_data recover_start=0.0 recover_end=1.0
    bin/pycc -x ion.agents.agentctrl.AgentControl preload_id='CP02PMUI-WF001_PD' op=start
    and others (see below and Confluence page)

TODO:
- Force terminate agents and clean up
- Change owner of resource
- Change contact info, metadata of resource based on spreadsheet
"""

__author__ = 'Michael Meisinger, Ian Katz, Bill French, Luke Campbell'

import csv
import datetime
import os
import shutil
import time

from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from pyon.core.object import IonObjectBase
from pyon.public import RT, log, PRED, OT, ImmediateProcess, BadRequest, NotFound, LCS, AS, EventPublisher, dict_merge, IonObject

from ion.core.includes.mi import DriverEvent
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.sa.observatory.deployment_util import DeploymentUtil
from ion.services.sa.observatory.observatory_util import ObservatoryUtil
from ion.services.sa.observatory.deployment_util import DeploymentUtil

from ion.util.parse_utils import parse_dict, get_typed_value
from ion.util.datastore.resuse import ResourceUseInfo

from interface.objects import AgentCommand, Site, TemporalBounds, AgentInstance, AgentDefinition, Device, DeviceModel, \
    ProcessStateEnum
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceProcessClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

ARG_HELP = {
    "help":         "prints help for operation",
    "instrument":   "name or resource uuid of instrument device",
    "platform":     "name or resource uuid of platform device",
    "device_name":  "name or resource uuid of a device resource",
    "resource_id":  "resource uuid for any resource in the system",
    "agent_name":   "name of agent instance. Resource ids (uuid) can be used instead of names.",
    "preload_id":   "preload (PRE) id or comma separated list of preload ids of resources",
    "recurse":      "if True, execute op child devices/sites if existing",
    "fail_fast":    "if True, exit after the first exception. Otherwise log errors only",
    "recover_start": "floating point string representing seconds since 1900-01-01 00:00:00 (NTP64 Epoch)",
    "recover_end":  "floating point string representing seconds since 1900-01-01 00:00:00 (NTP64 Epoch)",
    "force":        "if True, ignore some warning conditions and move on or clear up",
    "autoclean":    "if True, try to clean up resources directly after failed operations",
    "verbose":      "if True, log more messages for detailed steps",
    "dryrun":       "if True, log attempted actions but don't execute them (use verbose=True to see many details)",
    "clone_id":     "provides suffix for a cloned preload id, e.g. CP02PMUI-WP001_PD -> CP02PMUI-WP001_PD_CLONE1",
    "attr_key":     "provides the name of an attribute to set",
    "attr_value":   "provides the value of an attribute to set",
    "cfg":          "name of a CSV file with lookup values",
    "activate":     "if True, puts agent into streaming mode after start (default: True)",
    "lcstate":      "target lcstate",
    "availability": "target availability state",
    "facility":     "a facility (Org) identified by governance name, preload id, name or uuid",
    "role":         "a user role identified by governance name, preload id, name or uuid within the facility (Org)",
    "user":         "a user or actor identified by name, preload ir or uuid",
    "agent":        "comma separate list of preload ids of agent definitions",
}

RES_ARG_LIST = ["resource_id", "preload_id"]
DEV_ARG_LIST = RES_ARG_LIST + ["instrument", "platform", "device_name", "agent_name"]
COMMON_ARG_LIST = ["help", "recurse", "fail_fast", "force", "verbose", "dryrun"]

OP_HELP = [
    ("start_agent", dict(
        alias=["start", "load"],
        opmsg="Start agent instance",
        opmsg_ext=["must provide a device or agent name or id",
                   "option: don't put it into streaming mode",
                   "option: provide start and stop date for instrument agent reachback recover"],
        args=DEV_ARG_LIST + ["activate"] + COMMON_ARG_LIST)),
    ("start", dict(
        opmsg="Alias for start_agent")),
    ("load", dict(
        opmsg="Alias for start_agent")),
    ("stop_agent", dict(
        alias=["stop"],
        opmsg="Stop agent instance",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("stop", dict(
        opmsg="Alias for stop_agent")),
    ("configure_instance", dict(
        opmsg="Update the AgentInstance driver_config using a config CSV lookup file",
        args=DEV_ARG_LIST + ["cfg"] + COMMON_ARG_LIST)),
    ("set_attributes", dict(
        opmsg="Update resource attributes using a CSV lookup file",
        args=DEV_ARG_LIST + ["cfg"] + COMMON_ARG_LIST)),
    ("activate_persistence", dict(
        opmsg="Activate persistence for the data products of the device",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("suspend_persistence", dict(
        opmsg="Suspend persistence for the data products of the device",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("cleanup_persistence", dict(
        opmsg="Delete remnant persistent records about activated persistence for a device in the system",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("cleanup_agent", dict(
        opmsg="Delete remnant persistent records about running agents for a device in the system",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("recover_data", dict(
        opmsg="Issue data reachback command for instrument agents",
        opmsg_ext=["requires start and stop dates"],
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("set_calibration", dict(
        opmsg="Add or replace calibration information for a device and its data products",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("clear_saved_state", dict(
        opmsg="Clear out the saved_agent_state in agent instance resource",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("clear_status", dict(
        opmsg="Clear out the device status",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("set_lcstate", dict(
        opmsg="Set resource lifecycle state",
        args=RES_ARG_LIST + ["lcstate"] + COMMON_ARG_LIST)),
    ("set_availability", dict(
        opmsg="Set resource availability state",
        args=RES_ARG_LIST + ["availability"] + COMMON_ARG_LIST)),
    ("share_resource", dict(
        opmsg="Share resource in given facility",
        args=RES_ARG_LIST + ["facility"] + COMMON_ARG_LIST)),
    ("unshare_resource", dict(
        opmsg="Remove resource from given facility",
        args=RES_ARG_LIST + ["facility"] + COMMON_ARG_LIST)),
    ("enroll_member", dict(
        opmsg="Add user as member to a facility (Org)",
        args=RES_ARG_LIST + ["facility"] + COMMON_ARG_LIST)),
    ("remove_member", dict(
        opmsg="Remove user as member of a facility (Org)",
        args=RES_ARG_LIST + ["facility"] + COMMON_ARG_LIST)),
    ("grant_role", dict(
        opmsg="For given user, grant a role in a facility (Org)",
        args=RES_ARG_LIST + ["facility", "role"] + COMMON_ARG_LIST)),
    ("revoke_role", dict(
        opmsg="For given user, revoke a role in a facility (Org)",
        args=RES_ARG_LIST + ["facility", "role"] + COMMON_ARG_LIST)),
    ("create_commitment", dict(
        opmsg="Set a commitment for a user and a resource in a facility (Org)",
        args=RES_ARG_LIST + ["facility"] + COMMON_ARG_LIST)),
    ("retire_commitment", dict(
        opmsg="Retire a commitment for a user and a resource in a facility (Org)",
        args=RES_ARG_LIST + ["facility"] + COMMON_ARG_LIST)),
    ("set_owner", dict(
        opmsg="Set the owner user/actor for given resource, replacing current owner if existing",
        args=RES_ARG_LIST + ["user"] + COMMON_ARG_LIST)),
    ("set_agentdef", dict(
        opmsg="Reassigns the agent definition",
        args=RES_ARG_LIST + ["agent"] + COMMON_ARG_LIST)),
    ("create_dataset", dict(
        opmsg="Create Dataset resource and coverage for a device, but don't activate ingestion worker",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("delete_dataset", dict(
        opmsg="Remove Dataset resource and coverage for a device",
        opmsg_ext=["can also be called on a Dataset resource directly"],
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("delete_all_data", dict(
        opmsg="Remove all device related DataProduct, StreamDefinition, Dataset, resources and coverages",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("delete_all_device", dict(
        opmsg="Remove all device related resources and all from delete_all_data",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("delete_all_device", dict(
        opmsg="Remove all device related resources and all from delete_all_data",
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("delete_site", dict(
        opmsg="Remove site resources",
        args=RES_ARG_LIST + COMMON_ARG_LIST)),
    ("activate_deployment", dict(
        opmsg="Activate a deployment",
        opmsg_ext=["if a device is provided, activate the current available deployment"],
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("deactivate_deployment", dict(
        opmsg="Deactivate a deployment",
        opmsg_ext=["if a device is provided, deactivate the current deployment"],
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("clone_device", dict(
        opmsg="Clone a device into a new device with similar associations, new agent instance and new data products.",
        opmsg_ext=["the clone_id determines the PRE id suffix used to uniquely identify cloned resources",
                   "a CSV file can provide attribute values for the cloned resources"],
        args=DEV_ARG_LIST + COMMON_ARG_LIST)),
    ("clone_deployment", dict(
        opmsg="Clone a deployment into a new deployment with similar associations.",
        opmsg_ext=["the clone_id determines the PRE id suffix used to uniquely identify cloned resources",
                   "a CSV file can provide attribute values for the cloned resources"],
        args=RES_ARG_LIST + COMMON_ARG_LIST)),
    ("list_persistence", dict(
        opmsg="Prints a report of currently active persistence",
        args=["verbose"])),
    ("list_agents", dict(
        opmsg="Prints a report of currently active agents",
        args=["verbose"])),
    ("list_containers", dict(
        opmsg="Prints a report of currently active containers",
        args=["verbose"])),
    ("list_services", dict(
        opmsg="Prints a report of currently active services",
        args=["verbose"])),
    ("show_use", dict(
        opmsg="Shows associations and attributes for a resource",
        args=DEV_ARG_LIST + ["verbose"])),
    ("show_dataset", dict(
        opmsg="Prints a report about a dataset (coverage)",
        args=RES_ARG_LIST + ["verbose"])),
    ("set_sys_attribute", dict(
        opmsg="Sets a system attribute, such as MI version in the directory",
        opmsg_ext=["must provide attr_key and attr_value"],
        args=RES_ARG_LIST + ["attr_key", "attr_value"])),
    ("help", dict(
        opmsg="Lists available operations",
        args=["verbose"])),
]

class AgentControl(ImmediateProcess):
    def on_start(self):
        log.info("======================== ION AgentControl ========================")
        self.rr = self.container.resource_registry

        self.op = self.CFG.get("op", "start")
        # Map op aliases to internal names
        self.op = dict(start="start_agent", load="start_agent", stop="stop_agent").get(self.op, self.op)
        if not self.op or self.op.startswith("_") or not hasattr(self, self.op):
            raise BadRequest("Operation %s unknown", self.op)
        log.info("OPERATION: %s", self.op)

        dataset_name = self.CFG.get("dataset", None)
        device_name = self.CFG.get("device_name", None) or self.CFG.get("instrument", None) or self.CFG.get("platform", None) or self.CFG.get("resource_id", None)
        agent_name = self.CFG.get("agent_name", None)
        resource_name = dataset_name or device_name or agent_name

        self.recurse = self.CFG.get("recurse", False)
        self.fail_fast = self.CFG.get("fail_fast", False)
        self.force = self.CFG.get("force", False)
        self.autoclean = self.CFG.get("autoclean", False)
        self.verbose = self.CFG.get("verbose", False)
        self.dryrun = self.CFG.get("dryrun", False)
        self.show_ophelp = self.CFG.get("help", False)
        self.timeout = self.CFG.get("timeout", 120)
        self.cfg_mappings = {}
        self.system_actor = None
        self.errors = []
        self._recover_data_status = {'ignored': [], 'success': [], 'fail': []}

        preload_id = self.CFG.get("preload_id", None)
        if self.op == "help":
            self.help()
            return
        elif self.show_ophelp:
            self._op_help()
            return
        elif preload_id:
            preload_ids = preload_id.split(",")
            for pid in preload_ids:
                res_obj = self._get_resource_by_alt_id("PRE", pid)
                if res_obj:
                    log.info("Found preload id=%s as %s '%s' id=%s", pid, res_obj.type_, res_obj.name, res_obj._id)
                    self._execute(res_obj)
                else:
                    log.warn("Preload id=%s not found!", pid)

        elif self.op in {"list_persistence", "list_agents", "list_containers", "list_services", "set_sys_attribute"}:
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
            #log.warn("Unexpected resource type: %s", resource.type_)
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

    def help(self):
        print "HELP for AgentControl operations:"
        max_wid = max(len(op_name) for op_name, _ in OP_HELP)
        for op_name, op_help in OP_HELP:
            op_msg = op_help.get("opmsg", "")
            print " %s:%s %s" % (op_name, " "*(max_wid-len(op_name)), op_msg)
            if self.verbose and op_help.get("opmsg_ext", ""):
                for ext_help in op_help["opmsg_ext"]:
                    print " "*(max_wid + 4), ext_help
        if not self.verbose:
            print "Set verbose=True to show help details."
        print "Run op=xxx help=True for help for a specific operation."


    def _op_help(self):
        op_helps = [(op_name, op_help) for op_name, op_help in OP_HELP if op_name == self.op]
        if not op_helps:
            print "No help found for op=%s" % self.op
            return
        op_name, op_help = op_helps[0]
        print "HELP for operation %s:" % self.op
        print op_help.get("opmsg", "")
        for ext_help in op_help.get("opmsg_ext", ""):
            print " ", ext_help
        op_args = op_help.get("args", None)
        if op_args:
            print "Available arguments:"
            max_wid = max(len(oa) for oa in op_args)
            for oa in op_args:
                arg_help = ARG_HELP.get(oa, "")
                print "  %s:%s %s" % (oa, " "*(max_wid-len(oa)), arg_help)

        if not self.verbose:
            print "Set verbose=True to show operation help details."

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
        cfg_id = getattr(res_obj, "_id", "?")
        dev_cfg = cfg_dict.get(cfg_id, None)
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
                    cfg_id = pre_id.split("_", 1)[0]
                    dev_cfg = cfg_dict.get(cfg_id, None)
        if not dev_cfg and getattr(res_obj, "serial_number", None):
            if res_obj.type_ == RT.InstrumentDevice:
                # Find config by instrument series + serial number, e.g. CDTBPN:123123
                model_obj = self.rr.read_object(res_obj._id, PRED.hasModel, id_only=False)
                cfg_id = "%s:%s" % (model_obj.series_id, res_obj.serial_number)
                dev_cfg = cfg_dict.get(cfg_id, None)
            elif res_obj.type_ == RT.PlatformDevice:
                # Find config by platform node type + serial number, e.g. WP:OOI-231
                model_obj = self.rr.read_object(res_obj._id, PRED.hasModel, id_only=False)
                ooi_id = self._get_alt_id(model_obj, "OOI") or "?"
                nodetype = ooi_id.split("_", 1)[0]
                cfg_id = "%s:%s" % (nodetype, res_obj.serial_number)
                dev_cfg = cfg_dict.get(cfg_id, None)

        return cfg_id, dev_cfg

    def _add_attribute_row(self, row, cfg_dict):
        """Parse a value row from a CSV row dict"""
        device_id = row["ID"]
        attr_name = row["Attribute"]
        param_name = row["Name"]
        param_value = row["Value"]
        param_type = row.get("Type", "str") or "str"
        ignore_row = row.get("Ignore", "") == "Yes"
        if ignore_row or not device_id or not (attr_name or param_name):
            # This is a comment or empty row
            return
        if not param_name:
            log.warn("Device %s row %s value %s has no name", device_id, attr_name, param_value)
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
        resource_id = getattr(res_obj, "_id", "")
        for attr_name, attr_cfg in res_cfg.iteritems():
            if hasattr(res_obj, attr_name):
                if attr_name in {"_id", "_rev", "type_", "alt_ids", "lcstate", "availability"}:
                    log.warn("Attribute %s cannot be modified", attr_name)
                    continue
                if isinstance(attr_cfg, dict) and isinstance(res_obj, IonObjectBase):
                    pass
                elif type(getattr(res_obj, attr_name)) != type(attr_cfg):
                    log.warn("Attribute %s incompatible type: %s, expected %s", attr_name, type(attr_cfg), type(getattr(res_obj, attr_name)))
                    continue

                attr_val = getattr(res_obj, attr_name)
                if isinstance(attr_cfg, dict) and isinstance(attr_val, IonObjectBase):
                    # TODO: Nested objects
                    for an, av in attr_cfg.iteritems():
                        setattr(attr_val, an, av)
                elif isinstance(attr_cfg, dict) and "_replace" in attr_cfg:
                    attr_cfg.pop("_replace")
                    if self.verbose:
                        log.debug("Change resource %s attribute %s: OLD=%s NEW=%s", resource_id, attr_name, getattr(res_obj, attr_name), attr_cfg)
                    setattr(res_obj, attr_name, attr_cfg)
                elif isinstance(attr_cfg, dict):
                    if self.verbose:
                        log.debug("Change resource %s attribute %s: OLD=%s NEW=%s", resource_id, attr_name, getattr(res_obj, attr_name), dict_merge(attr_val, attr_cfg))
                    dict_merge(attr_val, attr_cfg, inplace=True)
                else:
                    if self.verbose:
                        log.debug("Change resource %s attribute %s: OLD=%s NEW=%s", resource_id, attr_name, getattr(res_obj, attr_name), attr_cfg)
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

    def _parse_attributes_cfg(self, require_cfg=True):
        self.cfg_mappings = {}

        cfg_file = self.CFG.get("cfg", None)
        if not cfg_file:
            if require_cfg:
                raise BadRequest("No cfg argument provided")
            else:
                return

        with open(cfg_file, "rU") as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                self._add_attribute_row(row, self.cfg_mappings)

    def set_attributes(self, agent_instance_id, resource_id):
        self._parse_attributes_cfg()

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

    def set_lcstate(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        target_lcs = self.CFG.get("lcstate", None)
        if target_lcs not in LCS:
            raise BadRequest("Illegal lcstate: %s" % target_lcs)
        if self.verbose:
            log.debug("Current resource %s '%s' lcstate: %s", resource_id, res_obj.name, res_obj.lcstate)
            log.debug("Setting resource %s '%s' lcstate: %s", resource_id, res_obj.name, target_lcs)
        if not self.dryrun:
            self.rr.set_lifecycle_state(resource_id, target_lcs)

    def set_availability(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        target_as = self.CFG.get("availability", None)
        if target_as not in AS:
            raise BadRequest("Illegal availability: %s" % target_as)
        if self.verbose:
            log.debug("Current resource %s '%s' availability: %s", resource_id, res_obj.name, res_obj.availability)
            log.debug("Setting resource %s '%s' availability: %s", resource_id, res_obj.name, target_as)
        if not self.dryrun:
            self.rr.set_lifecycle_state(resource_id, target_as)

    def _get_org(self, facility):
        """Return an org_id for a given Org identifying string, or None if not found"""
        # Org governance name
        res_list, _ = self.rr.find_resources_ext(restype=RT.Org, attr_name="org_governance_name", attr_value=facility, id_only=False)
        if res_list and res_list[0].type_ == RT.Org:
            return res_list[0]._id
        # Org resource name
        res_list, _ = self.rr.find_resources(restype=RT.Org, name=facility, id_only=False)
        if res_list and res_list[0].type_ == RT.Org:
            return res_list[0]._id
        # Preload ID
        res_list, _ = self.rr.find_resources_ext(restype=RT.Org, alt_id_ns="PRE", alt_id=facility, id_only=False)
        if res_list and res_list[0].type_ == RT.Org:
            return res_list[0]._id
        # resource uuid
        try:
            res_obj = self.rr.read(facility)
            if res_obj.type_ == RT.Org:
                return res_obj._id
        except NotFound:
            pass

    def share_resource(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        facility = self.CFG.get("facility", None)
        org_id = self._get_org(facility)
        if not org_id:
            raise BadRequest("Facility (Org) %s not found" % facility)
        if self.verbose:
            log.debug("Sharing resource %s '%s' in facility: %s '%s'", resource_id, res_obj.name, org_id, facility)
        if not self.dryrun:
            obs_ms = ObservatoryManagementServiceProcessClient(process=self)
            obs_ms.assign_resource_to_observatory_org(resource_id, org_id, headers=self._get_system_actor_headers())

    def unshare_resource(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        facility = self.CFG.get("facility", None)
        org_id = self._get_org(facility)
        if not org_id:
            raise BadRequest("Facility (Org) %s not found" % facility)
        if self.verbose:
            log.debug("Removing resource %s '%s' from facility: %s '%s'", resource_id, res_obj.name, org_id, facility)
        if not self.dryrun:
            obs_ms = ObservatoryManagementServiceProcessClient(process=self)
            obs_ms.unassign_resource_from_observatory_org(resource_id, org_id, headers=self._get_system_actor_headers())

    def enroll_member(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        if res_obj.type_ == RT.UserInfo:
            actor_obj = self.rr.read_subject(RT.ActorIdentity, PRED.hasInfo, resource_id, id_only=False)
        elif res_obj.type_ == RT.ActorIdentity:
            actor_obj = res_obj
        else:
            raise BadRequest("Resource not a user or actor: %s %s", resource_id, res_obj.type_)
        facility = self.CFG.get("facility", None)
        org_id = self._get_org(facility)
        if not org_id:
            raise BadRequest("Facility (Org) %s not found" % facility)
        if self.verbose:
            log.debug("Adding user/actor %s '%s' to facility: %s '%s'", resource_id, res_obj.name, org_id, facility)
        if not self.dryrun:
            orgms = OrgManagementServiceProcessClient(process=self)
            orgms.enroll_member(org_id, actor_obj._id, headers=self._get_system_actor_headers())

    def remove_member(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        if res_obj.type_ == RT.UserInfo:
            actor_obj = self.rr.read_subject(RT.ActorIdentity, PRED.hasInfo, resource_id, id_only=False)
        elif res_obj.type_ == RT.ActorIdentity:
            actor_obj = res_obj
        else:
            raise BadRequest("Resource not a user or actor: %s %s", resource_id, res_obj.type_)
        facility = self.CFG.get("facility", None)
        org_id = self._get_org(facility)
        if not org_id:
            raise BadRequest("Facility (Org) %s not found" % facility)
        if self.verbose:
            log.debug("Removing user/actor %s '%s' from facility: %s '%s'", resource_id, res_obj.name, org_id, facility)
        if not self.dryrun:
            orgms = OrgManagementServiceProcessClient(process=self)
            orgms.cancel_member_enrollment(org_id, actor_obj._id, headers=self._get_system_actor_headers())

    def _get_role_in_org(self, role, org_id):
        """Return a role governance name for a given role identifying string, or None if not found"""
        role_list, _ = self.rr.find_objects(org_id, PRED.hasRole, RT.UserRole, id_only=False)
        if not role_list:
            return
        for ur in role_list:
            if ur.governance_name == role:
                return ur.governance_name
            elif ur.name == role:
                return ur.governance_name
            elif ur._id == role:
                return ur.governance_name
            elif self._get_alt_id(ur, "PRE") == role:
                return ur.governance_name

    def grant_role(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        if res_obj.type_ == RT.UserInfo:
            actor_obj = self.rr.read_subject(RT.ActorIdentity, PRED.hasInfo, resource_id, id_only=False)
        elif res_obj.type_ == RT.ActorIdentity:
            actor_obj = res_obj
        else:
            raise BadRequest("Resource not a user or actor: %s %s", resource_id, res_obj.type_)
        facility = self.CFG.get("facility", None)
        org_id = self._get_org(facility)
        if not org_id:
            raise BadRequest("Facility (Org) %s not found" % facility)
        role = self.CFG.get("role", None)
        role_id = self._get_role_in_org(role, org_id)
        if not role_id:
            raise BadRequest("Role %s not found in facility %s" % (role, facility))
        if self.verbose:
            log.debug("Granting role %s to user/actor %s '%s' in facility: %s '%s'", role, resource_id, res_obj.name, org_id, facility)
        if not self.dryrun:
            orgms = OrgManagementServiceProcessClient(process=self)
            orgms.grant_role(org_id, actor_obj._id, role_id, headers=self._get_system_actor_headers())

    def revoke_role(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        if res_obj.type_ == RT.UserInfo:
            actor_obj = self.rr.read_subject(RT.ActorIdentity, PRED.hasInfo, resource_id, id_only=False)
        elif res_obj.type_ == RT.ActorIdentity:
            actor_obj = res_obj
        else:
            raise BadRequest("Resource not a user or actor: %s %s", resource_id, res_obj.type_)
        facility = self.CFG.get("facility", None)
        org_id = self._get_org(facility)
        if not org_id:
            raise BadRequest("Facility (Org) %s not found" % facility)
        role = self.CFG.get("role", None)
        role_id = self._get_role_in_org(role, org_id)
        if not role_id:
            raise BadRequest("Role %s not found in facility %s" % (role, facility))
        if self.verbose:
            log.debug("Revoking role %s for user/actor %s '%s' in facility: %s '%s'", role, resource_id, res_obj.name, org_id, facility)
        if not self.dryrun:
            orgms = OrgManagementServiceProcessClient(process=self)
            orgms.revoke_role(org_id, actor_obj._id, role_id, headers=self._get_system_actor_headers())

    def create_commitment(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        facility = self.CFG.get("facility", None)
        org_id = self._get_org(facility)
        if not org_id:
            raise BadRequest("Facility (Org) %s not found" % facility)

        # Need actor, commitment type, additional: validity
        raise NotImplemented()

    def retire_commitment(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        facility = self.CFG.get("facility", None)
        org_id = self._get_org(facility)
        if not org_id:
            raise BadRequest("Facility (Org) %s not found" % facility)
        raise NotImplemented()

    def _get_actor(self, user):
        """Return an actor id for given user or actor identifying string, or None if not found"""
        def get_actor_from_list(rlist):
            for res in rlist:
                if res.type_ == RT.UserInfo:
                    actor_obj = self.rr.read_subject(RT.ActorIdentity, PRED.hasInfo, res._id, id_only=False)
                    return actor_obj._id
                elif res.type_ == RT.ActorIdentity:
                    return res._id
        # User/actor resource preload id
        res_list, _ = self.rr.find_resources_ext(alt_id_ns="PRE", alt_id=user, id_only=False)
        actor_id = get_actor_from_list(res_list)
        if actor_id:
            return actor_id
        # User/actor resource name
        res_list, _ = self.rr.find_resources(name=user, id_only=False)
        actor_id = get_actor_from_list(res_list)
        if actor_id:
            return actor_id
        # resource uuid
        try:
            res_obj = self.rr.read(user)
            actor_id = get_actor_from_list([res_obj])
            if actor_id:
                return actor_id
        except NotFound:
            pass

    def set_owner(self, agent_instance_id, resource_id):
        res_obj = self.rr.read(resource_id)
        user = self.CFG.get("user", None)
        actor_id = self._get_actor(user)
        if not actor_id:
            raise BadRequest("User/actor %s not found" % user)
        owner_list, _ = self.rr.find_objects(resource_id, PRED.hasOwner, id_only=False)
        if self.verbose:
            if owner_list:
                log.debug("Current resource %s '%s' owners: %s", resource_id, res_obj.name, [o._id for o in owner_list])
            log.debug("Setting owner %s '%s' for resource: %s '%s'", user, actor_id, resource_id, res_obj.name)
        if not self.dryrun:
            for owner in owner_list:
                assoc = self.rr.get_association(resource_id, PRED.hasOwner, owner._id)
                self.rr.delete_association(assoc)
            self.rr.create_association(resource_id, PRED.hasOwner, actor_id)

    def set_agentdef(self, agent_instance_id, resource_id):
        if not agent_instance_id:
            return
        res_obj = self.rr.read(resource_id)
        ai_obj = self.rr.read(agent_instance_id)
        agents = self.CFG.get("agent", None)
        if not agents:
            raise BadRequest("Must provide agent argument")
        cur_adef_obj = self.rr.read_object(agent_instance_id, PRED.hasAgentDefinition, id_only=False)
        cur_adef_model_obj = self.rr.read_object(cur_adef_obj._id, PRED.hasModel, id_only=False)

        # Iterate through the list of agents and see if one matches by model
        for agent in agents.split(","):
            adefs, _ = self.rr.find_resources_ext(alt_id_ns="PRE", alt_id=agent, id_only=False)
            if not adefs:
                continue
            adef_obj = adefs[0]
            if not isinstance(adef_obj, AgentDefinition):
                raise BadRequest("Provide agent ID %s is not an AgentDefinition" % agent)
            if cur_adef_obj._id == adef_obj._id:
                if self.verbose:
                    log.debug("Current Device %s '%s' (AI %s) AgentDefinition already assigned: %s '%s'", resource_id, res_obj.name,
                              agent_instance_id, cur_adef_obj._id, cur_adef_obj.name)
                    return

            adef_model_obj = self.rr.read_object(cur_adef_obj._id, PRED.hasModel, id_only=False)
            if cur_adef_model_obj._id == adef_model_obj._id:
                if self.verbose:
                    log.debug("Current Device %s '%s' (AI %s) AgentDefinition: %s '%s'", resource_id, res_obj.name,
                              agent_instance_id, cur_adef_obj._id, cur_adef_obj.name)
                cur_assoc = self.rr.get_association(agent_instance_id, PRED.hasAgentDefinition, cur_adef_obj._id, id_only=True)
                if not self.dryrun:
                    self.rr.delete_association(cur_assoc)

                log.info("Reassign Device %s '%s' (AI %s) to AgentDefinition: %s '%s' (matching model '%s')", resource_id, res_obj.name,
                          agent_instance_id, adef_obj._id, adef_obj.name, cur_adef_model_obj.name)
                if not self.dryrun:
                    self.rr.create_association(agent_instance_id, PRED.hasAgentDefinition, adef_obj._id)
                break

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

        count_ds = 0
        if res_obj.type_ == RT.Dataset:
            self._delete_dataset(resource_id)
            count_ds += 1
        elif isinstance(res_obj, Device):
            # Find data products from device id
            dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, RT.DataProduct, id_only=False)
            for dp_obj in dp_objs:
                if dpms.is_persisted(dp_obj._id, headers=self._get_system_actor_headers()):
                    if self.force:
                        log.warn("DataProduct %s '%s' is currently persisted - continuing", dp_obj._id, dp_obj.name)
                    else:
                        raise BadRequest("DataProduct %s '%s' is currently persisted. Use force=True to ignore", dp_obj._id, dp_obj.name)

                ds_ids, _ = self.rr.find_objects(dp_obj._id, PRED.hasDataset, RT.Dataset, id_only=True)
                for ds_id in ds_ids:
                    self._delete_dataset(ds_id)

        log.info("Datasets and coverages deleted for device %s '%s': %s", resource_id, res_obj.name, count_ds)

    def _delete_dataset(self, dataset_id):
        # Delete coverage
        cov_path = DatasetManagementService._get_coverage_path(dataset_id)
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
            log.debug("Delete Dataset: %s", dataset_id)
        if not self.dryrun:
            self.rr.delete(dataset_id)

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
        obs_ms = ObservatoryManagementServiceProcessClient(process=self)
        dep_util = DeploymentUtil(self.container)
        res_obj = self.rr.read(resource_id)
        if res_obj.type_ == RT.Deployment:
            obs_ms.activate_deployment(resource_id, headers=self._get_system_actor_headers())
        elif isinstance(res_obj, Device):
            # For current device, find all deployments. Activate the one that
            dep_objs, _ = self.rr.find_objects(resource_id, PRED.hasDeployment, RT.Deployment, id_only=False)
            if self.verbose:
                log.debug("Device %s has %s deployments", resource_id, len(dep_objs))
            if dep_objs:
                current_dep = dep_util.get_current_deployment(dep_objs, only_deployed=False, best_guess=self.force)
                if current_dep:
                    log.info("Activating deployment %s", current_dep._id)
                    if not self.dryrun:
                        obs_ms.activate_deployment(current_dep._id, headers=self._get_system_actor_headers())

    def deactivate_deployment(self, agent_instance_id, resource_id):
        obs_ms = ObservatoryManagementServiceProcessClient(process=self)
        dep_util = DeploymentUtil(self.container)
        res_obj = self.rr.read(resource_id)
        if res_obj.type_ == RT.Deployment:
            obs_ms.deactivate_deployment(resource_id, headers=self._get_system_actor_headers())
        elif isinstance(res_obj, Device):
            dep_objs, _ = self.rr.find_objects(resource_id, PRED.hasDeployment, RT.Deployment, id_only=False)
            if self.verbose:
                log.debug("Device %s has %s deployments", resource_id, len(dep_objs))
            if dep_objs:
                current_dep = dep_util.get_current_deployment(dep_objs, only_deployed=True, best_guess=self.force)
                if current_dep:
                    log.info("Deactivating deployment %s", current_dep._id)
                    if not self.dryrun:
                        obs_ms.deactivate_deployment(current_dep._id, headers=self._get_system_actor_headers())

    def _clone_res_obj(self, res_obj, clone_id):
        res_pre_id = self._get_alt_id(res_obj, "PRE")
        resnew_pre_id = res_pre_id + "_" + clone_id
        if self._get_resource_by_alt_id("PRE", resnew_pre_id):
            raise BadRequest("%s %s clone %s already exists", res_obj.type_, res_obj._id, resnew_pre_id)

        res_dict = res_obj.__dict__.copy()
        res_type = res_dict.pop("type_")
        for attr in ["_id", "_rev", "lcstate", "availability", "alt_ids"]:
            res_dict.pop(attr)

        newres_obj = IonObject(res_type, **res_dict)
        self._set_alt_id(newres_obj, "CLONE:%s" % res_obj._id)
        if res_pre_id:
            self._set_alt_id(newres_obj, "PRE:" + resnew_pre_id)
        newres_obj.name = res_obj.name + " (cloned)"
        newres_obj.description = "Cloned from %s" % res_obj._id

        return newres_obj, res_pre_id, resnew_pre_id

    def _clone_org_share(self, res_id, newres_id):
        orgms = OrgManagementServiceProcessClient(process=self)
        # Share in Orgs
        org_ids, _ = self.rr.find_subjects(RT.Org, PRED.hasResource, res_id, id_only=True)
        for org_id in org_ids:
            if self.verbose:
                log.debug("Share cloned resource %s in org %s", newres_id, org_id)
            orgms.share_resource(org_id, newres_id, headers=self._get_system_actor_headers())

    def _clone_device(self, res_obj, clone_id):
        ims = InstrumentManagementServiceProcessClient(process=self)

        resource_id = res_obj._id
        newdev_obj, dev_pre_id, devnew_pre_id = self._clone_res_obj(res_obj, clone_id)
        newdev_obj.serial_number = ""
        newdev_obj.ooi_property_number = ""
        if newdev_obj.temporal_bounds:
            newdev_obj.temporal_bounds.start_datetime = str(int(time.time()))
        else:
            newdev_obj.temporal_bounds = IonObject(OT.TemporalBounds)
            newdev_obj.temporal_bounds.start_datetime = str(int(time.time()))
            newdev_obj.temporal_bounds.end_datetime = "2650838400"  # 2054-01-01

        cfg_id, dev_cfg = self._get_resource_cfg(newdev_obj, self.cfg_mappings)
        self._update_attributes(newdev_obj, dev_cfg)
        if newdev_obj.serial_number and "serial#" in newdev_obj.name:
            newdev_obj.name = newdev_obj.name.split("serial#", 1)[0] + "serial# " + newdev_obj.serial_number

        # Create resource
        if res_obj.type_ == RT.InstrumentDevice:
            newdev_id = ims.create_instrument_device(newdev_obj, headers=self._get_system_actor_headers())
        else:
            newdev_id = ims.create_platform_device(newdev_obj, headers=self._get_system_actor_headers())
        self.clone_map[resource_id] = newdev_id
        log.debug("Cloned device %s %s as: %s %s '%s'", resource_id,
                  dev_pre_id, newdev_id, devnew_pre_id, newdev_obj.name)
        if self.verbose:
            newdev_obj1 = self.rr.read(newdev_id)
            log.debug("Device details: %s", newdev_obj1)

        # Association to model
        model_id = self.rr.read_object(resource_id, PRED.hasModel, id_only=True)
        self.rr.create_association(newdev_id, PRED.hasModel, model_id)

        # Mirror associations to parent device
        try:
            parent_id = self.rr.read_subject(RT.PlatformDevice, PRED.hasDevice, resource_id, id_only=True)
            if parent_id and parent_id in self.clone_map:
                # This cloned device is a child as part of a recurse
                self.rr.create_association(self.clone_map[parent_id], PRED.hasDevice, newdev_id)
        except NotFound:
            pass  # No parent found

        # Share in Orgs
        self._clone_org_share(resource_id, newdev_id)

        return newdev_id, newdev_obj

    def _clone_agent_instance(self, ai_obj, clone_id, newdev_id, newdev_obj):
        ims = InstrumentManagementServiceProcessClient(process=self)
        dams = DataAcquisitionManagementServiceProcessClient(process=self)

        agent_instance_id = ai_obj._id
        newai_obj, ai_pre_id, ainew_pre_id = self._clone_res_obj(ai_obj, clone_id)
        if newai_obj.type_ == RT.ExternalDatasetAgentInstance:
            newai_obj.driver_config = dict(startup_config=dict(parser={}, harvester={}), max_records=50)
        else:
            newai_obj.driver_config = {}
        newai_obj.saved_agent_state = {}

        cfg_id, ai_cfg = self._get_resource_cfg(newai_obj, self.cfg_mappings)
        self._update_attributes(newai_obj, ai_cfg)
        if newdev_obj.serial_number and "serial#" in newai_obj.name:
            newai_obj.name = newai_obj.name.split("serial#", 1)[0] + "serial# " + newdev_obj.serial_number

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
        log.debug("Cloned agent instance %s %s as: %s %s '%s'", agent_instance_id,
                  ai_pre_id, newai_id, ainew_pre_id, newai_obj.name)
        if self.verbose:
            newai_obj1 = self.rr.read(newai_id)
            log.debug("Agent instance details: %s", newai_obj1)

        # Share in Orgs
        self._clone_org_share(agent_instance_id, newai_id)

        return newai_id, newai_obj

    def _clone_data_product(self, dp_obj, clone_id, is_output=True, parent_dp_id=None, device_id=None, newdev_obj=None):
        dpms = DataProductManagementServiceProcessClient(process=self)
        psms = PubsubManagementServiceProcessClient(process=self)

        newdp_obj, dp_pre_id, dpnew_pre_id = self._clone_res_obj(dp_obj, clone_id)

        cfg_id, dp_cfg = self._get_resource_cfg(newdp_obj, self.cfg_mappings)
        self._update_attributes(newdp_obj, dp_cfg)
        if newdev_obj and newdev_obj.serial_number and "serial#" in newdp_obj.name:
            newdp_obj.name = newdp_obj.name.split("serial#", 1)[0] + "serial# " + newdev_obj.serial_number + ")"

        # Duplicate StreamDefinition
        sdef_obj = self.rr.read_object(dp_obj._id, PRED.hasStreamDefinition, RT.StreamDefinition, id_only=False)
        pdict_id = self.rr.read_object(sdef_obj._id, PRED.hasParameterDictionary, RT.ParameterDictionary, id_only=True)

        newsdef_id = psms.create_stream_definition(name=sdef_obj.name + " (cloned)",
                                                   parameter_dictionary_id=pdict_id,
                                                   stream_configuration=sdef_obj.stream_configuration,
                                                   available_fields=sdef_obj.available_fields,
                                                   headers=self._get_system_actor_headers())

        # Update alt_ids (StreamDefinition has a non-compliant create signature)
        newsdef_obj = self.rr.read(newsdef_id)
        newsdef_obj.description = "Cloned from %s" % sdef_obj._id
        res_pre_id = self._get_alt_id(sdef_obj, "PRE")
        resnew_pre_id = res_pre_id + "_" + clone_id
        self._set_alt_id(newsdef_obj, "CLONE:%s" % sdef_obj._id)
        if res_pre_id:
            self._set_alt_id(newsdef_obj, "PRE:" + resnew_pre_id)
        newsdef_obj.addl["stream_use"] = sdef_obj.addl.get("stream_use", "")

        cfg_id, sdef_cfg = self._get_resource_cfg(newsdef_obj, self.cfg_mappings)
        self._update_attributes(newsdef_obj, sdef_cfg)

        self.rr.update(newsdef_obj)

        log.debug("Cloned StreamDefinition %s %s as: %s %s '%s'", sdef_obj._id,
                  res_pre_id, newsdef_id, resnew_pre_id, newsdef_obj.name)

        # Create agent instance, association to agent definition and to device
        newdp_id = dpms.create_data_product(newdp_obj,
                                            stream_definition_id=newsdef_id,
                                            parent_data_product_id=parent_dp_id,
                                            headers=self._get_system_actor_headers())

        log.debug("Cloned data product %s %s as: %s %s '%s'", dp_obj._id,
                  dp_pre_id, newdp_id, dpnew_pre_id, newdp_obj.name)
        if self.verbose:
            newdp_obj1 = self.rr.read(newdp_id)
            log.debug("Data product details: %s", newdp_obj1)

        # Share in Orgs
        self._clone_org_share(dp_obj._id, newdp_id)

        # Associations hasOutputProduct
        if device_id:
            self.rr.create_association(newdp_id, PRED.hasSource, device_id)
            if self.verbose:
                log.debug(" Created association DP %s %s Device %s", newdp_id, PRED.hasSource, device_id)
            if is_output:
                self.rr.create_association(device_id, PRED.hasOutputProduct, newdp_id)
                if self.verbose:
                    log.debug(" Created association Device %s %s DP %s", device_id, PRED.hasOutputProduct, newdp_id)

        # Association hasSource
        targ_objs, _ = self.rr.find_objects(dp_obj._id, PRED.hasSource, id_only=False)
        for targ_obj in targ_objs:
            if isinstance(targ_obj, Site):
                self.rr.create_association(newdp_id, PRED.hasSource, targ_obj._id)
                if self.verbose:
                    log.debug(" Created association Site %s %s DP %s", newdp_id, PRED.hasSource, targ_obj._id)

        # Create dataset
        if is_output:
            dpms.create_dataset_for_data_product(newdp_id, headers=self._get_system_actor_headers())
            log.debug(" Created dataset for data product %s %s '%s'", newdp_id, dpnew_pre_id, newdp_obj.name)

        return newdp_id, newdp_obj

    # Potential Issues: Names etc attributes are not final at time of create, but DataProducer, Stream etc
    # names are derived from these names.
    # - Stream.routing_key is derived from DP name. Issue?
    def clone_device(self, agent_instance_id, resource_id):
        """Clones the given device with its associations and makes a few modifications"""
        if not hasattr(self, "clone_map"):
            self.clone_map = {}   # Holds a map of old to new devices

        clone_id = self.CFG.get("clone_id", None)
        if not clone_id:
            raise BadRequest("Must provide clone_id argument")

        self._parse_attributes_cfg(require_cfg=False)

        # (1) Clone device and associations
        res_obj = self.rr.read(resource_id)
        if not isinstance(res_obj, Device):
            raise BadRequest("Given resource %s is not a Device: %s" % (resource_id, res_obj.type_))

        newdev_id, newdev_obj = self._clone_device(res_obj, clone_id)

        # (2) Clone agent instance and associations
        if agent_instance_id:
            ai_obj = self.rr.read(agent_instance_id)
            newai_id, newai_obj = self._clone_agent_instance(ai_obj, clone_id, newdev_id, newdev_obj)

        # (3) Clone data products: output data products with derived data products
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, RT.DataProduct, id_only=False)
        for dp_obj in dp_objs:
            newdp_id, newdp_obj = self._clone_data_product(dp_obj, clone_id, device_id=newdev_id, newdev_obj=newdev_obj)

            # Clone derived data products for an output data product
            dp_objs1, _ = self.rr.find_subjects(RT.DataProduct, PRED.hasDataProductParent, dp_obj._id, id_only=False)
            for dp_obj1 in dp_objs1:
                self._clone_data_product(dp_obj1, clone_id, is_output=False, parent_dp_id=newdp_id, device_id=newdev_id, newdev_obj=newdev_obj)

    def _clone_deployment(self, res_obj, clone_id):
        oms = ObservatoryManagementServiceProcessClient(process=self)

        resource_id = res_obj._id
        newdep_obj, dep_pre_id, depnew_pre_id = self._clone_res_obj(res_obj, clone_id)
        for const in newdep_obj.constraint_list:
            if const.type_ == OT.TemporalBounds:
                const.start_datetime = str(int(time.time()))

        cfg_id, dep_cfg = self._get_resource_cfg(newdep_obj, self.cfg_mappings)
        self._update_attributes(newdep_obj, dep_cfg)

        try:
            site_id = self.rr.read_subject(RT.PlatformSite, PRED.hasDeployment, resource_id, id_only=True)
        except NotFound:
            site_id = self.rr.read_subject(RT.InstrumentSite, PRED.hasDeployment, resource_id, id_only=True)
        try:
            device_obj = self.rr.read_subject(RT.PlatformDevice, PRED.hasDeployment, resource_id, id_only=False)
        except NotFound:
            device_obj = self.rr.read_subject(RT.InstrumentDevice, PRED.hasDeployment, resource_id, id_only=False)

        if device_obj.serial_number and "serial#" in newdep_obj.name and " to site " in newdep_obj.name:
            part1 = newdep_obj.name.split("serial#", 1)[0]
            part2 = newdep_obj.name.split(" to site ", 1)[1]
            newdep_obj.name = part1 + "serial# " + device_obj.serial_number + " to site " + part2

        # Determine device tree from current deployment device
        # TODO: This does NOT work if the cloned device assembly was not cloned as a full tree
        # In this case there is no way guessing which instruments belong to which ports without a mapping csv
        ou = ObservatoryUtil()
        dev_child_devices = ou.get_child_devices(device_obj._id)
        self.clone_map = {}
        def build_map(dev_id, dev_child_devices):
            dev_obj = self.rr.read(dev_id)
            dev_pre_id = self._get_alt_id(dev_obj, "PRE")
            if dev_pre_id:
                newdev_obj = self._get_resource_by_alt_id("PRE", dev_pre_id + "_" + clone_id)
                if newdev_obj:
                    self.clone_map[dev_id] = newdev_obj._id
                else:
                    if self.force:
                        log.warn("Cannot find cloned device for device %s" % dev_id)
                    else:
                        raise BadRequest("Cannot find cloned device for device %s" % dev_id)
            else:
                if self.force:
                    log.warn("Cannot find PRE id of device %s" % dev_id)
                else:
                    raise BadRequest("Cannot find PRE id of device %s" % dev_id)
            for _, ch_id, _ in dev_child_devices.get(dev_id, []):
                build_map(ch_id, dev_child_devices)
        build_map(device_obj._id, dev_child_devices)

        newdev_id = self.clone_map[device_obj._id]

        # Revise port assignments
        new_port_assignments = {}
        for dev_id, dev_port in res_obj.port_assignments.iteritems():
            if dev_id in self.clone_map:
                new_port_assignments[self.clone_map[dev_id]] = dev_port
                if hasattr(dev_port, "parent_id"):
                    if dev_port.parent_id in self.clone_map:
                        dev_port.parent_id = self.clone_map[dev_port.parent_id]
                    else:
                        log.warn("Could not find parent device id %s in cloned device tree", dev_port.parent_id)
            else:
                if self.force:
                    log.warn("Could not find device id %s in cloned device tree", dev_id)
                else:
                    raise BadRequest("Could not find device id %s in cloned device tree" % dev_id)
        newdep_obj.port_assignments = new_port_assignments
        if self.verbose:
            log.debug("Cloned port_assignments: %s", newdep_obj.port_assignments)

        # Create Deployment resource with associations
        newdep_id = oms.create_deployment(newdep_obj,
                                          site_id=site_id,
                                          device_id=newdev_id,
                                          headers=self._get_system_actor_headers())

        log.debug("Cloned deployment %s %s as: %s %s '%s'", resource_id,
                  dep_pre_id, newdep_id, depnew_pre_id, newdep_obj.name)

        # Orgs
        self._clone_org_share(resource_id, newdep_id)

        return newdep_id, newdep_obj

    def clone_deployment(self, agent_instance_id, resource_id):
        self._parse_attributes_cfg(require_cfg=False)

        clone_id = self.CFG.get("clone_id", None)
        if not clone_id:
            raise BadRequest("Must provide clone_id argument")

        res_obj = self.rr.read(resource_id)
        if not res_obj.type_ == RT.Deployment:
            raise BadRequest("Given resource %s is not a Deployment: %s" % (resource_id, res_obj.type_))

        self._clone_deployment(res_obj, clone_id)

    def list_agents(self):
        res_info = ResourceUseInfo(self.container, self.verbose)
        res_info.list_agents()

    def list_containers(self):
        res_info = ResourceUseInfo(self.container, self.verbose)
        res_info.list_containers()

    def list_services(self):
        res_info = ResourceUseInfo(self.container, self.verbose)
        res_info.list_services()

    def list_persistence(self):
        res_info = ResourceUseInfo(self.container, self.verbose)
        res_info.list_persistence()

    def show_use(self, agent_instance_id, resource_id):
        res_info = ResourceUseInfo(self.container, self.verbose)
        res_info.show_use(resource_id)

    def show_dataset(self, agent_instance_id, resource_id):
        res_info = ResourceUseInfo(self.container, self.verbose)
        res_info.show_dataset(resource_id)

    def set_sys_attribute(self):
        sys_attrs = self.container.directory.lookup("/System")
        if self.verbose:
            log.debug("Current system attributes: %s", sys_attrs)
        attr_name = self.CFG.get("attr_key", "")
        if not attr_name:
            log.warn("System attribute name missing")
            return
        attr_val = self.CFG.get("attr_value", None)
        if attr_val is None:
            log.warn("System attribute %s value missing", attr_name)
            return
        if not sys_attrs:
            sys_attrs = {}
        if attr_val == "DELETE":
            sys_attrs.pop(attr_name, None)
        else:
            sys_attrs[attr_name] = attr_val
        self.container.directory.register("/", "System", **sys_attrs)

ImportDataset = AgentControl
