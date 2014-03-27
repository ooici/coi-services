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
- delete_all_site: remove all device resources as in delete_all_device and matching site resources
- activate_deployment: If device has Deployments, activate the current one
- deactivate_deployment: If device has an active Deployment, deactivate it

Supports the following arguments:
- instrument, platform, device_name: name of device. Resource ids (uuid) can be used instead of names.
- agent_name: name of agent instance. Resource ids (uuid) can be used instead of names.
- preload_id: preload id or list of preload ids of devices
- recurse: given platform device, execute op on platform and all child platforms and instruments
- fail_fast: if True, exit after the first exception. Otherwise log errors only
- recover_start, recover_end: floating point strings representing seconds since 1900-01-01 00:00:00 (NTP64 Epoch)
- force: if True, ignore some warning conditions and move on or clear up
- verbose: if True, log more messages for detailed steps

Invoke via command line like this:
    bin/pycc -x ion.agents.agentctrl.AgentControl instrument='CTDPF'
    bin/pycc -x ion.agents.agentctrl.AgentControl device_name='uuid' op=start activate=False
    bin/pycc -x ion.agents.agentctrl.AgentControl agent_name='uuid' op=stop
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=start recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=config_instance cfg=file.csv recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl platform='uuid' op=set_calibration cfg=file.csv recurse=True
    bin/pycc -x ion.agents.agentctrl.AgentControl instrument='CTDPF' op=recover_data recover_start=0.0 recover_end=1.0
    and others (see above and Confluence page)

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
import os
import shutil
import time

from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from pyon.public import RT, log, PRED, OT, ImmediateProcess, BadRequest, NotFound, LCS, EventPublisher

from ion.core.includes.mi import DriverEvent
from ion.util.parse_utils import parse_dict

from interface.objects import AgentCommand, Site, TemporalBounds, AgentInstance, Device
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceProcessClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceProcessClient

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService


class AgentControl(ImmediateProcess):
    def on_start(self):
        log.info("======================== ION AgentControl ========================")
        self.rr = self.container.resource_registry

        self.op = self.CFG.get("op", "start")
        if not self.op or self.op.startswith("_") or (not hasattr(self, self.op) and self.op not in {"start", "stop", "load"}):
            raise BadRequest("Operation %s unknown", self.op)

        dataset_name = self.CFG.get("dataset", None)
        device_name = self.CFG.get("device_name", None) or self.CFG.get("instrument", None) or self.CFG.get("platform", None)
        agent_name = self.CFG.get("agent_name", None)
        resource_name = dataset_name or device_name or agent_name

        self.recurse = self.CFG.get("recurse", False)
        self.fail_fast = self.CFG.get("fail_fast", False)
        self.force = self.CFG.get("force", False)
        self.verbose = self.CFG.get("verbose", False)
        self.cfg_mappings = {}
        self.system_actor = None
        self.errors = []
        self._recover_data_status = {'ignored': [], 'success': [], 'fail': []}

        preload_id = self.CFG.get("preload_id", None)
        if preload_id:
            preload_ids = preload_id.split(",")
            for pid in preload_ids:
                res_objs, _ = self.rr.find_resources_ext(alt_id_ns="PRE", alt_id=pid, id_only=False)
                if res_objs:
                    res_obj = res_objs[0]
                    log.info("Found preload id=%s as %s '%s' id=%s", pid, res_obj.type_, res_obj.name, res_obj._id)
                    self._execute(res_obj)

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
        else:
            log.warn("Unexpected resource type: %s", resource.type_)

        # This does all the work and will recurse if desired
        self._execute_op(ai_id, resource_id)

    def _execute_op(self, agent_instance_id, resource_id):
        """Executes op on one resource/agent including recursive applications."""
        child_devices = None  # Need to save this for the case the device is deleted during the op
        try:
            if resource_id is None and agent_instance_id:
                resource_id = self.rr.read_subject(object=agent_instance_id,
                                                   predicate=PRED.hasAgentInstance,
                                                   id_only=True)

            res_obj = self.rr.read(resource_id)
            child_devices, _ = self.rr.find_objects(resource_id, PRED.hasDevice, id_only=False)

            if agent_instance_id is None:
                agent_instance_id = self._get_agent_instance_id(resource_id)

            log.info("--- Executing %s on '%s' id=%s agent=%s ---", self.op, res_obj.name, resource_id, agent_instance_id)


            if self.op == "start" or self.op == "load":
                self.start_agent(agent_instance_id, resource_id)
            elif self.op == "stop":
                self.stop_agent(agent_instance_id, resource_id)
            elif hasattr(self, self.op):
                opfunc = getattr(self, self.op)
                opfunc(agent_instance_id, resource_id)

        except Exception:
            self._log_error(agent_instance_id, resource_id, logexc=True,
                            msg="Failed op=%s on device=%s agent=%s" % (self.op, resource_id, agent_instance_id))
            if self.fail_fast:
                raise

        if self.recurse:
            if child_devices is None:
                child_devices, _ = self.rr.find_objects(resource_id, PRED.hasDevice, id_only=False)
            if child_devices:
                log.debug("recurse==True. Executing op=%s on %s child devices", self.op, len(child_devices))
            for ch_obj in child_devices:
                ch_id = ch_obj._id
                agent_instance_id = None
                try:
                    self._execute_op(None, ch_id)
                except Exception:
                    self._log_error(agent_instance_id, resource_id, logexc=True,
                                    msg="Failed op=%s on child device=%s agent=%s" % (self.op, ch_id, agent_instance_id))
                    if self.fail_fast:
                        raise

        if self.op == "recover_data":
            self._recover_data_report()

    def _get_agent_instance_id(self, resource_id):
        aids, _ = self.rr.find_objects(subject=resource_id,
                                       predicate=PRED.hasAgentInstance,
                                       id_only=True)

        if len(aids) > 1:
            raise BadRequest("Multiple agent instances found")
        if aids:
            log.debug("Found agent instance ID: %s", aids[0])
            return aids[0]

        log.warn("Agent instance not found for device=%s", resource_id)
        return None

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
            log.warn("Could not %s agent %s for device %s", self.op, agent_instance_id, resource_id)
            return

        res_obj = self.rr.read(resource_id)

        log.info('Starting agent...')
        if res_obj.type_ == RT.ExternalDatasetAgentInstance or res_obj == RT.ExternalDataset:
            dams = DataAcquisitionManagementServiceProcessClient(process=self)
            dams.start_external_dataset_agent_instance(agent_instance_id, headers=self._get_system_actor_headers())
        elif res_obj.type_ == RT.InstrumentDevice:
            ims = InstrumentManagementServiceProcessClient(process=self)
            ims.start_instrument_agent_instance(agent_instance_id, headers=self._get_system_actor_headers())
        elif res_obj.type_ == RT.PlatformDevice:
            ims = InstrumentManagementServiceProcessClient(process=self)
            ims.start_platform_agent_instance(agent_instance_id, headers=self._get_system_actor_headers())
        else:
            BadRequest("Attempt to start unsupported agent type: %s", res_obj.type_)
        log.info('Agent started!')

        activate = self.CFG.get("activate", True)
        if activate:
            log.info('Activating agent...')
            client = ResourceAgentClient(resource_id, process=self)
            client.execute_agent(AgentCommand(command=ResourceAgentEvent.INITIALIZE), headers=self._get_system_actor_headers())
            client.execute_agent(AgentCommand(command=ResourceAgentEvent.GO_ACTIVE), headers=self._get_system_actor_headers())
            client.execute_agent(AgentCommand(command=ResourceAgentEvent.RUN), headers=self._get_system_actor_headers())
            client.execute_resource(command=AgentCommand(command=DriverEvent.START_AUTOSAMPLE), headers=self._get_system_actor_headers())

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
                dams.stop_external_dataset_agent_instance(agent_instance_id, headers=self._get_system_actor_headers())
            except NotFound:
                log.warn("Agent for resource %s not found", resource_id)
        elif res_obj.type_ == RT.InstrumentDevice:
            ims = InstrumentManagementServiceProcessClient(process=self)
            try:
                ims.stop_instrument_agent_instance(agent_instance_id, headers=self._get_system_actor_headers())
            except NotFound:
                log.warn("Agent for resource %s not found", resource_id)
        elif res_obj.type_ == RT.PlatformDevice:
            ims = InstrumentManagementServiceProcessClient(process=self)
            try:
                ims.stop_platform_agent_instance(agent_instance_id, headers=self._get_system_actor_headers())
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
        alt_ids = []
        for aid in res_obj.alt_ids:
            if aid.startswith('PRE:') and aid.endswith('_ID'):
                alt_ids.append(aid[4:-3])
            elif aid.startswith('PRE:ID'):
                alt_ids.append(aid[4:])

        device_rd = alt_ids[0] if alt_ids else None



        dev_cfg = self.cfg_mappings.get(resource_id, None) or self.cfg_mappings.get(device_rd, None)
        if not dev_cfg:
            raise NotFound("Could not reference designator %s in %s" % (self.cfg_mappings, device_rd))
        log.info("Setting calibration for device %s (RD %s) '%s': %s", resource_id, device_rd, res_obj.name, dev_cfg)

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
                        cov.set_parameter_values(cal_name, value)
                    else:
                        log.warn(" Calibration %s not found in dataset", cal_name)
                publisher.publish_event(origin=dataset_id, description="Calibrations Updated")
        publisher.close()
        log.info("Calibration set for data product '%s' in %s coverages", dp_obj.name, len(dataset_ids))

    def activate_persistence(self, agent_instance_id, resource_id):
        if not resource_id:
            log.warn("Could not %s for device %s", self.op, resource_id)
            return

        dpms = DataProductManagementServiceProcessClient(process=self)
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, id_only=False)
        for dp in dp_objs:
            try:
                log.info("Activating persistence for '%s'", dp.name)
                dpms.activate_data_product_persistence(dp._id, headers=self._get_system_actor_headers())
            except Exception:
                self._log_error(agent_instance_id, resource_id, logexc=True,
                                msg="Could not activate persistence for dp_id=%s" % (dp._id))

    def suspend_persistence(self, agent_instance_id, resource_id):
        if not resource_id:
            log.warn("Could not op=%s for device %s", self.op, resource_id)
            return

        dpms = DataProductManagementServiceProcessClient(process=self)
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, id_only=False)
        for dp in dp_objs:
            try:
                log.info("Suspending persistence for '%s'", dp.name)
                dpms.suspend_data_product_persistence(dp._id, headers=self._get_system_actor_headers())
            except Exception:
                self._log_error(agent_instance_id, resource_id, logexc=True,
                                msg="Could not suspend persistence for dp_id=%s" % (dp._id))
                if self.force:
                    self._cleanup_persistence(dp)

    def cleanup_persistence(self, agent_instance_id, resource_id):
        if not resource_id:
            log.warn("Could not op=%s for device %s", self.op, resource_id)
            return

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
                st_obj.persisted = False
                self.rr.update(st_obj)

                assocs = self.rr.find_associations(PRED.hasStream, object=st_obj._id, id_only=False)
                for assoc in assocs:
                    if assoc.st == RT.Dataset:
                        self.rr.delete_association(assoc)
                        count_ds += 1

                # Delete Subscription, ExchangeName, Process
                ingcfg_id = data_product_obj.dataset_configuration_id
                if ingcfg_id:
                    sub_objs, _ = self.rr.find_objects(ingcfg_id, predicate=PRED.hasSubscription, id_only=False)
                    for sub_obj in sub_objs:
                        if self.rr.find_associations(subject=sub_obj._id, object=st_obj._id):
                            # TODO: Could attempt Exchange _unbind cleanup. Not needed for now

                            # Association between IngestionConfiguration and Subscription deleted with Subscription
                            # Association between Subscription and Stream deleted with Subscription

                            xn_ids, _ = self.rr.find_subjects(object=sub_obj._id, predicate=PRED.hasSubscription,
                                                               subject_type=RT.ExchangeName, id_only=True)
                            if xn_ids:
                                self.rr.rr_store.delete_mult(xn_ids)
                                count_xn += len(xn_ids)

                            # Delete Subscription with all associations
                            self.rr.delete(sub_obj._id)
                            count_sub += 1

            log.info("Persistence cleaned up for data product %s '%s': %s Subscription, %s ExchangeName, %s Stream assoc",
                     data_product_obj._id, data_product_obj.name, count_sub, count_xn, count_ds)

        except Exception:
            self._log_error(None, None, logexc=True,
                            msg="Could not cleanup persistence for dp_id=%s" % (data_product_obj._id))

    def cleanup_agent(self, agent_instance_id, resource_id):
        """Deletes remnant persistent records about running agents in the system"""
        if not resource_id:
            log.warn("Could not op=%s for device %s", self.op, resource_id)
            return

        # Cleanup directory entry
        agent_procs = self.container.directory.find_by_value('/Agents', 'resource_id', resource_id)
        if agent_procs:
            for ap in agent_procs:
                self.container.directory.unregister_safe("/Agents", ap.key)

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
        res_obj = self.rr.read(resource_id)
        dpms = DataProductManagementServiceProcessClient(process=self)

        # Find data products from device id
        dp_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, RT.DataProduct, id_only=False)
        for dp_obj in dp_objs:
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
                    raise BadRequest("DataProduct %s '%s' is currently persisted", dp_obj._id, dp_obj.name)

            ds_objs, _ = self.rr.find_objects(dp_obj._id, PRED.hasDataset, RT.Dataset, id_only=False)
            for ds_obj in ds_objs:
                # Delete coverage
                cov_path = DatasetManagementService._get_coverage_path(ds_obj._id)
                if os.path.exists(cov_path):
                    log.info("Removing coverage tree at %s", cov_path)
                    shutil.rmtree(cov_path)
                else:
                    if self.force:
                        log.warn("Coverage path does not exist %s" % cov_path)
                    else:
                        raise OSError("Coverage path does not exist %s" % cov_path)

                # Delete Dataset and associations
                self.rr.delete(ds_obj._id)
                count_ds += 1

        log.info("Datasets and coverages deleted for device %s '%s': %s", resource_id, res_obj.name, count_ds)

    def delete_all_data(self, agent_instance_id, resource_id):
        try:
            # Delete Dataset and coverage for all original DataProducts
            self.delete_dataset(agent_instance_id, resource_id)
        except Exception:
            self._log_error(agent_instance_id, resource_id, logexc=True,
                            msg="Could not delete dataset")

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

    def delete_all_device(self, agent_instance_id, resource_id):
        """Deletes all resources related to a device"""
        try:
            self.delete_all_data(agent_instance_id, resource_id)
        except Exception:
            self._log_error(agent_instance_id, resource_id, logexc=True,
                            msg="Could not delete all data")

        res_obj = self.rr.read(resource_id)

        # Find parsed data product from device id
        count_dev, count_dp, count_dep, count_ai = 0, 0, 0, 0

        # Delete DataProducers
        dp_ids, _ = self.rr.find_objects(resource_id, PRED.hasDataProducer, RT.DataProducer, id_only=True)
        if dp_ids:
            self.rr.rr_store.delete_mult(dp_ids)
            count_dp += len(dp_ids)

        # Delete Deployment
        dep_ids, _ = self.rr.find_objects(resource_id, PRED.hasDeployment, RT.Deployment, id_only=True)
        if dep_ids:
            self.rr.rr_store.delete_mult(dep_ids)
            count_dep += len(dep_ids)

        # Delete ExternalDatasetAgentInstance, InstrumentAgentInstance, PlatformAgentInstance
        ai_ids, _ = self.rr.find_objects(resource_id, PRED.hasAgentInstance, id_only=True)
        if ai_ids:
            self.rr.rr_store.delete_mult(ai_ids)
            count_ai += len(ai_ids)

        # Delete Device
        self.rr.delete(resource_id)
        count_dev += 1

        log.info("Device resources deleted for device %s '%s': %s Device, %s DataProducer, %s Deployment, %s AgentInstance",
                 resource_id, res_obj.name, count_dev, count_dp, count_dep, count_ai)

    def delete_all_site(self, agent_instance_id, resource_id):
        """Deletes all resources related to a device and its matching preload site"""
        res_obj = self.rr.read(resource_id)

        alt_ids = [aid[4:-3] for aid in res_obj.alt_ids if aid.startswith("PRE:") and (aid.endswith("_ID") or aid.endswith("_PD"))]
        device_rd = alt_ids[0] if alt_ids else None

        # Delete Site
        if device_rd:
            count_site = 0
            site_objs, _ = self.rr.find_resources_ext(alt_id_ns="PRE", alt_id=device_rd, id_only=False)
            if site_objs:
                site_obj = site_objs[0]
                if isinstance(site_obj, Site):
                    self.rr.delete(site_obj._id)
                    count_site += 1

            log.info("Site resources deleted for device %s '%s': %s Site",
                     resource_id, res_obj.name, count_site)

        # Delete all device resources
        self.delete_all_device(agent_instance_id, resource_id)

    def activate_deployment(self, agent_instance_id, resource_id):
        # For current device, find all deployments. Activate the one that
        dep_objs, _ = self.rr.find_objects(resource_id, PRED.hasDeployment, RT.Deployment, id_only=False)
        if dep_objs:
            current_dep = self._get_current_deployment(dep_objs, for_activate=True)
            if current_dep:
                obs_ms = ObservatoryManagementServiceProcessClient(process=self)
                obs_ms.activate_deployment(current_dep._id, headers=self._get_system_actor_headers())

    def deactivate_deployment(self, agent_instance_id, resource_id):
        dep_objs, _ = self.rr.find_objects(resource_id, PRED.hasDeployment, RT.Deployment, id_only=False)
        if dep_objs:
            current_dep = self._get_current_deployment(dep_objs, for_activate=False)
            if current_dep:
                obs_ms = ObservatoryManagementServiceProcessClient(process=self)
                obs_ms.deactivate_deployment(current_dep._id, headers=self._get_system_actor_headers())

    def _get_current_deployment(self, dep_objs, for_activate=True):
        # TODO: How to find current deployment - wait for R3 M193
        # First pass: Eliminate all RETIRED and past deployments (end date before now)
        now = time.time()
        filtered_dep = []
        for dep_obj in dep_objs:
            if dep_obj.lcstate == LCS.RETIRED:
                continue
            temp_const = [c for c in dep_obj.constraint_list if isinstance(c, TemporalBounds)]
            if not temp_const:
                continue
            temp_const = temp_const[0]
            if not temp_const.start_datetime or not temp_const.end_datetime:
                continue
            start_time = int(temp_const.start_datetime)
            end_time = int(temp_const.end_datetime)
            if end_time > now:
                filtered_dep.append((start_time, end_time, dep_obj))

        if not filtered_dep:
            return None
        if len(filtered_dep) == 1:
            return filtered_dep[0][2]

        filtered_dep = sorted(filtered_dep, key=lambda x: x[0])
        return filtered_dep[-1][2]


ImportDataset = AgentControl
