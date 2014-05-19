#!/usr/bin/env python

"""Deployment specific utilities"""

import time

from ooi.logging import log, TRACE, DEBUG

from pyon.core import bootstrap
from pyon.public import RT, PRED, LCS, OT, BadRequest

from interface.objects import TemporalBounds, GeospatialBounds, DeviceStatusType


TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


class DeploymentUtil(object):
    """Helper class to work with deployments"""

    DATE_NOW = "NOW"

    def __init__(self, container=None):
        self.container = container or bootstrap.container_instance
        self.rr = container.resource_registry

    def get_deployment_relations(self, deployment_id, return_objects=False):
        """Returns a tuple of site id and device id for a deployment"""
        # Find any existing deployment
        depsrc_objs, _ = self.rr.find_subjects(None, PRED.hasDeployment, deployment_id, id_only=False)
        dep_site, dep_dev = None, None
        for ds in depsrc_objs:
            if ds.type_ in (RT.PlatformSite, RT.InstrumentSite):
                dep_site = ds if return_objects else ds._id
            elif ds.type_ in (RT.PlatformDevice, RT.InstrumentDevice):
                dep_dev = ds if return_objects else ds._id
        return dep_site, dep_dev

    def get_deployments_relations(self, deployments, return_objects=False):
        """Returns a tuple of deployment_id to site and deployment_id to device maps"""
        dep_ids = {d._id for d in deployments}

        # Currently, the below causes n SQL queries and cannot filter by predicate. Better in R3
        # dep_src, dep_assoc = self.rr.find_subjects_mult(objects=list(dep_ids), predicate=PRED.hasDeployment, id_only=False)
        # dep_site_map = {da.o: ds for ds, da in zip(dep_src, dep_assoc) if da.st in (RT.PlatformSite, RT.InstrumentSite)}
        # dep_dev_map = {da.o: ds for ds, da in zip(dep_src, dep_assoc) if da.st in (RT.PlatformDevice, RT.InstrumentDevice)}

        # Currently the following is the fastest
        dep_assocs = self.rr.find_associations(predicate=PRED.hasDeployment, id_only=False)
        dep_assocs = [da for da in dep_assocs if da.ot == RT.Deployment and da.o in dep_ids]

        if return_objects:
            dep_rel_ids = list({assoc.s for assoc in dep_assocs})
            dep_rel_objs = self.rr.read_mult(dep_rel_ids)
            dep_rel_map = {rid: robj for rid, robj in zip(dep_rel_ids, dep_rel_objs)}
            dep_site_map = {da.o: dep_rel_map[da.s] for da in dep_assocs if da.st in (RT.PlatformSite, RT.InstrumentSite)}
            dep_dev_map = {da.o: dep_rel_map[da.s] for da in dep_assocs if da.st in (RT.PlatformDevice, RT.InstrumentDevice)}
        else:
            dep_site_map = {da.o: da.s for da in dep_assocs if da.st in (RT.PlatformSite, RT.InstrumentSite)}
            dep_dev_map = {da.o: da.s for da in dep_assocs if da.st in (RT.PlatformDevice, RT.InstrumentDevice)}

        return dep_site_map, dep_dev_map

    def get_site_primary_deployment(self, site_id):
        """Return the current deployment for given site"""
        if not site_id:
            return None
        dep_list, _ = self.rr.find_objects(site_id, PRED.hasDeployment, RT.Deployment, id_only=False)
        return self.get_current_deployment(dep_list, only_deployed=True)

    def get_device_primary_deployment(self, device_id):
        """Return the current deployment for given device"""
        if not device_id:
            return None
        dep_list, _ = self.rr.find_objects(device_id, PRED.hasDeployment, RT.Deployment, id_only=False)
        return self.get_current_deployment(dep_list, only_deployed=True)

    def get_temporal_constraint(self, deployment_obj):
        for cons in deployment_obj.constraint_list:
            if isinstance(cons, TemporalBounds):
                return cons

    def get_geospatial_constraint(self, deployment_obj):
        for cons in deployment_obj.constraint_list:
            if isinstance(cons, GeospatialBounds):
                return cons

    def set_temporal_constraint(self, deployment_obj, start_time=None, end_time=None):
        tc = self.get_temporal_constraint(deployment_obj)
        if tc is None:
            tc = TemporalBounds()
            deployment_obj.constraint_list.append(tc)
        if start_time == "NOW":
            start_time = str(int(time.time()))
        if end_time == "NOW":
            end_time = str(int(time.time()))
        if start_time is not None:
            tc.start_datetime = start_time
        if end_time is not None:
            tc.end_datetime = end_time

    def get_current_deployment(self, deployment_list, act_time=None, only_deployed=False, best_guess=False):
        """Return the most likely current deployment from given list of Deployment resources
        @param deployment_list  list of Deployment resource objects
        @param act_time  if set use this time (float) instread of time.time() for comparison
        @param only_deployed  if True, only consider only DEPLOYED lcstate
        @param best_guess  if True, guess the most likely current deployment (R2 hack)
        @retval  the Deployment object that matches or None

        # TODO: For a better way to find current active deployment wait for R3 M193 and then refactor
        # Procedure:
        # 1 Eliminate all RETIRED lcstate
        # 2 If only_deployed==True, eliminate all that are not DEPLOYED lcstate
        # 3 Eliminate all with illegal or missing TemporalBounds
        # 4 Eliminate past deployments (end date before now)
        # 5 Eliminate known future deployments (start date after now)
        # 6 Sort the remaining list by start date and return first (ambiguous!!)
        """
        if act_time is None:
            act_time = time.time()
        filtered_dep = []
        for dep_obj in deployment_list:
            if dep_obj.lcstate == LCS.RETIRED:
                continue
            if only_deployed and dep_obj.lcstate != LCS.DEPLOYED:
                continue
            temp_const = self.get_temporal_constraint(dep_obj)
            if not temp_const:
                continue
            if not temp_const.start_datetime or not temp_const.end_datetime:
                continue
            start_time = int(temp_const.start_datetime)
            if start_time > act_time:
                continue
            end_time = int(temp_const.end_datetime)
            if end_time > act_time:
                # TODO: This may not catch a current deployment if it has a past end_time set!
                filtered_dep.append((start_time, end_time, dep_obj))

        if not filtered_dep:
            return None
        if len(filtered_dep) == 1:
            return filtered_dep[0][2]

        if best_guess:
            log.warn("Cannot determine current deployment unambiguously - choosing earliest start date")
            filtered_dep = sorted(filtered_dep, key=lambda x: x[0])
            return filtered_dep[0][2]
        else:
            raise BadRequest("Cannot determine current deployment unambiguously - choosing earliest start date")

    def get_active_deployment(self, res_id, is_site=True, rr2=None):
        dep_objs = rr2.find_objects(res_id, PRED.hasDeployment, id_only=False)
        if dep_objs:
            return self.get_current_deployment(dep_objs, only_deployed=True)
        if is_site:
            parent_res = rr2.find_subjects(RT.PlatformSite, PRED.hasSite, res_id, id_only=True)
        else:
            parent_res = rr2.find_subjects(RT.PlatformDevice, PRED.hasDevice, res_id, id_only=True)
        if parent_res:
            return self.get_active_deployment(parent_res[0], is_site=is_site, rr2=rr2)

    def describe_deployments(self, deployments, status_map=None):
        """
        For a list of deployment IDs, generate a list of dicts with information about the deployments
        suitable for the UI table: [ { 'ui_column': 'string_value'... } , ...]
        @param deployments  list of Deployment resource objects
        @param status_map  map of device id to device status dict
        @retval list with Deployment info dicts coindexed with argument deployments list
        """
        dep_info_list = []
        dep_site_map, dep_dev_map = self.get_deployments_relations(deployments, return_objects=True)
        site_structure = status_map.get("_system", {}).get("devices", None) if status_map else None

        dep_by_id = {}
        for dep in deployments:
            dep_info = {}
            dep_info_list.append(dep_info)
            dep_by_id[dep._id] = dep_info

            # Set temporal bounds
            temp_const = self.get_temporal_constraint(dep)
            if temp_const:
                dep_info['start_time'] = time.strftime(TIME_FORMAT, time.gmtime(
                    float(temp_const.start_datetime))) if temp_const.start_datetime else ""
                dep_info['end_time'] = time.strftime(TIME_FORMAT, time.gmtime(
                    float(temp_const.end_datetime))) if temp_const.end_datetime else ""
            else:
                dep_info['start_time'] = dep_info['end_time'] = ""

            # Set device information
            device_obj = dep_dev_map.get(dep._id, None)
            if device_obj:
                dep_info['device_id'] = device_obj._id
                dep_info['device_name'] = device_obj.name
                dep_info['device_type'] = device_obj.type_
                dep_info['device_status'] = status_map.get(device_obj._id,{}).get("agg", DeviceStatusType.STATUS_UNKNOWN)
            else:
                log.warn("Deployment %s has no Device", dep._id)
                dep_info['device_id'] = dep_info['device_name'] = dep_info['device_type'] = dep_info['device_status'] = None

            # Set site information
            site_obj = dep_site_map.get(dep._id, None)
            if site_obj:
                dep_info['site_id'] = site_obj._id
                dep_info['site_name'] = site_obj.name
                dep_info['site_type'] = site_obj.type_
            else:
                log.warn("Deployment %s has no Site", dep._id)
                dep_info['site_id'] = dep_info['site_name'] = dep_info['site_type'] = None

            # Set status information
            if status_map and dep.lcstate == LCS.DEPLOYED:
                dep_info["is_primary"] = DeviceStatusType.STATUS_OK
                if site_structure and site_obj and device_obj and site_obj._id in site_structure:
                    try:
                        # Additionally check deployment date
                        now = time.time()
                        if temp_const and (now < float(temp_const.start_datetime) or now > float(temp_const.end_datetime)):
                            dep_info["is_primary"] = DeviceStatusType.STATUS_WARNING

                        # Additionally check assoc between site and device
                        site_deps = site_structure[site_obj._id]
                        if not any(True for st, did, dt in site_deps if did == device_obj._id and dt in (RT.PlatformDevice, RT.InstrumentDevice)):
                            dep_info["is_primary"] = DeviceStatusType.STATUS_WARNING
                    except Exception:
                        log.exception("Error determining site structure")
            else:
                dep_info["is_primary"] = DeviceStatusType.STATUS_UNKNOWN

            # Set site parent - seems unused currently, not gonna bother
            parent_site_obj = None
            if parent_site_obj:
                dep_info['parent_site_id'] = parent_site_obj._id
                dep_info['parent_site_name'] = parent_site_obj.name
                dep_info['parent_site_description'] = parent_site_obj.description
            else:
                #log.warn("Deployment %s has no parent Site", dep._id)
                dep_info['parent_site_id'] = dep_info['parent_site_name'] = dep_info['parent_site_description'] = None

        return dep_info_list
