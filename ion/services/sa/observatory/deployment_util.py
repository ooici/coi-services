#!/usr/bin/env python

"""Deployment specific utilities"""

import time

from ooi.logging import log, TRACE, DEBUG

from pyon.core import bootstrap
from pyon.public import RT, PRED, LCS, OT, BadRequest

from interface.objects import TemporalBounds, GeospatialBounds


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


def describe_deployments(deployments, context, instruments=None, instrument_status=None):
    """
    For a list of deployment IDs, generate a dict of dicts with information about the deployments
    suitable for the UI table: { deployment_id: { 'ui_column': 'string_value'... } }
    @param deployments  list of Deployment resource objects
    @param context  object to get the resource_registry from (e.g. container)
    @param instruments  list of InstrumentDevice resource objects
    @param instrument_status  coindexed list of status for InstrumentDevice to be added to respective Deployment
    @retval list with Deployment info dicts coindexed with argument deployments list
    """
    instruments = instruments or []
    instrument_status = instrument_status or []
    if not deployments:
        return []
    rr = context.resource_registry
    deployment_ids = [d._id for d in deployments]
    descriptions = {}
    for d in deployments:
        descriptions[d._id] = {'is_primary': False}
        # add start, end time
        time_constraint = None
        for constraint in d.constraint_list:
            if constraint.type_ == OT.TemporalBounds:
                if time_constraint:
                    log.warn('deployment %s has more than one time constraint (using first)', d.name)
                else:
                    time_constraint = constraint
        if time_constraint:
            descriptions[d._id]['start_time'] = time.strftime(TIME_FORMAT, time.gmtime(
                float(time_constraint.start_datetime))) if time_constraint.start_datetime else ""
            descriptions[d._id]['end_time'] = time.strftime(TIME_FORMAT, time.gmtime(
                float(time_constraint.end_datetime))) if time_constraint.end_datetime else ""
        else:
            descriptions[d._id]['start_time'] = descriptions[d._id]['end_time'] = ""

    # first get the all site and instrument objects
    site_ids = []
    objects, associations = rr.find_subjects_mult(objects=deployment_ids, id_only=False)
    if log.isEnabledFor(TRACE):
        log.trace('have %d deployment-associated objects, %d are hasDeployment', len(associations),
                  sum([1 if assoc.p==PRED.hasDeployment else 0 for assoc in associations]))
    for obj, assoc in zip(objects, associations):
        # if this is a hasDeployment association...
        if assoc.p == PRED.hasDeployment:
            description = descriptions[assoc.o]

            # always save the id in one known field (used by UI)
            description['resource_id'] = assoc.o

            # save site or device info in the description
            type = obj.type_
            if type in (RT.InstrumentSite, RT.PlatformSite):
                description['site_id'] = obj._id
                description['site_name'] = obj.name
                description['site_type'] = type
                if obj._id not in site_ids:
                    site_ids.append(obj._id)
            elif type in (RT.InstrumentDevice, RT.PlatformDevice):
                description['device_id'] = obj._id
                description['device_name'] = obj.name
                description['device_type'] = type
                for instrument, status in zip(instruments, instrument_status):
                    if obj._id == instrument._id:
                        description['device_status'] = status
            else:
                log.warn('unexpected association: %s %s %s %s %s', assoc.st, assoc.s, assoc.p, assoc.ot, assoc.o)

    # Make the code below more robust by ensuring that all description entries are present, even
    # if Deployment is missing some associations (OOIION-1183)
    for d in descriptions.values():
        if "site_id" not in d:
            d['site_id'] = d['site_name'] = d['site_type'] = None
        if "device_id" not in d:
            d['device_id'] = d['device_name'] = d['device_type'] = None

    # now look for hasDevice associations to determine which deployments are "primary" or "active"
    objects2, associations = rr.find_objects_mult(subjects=site_ids)
    if log.isEnabledFor(TRACE):
        log.trace('have %d site-associated objects, %d are hasDeployment', len(associations), sum([1 if assoc.p==PRED.hasDeployment else 0 for assoc in associations]))
    for obj, assoc in zip(objects2, associations):
        if assoc.p == PRED.hasDevice:
            found_match = False
            for description in descriptions.itervalues():
                if description.get('site_id', None) == assoc.s and description.get('device_id', None) == assoc.o:
                    if found_match:
                        log.warn('more than one primary deployment for site %s (%s) and device %s (%s)',
                                 assoc.s, description['site_name'], assoc.o, description['device_name'])
                    description['is_primary'] = found_match = True

    # finally get parents of sites using hasSite
    objects3, associations = rr.find_subjects_mult(objects=site_ids)
    if log.isEnabledFor(TRACE):
        log.trace('have %d site-associated objects, %d are hasDeployment', len(associations), sum([1 if assoc.p==PRED.hasDeployment else 0 for assoc in associations]))
    for obj, assoc in zip(objects3, associations):
        if assoc.p == PRED.hasSite:
            found_match = False
            for description in descriptions.itervalues():
                if description.get('site_id', None) == assoc.o:
                    if found_match:
                        log.warn('more than one parent for site %s (%s)', assoc.o, description['site_name'])
                    description['parent_site_id'] = obj._id
                    description['parent_site_name'] = obj.name
                    description['parent_site_description'] = obj.description

    # convert to array
    descriptions_list = [descriptions[d._id] for d in deployments]

    if log.isEnabledFor(DEBUG):
        log.debug('%d deployments, %d associated sites/devices, %d activations, %d missing status',
                  len(deployments), len(objects), len(objects2),
                  sum([0 if 'device_status' in d else 1 for d in descriptions_list]))

    return descriptions_list


