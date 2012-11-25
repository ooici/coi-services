#!/usr/bin/env python

"""Helper for observatory and device computed attributes, including aggregate status values"""

__author__ = 'Michael Meisinger, Maurice Manning, Ian Katz'

import time

from pyon.core.exception import BadRequest
from pyon.public import RT, PRED, OT, IonObject, log

from interface.objects import DeviceStatusType, DeviceCommsType
from interface.objects import ComputedValueAvailability, StatusType


class ObservatoryUtil(object):
    def __init__(self, process=None, container=None):
        self.process = process
        self.container = container if container else process.container

    # -------------------------------------------------------------------------
    # Observatory site traversal

    def get_child_sites(self, parent_site_id=None, org_id=None, exclude_types=None, include_parent=True, id_only=True):
        """
        Returns all child sites for a given parent site_id or a given org_id.
        Return type is a tuple of two elements.
        The first element is a dict mapping site_id to Site object (or None if id_only==True).
        The second element is a dict mapping site_id to a list of direct child site_ids.
        """
        if exclude_types is None:
            exclude_types = []
        if org_id:
            obsite_ids,_ = self.container.resource_registry.find_objects(
                org_id, PRED.hasResource, RT.Observatory, id_only=True)
            if not obsite_ids:
                return {}, {}
            parent_site_list = set(obsite_ids)
        elif parent_site_id:
            parent_site_list = set([parent_site_id])
        else:
            raise BadRequest("Must provide either parent_site_id or org_id")

        matchlist = []
        ancestors = {}
        parents = self._get_site_parents()
        for rid, (st, psid, pt) in parents.iteritems():
            if st in exclude_types:
                continue
            parent_stack = [rid, psid]
            while psid:
                if psid in parent_site_list or (include_parent and rid in parent_site_list):
                    matchlist.append(rid)
                    par = parent_stack.pop()
                    while parent_stack:
                        ch = parent_stack.pop()
                        if par not in ancestors:
                            ancestors[par] = []
                        if ch not in ancestors[par]:
                            ancestors[par].append(ch)
                        par = ch
                    psid = None
                else:
                    _,psid,_ = parents.get(psid, (None,None,None))
                    parent_stack.append(psid)

        if id_only:
            child_site_dict = dict(zip(matchlist, [None]*len(matchlist)))
        else:
            all_res = self.container.resource_registry.read_mult(matchlist)
            child_site_dict = dict(zip([res._id for res in all_res], all_res))

        return child_site_dict, ancestors

    def _get_site_parents(self):
        """Returns a dict mapping a site_id to site type and parent site_id."""
        # This function makes one RR call retrieving all hasSite associations.
        # @TODO: exclude retired sites
        # @TODO: see if this can be done with an id_only=False argument
        parents = {}
        assoc_list = self.container.resource_registry.find_associations(predicate=PRED.hasSite, id_only=False)
        for assoc in assoc_list:
            parents[assoc.o] = (assoc.ot, assoc.s, assoc.st)
        return parents

    def get_site_devices(self, site_list):
        """
        Returns a dict of site_id mapped to site type, device_id, device type, based on hasDevice association.
        """
        site_devices = self._get_site_devices()
        res_sites = {}
        for site_id in site_list:
            res_sites[site_id] = site_devices.get(site_id, None)
        return res_sites

    def _get_site_devices(self):
        """
        Returns a dict mapping a site_id to site type, device_id, device type based on hasDevice association.
        """
        sites = {}
        assoc_list = self.container.resource_registry.find_associations(predicate=PRED.hasDevice, id_only=False)
        for assoc in assoc_list:
            if assoc.st in [RT.PlatformSite, RT.InstrumentSite]:
                sites[assoc.s] = (assoc.st, assoc.o, assoc.ot)
        return sites

    def get_child_devices(self, device_id):
        child_devices = self._get_child_devices()
        all_children = set([device_id])
        def add_children(dev_id):
            ch_list = child_devices.get(dev_id, [])
            for _,ch_id,_ in ch_list:
                all_children.add(ch_id)
                add_children(ch_id)
        add_children(device_id)
        for dev_id in list(child_devices.keys()):
            if dev_id not in all_children:
                del child_devices[dev_id]
        if device_id not in child_devices:
            child_devices[device_id] = None
        return child_devices

    def _get_child_devices(self):
        """
        Returns a dict mapping a device_id to parent type, child device_id, child type based on hasDevice association.
        """
        sites = {}
        assoc_list = self.container.resource_registry.find_associations(predicate=PRED.hasDevice, id_only=False)
        for assoc in assoc_list:
            if assoc.st in [RT.PlatformDevice, RT.InstrumentDevice] and assoc.ot in [RT.PlatformDevice, RT.InstrumentDevice]:
                if assoc.s not in sites:
                    sites[assoc.s] = []
                sites[assoc.s].append((assoc.st, assoc.o, assoc.ot))
        return sites

    def _get_status_events(self, device_list=None):
        min_ts = str(int(time.time() - 3) * 1000)
        pwr_events = self.container.event_repository.find_events(event_type=OT.DeviceStatusEvent, start_ts=min_ts, descending=True)
        comm_events = self.container.event_repository.find_events(event_type=OT.DeviceCommsEvent, start_ts=min_ts, descending=True)
        events = []
        events.extend(pwr_events)
        events.extend(comm_events)
        device_events = {}
        for event in events:
            if device_list and event.origin not in device_list:
                continue
            if event.origin not in device_events:
                device_events[event.origin] = []
            device_events[event.origin].append(event)
        return device_events

    # -------------------------------------------------------------------------
    # Status roll up

    def get_status_roll_ups(self, res_id, res_type=None):
        """
        For given parent device/site/org res_id compute the status roll ups.
        The result is a dict of id with value dict of status values
        """
        if not res_type:
            res_obj = self.container.resource_registry.read(res_id)
            res_type = res_obj._get_type()

        if res_type in [RT.Org, RT.Observatory, RT.Subsite, RT.PlatformSite, RT.InstrumentSite]:
            if res_type == RT.Org:
                child_sites, site_ancestors = self.get_child_sites(org_id=res_id, id_only=True)
            else:
                child_sites, site_ancestors = self.get_child_sites(parent_site_id=res_id, id_only=True)

            site_devices = self.get_site_devices(child_sites.keys())
            device_events = self._get_status_events()

            site_status = {}
            def get_site_status(site_id):
                if site_id in site_status:
                    return site_status[site_id]
                elif site_ancestors.get(site_id, None):
                    ch_stat_list = []
                    for ch_id in site_ancestors[site_id]:
                        ch_status = get_site_status(ch_id)
                        site_status[ch_id] = ch_status
                        ch_stat_list.append(ch_status)
                    s_status = self._rollup_statuses(ch_stat_list)
                    site_status[site_id] = s_status
                    return s_status
                else:
                    device_info = site_devices.get(site_id, None)
                    if not device_info:
                        s_status = dict(power=StatusType.STATUS_UNKNOWN, comms=StatusType.STATUS_UNKNOWN,
                            data=StatusType.STATUS_UNKNOWN, loc=StatusType.STATUS_UNKNOWN, all=StatusType.STATUS_UNKNOWN)
                    else:
                        device_id = device_info[1]
                        s_status = self._compute_status(device_id, device_events)
                    site_status[site_id] = s_status
                    return s_status

            for site_id in child_sites.keys():
                get_site_status(site_id)

            return site_status

        elif res_type in [RT.PlatformDevice, RT.InstrumentDevice]:
            # See if current device has child devices
            child_devices = self.get_child_devices(res_id)
            print child_devices, res_id
            device_events = self._get_status_events()

            device_status = {}
            def get_device_status(device_id):
                if device_id in device_status:
                    return device_status[device_id]
                elif child_devices.get(device_id, None):
                    ch_stat_list = []
                    for _,ch_id,_ in child_devices[device_id]:
                        ch_status = get_device_status(ch_id)
                        device_status[ch_id] = ch_status
                        ch_stat_list.append(ch_status)
                    s_status = self._rollup_statuses(ch_stat_list)
                    device_status[device_id] = s_status
                    return s_status
                else:
                    s_status = self._compute_status(device_id, device_events)
                    device_status[device_id] = s_status
                    return s_status

            for device_id in child_devices.keys():
                get_device_status(device_id)

            return device_status
        else:
            raise BadRequest("Unsupported resource type: %s", res_type)

    def _compute_status(self, device_id, device_events):
        status = dict(power=StatusType.STATUS_UNKNOWN, comms=StatusType.STATUS_UNKNOWN,
            data=StatusType.STATUS_UNKNOWN, loc=StatusType.STATUS_UNKNOWN, all=StatusType.STATUS_UNKNOWN)
        dev_events = device_events.get(device_id, [])
        for event in dev_events:
            event_type = event._get_type()
            if event_type == OT.DeviceStatusEvent and event.state == DeviceStatusType.OUT_OF_RANGE:
                status['power'] = StatusType.STATUS_WARNING
            elif event_type == OT.DeviceCommsEvent and event.state == DeviceCommsType.DATA_DELIVERY_INTERRUPTION:
                status['comms'] = StatusType.STATUS_WARNING
            # @TODO data, loc

        status['agg'] = self._consolidate_status(status.values())
        return status

    def _consolidate_status(self, statuses):
        """Intelligently merge statuses with current value"""

        # Any critical means all critical
        if StatusType.STATUS_CRITICAL in statuses:
            return StatusType.STATUS_CRITICAL

        # Any warning means all warning
        if StatusType.STATUS_WARNING in statuses:
            return StatusType.STATUS_WARNING

        # Any unknown is fine unless some are ok -- then it's a warning
        if StatusType.STATUS_OK in statuses:
            if StatusType.STATUS_UNKNOWN in statuses:
                return StatusType.STATUS_WARNING
            else:
                return StatusType.STATUS_OK

        # 0 results are OK, 0 or more are unknown
        return StatusType.STATUS_UNKNOWN

    def _rollup_statuses(self, status_list):
        """For a list of child status dicts, compute the rollup statuses"""
        rollup_status = {}
        rollup_status['power'] = self._consolidate_status([stat['power'] for stat in status_list])
        rollup_status['comms'] = self._consolidate_status([stat['comms'] for stat in status_list])
        rollup_status['data'] = self._consolidate_status([stat['data'] for stat in status_list])
        rollup_status['loc'] = self._consolidate_status([stat['loc'] for stat in status_list])
        rollup_status['agg'] = self._consolidate_status(rollup_status.values())
        return rollup_status

