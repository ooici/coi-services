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

    def get_child_sites(self, parent_site_id=None, org_id=None, exclude_types=None, include_parents=True, id_only=True):
        """
        Returns all child sites and parent site for a given parent site_id.
        Returns all child sites and org for a given org_id.
        Return type is a tuple of two elements.
        The first element is a dict mapping site_id to Site object (or None if id_only==True).
        The second element is a dict mapping site_id to a list of direct child site_ids.
        @param include_parents if True, walk up the parents all the way to the root and include
        @param id_only if True, return Site objects
        """
        if parent_site_id and org_id:
            raise BadRequest("Either parent_site_id OR org_id supported!")
        if exclude_types is None:
            exclude_types = []

        parents = self._get_site_parents()   # Note: root elements are not in list

        if org_id:
            obsite_ids,_ = self.container.resource_registry.find_objects(
                org_id, PRED.hasResource, RT.Observatory, id_only=True)
            if not obsite_ids:
                return {}, {}
            parent_site_id = org_id
            for obsite_id in obsite_ids:
                parents[obsite_id] = ('Observatory', org_id, 'Org')
        elif parent_site_id:
            if parent_site_id not in parents:
                parents[parent_site_id] = ('Observatory', None, 'Org')
        else:
            raise BadRequest("Must provide either parent_site_id or org_id")

        matchlist = []  # sites with wanted parent
        ancestors = {}  # child ids for sites in result set
        for site_id, (st, parent_id, pt) in parents.iteritems():
            # Iterate through sites and find the ones with a wanted parent
            if st in exclude_types:
                continue
            parent_stack = [site_id, parent_id]
            while parent_id:
                # Walk up to parents
                if parent_id == parent_site_id:
                    matchlist.append(site_id)
                    # Fill out ancestors
                    par = parent_stack.pop()
                    while parent_stack:
                        ch = parent_stack.pop()
                        if par not in ancestors:
                            ancestors[par] = []
                        if ch not in ancestors[par]:
                            ancestors[par].append(ch)
                        par = ch
                    parent_id = None
                else:
                    _,parent_id,_ = parents.get(parent_id, (None,None,None))
                    parent_stack.append(parent_id)

        # Go all the way up to the roots
        if include_parents:
            matchlist.append(parent_site_id)
            child_id = parent_site_id
            parent = parents.get(child_id, None)
            while parent:
                st, parent_id, pt = parent
                if parent_id:
                    matchlist.append(parent_id)
                    if parent_id not in ancestors:
                        ancestors[parent_id] = []
                    ancestors[parent_id].append(child_id)
                child_id = parent_id
                parent = parents.get(child_id, None)

        if id_only:
            child_site_dict = dict(zip(matchlist, [None]*len(matchlist)))
        else:
            all_res = self.container.resource_registry.read_mult(matchlist) if matchlist else []
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
            child_devices[device_id] = []
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
        # Events come back as 3-tuples of (id, key, obj)
        pwr_events = self.container.event_repository.find_events(event_type=OT.DeviceStatusEvent, start_ts=min_ts, descending=True)
        comm_events = self.container.event_repository.find_events(event_type=OT.DeviceCommsEvent, start_ts=min_ts, descending=True)
        events = []
        events.extend(pwr_events)
        events.extend(comm_events)
        device_events = {}
        for ev_id, ev_key, event in events:
            if device_list and event.origin not in device_list:
                continue
            if event.origin not in device_events:
                device_events[event.origin] = []
            device_events[event.origin].append(event)
        return device_events

    def get_site_root(self, res_id, site_parents=None, ancestors=None):
        if ancestors:
            site_parents = {}
            for site_id, ch_ids in ancestors.iteritems():
                if ch_ids:
                    for ch_id in ch_ids:
                        site_parents[ch_id] = ('', site_id, '')

        parent_id = res_id
        parent = site_parents.get(parent_id, None)
        while parent:
            _,pid,_ = parent
            parent_id = pid
            parent = site_parents.get(parent_id, None)
        return parent_id

    # -------------------------------------------------------------------------
    # Status roll up

    def get_status_roll_ups(self, res_id, res_type=None, include_structure=False):
        """
        For given parent device/site/org res_id compute the status roll ups.
        The result is a dict of id with value dict of status values.
        """
        if not res_type:
            res_obj = self.container.resource_registry.read(res_id)
            res_type = res_obj._get_type()

        def get_site_status(site_id, status_rollup, site_ancestors):
            """For one site, compute the aggregate status and recurse to child sites if necessary"""
            if site_id in status_rollup:
                return status_rollup[site_id]
            elif site_ancestors.get(site_id, None):
                ch_stat_list = []
                for ch_id in site_ancestors[site_id]:
                    ch_status = get_site_status(ch_id, status_rollup, site_ancestors)
                    status_rollup[ch_id] = ch_status
                    ch_stat_list.append(ch_status)

                # See if there is a device deployed -  include into rollup in addition to child sites
                device_info = site_devices.get(site_id, None)
                if device_info:
                    device_id = device_info[1]
                    d_status = self._compute_status(device_id, device_events)
                    status_rollup[device_id] = d_status
                    ch_stat_list.append(d_status)

                s_status = self._rollup_statuses(ch_stat_list)
                status_rollup[site_id] = s_status
                return s_status
            else:
                device_info = site_devices.get(site_id, None)
                if not device_info:
                    s_status = dict(power=StatusType.STATUS_UNKNOWN, comms=StatusType.STATUS_UNKNOWN,
                        data=StatusType.STATUS_UNKNOWN, loc=StatusType.STATUS_UNKNOWN, agg=StatusType.STATUS_UNKNOWN)
                else:
                    device_id = device_info[1]
                    s_status = self._compute_status(device_id, device_events)
                status_rollup[site_id] = s_status
                return s_status

        def get_device_status(device_id, status_rollup, child_devices):
            """For one device, compute the aggregate status and recurse to child devices if necessary"""

            if device_id in status_rollup:
                return status_rollup[device_id]
            elif child_devices.get(device_id, None):
                ch_stat_list = []
                for _,ch_id,_ in child_devices[device_id]:
                    ch_status = get_device_status(ch_id, status_rollup, child_devices)
                    status_rollup[ch_id] = ch_status
                    ch_stat_list.append(ch_status)
                d_status = self._rollup_statuses(ch_stat_list)
                status_rollup[device_id] = d_status
                return d_status
            else:
                d_status = self._compute_status(device_id, device_events)
                status_rollup[device_id] = d_status
                return d_status

        # Do the status rollup work. Different modes dependent on type of resource (org, site, device)
        if res_type in [RT.Org, RT.Observatory, RT.Subsite, RT.PlatformSite, RT.InstrumentSite]:
            if res_type == RT.Org:
                child_sites, site_ancestors = self.get_child_sites(org_id=res_id, id_only=not include_structure)
            else:
                child_sites, site_ancestors = self.get_child_sites(parent_site_id=res_id, id_only=not include_structure)

            site_devices = self.get_site_devices(child_sites.keys())
            device_events = self._get_status_events()

            status_rollup = {}
            get_site_status(res_id, status_rollup, site_ancestors)
            for site_id in child_sites.keys():
                get_site_status(site_id, status_rollup, site_ancestors)

            # Stuff extra information into the result
            if include_structure:
                status_rollup['_system'] = dict(res_id=res_id, res_type=res_type,
                    sites=child_sites, ancestors=site_ancestors, devices=site_devices)

            return status_rollup

        elif res_type in [RT.PlatformDevice, RT.InstrumentDevice]:
            # See if current device has child devices
            child_devices = self.get_child_devices(res_id)
            device_events = self._get_status_events()

            status_rollup = {}
            get_device_status(res_id, status_rollup, child_devices)
            for device_id in child_devices.keys():
                get_device_status(device_id, status_rollup, child_devices)

            # Stuff extra information into the result
            if include_structure:
                status_rollup['_system'] = dict(res_id=res_id, res_type=res_type,
                    ancestors=child_devices)

            return status_rollup

        else:
            raise BadRequest("Unsupported resource type: %s", res_type)

    def _compute_status(self, device_id, device_events):
        status = dict(power=StatusType.STATUS_OK, comms=StatusType.STATUS_OK,
            data=StatusType.STATUS_OK, loc=StatusType.STATUS_OK)
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

    def _consolidate_status(self, statuses, warn_if_unknown=False):
        """Intelligently merge statuses with current value"""

        # Any critical means all critical
        if StatusType.STATUS_CRITICAL in statuses:
            return StatusType.STATUS_CRITICAL

        # Any warning means all warning
        if StatusType.STATUS_WARNING in statuses:
            return StatusType.STATUS_WARNING

        # Any unknown is fine unless some are ok -- then it's a warning
        if StatusType.STATUS_OK in statuses:
            if StatusType.STATUS_UNKNOWN in statuses and warn_if_unknown:
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

