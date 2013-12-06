#!/usr/bin/env python

"""Helper for observatory and device computed attributes, including aggregate status values"""

__author__ = 'Michael Meisinger, Maurice Manning'

from pyon.core import bootstrap
from pyon.core.exception import BadRequest
from pyon.public import RT, PRED, log

from interface.objects import DeviceStatusType, AggregateStatusType


class ObservatoryUtil(object):
    def __init__(self, process=None, container=None, enhanced_rr=None, device_status_mgr=None):
        self.process = process
        self.container = container or bootstrap.container_instance
        self.RR2 = enhanced_rr
        self.RR = enhanced_rr or self.container.resource_registry if self.container else None
        self.device_status_mgr = device_status_mgr


    # -------------------------------------------------------------------------
    # Resource registry access

    def _set_enhanced_rr(self, enhanced_rr=None):
        self.RR2 = enhanced_rr

    def _get_predicate_assocs(self, predicate):
        if self.RR2:
            if not self.RR2.has_cached_predicate(predicate):
                self.RR2.cache_predicate(predicate)
            assoc_list = self.RR2.get_cached_associations(predicate)
        else:
            assoc_list = self.container.resource_registry.find_associations(predicate=predicate, id_only=False)
        return assoc_list

    def _find_objects(self, subject, predicate, object_type='', id_only=False):
        if self.RR2:
            return self.RR2.find_objects(subject, predicate, object_type, id_only=id_only), None
        else:
            return self.container.resource_registry.find_objects(subject, predicate, object_type, id_only=id_only)

    # -------------------------------------------------------------------------
    # Observatory site traversal

    def get_child_sites(self, parent_site_id=None, org_id=None, exclude_types=None, include_parents=True, id_only=True):
        """
        Returns all child sites and parent site for a given parent site_id.
        Returns all child sites and org for a given org_id.
        Return type is a tuple (site_resources, site_children) of two elements.
        - site_resources is a dict mapping site_id to Site object (or None if id_only==True).
        - site_children is a dict mapping site_id to a list of direct child site_ids.
        @param include_parents if True, walk up the parents all the way to the root and include
        @param id_only if True, return Site objects
        """
        if parent_site_id and org_id:
            raise BadRequest("Either parent_site_id OR org_id supported!")
        if exclude_types is None:
            exclude_types = []

        parents = self._get_site_parents()   # Note: root elements are not in list

        if org_id:
            obsite_ids,_ = self._find_objects(org_id, PRED.hasResource, RT.Observatory, id_only=True)
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
            all_res = self.RR.read_mult(matchlist) if matchlist else []
            child_site_dict = dict(zip([res._id for res in all_res], all_res))

        return child_site_dict, ancestors

    def _get_site_parents(self):
        """Returns a dict mapping a site_id to site type and parent site_id."""
        # This function makes one RR call retrieving all hasSite associations.
        # @TODO: see if this can be done with an id_only=False argument
        parents = {}
        assoc_list = self._get_predicate_assocs(PRED.hasSite)
        for assoc in assoc_list:
            parents[assoc.o] = (assoc.ot, assoc.s, assoc.st)
        return parents

    def get_device_relations(self, site_list):
        """
        Returns a dict of site_id or device_id mapped to list of (site/device type, device_id, device type)
        tuples, or None, based on hasDevice associations.
        This is a combination of 2 results: site->device(primary) and device(parent)->device(child)
        """
        assoc_list = self._get_predicate_assocs(PRED.hasDevice)

        res_dict = {}

        site_devices = self.get_site_devices(site_list, assoc_list=assoc_list)
        res_dict.update(site_devices)

        # Add information for each device
        device_ids = [tuple_list[0][1] for tuple_list in site_devices.values() if tuple_list]
        for device_id in device_ids:
            res_dict.update(self.get_child_devices(device_id, assoc_list=assoc_list))

        return res_dict

    def get_site_devices(self, site_list, assoc_list=None):
        """
        Returns a dict of site_id mapped to a list of (site type, device_id, device type) tuples,
        based on hasDevice association for given site_list.
        """
        site_devices = self._get_site_devices(assoc_list=assoc_list)
        res_sites = {}
        for site_id in site_list:
            sd_tup = site_devices.get(site_id, None)
            res_sites[site_id] = [sd_tup] if sd_tup else []
        return res_sites

    def _get_site_devices(self, assoc_list=None):
        """
        Returns a dict of site_id mapped to a list of (site type, device_id, device type) tuples,
        based on hasDevice association for all sites.
        """
        sites = {}
        if not assoc_list:
            assoc_list = self._get_predicate_assocs(PRED.hasDevice)
        for assoc in assoc_list:
            if assoc.st in [RT.PlatformSite, RT.InstrumentSite]:
                sites[assoc.s] = (assoc.st, assoc.o, assoc.ot)
        return sites

    def get_child_devices(self, device_id, assoc_list=None):
        """Returns a dict of keys device_id and all children of device_id to
        lists of 3-tuples (parent type, child id, child type
        """
        child_devices = self._get_child_devices(assoc_list=assoc_list)
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
        for dev_id in all_children:
            if dev_id not in child_devices:
                child_devices[dev_id] = []
        return child_devices

    def _get_child_devices(self, assoc_list=None):
        """
        Returns a dict mapping a device_id to parent type, child device_id, child type based on hasDevice association.
        """
        sites = {}
        if not assoc_list:
            assoc_list = self._get_predicate_assocs(PRED.hasDevice)
        for assoc in assoc_list:
            if assoc.st in [RT.PlatformDevice, RT.InstrumentDevice] and assoc.ot in [RT.PlatformDevice, RT.InstrumentDevice]:
                if assoc.s not in sites:
                    sites[assoc.s] = []
                sites[assoc.s].append((assoc.st, assoc.o, assoc.ot))
        return sites



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
    # Finding data products

    def get_resource_data_products(self, res_list, assoc_list=None):
        """
        Returns a dict of resource id mapped to data product id based on hasSource association.
        """
        device_dps = self._get_data_products_by_source(assoc_list=assoc_list)
        res_dps = {}
        for dev_id in res_list:
            res_dps[dev_id] = device_dps.get(dev_id, None)
        return res_dps

    def _get_data_products_by_source(self, assoc_list=None):
        """
        Returns a dict of resource_id (site/device) mapped to data product id based on hasSource association.
        """
        data_products = {}
        if not assoc_list:
            assoc_list = self._get_predicate_assocs(PRED.hasSource)
        for assoc in assoc_list:
            if assoc.st == RT.DataProduct:
                data_products.setdefault(assoc.o, []).append(assoc.s)
        return data_products

    def get_site_data_products(self, res_id, res_type=None, include_sites=False, include_devices=False, include_data_products=False):
        """
        Determines efficiently all data products for the given site and child sites.
        For given site_id, first determine all child sites (following child hasSite associations).
        Then find all currently primary devices to all child sites (following hasDevice associations).
        Then find all data products that are derived from the devices (following hasSource associations).
        @retval A dict containing the following keys:
                "site_resources": A dict mapping site_id to Site resource object (if include_sites==True) or None
                "site_children": A dict mapping site/org id to list of site ids for children
                "site_devices": A dict mapping site id to tuple (site type, device id, device type)
                "device_resources": A dict mapping device_id to Device object (if include_devices==True)
                "device_data_products": A dict mapping device_id to data_product_id
                "data_product_resources": A dict mapping data_product_id to DataProduct resource object
        """
        res_obj = None
        if not res_type:
            res_obj = self.RR.read(res_id)
            res_type = res_obj._get_type()

        device_list = []
        child_sites, site_devices, site_ancestors = None, None, None
        if res_type in [RT.Org, RT.Observatory, RT.Subsite, RT.PlatformSite, RT.InstrumentSite]:
            if res_type == RT.Org:
                child_sites, site_ancestors = self.get_child_sites(org_id=res_id, include_parents=False, id_only=not include_devices)
            else:
                child_sites, site_ancestors = self.get_child_sites(parent_site_id=res_id, include_parents=False, id_only=not include_devices)
                child_sites[res_id] = res_obj or self.RR.read(res_id) if include_sites else None

            site_devices = self.get_device_relations(child_sites.keys())
            device_list = list({tup[1] for key,dev_list in site_devices.iteritems() if dev_list for tup in dev_list})

        elif res_type in [RT.PlatformDevice, RT.InstrumentDevice]:
            # See if current device has child devices
            device_list = list(set(self.get_child_devices(res_id)))

        else:
            raise BadRequest("Unsupported resource type: %s" % res_type)

        device_objs = self.RR.read_mult(device_list) if include_devices else None

        res_list = device_list + child_sites.keys() if child_sites is not None else []
        device_dps = self.get_resource_data_products(res_list)

        if include_data_products:
            dpid_list = list({dp_id for device_id, dp_list in device_dps.iteritems() if dp_list is not None for dp_id in dp_list if dp_id is not None})
            dpo_list = self.RR.read_mult(dpid_list)
            dp_objs = dict(zip(dpid_list, dpo_list))
        else:
            dp_objs = None

        res_dict = dict(
            site_resources=child_sites,
            site_children=site_ancestors,
            site_devices=site_devices,
            device_resources=device_objs,
            device_data_products=device_dps,
            data_product_resources=dp_objs,
        )

        return res_dict


    # -------------------------------------------------------------------------
    # Status roll up

    def get_status_roll_ups(self, res_id, res_type=None, include_structure=False):
        """
        For given parent device/site/org res_id compute the status roll ups.
        The result is a dict of id with value dict of status values.
        Includes all parents of given site as well.
        """
        if not res_type:
            res_obj = self.container.resource_registry.read(res_id)
            res_type = res_obj._get_type()

        def get_site_status(site_id, status_rollup, site_ancestors, site_devices, status_by_device):
            """For one site, compute the aggregate status and recurse to child sites if necessary"""
            if site_id in status_rollup:
                return status_rollup[site_id]   # If known return and don't recompute

            ch_stat_list = []  # Status dicts to roll up into current
            if site_ancestors.get(site_id, None):
                for ch_id in site_ancestors[site_id]:
                    ch_status = get_site_status(ch_id, status_rollup, site_ancestors, site_devices, status_by_device)
                    status_rollup[ch_id] = ch_status
                    ch_stat_list.append(ch_status)

            # See if there is a device deployed -  include into rollup in addition to child sites
            # Note: Only look at this device status, not roll-up status of all child devices
            device_info = site_devices.get(site_id, None)
            if device_info:
                if len(device_info) == 1:
                    device_id = device_info[0][1]
                    d_status = self._compute_status(device_id, status_by_device)
                    status_rollup[device_id] = d_status
                    ch_stat_list.append(d_status)
                else:
                    raise BadRequest("More than one device found for site %s" % site_id)

            status_rollup[site_id] = self._rollup_statuses(ch_stat_list)
            return status_rollup[site_id]

        def get_device_status(device_id, status_rollup, child_devices, status_by_device):
            """For one device, compute the aggregate status and recurse to child devices if necessary"""

            if device_id in status_rollup:
                return status_rollup[device_id]

            ch_stat_list = []
            if child_devices.get(device_id, None):
                for _,ch_id,_ in child_devices[device_id]:
                    ch_status = get_device_status(ch_id, status_rollup, child_devices, status_by_device)
                    status_rollup[ch_id] = ch_status
                    ch_stat_list.append(ch_status)

            d_status = self._compute_status(device_id, status_by_device)
            ch_stat_list.append(d_status)

            status_rollup[device_id] = self._rollup_statuses(ch_stat_list)
            return status_rollup[device_id]


        # Do the status rollup work. Different modes dependent on type of resource (org, site, device)
        if res_type in {RT.Org, RT.Observatory, RT.Subsite, RT.PlatformSite, RT.InstrumentSite}:
            if res_type == RT.Org:
                child_sites, site_ancestors = self.get_child_sites(org_id=res_id, id_only=not include_structure)
            else:
                child_sites, site_ancestors = self.get_child_sites(parent_site_id=res_id, id_only=not include_structure)

            site_devices = self.get_device_relations(child_sites.keys())
            device_list = list({tup[1] for key,dev_list in site_devices.iteritems() if dev_list for tup in dev_list})
            dev_status_list = self._get_device_status_list(device_list)
            status_by_device = dict(zip(device_list, dev_status_list))

            status_rollup = {}
            get_site_status(res_id, status_rollup, site_ancestors, site_devices, status_by_device)
            for site_id in child_sites.keys():
                get_site_status(site_id, status_rollup, site_ancestors, site_devices, status_by_device)

            # Stuff extra information into the result
            if include_structure:
                status_rollup['_system'] = dict(res_id=res_id, res_type=res_type,
                    sites=child_sites, ancestors=site_ancestors, devices=site_devices)

            return status_rollup

        elif res_type in [RT.PlatformDevice, RT.InstrumentDevice]:
            # See if current device has child devices
            child_devices = self.get_child_devices(res_id)
            device_list = list(set(child_devices))
            dev_status_list = self._get_device_status_list(device_list)
            status_by_device = dict(zip(device_list, dev_status_list))

            status_rollup = {}
            get_device_status(res_id, status_rollup, child_devices, status_by_device)
            for device_id in child_devices.keys():
                get_device_status(device_id, status_rollup, child_devices, status_by_device)

            # Stuff extra information into the result
            if include_structure:
                status_rollup['_system'] = dict(res_id=res_id, res_type=res_type,
                    ancestors=child_devices)

            return status_rollup

        else:
            raise BadRequest("Unsupported resource type: %s", res_type)

    def _get_device_status_list(self, device_list=None):
        dev_state_list = self.device_status_mgr.read_states(device_list)
        return dev_state_list

    def _compute_status(self, device_id, status_by_device):
        """For a device_id, extract status from status dict, using reasonable defaults"""
        status = {AggregateStatusType.AGGREGATE_POWER: DeviceStatusType.STATUS_UNKNOWN,
                  AggregateStatusType.AGGREGATE_COMMS: DeviceStatusType.STATUS_UNKNOWN,
                  AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_UNKNOWN,
                  AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_UNKNOWN}
        dev_status = status_by_device.get(device_id, None)
        if dev_status and "agg_status" in dev_status:
            if "DEVICE AGENT ACTIVE":
                # TODO: Check current device state.
                # Set all statuses to persisted value if existing and default to OK
                for status_name in status.keys():
                    status[status_name] = dev_status["agg_status"].get(status_name, {}).get("status", DeviceStatusType.STATUS_OK)
                    # TODO: Check last update time. Assume OK if before last agent restart
            else:
                # Agent is not running. Keep statuses as UNKNOWN
                pass

        status['agg'] = self._consolidate_status(status.values())
        return status

    def _rollup_statuses(self, status_list):
        """For a list of child status dicts, compute the rollup statuses"""
        rollup_status = {
            AggregateStatusType.AGGREGATE_POWER: self._consolidate_status(
                [stat[AggregateStatusType.AGGREGATE_POWER] for stat in status_list]),
            AggregateStatusType.AGGREGATE_COMMS: self._consolidate_status(
                [stat[AggregateStatusType.AGGREGATE_COMMS] for stat in status_list]),
            AggregateStatusType.AGGREGATE_DATA: self._consolidate_status(
                [stat[AggregateStatusType.AGGREGATE_DATA] for stat in status_list]),
            AggregateStatusType.AGGREGATE_LOCATION: self._consolidate_status(
                [stat[AggregateStatusType.AGGREGATE_LOCATION] for stat in status_list]),
        }

        rollup_status['agg'] = self._consolidate_status(rollup_status.values())
        return rollup_status

    def _consolidate_status(self, statuses, warn_if_unknown=False):
        """Intelligently merge statuses with current value"""

        # Any critical means all critical
        if DeviceStatusType.STATUS_CRITICAL in statuses:
            return DeviceStatusType.STATUS_CRITICAL

        # Any warning means all warning
        if DeviceStatusType.STATUS_WARNING in statuses:
            return DeviceStatusType.STATUS_WARNING

        # Any unknown is fine unless some are ok -- then it's a warning
        if DeviceStatusType.STATUS_OK in statuses:
            if DeviceStatusType.STATUS_UNKNOWN in statuses and warn_if_unknown:
                return DeviceStatusType.STATUS_WARNING
            else:
                return DeviceStatusType.STATUS_OK

        # 0 results are OK, 0 or more are unknown
        return DeviceStatusType.STATUS_UNKNOWN

