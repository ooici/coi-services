#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.rollx_builder
@author   Ian Katz
"""
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from ooi.logging import log
from pyon.agent.agent import ResourceAgentClient
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound, Unauthorized


from interface.objects import ComputedValueAvailability, ComputedIntValue
from interface.objects import AggregateStatusType, DeviceStatusType
from pyon.ion.resource import PRED


class RollXBuilder(object):
    """
    for rollups and rolldowns
    """

    def __init__(self, process=None):
        """
        the process should be the "self" of a service instance
        """
        assert process
        self.process = process
        self.RR2 = EnhancedResourceRegistryClient(self.process.clients.resource_registry)



    def get_toplevel_platformsite(self, site_id):
        if not self.RR2.has_cached_prediate(PRED.hasSite):
            self.RR2.cache_predicate(PRED.hasSite)

        parent_ids = self.RR2.find_platform_site_ids_by_platform_site_using_has_site(site_id)

        if 0 == len(parent_ids):
            return site_id  # assume this to be the top level

        return self.get_toplevel_platformsite(parent_ids[0])


    def get_parent_network_nodes(self, site_id):
        if not self.RR2.has_cached_prediate(PRED.hasNetworkParent):
            self.RR2.cache_predicate(PRED.hasNetworkParent)

        def get_h(acc, some_id):
            acc.append(some_id)
            parent_ids = self.RR2.find_platform_device_ids_of_platform_device_using_has_network_parent(site_id)
            if 0 == len(parent_ids):
                return acc
            return get_h(acc, parent_ids[0])

        return get_h([], site_id)

    def get_toplevel_network_node(self, device_id):
        if not self.RR2.has_cached_prediate(PRED.hasNetworkParent):
            self.RR2.cache_predicate(PRED.hasNetworkParent)

        parent_ids = self.RR2.find_platform_device_ids_of_platform_device_using_has_network_parent(device_id)

        if 0 == len(parent_ids):
            # it can only be the network parent if it has this association
            if 0 < len(self.RR2.find_platform_device_ids_by_platform_device_using_has_network_parent(device_id)):
                return device_id # assume this to be top level
            else:
                return None

        return self.get_toplevel_network_node(parent_ids[0])


    def get_site_hierarchy(self, site_id, site_val_fn):
        """
        return (child_sites, site_ancestors)
        where child_sites is a dict mapping all child site ids to the value of site_val_fn(child_site_id)
          and site_ancestors is a dict mapping all site ids to a list of their hasSite children.
        """

        if not self.RR2.has_cached_prediate(PRED.hasSite):
            self.RR2.cache_predicate(PRED.hasSite)

        full_list = [site_id]
        acc = {}
        def _get_ancestors_h(s_id):
            s_child_ids = self.RR2.find_objects(s_id, PRED.hasSite, id_only=True)
            if s_child_ids:
                acc[s_id] = s_child_ids
            for scid in s_child_ids:
                _get_ancestors_h(scid)
                full_list.append(scid)

        _get_ancestors_h(site_id)

        return dict([(s, site_val_fn(s)) for s in full_list]), acc


    def get_network_hierarchy(self, device_id, device__val_fn):
        """
        return (child_sites, site_ancestors)
        where child_sites is a dict mapping all child site ids to the value of site_val_fn(child_site_id)
          and site_ancestors is a dict mapping all site ids to a list of their children as per
        """

        if not self.RR2.has_cached_prediate(PRED.hasNetworkParent):
            self.RR2.cache_predicate(PRED.hasNetworkParent)

        full_list = [device_id]
        acc = {}
        def _get_ancestors_h(d_id):
            d_child_ids = self.RR2.find_subjects(d_id, PRED.hasNetworkParent, id_only=True)
            if d_child_ids:
                acc[d_id] = d_child_ids
            for scid in d_child_ids:
                _get_ancestors_h(scid)
                full_list.append(scid)

        _get_ancestors_h(device_id)

        return dict([(d, device__val_fn(d)) for d in full_list]), acc
