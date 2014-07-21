#!/usr/bin/env python

"""Helper for Asset Tracking Attributes, Attribute Specifications, Attribute Values and Computed Attributes"""

__author__ = 'Matt Arrott'

from pyon.core import bootstrap
from pyon.core.exception import BadRequest
from pyon.public import RT, PRED, log

class AssetTrackingUtil(object):
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
