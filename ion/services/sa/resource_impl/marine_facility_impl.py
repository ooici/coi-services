#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.management.marine_facility_impl
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class MarineFacilityImpl(ResourceSimpleImpl):
    """
    @brief resource management for Marine_Facility resources
    """

    def _primary_object_name(self):
        return RT.MarineFacility

    def _primary_object_label(self):
        return "marine_facility"

    def link_platform(self, marine_facility_id='', platform_device_id=''):
        return self._link_resources(marine_facility_id, PRED.hasPlatform, platform_device_id)

    def unlink_platform(self, marine_facility_id='', platform_device_id=''):
        return self._unlink_resources(marine_facility_id, PRED.hasPlatform, platform_device_id)

    def link_site(self, marine_facility_id='', site_id=''):
        return self._link_resources(marine_facility_id, PRED.hasSite, site_id)

    def unlink_site(self, marine_facility_id='', site_id=''):
        return self._unlink_resources(marine_facility_id, PRED.hasSite, site_id)

    def find_having_platform(self, platform_device_id):
        return self._find_having(PRED.hasPlatform, platform_device_id)

    def find_stemming_platform(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasPlatform, RT.LogicalPlatform)

    def find_having_site(self, site_id):
        return self._find_having(PRED.hasSite, site_id)

    def find_stemming_site(self, site_id):
        return self._find_stemming(site_id, PRED.hasSite, RT.Site)
