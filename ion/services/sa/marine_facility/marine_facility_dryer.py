#!/usr/bin/env python

"""
@package  ion.services.sa.marine_facility_management.marine_facility_dryer
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import AT, RT

from ion.services.sa.marine_facility.mpms_simple import MPMSsimple

class MarineFacilityDryer(MPMSsimple):
    """
    @brief resource management for Marine_Facility resources
    """

    def _primary_object_name(self):
        return RT.MarineFacility

    def _primary_object_label(self):
        return "marine_facility"

    def link_platform(self, marine_facility_id='', platform_device_id=''):
        return self._link_resources(marine_facility_id, AT.hasPlatform, platform_device_id)

    def unlink_platform(self, marine_facility_id='', platform_device_id=''):
        return self._unlink_resources(marine_facility_id, AT.hasPlatform, platform_device_id)

    def link_site(self, marine_facility_id='', site_id=''):
        return self._link_resources(marine_facility_id, AT.hasSite, site_id)

    def unlink_site(self, marine_facility_id='', site_id=''):
        return self._unlink_resources(marine_facility_id, AT.hasSite, site_id)

    def find_having_platform(self, platform_device_id):
        return self._find_having(AT.hasPlatform, platform_device_id)

    def find_stemming_platform(self, platform_device_id):
        return self._find_stemming(platform_device_id, AT.hasPlatform, RT.LogicalPlatform)

    def find_having_site(self, site_id):
        return self._find_having(AT.hasSite, site_id)

    def find_stemming_site(self, site_id):
        return self._find_stemming(site_id, AT.hasSite, RT.Site)
