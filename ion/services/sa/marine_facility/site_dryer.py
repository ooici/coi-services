#!/usr/bin/env python

"""
@package  ion.services.sa.marine_facility_management.site_dryer
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import AT, RT

from ion.services.sa.marine_facility.mpms_simple import MPMSsimple

class SiteDryer(MPMSsimple):
    """
    @brief resource management for Site resources
    """

    def _primary_object_name(self):
        return RT.Site

    def _primary_object_label(self):
        return "site"

    def link_platform(self, site_id='', logical_platform_id=''):
        return self._link_resources(site_id, AT.hasPlatform, logical_platform_id)

    def unlink_platform(self, site_id='', logical_platform_id=''):
        return self._unlink_resources(site_id, AT.hasPlatform, logical_platform_id)

    def link_site(self, site_id='', site_child_id=''):
        return self._link_resources(site_id, AT.hasSite, site_child_id)

    def unlink_site(self, site_id='', site_child_id=''):
        return self._unlink_resources(site_id, AT.hasSite, site_child_id)

    def find_having_platform(self, logical_platform_id):
        return self._find_having(AT.hasPlatform, logical_platform_id)

    def find_stemming_platform(self, logical_platform_id):
        return self._find_stemming(logical_platform_id, AT.hasPlatform, RT.LogicalPlatform)

    def find_having_site(self, site_child_id):
        return self._find_having(AT.hasSite, site_child_id)

    def find_stemming_site(self, site_id):
        return self._find_stemming(site_id, AT.hasSite, RT.Site)
