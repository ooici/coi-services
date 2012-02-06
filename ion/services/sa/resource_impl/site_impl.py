#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.management.site_impl
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class SiteImpl(ResourceSimpleImpl):
    """
    @brief resource management for Site resources
    """

    def _primary_object_name(self):
        return RT.Site

    def _primary_object_label(self):
        return "site"

    def link_platform(self, site_id='', logical_platform_id=''):
        return self._link_resources(site_id, PRED.hasPlatform, logical_platform_id)

    def unlink_platform(self, site_id='', logical_platform_id=''):
        return self._unlink_resources(site_id, PRED.hasPlatform, logical_platform_id)

    def link_site(self, site_id='', site_child_id=''):
        return self._link_resources(site_id, PRED.hasSite, site_child_id)

    def unlink_site(self, site_id='', site_child_id=''):
        return self._unlink_resources(site_id, PRED.hasSite, site_child_id)

    def find_having_platform(self, logical_platform_id):
        return self._find_having(PRED.hasPlatform, logical_platform_id)

    def find_stemming_platform(self, logical_platform_id):
        return self._find_stemming(logical_platform_id, PRED.hasPlatform, RT.LogicalPlatform)

    def find_having_site(self, site_child_id):
        return self._find_having(PRED.hasSite, site_child_id)

    def find_stemming_site(self, site_id):
        return self._find_stemming(site_id, PRED.hasSite, RT.Site)
