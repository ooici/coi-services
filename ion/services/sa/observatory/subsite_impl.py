#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.management.subsite_impl
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.observatory.site_impl import SiteImpl

class SubsiteImpl(SiteImpl):
    """
    @brief resource management for InstrumentSite resources
    """

    def _primary_object_name(self):
        return RT.Subsite

    def _primary_object_label(self):
        return "subsite"

    def on_pre_delete(self, obj_id, obj):
        #unlink from all parent sites

        for parent_site in self.find_having_site(obj_id):
            self.unlink_site(parent_site._id, obj_id)


        #unlink from all child sites
        for child_site in self.find_stemming_site(obj_id):
            self.unlink_site(child_site._id, obj_id)


    def find_stemming_site(self, site_id):
        return self.find_stemming_subsite(site_id) + self.find_stemming_platform_site(site_id)
        
    def find_stemming_subsite(self, site_id):
        return self._find_stemming(site_id, PRED.hasSite, RT.Subsite)

    def find_stemming_platform_site(self, site_id):
        return self._find_stemming(site_id, PRED.hasSite, RT.PlatformSite)

