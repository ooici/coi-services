#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.management.platform_site_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.resource_impl.site_impl import SiteImpl

class PlatformSiteImpl(SiteImpl):
    """
    @brief resource management for PlatformSite resources
    """

    def _primary_object_name(self):
        return RT.PlatformSite

    def _primary_object_label(self):
        return "platform_site"

    def link_agent(self, platform_site_id='', platform_agent_id=''):
        return self._link_resources(platform_site_id, PRED.hasAgent, platform_agent_id)

    def unlink_agent(self, platform_site_id='', platform_agent_id=''):
        return self._unlink_resources(platform_site_id, PRED.hasAgent, platform_agent_id)

    def link_device(self, platform_site_id='', platform_device_id=''):
        return self._link_resources(platform_site_id, PRED.hasDevice, platform_device_id)

    def unlink_device(self, platform_site_id='', platform_device_id=''):
        return self._unlink_resources(platform_site_id, PRED.hasDevice, platform_device_id)

    def link_model(self, platform_site_id='', platform_model_id=''):
        return self._link_resources_single_object(platform_site_id, PRED.hasModel, platform_model_id)

    def unlink_model(self, platform_site_id='', platform_model_id=''):
        return self._unlink_resources(platform_site_id, PRED.hasModel, platform_model_id)

    def find_having_agent(self, platform_agent_id):
        return self._find_having(PRED.hasAgent, platform_agent_id)

    def find_stemming_agent(self, platform_site_id):
        return self._find_stemming(platform_site_id, PRED.hasAgent, RT.PlatformAgent)

    def find_having_device(self, platform_device_id):
        return self._find_having(PRED.hasDevice, platform_device_id)

    def find_stemming_device(self, platform_site_id):
        return self._find_stemming(platform_site_id, PRED.hasDevice, RT.PlatformDevice)

    def find_having_model(self, instrument_model_id):
        return self._find_having(PRED.hasModel, instrument_model_id)

    def find_stemming_model(self, instrument_site_id):
        return self._find_stemming(instrument_site_id, PRED.hasModel, RT.InstrumentModel)


    def find_stemming_platform_site(self, site_id):
        return self._find_stemming(site_id, PRED.hasSite, RT.PlatformSite)

    def find_stemming_instrument_site(self, site_id):
        return self._find_stemming(site_id, PRED.hasSite, RT.InstrumentSite)



    def on_pre_delete(self, obj_id, obj):
        #todo: unlink parent/children sites, agents, models, devices?
        return
