#!/usr/bin/env python

"""
@package  ion.services.sa.observatory.management.instrument_site_impl
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.observatory.site_impl import SiteImpl

class InstrumentSiteImpl(SiteImpl):
    """
    @brief resource management for InstrumentSite resources
    """

    def _primary_object_name(self):
        return RT.InstrumentSite

    def _primary_object_label(self):
        return "instrument_site"

    def link_agent(self, instrument_site_id='', instrument_agent_id=''):
        return self._link_resources(instrument_site_id, PRED.hasAgent, instrument_agent_id)

    def unlink_agent(self, instrument_site_id='', instrument_agent_id=''):
        return self._unlink_resources(instrument_site_id, PRED.hasAgent, instrument_agent_id)

    def link_device(self, instrument_site_id='', instrument_device_id=''):
        return self._link_resources(instrument_site_id, PRED.hasDevice, instrument_device_id)

    def unlink_device(self, instrument_site_id='', instrument_device_id=''):
        return self._unlink_resources(instrument_site_id, PRED.hasDevice, instrument_device_id)

    def link_model(self, instrument_site_id='', instrument_model_id=''):
        return self._link_resources_single_object(instrument_site_id, PRED.hasModel, instrument_model_id)

    def unlink_model(self, instrument_site_id='', instrument_model_id=''):
        return self._unlink_resources(instrument_site_id, PRED.hasModel, instrument_model_id)

    def find_having_agent(self, instrument_agent_id):
        return self._find_having(PRED.hasAgent, instrument_agent_id)

    def find_stemming_agent(self, instrument_site_id):
        return self._find_stemming(instrument_site_id, PRED.hasAgent, RT.InstrumentAgent)
    
    def find_having_device(self, instrument_device_id):
        return self._find_having(PRED.hasDevice, instrument_device_id)

    def find_stemming_device(self, instrument_site_id):
        return self._find_stemming(instrument_site_id, PRED.hasDevice, RT.InstrumentDevice)

    def find_having_model(self, instrument_model_id):
        return self._find_having(PRED.hasModel, instrument_model_id)

    def find_stemming_model(self, instrument_site_id):
        return self._find_stemming(instrument_site_id, PRED.hasModel, RT.InstrumentModel)


    def on_pre_delete(self, obj_id, obj):
        #todo: unlink parent/children sites, agents, models, devices?
        return
        
