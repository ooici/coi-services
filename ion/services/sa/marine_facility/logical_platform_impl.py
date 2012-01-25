#!/usr/bin/env python

"""
@package  ion.services.sa.marine_facility_management.logical_platform_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import AT, RT

from ion.services.sa.marine_facility.mfms_simple import MFMSsimple

class LogicalPlatformImpl(MFMSsimple):
    """
    @brief resource management for LogicalPlatform resources
    """

    def _primary_object_name(self):
        return RT.LogicalPlatform

    def _primary_object_label(self):
        return "logical_platform"

    def link_agent(self, logical_platform_id='', platform_agent_id=''):
        return self._link_resources(logical_platform_id, AT.hasAgent, platform_agent_id)

    def unlink_agent(self, logical_platform_id='', platform_agent_id=''):
        return self._unlink_resources(logical_platform_id, AT.hasAgent, platform_agent_id)

    def link_instrument(self, logical_platform_id='', logical_instrument_id=''):
        return self._link_resources(logical_platform_id, AT.hasInstrument, logical_instrument_id)

    def unlink_instrument(self, logical_platform_id='', logical_instrument_id=''):
        return self._unlink_resources(logical_platform_id, AT.hasInstrument, logical_instrument_id)

    def link_platform(self, logical_platform_id='', logical_platform_child_id=''):
        return self._link_resources(logical_platform_id, AT.hasPlatform, logical_platform_child_id)

    def unlink_platform(self, logical_platform_id='', logical_platform_child_id=''):
        return self._unlink_resources(logical_platform_id, AT.hasPlatform, logical_platform_child_id)

    def find_having_agent(self, platform_agent_id):
        return self._find_having(AT.hasAgent, platform_agent_id)

    def find_stemming_agent(self, logical_platform_id):
        return self._find_stemming(logical_platform_id, AT.hasAgent, RT.PlatformAgent)

    def find_having_instrument(self, logical_instrument_id):
        return self._find_having(AT.hasInstrument, logical_instrument_id)

    def find_stemming_instrument(self, logical_platform_id):
        return self._find_stemming(logical_platform_id, AT.hasInstrument, RT.LogicalInstrument)

    def find_having_platform(self, logical_platform_child_id):
        return self._find_having(AT.hasPlatform, logical_platform_child_id)

    def find_stemming_platform(self, logical_platform_id):
        return self._find_stemming(logical_platform_id, AT.hasPlatform, RT.LogicalPlatform)
