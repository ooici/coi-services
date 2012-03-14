#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.platform_device_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.resource_impl.resource_impl import ResourceImpl

class PlatformDeviceImpl(ResourceImpl):
    """
    @brief resource management for PlatformDevice resources
    """

    def _primary_object_name(self):
        return RT.PlatformDevice

    def _primary_object_label(self):
        return "platform_device"

    def link_agent_instance(self, platform_device_id='', platform_agent_instance_id=''):
        return self._link_resources(platform_device_id, PRED.hasAgentInstance, platform_agent_instance_id)

    def unlink_agent_instance(self, platform_device_id='', platform_agent_instance_id=''):
        return self._unlink_resources(platform_device_id, PRED.hasAgentInstance, platform_agent_instance_id)

    def link_deployment(self, platform_device_id='', logical_platform_id=''):
        return self._link_resources(platform_device_id, PRED.hasDeployment, logical_platform_id)

    def unlink_deployment(self, platform_device_id='', logical_platform_id=''):
        return self._unlink_resources(platform_device_id, PRED.hasDeployment, logical_platform_id)

    def link_primary_deployment(self, platform_device_id='', logical_platform_id=''):
        return self._link_resources(platform_device_id, PRED.hasPrimaryDeployment, logical_platform_id)

    def unlink_primary_deployment(self, platform_device_id='', logical_platform_id=''):
        return self._unlink_resources(platform_device_id, PRED.hasPrimaryDeployment, logical_platform_id)

    def link_model(self, platform_device_id='', platform_model_id=''):
        return self._link_resources(platform_device_id, PRED.hasModel, platform_model_id)

    def unlink_model(self, platform_device_id='', platform_model_id=''):
        return self._unlink_resources(platform_device_id, PRED.hasModel, platform_model_id)

    def link_logicalmodel(self, logical_platform_id='', platform_model_id=''):
        return self._link_resources(logical_platform_id, PRED.hasModel, platform_model_id)

    def unlink_logicalmodel(self, logical_platform_id='', platform_model_id=''):
        return self._unlink_resources(logical_platform_id, PRED.hasModel, platform_model_id)

    def link_instrument(self, platform_device_id='', instrument_device_id=''):
        return self._link_resources(platform_device_id, PRED.hasInstrument, instrument_device_id)

    def unlink_instrument(self, platform_device_id='', instrument_device_id=''):
        return self._unlink_resources(platform_device_id, PRED.hasInstrument, instrument_device_id)


    ### finds

    def find_having_agent_instance(self, platform_agent_instance_id):
        return self._find_having(PRED.hasAgentInstance, platform_agent_instance_id)

    def find_stemming_agent_instance(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasAgentInstance, RT.PlatformAgentInstance)

    def find_having_assignment(self, logical_platform_id):
        return self._find_having(PRED.hasAssignment, logical_platform_id)

    def find_stemming_assignment(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasAssignment, RT.LogicalPlatform)

    def find_having_deployment(self, logical_platform_id):
        return self._find_having(PRED.hasDeployment, logical_platform_id)

    def find_stemming_deployment(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasDeployment, RT.LogicalPlatform)

    def find_having_model(self, platform_model_id):
        return self._find_having(PRED.hasModel, platform_model_id)

    def find_stemming_model(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasModel, RT.PlatformModel)

    def find_having_instrument(self, instrument_device_id):
        return self._find_having(PRED.hasInstrument, instrument_device_id)

    def find_stemming_instrument(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasInstrument, RT.InstrumentDevice)
