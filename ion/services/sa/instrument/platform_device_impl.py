#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.platform_device_impl
@author   Ian Katz
"""

from pyon.core.exception import BadRequest, Inconsistent
from pyon.public import PRED, RT, LCE

from ion.services.sa.resource_impl.resource_impl import ResourceImpl
from ion.services.sa.instrument.resource_lcs_policy import DevicePolicy

class PlatformDeviceImpl(ResourceImpl):
    """
    @brief resource management for PlatformDevice resources
    """

    def on_impl_init(self):
        self.policy = DevicePolicy(self.clients)

        self.add_lce_precondition(LCE.PLAN, self.use_policy(self.policy.lce_precondition_plan))
        self.add_lce_precondition(LCE.DEVELOP, self.use_policy(self.policy.lce_precondition_develop))
        self.add_lce_precondition(LCE.INTEGRATE, self.use_policy(self.policy.lce_precondition_integrate))
        self.add_lce_precondition(LCE.DEPLOY, self.use_policy(self.policy.lce_precondition_deploy))
        self.add_lce_precondition(LCE.RETIRE, self.use_policy(self.policy.lce_precondition_retire))

    def _primary_object_name(self):
        return RT.PlatformDevice

    def _primary_object_label(self):
        return "platform_device"

    def link_agent_instance(self, platform_device_id='', platform_agent_instance_id=''):
        return self._link_resources(platform_device_id, PRED.hasAgentInstance, platform_agent_instance_id)

    def unlink_agent_instance(self, platform_device_id='', platform_agent_instance_id=''):
        return self._unlink_resources(platform_device_id, PRED.hasAgentInstance, platform_agent_instance_id)

    def link_deployment(self, platform_device_id='', deployment_id=''):
        # make sure that only 1 site-device-deployment triangle exists at one time
        sites, _ = self.RR.find_subjects(RT.PlatformSite, PRED.hasDevice, platform_device_id, False)
        if 1 < len(sites):
            raise Inconsistent("Device is assigned to more than one site")
        if 1 == len(sites):
            site_deployments = self._find_stemming(sites[0]._id, PRED.hasDeployment, RT.Deployment)
            if 1 < len(site_deployments):
                raise Inconsistent("Site has more than one deployment")
            if 1 == len(site_deployments):
                if site_deployments[0]._id != deployment_id:
                    raise BadRequest("Site to which this device is assigned has a different deployment")

            for dev in self._find_stemming(sites[0]._id, PRED.hasDevice, RT.PlatformDevice):
                if 0 < len(self._find_stemming(dev, PRED.hasDeployment, RT.Deployment)):
                    raise BadRequest("Site already has a device with a deployment")

        return self._link_resources_single_object(platform_device_id, PRED.hasDeployment, deployment_id)

    def unlink_deployment(self, platform_device_id='', deployment_id=''):
        return self._unlink_resources(platform_device_id, PRED.hasDeployment, deployment_id)

    def link_model(self, platform_device_id='', platform_model_id=''):
        return self._link_resources(platform_device_id, PRED.hasModel, platform_model_id)

    def unlink_model(self, platform_device_id='', platform_model_id=''):
        return self._unlink_resources(platform_device_id, PRED.hasModel, platform_model_id)

    def link_instrument_device(self, platform_device_id='', instrument_device_id=''):
        return self._link_resources(platform_device_id, PRED.hasDevice, instrument_device_id)

    def unlink_instrument_device(self, platform_device_id='', instrument_device_id=''):
        return self._unlink_resources(platform_device_id, PRED.hasDevice, instrument_device_id)

    def link_platform_device(self, platform_device_id='', child_platform_device_id=''):
        return self._link_resources(platform_device_id, PRED.hasDevice, child_platform_device_id)

    def unlink_platform_device(self, platform_device_id='', instrument_device_id=''):
        return self._unlink_resources(platform_device_id, PRED.hasDevice, instrument_device_id)


    ### finds

    def find_having_agent_instance(self, platform_agent_instance_id):
        return self._find_having(PRED.hasAgentInstance, platform_agent_instance_id)

    def find_stemming_agent_instance(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasAgentInstance, RT.PlatformAgentInstance)

    def find_having_deployment(self, deployment_id):
        return self._find_having(PRED.hasDeployment, deployment_id)

    def find_stemming_deployment(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasDeployment, RT.Deployment)

    def find_having_model(self, platform_model_id):
        return self._find_having(PRED.hasModel, platform_model_id)

    def find_stemming_model(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasModel, RT.PlatformModel)

    def find_having_instrument_device(self, instrument_device_id):
        return self._find_having(PRED.hasDevice, instrument_device_id)

    def find_stemming_instrument_device(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasDevice, RT.InstrumentDevice)

    def find_having_platform_device(self, platform_device_id):
        return self._find_having(PRED.hasDevice, platform_device_id)

    def find_stemming_platform_device(self, platform_device_id):
        return self._find_stemming(platform_device_id, PRED.hasDevice, RT.PlatformDevice)


    # LIFECYCLE STATE PRECONDITIONS

    def lce_precondition_plan(self, platform_device_id):
        if 0 < len(self.find_stemming_model(platform_device_id)):
            return ""
        return "Can't have a planned platform_device without associated platform_model"



    def lce_precondition_develop(self, platform_device_id):
        if 0 < len(self.find_stemming_agent_instance(platform_device_id)):
            return ""
        return "Can't have a developed platform_device without associated platform_agent_instance"


    def lce_precondition_integrate(self, platform_device_id):
        has_passing_certification = True #todo.... get this programmatically somehow
        if has_passing_certification:
            return ""
        return "Can't have an integrated platform_device without certification"
