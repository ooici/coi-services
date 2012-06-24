#!/usr/bin/env python

"""
@package  ion.services.sa.observatory.management.platform_site_impl
@author   Ian Katz
"""

from pyon.core.exception import BadRequest, Inconsistent
from pyon.public import PRED, RT

from ion.services.sa.observatory.site_impl import SiteImpl

class PlatformSiteImpl(SiteImpl):
    """
    @brief resource management for PlatformSite resources
    """

    def _primary_object_name(self):
        return RT.PlatformSite

    def _primary_object_label(self):
        return "platform_site"


    def link_deployment(self, platform_site_id='', deployment_id=''):
        return self._link_resources_single_object(platform_site_id, PRED.hasDeployment, deployment_id)

    def unlink_deployment(self, platform_site_id='', deployment_id=''):
        return self._unlink_resources(platform_site_id, PRED.hasDeployment, deployment_id)

    def link_device(self, platform_site_id='', platform_device_id=''):
        # a device may not be linked to any other site
        if 0 < len(self._find_having(PRED.hasDevice, platform_device_id)):
            raise BadRequest("Platform device is already associated with a site")

        # make sure that only 1 site-device-deployment triangle exists at one time
        deployments_site = self.find_stemming_deployment(platform_site_id)
        if 1 < len(deployments_site):
            raise Inconsistent("The site is associated to multiple deployments!")

        deployments_inst = self._find_stemming(platform_site_id, PRED.hasDeployment, RT.Deployment)
        if 1 < len(deployments_inst):
            raise Inconsistent("The platform device is associated to multiple deployments!")

        if 1 == len(deployments_inst):
            if 1 == len(deployments_site):
                if deployments_site[0]._id != deployments_inst[0]._id:
                    raise BadRequest("The deployments of the device and site do not agree")

            for dev in self.find_stemming_device(platform_site_id):
                if 0 < len(self._find_stemming(dev, PRED.hasDeployment, RT.Deployment)):
                    raise BadRequest("Device has deployment, and site already has a device with deployment")

        return self._link_resources(platform_site_id, PRED.hasDevice, platform_device_id)

    def unlink_device(self, platform_site_id='', platform_device_id=''):
        return self._unlink_resources(platform_site_id, PRED.hasDevice, platform_device_id)

    def link_model(self, platform_site_id='', platform_model_id=''):
        return self._link_resources_single_object(platform_site_id, PRED.hasModel, platform_model_id)

    def unlink_model(self, platform_site_id='', platform_model_id=''):
        return self._unlink_resources(platform_site_id, PRED.hasModel, platform_model_id)

    def find_having_deployment(self, deployment_id):
        return self._find_having(PRED.hasDeployment, deployment_id)

    def find_stemming_deployment(self, platform_site_id):
        return self._find_stemming(platform_site_id, PRED.hasDeployment, RT.Deployment)

    def find_having_device(self, platform_device_id):
        return self._find_having(PRED.hasDevice, platform_device_id)

    def find_stemming_device(self, platform_site_id):
        return self._find_stemming(platform_site_id, PRED.hasDevice, RT.PlatformDevice)

    def find_having_model(self, platform_model_id):
        return self._find_having(PRED.hasModel, platform_model_id)

    def find_stemming_model(self, platform_site_id):
        return self._find_stemming(platform_site_id, PRED.hasModel, RT.PlatformModel)


    def find_stemming_platform_site(self, site_id):
        return self._find_stemming(site_id, PRED.hasSite, RT.PlatformSite)

    def find_stemming_instrument_site(self, site_id):
        return self._find_stemming(site_id, PRED.hasSite, RT.InstrumentSite)


    def find_stemming_site(self, site_id):
        return self.find_stemming_instrument_site(site_id) + self.find_stemming_platform_site(site_id)
        

    def on_pre_delete(self, obj_id, obj):
        #todo: unlink parent/children sites, agents, models, devices?
        return
