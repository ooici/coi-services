#!/usr/bin/env python

"""
@package  ion.services.sa.observatory.management.instrument_site_impl
@author   Ian Katz
"""


from pyon.core.exception import BadRequest, Inconsistent
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


    def link_deployment(self, instrument_site_id='', deployment_id=''):
        return self._link_resources_single_object(instrument_site_id, PRED.hasDeployment, deployment_id)

    def unlink_deployment(self, instrument_site_id='', deployment_id=''):
        return self._unlink_resources(instrument_site_id, PRED.hasDeployment, deployment_id)

    def link_device(self, instrument_site_id='', instrument_device_id=''):
        # a device may not be linked to any other site
        if 0 < len(self._find_having(PRED.hasDevice, instrument_device_id)):
            raise BadRequest("Instrument device is already associated with a site")

        # make sure that only 1 site-device-deployment triangle exists at one time
        deployments_site = self.find_stemming_deployment(instrument_site_id)
        if 1 < len(deployments_site):
            raise Inconsistent("The site is associated to multiple deployments!")

        deployments_inst = self._find_stemming(instrument_device_id, PRED.hasDeployment, RT.Deployment)
        if 1 < len(deployments_inst):
            raise Inconsistent("The instrument device is associated to multiple deployments!")

        if 1 == len(deployments_inst):
            if 1 == len(deployments_site):
                if deployments_site[0]._id != deployments_inst[0]._id:
                    raise BadRequest("The deployments of the device and site do not agree")

            for dev in self.find_stemming_device(instrument_site_id):
                if 0 < len(self._find_stemming(dev, PRED.hasDeployment, RT.Deployment)):
                    raise BadRequest("Device has deployment, and site already has a device with deployment")

        return self._link_resources(instrument_site_id, PRED.hasDevice, instrument_device_id)

    def unlink_device(self, instrument_site_id='', instrument_device_id=''):
        return self._unlink_resources(instrument_site_id, PRED.hasDevice, instrument_device_id)

    def link_model(self, instrument_site_id='', instrument_model_id=''):
        return self._link_resources_single_object(instrument_site_id, PRED.hasModel, instrument_model_id)

    def unlink_model(self, instrument_site_id='', instrument_model_id=''):
        return self._unlink_resources(instrument_site_id, PRED.hasModel, instrument_model_id)

    def link_output_product(self, site_id, data_product_id):
        # output product can't be linked to any other site, this site can't be linked to any other output product
        if 0 < len(self.find_stemming_output_product(site_id)):
            raise BadRequest("Site already has an output data product assigned")

        return self._link_resources_single_subject(site_id, PRED.hasOutputProduct, data_product_id)

    def unlink_output_product(self, site_id, data_product_id):
        return self._unlink_resources(site_id, PRED.hasOutputProduct, data_product_id)

    def find_having_deployment(self, deployment_id):
        return self._find_having(PRED.hasDeployment, deployment_id)

    def find_stemming_deployment(self, instrument_site_id):
        return self._find_stemming(instrument_site_id, PRED.hasDeployment, RT.Deployment)

    def find_having_device(self, instrument_device_id):
        return self._find_having(PRED.hasDevice, instrument_device_id)

    def find_stemming_device(self, instrument_site_id):
        return self._find_stemming(instrument_site_id, PRED.hasDevice, RT.InstrumentDevice)

    def find_having_model(self, instrument_model_id):
        return self._find_having(PRED.hasModel, instrument_model_id)

    def find_stemming_model(self, instrument_site_id):
        return self._find_stemming(instrument_site_id, PRED.hasModel, RT.InstrumentModel)

    def find_having_output_product(self, data_product_id):
        return self._find_having(PRED.hasOutputProduct, data_product_id)

    def find_stemming_output_product(self, site_id):
        return self._find_stemming(site_id, PRED.hasModel, RT.DataProduct)

    def on_pre_delete(self, obj_id, obj):
        #todo: unlink parent/children sites, agents, models, devices?
        return
        
