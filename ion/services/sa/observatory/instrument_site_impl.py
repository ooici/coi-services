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
        return self._link_resources(instrument_site_id, PRED.hasDeployment, deployment_id)

    def unlink_deployment(self, instrument_site_id='', deployment_id=''):
        return self._unlink_resources(instrument_site_id, PRED.hasDeployment, deployment_id)

    def link_device(self, instrument_site_id='', instrument_device_id=''):
        # a device may not be linked to any other site
        if 0 < len(self._find_having(PRED.hasDevice, instrument_device_id)):
            raise BadRequest("Platform device is already associated with a site")

        if 0 < len(self.find_stemming_device(instrument_site_id)):
            raise BadRequest("Platform site already has an associated device")

        # make sure that the device and site share a deployment
        deployments_dev = self._find_stemming(instrument_device_id, PRED.hasDeployment, RT.Deployment)
        deployments_site = self._find_stemming(instrument_site_id, PRED.hasDeployment, RT.Deployment)

        found_depl = None

        for dd in deployments_dev:
            for sd in deployments_site:
                if dd._id == sd._id:
                    found_depl = dd
                    break
            if found_depl:
                break

        if not found_depl:
            raise BadRequest("Device and site do not share a deployment")


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
        
