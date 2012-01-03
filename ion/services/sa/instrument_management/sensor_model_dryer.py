#!/usr/bin/env python

"""
@package  ion.services.sa.instrument_management.sensor_model_dryer
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT

from ion.services.sa.instrument_management.ims_simple import IMSsimple

class SensorModelDryer(IMSsimple):
    """
    @brief resource management for SensorModel resources
    """

    def _primary_object_name(self):
        return RT.SensorModel

    def _primary_object_label(self):
        return "sensor_model"
