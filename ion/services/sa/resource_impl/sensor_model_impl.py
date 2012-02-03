#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.sensor_model_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class SensorModelImpl(ResourceSimpleImpl):
    """
    @brief resource management for SensorModel resources
    """

    def _primary_object_name(self):
        return RT.SensorModel

    def _primary_object_label(self):
        return "sensor_model"
