#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.sensor_device_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.resource_impl.resource_impl import ResourceImpl

class SensorDeviceImpl(ResourceImpl):
    """
    @brief resource management for SensorDevice resources
    """

    def _primary_object_name(self):
        return RT.SensorDevice

    def _primary_object_label(self):
        return "sensor_device"

    def link_model(self, sensor_device_id='', sensor_model_id=''):
        return self._link_resources(sensor_device_id, PRED.hasModel, sensor_model_id)

    def unlink_model(self, sensor_device_id='', sensor_model_id=''):
        return self._unlink_resources(sensor_device_id, PRED.hasModel, sensor_model_id)

    def find_having_model(self, sensor_model_id):
        return self._find_having(PRED.hasModel, sensor_model_id)

    def find_stemming_model(self, sensor_device_id):
        return self._find_stemming(sensor_device_id, PRED.hasModel, RT.SensorModel)
