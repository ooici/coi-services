#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import AT, RT

from ion.services.sa.instrument_management.ims_worker import IMSworker

class SensorDeviceWorker(IMSworker):

    def _primary_object_name(self):
        return RT.SensorDevice

    def _primary_object_label(self):
        return "sensor_device"

    def link_model(self, sensor_device_id='', sensor_model_id=''):
        return self.link_resources(sensor_device_id, AT.hasModel, sensor_model_id)

    def unlink_model(self, sensor_device_id='', sensor_model_id=''):
        return self.unlink_resources(sensor_device_id, AT.hasModel, sensor_model_id)

