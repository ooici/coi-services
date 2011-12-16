#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound

from ion.services.sa.instrument_management.ims_worker import IMSworker

class SensorDeviceWorker(IMSworker):

    def _primary_object_name(self):
        return "SensorDevice"

    def _primary_object_label(self):
        return "sensor_device"

    def link_model(self, sensor_device_id='', sensor_model_id=''):
        raise NotImplementedError()

    def unlink_model(self, sensor_device_id='', sensor_model_id=''):
        raise NotImplementedError()

