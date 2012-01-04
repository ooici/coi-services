#!/usr/bin/env python

"""
@file ion/services/sa/instrument/test/test_instrument_management_service.py
@author Ian Katz
@test ion.services.sa.instrument.instrument_management_service Unit test suite to cover all service code
"""

from mock import Mock #, sentinel, patch
from pyon.util.unit_test import PyonTestCase
from ion.services.sa.instrument.instrument_management_service import InstrumentManagementService
from nose.plugins.attrib import attr

from pyon.util.log import log

#from pyon.core.bootstrap import IonObject

from ion.services.sa.resource_dryer_metatest import ResourceDryerMetatest

from ion.services.sa.instrument.instrument_agent_instance_dryer import InstrumentAgentInstanceDryer
from ion.services.sa.instrument.instrument_agent_dryer import InstrumentAgentDryer
from ion.services.sa.instrument.instrument_device_dryer import InstrumentDeviceDryer
from ion.services.sa.instrument.instrument_model_dryer import InstrumentModelDryer
from ion.services.sa.instrument.platform_agent_instance_dryer import PlatformAgentInstanceDryer
from ion.services.sa.instrument.platform_agent_dryer import PlatformAgentDryer
from ion.services.sa.instrument.platform_device_dryer import PlatformDeviceDryer
from ion.services.sa.instrument.platform_model_dryer import PlatformModelDryer
from ion.services.sa.instrument.sensor_device_dryer import SensorDeviceDryer
from ion.services.sa.instrument.sensor_model_dryer import SensorModelDryer

#from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
#import unittest

@attr('UNIT', group='sa')
class TestInstrumentManagement(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.instrument.instrument_management_service.IonObject')
        #self.mock_ionobj = IonObject
        mock_clients = self._create_service_mock('instrument_management_service')

        self.instrument_mgmt_service = InstrumentManagementService()
        self.instrument_mgmt_service.clients = mock_clients
        
        # must call this manually
        self.instrument_mgmt_service.on_init()

rwm = ResourceDryerMetatest(TestInstrumentManagement, InstrumentManagementService, log)

rwm.add_resource_dryer_unittests(InstrumentAgentInstanceDryer, {"exchange-name": "rhubarb"})
rwm.add_resource_dryer_unittests(InstrumentAgentDryer, {"agent_version": "3", "time_source": "the universe"})
rwm.add_resource_dryer_unittests(InstrumentDeviceDryer, {"serialnumber": "123", "firmwareversion": "x"})
rwm.add_resource_dryer_unittests(InstrumentModelDryer, {"model": "redundant?", "weight": 20000})
rwm.add_resource_dryer_unittests(PlatformAgentInstanceDryer, {"exchange-name": "sausage"})
rwm.add_resource_dryer_unittests(PlatformAgentDryer, {"tbd": "the big donut"})
rwm.add_resource_dryer_unittests(PlatformDeviceDryer, {"serial_number": "2345"})
rwm.add_resource_dryer_unittests(PlatformModelDryer, {"tbd": "tammy breathed deeply"})
rwm.add_resource_dryer_unittests(SensorDeviceDryer, {"serialnumber": "123"})
rwm.add_resource_dryer_unittests(SensorModelDryer, {"model": "redundant field?", "weight": 2})


