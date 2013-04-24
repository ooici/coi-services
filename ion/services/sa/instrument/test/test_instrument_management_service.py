#!/usr/bin/env python

"""
@file ion/services/sa/instrument/test/test_instrument_management_service.py
@author Ian Katz
@test ion.services.sa.instrument.instrument_management_service Unit test suite to cover all service code
"""


#from mock import Mock #, sentinel, patch
from ion.services.sa.instrument.instrument_management_service import InstrumentManagementService
from ion.services.sa.test.helpers import UnitTestGenerator
from nose.plugins.attrib import attr



from ooi.logging import log


#from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
import unittest
from pyon.ion.resource import RT
from pyon.util.unit_test import PyonTestCase

unittest # block pycharm inspection

@attr('UNIT', group='sa')
class TestInstrumentManagement(PyonTestCase):

    def setUp(self):

        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.instrument.instrument_management_service.IonObject')

        #self.mock_ionobj = IonObject
        mock_clients = self._create_service_mock('instrument_management')

        self.instrument_mgmt_service = InstrumentManagementService()
        self.instrument_mgmt_service.clients = mock_clients
        
        # must call this manually
        self.instrument_mgmt_service.on_init()

        self.addCleanup(delattr, self, "instrument_mgmt_service")
        self.addCleanup(delattr, self, "mock_ionobj")
        #self.resource_impl_cleanup()

        log.debug("setUp complete")

    #def resource_impl_cleanup(self):
        #pass


utg = UnitTestGenerator(TestInstrumentManagement,
                        InstrumentManagementService)

utg.test_all_in_one(True)

utg.add_resource_unittests(RT.InstrumentAgentInstance, "instrument_agent_instance", {})
utg.add_resource_unittests(RT.InstrumentAgent, "instrument_agent", {"driver_module": "potato"})
utg.add_resource_unittests(RT.InstrumentDevice, "instrument_device", {"serial_number": "123", "firmware_version": "x"})
utg.add_resource_unittests(RT.InstrumentModel, "instrument_model")
utg.add_resource_unittests(RT.PlatformAgentInstance, "platform_agent_instance", {})
utg.add_resource_unittests(RT.PlatformAgent, "platform_agent", {"description": "the big donut"})
utg.add_resource_unittests(RT.PlatformDevice, "platform_device", {"serial_number": "2345"})
utg.add_resource_unittests(RT.PlatformModel, "platform_model", {"description": "desc"})
utg.add_resource_unittests(RT.SensorDevice, "sensor_device", {"serial_number": "123"})
utg.add_resource_unittests(RT.SensorModel, "sensor_model")