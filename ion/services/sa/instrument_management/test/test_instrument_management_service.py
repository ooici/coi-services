#!/usr/bin/env python

"""
@file ion/services/sa/instrument_management/test/test_instrument_management_service.py
@author Ian Katz
@test ion.services.sa.instrument_management.instrument_management_service Unit test suite to cover all service code
"""

from mock import Mock #, sentinel, patch
from pyon.util.unit_test import PyonTestCase
from ion.services.sa.instrument_management.instrument_management_service import InstrumentManagementService
from nose.plugins.attrib import attr

from pyon.util.log import log

#from pyon.core.bootstrap import IonObject

#from ion.services.sa.resource_worker_metatest import add_resource_worker_unittests
from ion.services.sa.resource_worker_metatest import ResourceWorkerMetatest

from ion.services.sa.instrument_management.instrument_agent_instance_worker import InstrumentAgentInstanceWorker

from ion.services.sa.instrument_management.instrument_agent_worker import InstrumentAgentWorker
from ion.services.sa.instrument_management.instrument_device_worker import InstrumentDeviceWorker
from ion.services.sa.instrument_management.instrument_model_worker import InstrumentModelWorker
from ion.services.sa.instrument_management.logical_instrument_worker import LogicalInstrumentWorker
from ion.services.sa.instrument_management.logical_platform_worker import LogicalPlatformWorker
from ion.services.sa.instrument_management.platform_agent_instance_worker import PlatformAgentInstanceWorker
from ion.services.sa.instrument_management.platform_agent_worker import PlatformAgentWorker
from ion.services.sa.instrument_management.platform_device_worker import PlatformDeviceWorker
from ion.services.sa.instrument_management.platform_model_worker import PlatformModelWorker
from ion.services.sa.instrument_management.sensor_device_worker import SensorDeviceWorker
from ion.services.sa.instrument_management.sensor_model_worker import SensorModelWorker

#from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
#import unittest

@attr('UNIT', group='sa')
class TestInstrumentManagement(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.instrument_management.instrument_management_service.IonObject')
        #self.mock_ionobj = IonObject
        mock_clients = self._create_service_mock('instrument_management_service')

        self.instrument_mgmt_service = InstrumentManagementService()
        self.instrument_mgmt_service.clients = mock_clients
        
        # must call this manually
        self.instrument_mgmt_service.on_init()

rwm = ResourceWorkerMetatest(TestInstrumentManagement, InstrumentManagementService, log)

rwm.add_resource_worker_unittests(InstrumentAgentInstanceWorker, {"exchange-name": "rhubarb"})
rwm.add_resource_worker_unittests(InstrumentAgentWorker, {})
rwm.add_resource_worker_unittests(InstrumentDeviceWorker, {})
rwm.add_resource_worker_unittests(InstrumentModelWorker, {})
rwm.add_resource_worker_unittests(LogicalInstrumentWorker, {})
rwm.add_resource_worker_unittests(LogicalPlatformWorker, {})
rwm.add_resource_worker_unittests(PlatformAgentInstanceWorker, {})
rwm.add_resource_worker_unittests(PlatformAgentWorker, {})
rwm.add_resource_worker_unittests(PlatformDeviceWorker, {})
rwm.add_resource_worker_unittests(PlatformModelWorker, {})
rwm.add_resource_worker_unittests(SensorDeviceWorker, {})
rwm.add_resource_worker_unittests(SensorModelWorker, {})


"""
add_resource_worker_unittests(InstrumentAgentInstanceWorker, 
                              TestInstrumentManagement, 
                              InstrumentManagementService,
                              log, 
                              {"exchange_name": "rhubarb"})
"""
