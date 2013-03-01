#!/usr/bin/env python

"""
@file ion/services/sa/instrument/test/test_instrument_management_service.py
@author Ian Katz
@test ion.services.sa.instrument.instrument_management_service Unit test suite to cover all service code
"""


#from mock import Mock #, sentinel, patch
from ion.services.sa.instrument.instrument_management_service import InstrumentManagementService
from nose.plugins.attrib import attr



from ooi.logging import log


#from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
import unittest
from pyon.util.unit_test import PyonTestCase


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

    #def resource_impl_cleanup(self):
        #pass


