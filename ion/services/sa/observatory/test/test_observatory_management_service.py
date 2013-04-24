#!/usr/bin/env python

'''
@file ion/services/sa/observatory/test/test_observatory_management_service.py
@author Maurice Manning
@author Ian Katz
@test ion.services.sa.observatory.observatory Unit test suite to cover all service code
'''

#from mock import Mock , sentinel, patch
from ion.services.sa.observatory.observatory_management_service import ObservatoryManagementService
from ion.services.sa.test.helpers import UnitTestGenerator
from nose.plugins.attrib import attr
from pyon.ion.resource import RT


from ooi.logging import log






#from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
#import unittest
from pyon.util.unit_test import PyonTestCase


@attr('UNIT', group='sa')
class TestObservatoryManagement(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.observatory.observatory_management_service.IonObject')
        
        #self.mock_ionobj = IonObject
        mock_clients = self._create_service_mock('observatory_management')

        self.observatory_mgmt_service = ObservatoryManagementService()
        self.observatory_mgmt_service.clients = mock_clients
        
        # must call this manually
        self.observatory_mgmt_service.on_init()

utg = UnitTestGenerator(TestObservatoryManagement,
                        ObservatoryManagementService)

utg.test_all_in_one(True)

utg.add_resource_unittests(RT.Deployment, "deployment")
utg.add_resource_unittests(RT.Observatory, "observatory")
utg.add_resource_unittests(RT.Subsite, "subsite")
utg.add_resource_unittests(RT.PlatformSite, "platform_site")
utg.add_resource_unittests(RT.InstrumentSite, "instrument_site")
