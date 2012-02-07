#!/usr/bin/env python

'''
@file ion/services/sa/marine_facility/test/test_marine_facility_management_service.py
@author Maurice Manning
@author Ian Katz
@test ion.services.sa.marine_facility.marine_facility Unit test suite to cover all service code
'''

#from mock import Mock , sentinel, patch
from ion.services.sa.marine_facility.marine_facility_management_service import MarineFacilityManagementService
from nose.plugins.attrib import attr
from pyon.public import PRED #, RT


from pyon.util.log import log

from ion.services.sa.resource_impl.resource_impl_metatest import ResourceImplMetatest

from ion.services.sa.resource_impl.logical_instrument_impl import LogicalInstrumentImpl
from ion.services.sa.resource_impl.logical_platform_impl import LogicalPlatformImpl
from ion.services.sa.resource_impl.marine_facility_impl import MarineFacilityImpl
from ion.services.sa.resource_impl.site_impl import SiteImpl







#from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
#import unittest
from pyon.util.unit_test import PyonTestCase


@attr('UNIT', group='sa')
class TestMarineFacilityManagement(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.marine_facility.marine_facility_management_service.IonObject')
        
        #self.mock_ionobj = IonObject
        mock_clients = self._create_service_mock('marine_facility_management')

        self.marine_facility_mgmt_service = MarineFacilityManagementService()
        self.marine_facility_mgmt_service.clients = mock_clients
        
        # must call this manually
        self.marine_facility_mgmt_service.on_init()


rim = ResourceImplMetatest(TestMarineFacilityManagement, MarineFacilityManagementService, log)

rim.add_resource_impl_unittests(LogicalInstrumentImpl, {})
rim.add_resource_impl_unittests(LogicalPlatformImpl, {"buoyname": "steve", "buoyheight": "3"})
rim.add_resource_impl_unittests(MarineFacilityImpl, {})
rim.add_resource_impl_unittests(SiteImpl, {})


