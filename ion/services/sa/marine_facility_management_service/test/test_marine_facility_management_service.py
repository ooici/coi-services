#!/usr/bin/env python

'''
@file ion/services/sa/marine_facility_management_service/test/test_marine_facility_management_service.py
@author Maurice Manning
@author Ian Katz
@test ion.services.sa.marine_facility_management_service.marine_facility_management_service Unit test suite to cover all service code
'''

from mock import Mock, sentinel, patch
from pyon.util.unit_test import PyonTestCase
from ion.services.sa.marine_facility_management_service.marine_facility_management_service import MarineFacilityManagementService
from nose.plugins.attrib import attr
from pyon.public import AT, RT

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
import unittest

from pyon.util.log import log

from ion.services.sa.resource_dryer_metatest import ResourceDryerMetatest

from ion.services.sa.marine_facility_management_service.logical_instrument_dryer import LogicalInstrumentDryer
from ion.services.sa.marine_facility_management_service.logical_platform_dryer import LogicalPlatformDryer
from ion.services.sa.marine_facility_management_service.marine_facility_dryer import MarineFacilityDryer
from ion.services.sa.marine_facility_management_service.site_dryer import SiteDryer


@attr('UNIT', group='sa')
class TestMarineFacilityManagement(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.marine_facility_management_service.marine_facility_management_service.IonObject')
        #self.mock_ionobj = IonObject
        mock_clients = self._create_service_mock('marine_facility_management')

        self.marine_facility_mgmt_service = MarineFacilityManagementService()
        self.marine_facility_mgmt_service.clients = mock_clients
        
        # must call this manually
        self.marine_facility_mgmt_service.on_init()



    def test_assign_instrument(self):
        self.marine_facility_mgmt_service.assign_instrument('111', '222')

        self.marine_facility_mgmt_service.clients.resource_registry.create_association.assert_called_once_with('222', AT.hasInstrument, '111', None)

rwm = ResourceDryerMetatest(TestMarineFacilityManagement, MarineFacilityManagementService, log)

rwm.add_resource_dryer_unittests(LogicalInstrumentDryer, {})
rwm.add_resource_dryer_unittests(LogicalPlatformDryer, {"buoyname": "steve", "buoyheight": "3"})
rwm.add_resource_dryer_unittests(MarineFacilityDryer, {})
rwm.add_resource_dryer_unittests(SiteDryer, {})


