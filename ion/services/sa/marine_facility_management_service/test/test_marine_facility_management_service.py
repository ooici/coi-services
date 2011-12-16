#!/usr/bin/env python

'''
@file ion/services/sa/marine_facility_management_service/test/test_marine_facility_management_service.py
@author Maurice Manning
@test ion.services.sa.marine_facility_management_service.marine_facility_management_service Unit test suite to cover all service code
'''

from mock import Mock, sentinel, patch
from pyon.util.unit_test import PyonTestCase
from ion.services.sa.marine_facility_management_service.marine_facility_management_service import MarineFacilityManagementService
from nose.plugins.attrib import attr
from pyon.public import AT, RT

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
import unittest

@attr('UNIT', group='mmm')
class TestMarineFacilityManagement(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('ion.services.sa.marine_facility_management_service.marine_facility_management_service.IonObject')
        mock_clients = self._create_service_mock('marine_facility_management')

        self.marine_facility_mgmt_service = MarineFacilityManagementService()
        self.marine_facility_mgmt_service.clients = mock_clients

        # save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_read = mock_clients.resource_registry.read
        self.mock_create_association = mock_clients.resource_registry.create_association

        self.marine_facility = Mock()
        self.marine_facility.name = 'CISN'
        self.marine_facility.type = 'Observatory'
        self.marine_facility.description = 'CI example facility'


        self.site = Mock()
        self.site.name = 'Site1'
        self.site.type = 'CISN site'
        self.site.description = 'CI example site'

        self.logical_platform = Mock()
        self.logical_platform.name = 'platform1'
        self.logical_platform.description = 'platform description'

        self.instrument = Mock()
        self.instrument.name = 'inst'
        self.instrument.description = 'inst desc'

    ##############################################################################################
    #
    #  Marine Facility
    #
    ##############################################################################################



    def test_create_marine_facility(self):
        self.mock_create.return_value = ('111', 'bla')

        marine_facility_id = self.marine_facility_mgmt_service.create_marine_facility(self.marine_facility)

        self.mock_create.assert_called_once_with(self.marine_facility)
        self.assertEqual(marine_facility_id, '111')

    def test_read_and_update_marine_facility(self):
        self.mock_read.return_value = self.marine_facility

        mf = self.marine_facility_mgmt_service.read_marine_facility('111')

        assert mf is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        mf.type = 'abcd'
        self.mock_update.return_value = ['111', 2]

        self.marine_facility_mgmt_service.update_marine_facility(mf)
        self.mock_update.assert_called_once_with(mf)


    def test_delete_marine_facility(self):
        self.mock_read.return_value = self.marine_facility

        self.marine_facility_mgmt_service.delete_marine_facility('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with(self.marine_facility)

    def test_read_marine_facility_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.marine_facility_mgmt_service.read_marine_facility('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Marine Facility bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_marine_facility_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.marine_facility_mgmt_service.delete_marine_facility('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Marine Facility bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')



    ##############################################################################################
    #
    #  Site
    #
    ##############################################################################################
        
    def test_create_site(self):
        self.mock_create.return_value = ('111', 'bla')

        site_id = self.marine_facility_mgmt_service.create_site(self.site)

        self.mock_create.assert_called_once_with(self.site)
        self.assertEqual(site_id, '111')

    def test_read_and_update_site(self):
        self.mock_read.return_value = self.site

        s = self.marine_facility_mgmt_service.read_site('111')

        assert s is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        s.type = 'Bar'
        self.mock_update.return_value = ['111', 2]

        self.marine_facility_mgmt_service.update_site(s)
        self.mock_update.assert_called_once_with(s)


    def test_delete_site(self):
        self.mock_read.return_value = self.site

        self.marine_facility_mgmt_service.delete_site('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with(self.site)

    def test_read_site_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.marine_facility_mgmt_service.read_site('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Site bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_site_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.marine_facility_mgmt_service.delete_site('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Site bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_assign_site(self):
        self.marine_facility_mgmt_service.assign_site('111', '222')

        self.mock_create_association.assert_called_once_with('222', AT.hasSite, '111', None)



    ##############################################################################################
    #
    #  Logical Platform
    #
    ##############################################################################################


    def test_create_logical_platform(self):
        self.mock_create.return_value = ('111', 'bla')

        logical_platform_id = self.marine_facility_mgmt_service.create_logical_platform(self.logical_platform)

        self.mock_create.assert_called_once_with(self.logical_platform)
        self.assertEqual(logical_platform_id, '111')

    def test_read_and_update_logical_platform(self):
        self.mock_read.return_value = self.logical_platform

        lp = self.marine_facility_mgmt_service.read_logical_platform('111')

        assert lp is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        lp.type = 'Bar'
        self.mock_update.return_value = ['111', 2]

        self.marine_facility_mgmt_service.update_logical_platform(lp)
        self.mock_update.assert_called_once_with(lp)


    def test_delete_logical_platform(self):
        self.mock_read.return_value = self.logical_platform

        self.marine_facility_mgmt_service.delete_logical_platform('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with(self.logical_platform)

    def test_read_logical_platform_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.marine_facility_mgmt_service.read_logical_platform('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Logical platform bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_logical_platform_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.marine_facility_mgmt_service.delete_logical_platform('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Logical platform bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_assign_platform(self):
        self.marine_facility_mgmt_service.assign_platform('111', '222')

        self.mock_create_association.assert_called_once_with('222', AT.hasPlatform, '111', None)


    ##############################################################################################
    #
    #  DataSource
    #
    ##############################################################################################


    def test_create_logical_instrument(self):
        self.mock_create.return_value = ('111', 'bla')

        logical_instrument_id = self.marine_facility_mgmt_service.create_logical_instrument(self.instrument)

        self.mock_create.assert_called_once_with(self.instrument)
        self.assertEqual(logical_instrument_id, '111')

    def test_read_and_update_logical_instrument(self):
        self.mock_read.return_value = self.instrument

        li = self.marine_facility_mgmt_service.read_logical_instrument('111')

        assert li is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        li.type = 'Bar'
        self.mock_update.return_value = ['111', 2]

        self.marine_facility_mgmt_service.update_logical_instrument(li)
        self.mock_update.assert_called_once_with(li)


    def test_delete_logical_instrument(self):
        self.mock_read.return_value = self.instrument

        self.marine_facility_mgmt_service.delete_logical_instrument('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with(self.instrument)

    def test_read_logical_instrument_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.marine_facility_mgmt_service.read_logical_instrument('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Logical instrument bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_logical_instrument_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.marine_facility_mgmt_service.delete_logical_instrument('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Logical instrument bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')


    def test_assign_instrument(self):
        self.marine_facility_mgmt_service.assign_instrument('111', '222')

        self.mock_create_association.assert_called_once_with('222', AT.hasInstrument, '111', None)