#!/usr/bin/env python

'''
@file ion/services/sa/observatory/test/test_observatory_management_service.py
@author Maurice Manning
@author Ian Katz
@test ion.services.sa.observatory.observatory Unit test suite to cover all service code
'''

#from mock import Mock , sentinel, patch
from ion.services.sa.observatory.observatory_management_service import ObservatoryManagementService
from nose.plugins.attrib import attr
from pyon.public import PRED #, RT


from pyon.util.log import log

from ion.services.sa.resource_impl.resource_impl_metatest import ResourceImplMetatest

from ion.services.sa.observatory.instrument_site_impl import InstrumentSiteImpl
from ion.services.sa.observatory.platform_site_impl import PlatformSiteImpl
from ion.services.sa.observatory.observatory_impl import ObservatoryImpl
from ion.services.sa.observatory.subsite_impl import SubsiteImpl







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


rim = ResourceImplMetatest(TestObservatoryManagement, ObservatoryManagementService, log)

rim.add_resource_impl_unittests(InstrumentSiteImpl, {})
rim.add_resource_impl_unittests(PlatformSiteImpl, {})
rim.add_resource_impl_unittests(ObservatoryImpl, {})
rim.add_resource_impl_unittests(SubsiteImpl, {})


