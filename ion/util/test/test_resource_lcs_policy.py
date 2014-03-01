#!/usr/bin/env python

"""
@file ion/util/test/test_resource_lcs_policy.py
@author Ian Katz
@test ion.util.resource_lcs_policy Unit test suite
"""

from unittest.case import SkipTest
from ion.services.sa.test.helpers import any_old
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from mock import Mock #, sentinel, patch
from ion.util.resource_lcs_policy import ResourceLCSPolicy
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Inconsistent, NotFound
from pyon.ion.resource import RT, PRED, LCE
from pyon.util.containers import DotDict
from pyon.util.unit_test import PyonTestCase


@attr('UNIT', group='sa')
class TestResourceLCSPolicy(PyonTestCase):

    def setUp(self):
        self.clients = DotDict()

        self.rr = Mock()
        self.RR2 = EnhancedResourceRegistryClient(self.rr)

        self.clients.resource_registry = self.rr

        self.policy = ResourceLCSPolicy(self.clients)



    def test_events(self):
        self.rr.read.return_value = Mock()

        #if True: self.fail("%s" % LCE.keys())
        for event in LCE.values():
            self.rr.reset_mock()
            success, msg = self.policy.check_lcs_precondition_satisfied("rsrc_id", event)
            self.assertTrue(success)
            self.assertEqual("ResourceLCSPolicy base class not overridden!", msg)