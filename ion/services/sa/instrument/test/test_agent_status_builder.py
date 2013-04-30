#!/usr/bin/env python

"""
@author Ian Katz
"""
from interface.objects import DeviceStatusType
from ion.services.sa.instrument.status_builder import AgentStatusBuilder
from mock import Mock
from nose.plugins.attrib import attr

from ooi.logging import log


#from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
import unittest
from pyon.ion.resource import RT
from pyon.util.unit_test import PyonTestCase

unittest # block pycharm inspection

@attr('UNIT', group='sa')
class TestAgentStatusBuilder(PyonTestCase):

    def setUp(self):
        self.ASB = AgentStatusBuilder(Mock())


    def test_crush_list(self):

        some_values = []
        self.assertEqual(DeviceStatusType.STATUS_UNKNOWN, self.ASB._crush_status_list(some_values))
        def add_and_check(onestatus):
            oldsize = len(some_values)
            some_values.append(onestatus)
            self.assertEqual(oldsize + 1, len(some_values))
            self.assertEqual(onestatus, self.ASB._crush_status_list(some_values))

        # each successive addition here should become the returned status -- they are done in increasing importance
        add_and_check(DeviceStatusType.STATUS_UNKNOWN)
        add_and_check(DeviceStatusType.STATUS_OK)
        add_and_check(DeviceStatusType.STATUS_WARNING)
        add_and_check(DeviceStatusType.STATUS_CRITICAL)
