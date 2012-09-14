#!/usr/bin/env python

"""
@package ion.agents.platform.oms.test.test_oms_simple
@file    ion/agents/platform/oms/test/test_oms_simple.py
@author  Carlos Rueda
@brief   Test cases for OmsClient.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.oms.oms_client_factory import OmsClientFactory
from ion.agents.platform.oms.test.oms_test_mixin import OmsTestMixin
import unittest

from nose.plugins.attrib import attr


@attr('INT', group='sa')
class Test(unittest.TestCase, OmsTestMixin):

    @classmethod
    def setUpClass(cls):
        cls.oms = OmsClientFactory.create_instance()
