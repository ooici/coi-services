#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.simulator.test.test_oms_simulator
@file    ion/agents/platform/rsn/simulator/test/test_oms_simulator.py
@author  Carlos Rueda
@brief   Test cases for the simulator, by default in embedded form,
         but the OMS environment variable can be used to indicate other.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from ion.agents.platform.rsn.simulator.logger import Logger
Logger.set_logger(log)

from pyon.util.unit_test import IonUnitTestCase

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.test.oms_test_mixin import OmsTestMixin

from nose.plugins.attrib import attr
import unittest
import os


#
# Skip if OMS environment variable is defined: this unit test is specifically
# for the simulator run in embedded form. Other tests, eg., test_oms_client,
# are more flexible (and "integration" in nature), allowing to test against
# not only the simulator (launched in whatever form) but also the real RSN OMS
# endpoint.
#

@unittest.skipIf(os.getenv("OMS") is not None, "OMS environment variable is defined.")
@attr('UNIT', group='sa')
class Test(IonUnitTestCase, OmsTestMixin):
    """
    Test cases for the simulator, which is instantiated directly (ie.,
    no connection to external simulator is involved).
    """

    @classmethod
    def setUpClass(cls):
        OmsTestMixin.setUpClass()
        cls.oms = CIOMSClientFactory.create_instance()

    @classmethod
    def tearDownClass(cls):
        CIOMSClientFactory.destroy_instance(cls.oms)
