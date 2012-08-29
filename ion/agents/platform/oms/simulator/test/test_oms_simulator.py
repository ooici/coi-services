#!/usr/bin/env python

"""
@package ion.agents.platform.oms.simulator.test.test_oms_simulator
@file    ion/agents/platform/oms/simulator/test/test_oms_simulator.py
@author  Carlos Rueda
@brief   Test cases for the simulator
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.oms.simulator.oms_simulator import OmsSimulator
from ion.agents.platform.oms.test.oms_test_mixin import OmsTestMixin

import unittest
from nose.plugins.attrib import attr


@attr('UNIT', group='sa')
class Test(unittest.TestCase, OmsTestMixin):
    """
    Test cases for the simulator, which is instantiated directly (ie.,
    no connection to external simulator is involved).
    """

    @classmethod
    def setUpClass(cls):
        cls.oms = OmsSimulator()
