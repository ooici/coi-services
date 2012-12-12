#!/usr/bin/env python

"""
@package ion.agents.platform.oms.simulator.test.test_oms_simulator
@file    ion/agents/platform/oms/simulator/test/test_oms_simulator.py
@author  Carlos Rueda
@brief   Test cases for the simulator
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from ion.agents.platform.oms.simulator.logger import Logger
Logger.set_logger(log)

from pyon.util.unit_test import IonUnitTestCase

from ion.agents.platform.oms.simulator.oms_simulator import OmsSimulator
from ion.agents.platform.oms.test.oms_test_mixin import OmsTestMixin

from nose.plugins.attrib import attr


@attr('NOTUNIT', group='sa')
class Test(IonUnitTestCase, OmsTestMixin):
    """
    Test cases for the simulator, which is instantiated directly (ie.,
    no connection to external simulator is involved).
    """

    @classmethod
    def setUpClass(cls):
        OmsTestMixin.setUpClass()
        OmsTestMixin.start_http_server()
        cls.oms = OmsSimulator()

    @classmethod
    def tearDownClass(cls):
        cls.oms._deactivate_simulator()
        event_notifications = OmsTestMixin.stop_http_server()
        log.info("event_notifications = %s" % str(event_notifications))
