#!/usr/bin/env python

"""
@package ion.agents.platform.oms.test.test_oms_simple
@file    ion/agents/platform/oms/test/test_oms_simple.py
@author  Carlos Rueda
@brief   Test cases for OmsClient.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from pyon.public import log
from ion.agents.platform.oms.simulator.logger import Logger
Logger.set_logger(log)

from pyon.util.int_test import IonIntegrationTestCase

from ion.agents.platform.oms.oms_client_factory import OmsClientFactory
from ion.agents.platform.oms.test.oms_test_mixin import OmsTestMixin

from nose.plugins.attrib import attr


@attr('INT', group='sa')
class Test(IonIntegrationTestCase, OmsTestMixin):

    @classmethod
    def setUpClass(cls):
        cls.oms = OmsClientFactory.create_instance()
        OmsTestMixin.start_http_server()

    @classmethod
    def tearDownClass(cls):
        alarm_notifications = OmsTestMixin.stop_http_server()
        log.info("alarm_notifications = %s" % str(alarm_notifications))
