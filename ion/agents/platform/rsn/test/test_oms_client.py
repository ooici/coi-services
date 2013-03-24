#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.test.test_oms_simple
@file    ion/agents/platform/rsn/test/test_oms_simple.py
@author  Carlos Rueda
@brief   Test cases for CIOMSClient.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from pyon.public import log
from ion.agents.platform.rsn.simulator.logger import Logger
Logger.set_logger(log)

from pyon.util.int_test import IonIntegrationTestCase

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory
from ion.agents.platform.rsn.test.oms_test_mixin import OmsTestMixin

from nose.plugins.attrib import attr


@attr('INT', group='sa')
class Test(IonIntegrationTestCase, OmsTestMixin):

    @classmethod
    def setUpClass(cls):
        OmsTestMixin.setUpClass()
        cls.oms = CIOMSClientFactory.create_instance()
        OmsTestMixin.start_http_server()

    @classmethod
    def tearDownClass(cls):
        CIOMSClientFactory.destroy_instance(cls.oms)
        event_notifications = OmsTestMixin.stop_http_server()
        log.info("event_notifications = %s" % str(event_notifications))
