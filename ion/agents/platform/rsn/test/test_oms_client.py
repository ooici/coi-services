#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.test.test_oms_client
@file    ion/agents/platform/rsn/test/test_oms_client.py
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

import os


@attr('INT', group='sa')
class Test(IonIntegrationTestCase, OmsTestMixin):

    @classmethod
    def setUpClass(cls):
        OmsTestMixin.setUpClass()

    def setUp(self):
        oms_uri = os.getenv('OMS', "launchsimulator")
        oms_uri = self._dispatch_simulator(oms_uri)
        log.debug("oms_uri = %s", oms_uri)
        self.oms = CIOMSClientFactory.create_instance(oms_uri)
        OmsTestMixin.start_http_server()

        def done():
            CIOMSClientFactory.destroy_instance(self.oms)
            event_notifications = OmsTestMixin.stop_http_server()
            log.info("event_notifications = %s" % str(event_notifications))

        self.addCleanup(done)
