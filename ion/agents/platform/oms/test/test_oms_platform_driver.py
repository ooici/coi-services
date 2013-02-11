#!/usr/bin/env python

"""
@package ion.agents.platform.oms.test.test_oms_platform_driver
@file    ion/agents/platform/oms/test/test_oms_platform_driver.py
@author  Carlos Rueda
@brief   Basic OmsPlatformDriver tests
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from ion.agents.platform.oms.oms_platform_driver import OmsPlatformDriver

from pyon.util.int_test import IonIntegrationTestCase

import time
import ntplib
import os
from nose.plugins.attrib import attr

from gevent import sleep

from ion.agents.platform.test.helper import HelperTestMixin


DVR_CONFIG = {
    'oms_uri': os.getenv('OMS', 'embsimulator'),
}


@attr('INT', group='sa')
class TestOmsPlatformDriver(IonIntegrationTestCase, HelperTestMixin):

    @classmethod
    def setUpClass(cls):
        HelperTestMixin.setUpClass()

    def setUp(self):
        platform_id = self.PLATFORM_ID
        self._plat_driver = OmsPlatformDriver(platform_id, DVR_CONFIG)

        self._plat_driver.set_event_listener(self.evt_recv)

    def evt_recv(self, driver_event):
        log.debug('GOT driver_event=%s', str(driver_event))

    def tearDown(self):
        self._plat_driver.destroy()

    def _ping(self):
        result = self._plat_driver.ping()
        self.assertEquals("PONG", result)

    def _go_active(self):
        self._plat_driver.go_active()

    def _get_attribute_values(self):
        attrNames = self.ATTR_NAMES
        cur_time = ntplib.system_to_ntp_time(time.time())
        from_time = cur_time - 50  # a 50-sec time window
        attr_values = self._plat_driver.get_attribute_values(attrNames, from_time)
        log.info("attr_values = %s" % str(attr_values))
        self.assertIsInstance(attr_values, dict)
        for attr_name in attrNames:
            self.assertTrue(attr_name in attr_values)

    def _start_event_dispatch(self):
        params = {}  # TODO params not used yet
        self._plat_driver.start_event_dispatch(params)

    def _stop_event_dispatch(self):
        self._plat_driver.stop_event_dispatch()

    def test(self):

        self._ping()
        self._go_active()

        self._get_attribute_values()

        self._start_event_dispatch()

        log.info("sleeping to eventually see some events...")
        sleep(15)

        self._stop_event_dispatch()
