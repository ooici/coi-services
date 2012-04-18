#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.driver0 import BarsInstrumentDriver
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsParameter

from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode

import time

from ion.services.mi.drivers.uw_bars.test import BarsTestCase
from nose.plugins.attrib import attr
import unittest
import os


# explicit run_it because of threading + gevent-monkey-patching issues
#@unittest.skipIf(os.getenv('run_it') is None, 'define run_it to run this.')
@unittest.skip('Need to align.')
@attr('UNIT', group='mi')
class DriverTest(BarsTestCase):

    def _connect(self):

        self.driver = BarsInstrumentDriver()
        driver = self.driver

        self.assertEqual(DriverState.UNCONFIGURED, driver.get_current_state())

        # initialize
        result = driver.initialize([BarsChannel.INSTRUMENT])
        self.assertEqual(DriverState.UNCONFIGURED, driver.get_current_state())
        print "driver state = %s" % str(driver.get_current_state())

        # configure
        configs = {BarsChannel.INSTRUMENT: self.config}
        result = driver.configure(configs)
        self.assertEqual(DriverState.DISCONNECTED, driver.get_current_state())
        print "driver state = %s" % str(driver.get_current_state())

        # connect
        result = driver.connect([BarsChannel.INSTRUMENT])
        print "connect result = %s" % str(result)
        self._assert_auto_sample()

        print "sleeping for a bit to see data streaming"
        time.sleep(4)

    def _disconnect(self):
        print "disconnecting"
        driver = self.driver
        result = driver.disconnect([BarsChannel.INSTRUMENT])
        self.assertEqual(DriverState.DISCONNECTED, driver.get_current_state())
        print "driver state = %s" % str(driver.get_current_state())

    def test_connect_disconnect(self):
        self._connect()
        self._disconnect()

    def _get(self, params):
        driver = self.driver

        result = driver.get(params)
        print "get result = %s" % str(result)
        assert isinstance(result, dict)

        self._assert_auto_sample()

        return result

    def test_get(self):
        self._connect()

        cp = (BarsChannel.INSTRUMENT, BarsParameter.TIME_BETWEEN_BURSTS)
        params = [cp]

        result = self._get(params)
        seconds = result.get(cp)
        assert isinstance(seconds, int)

        time.sleep(1)

        self._disconnect()

    def test_get_set(self):
        self._connect()

        cp = (BarsChannel.INSTRUMENT, BarsParameter.TIME_BETWEEN_BURSTS)
        params = [cp]

        result = self._get(params)
        seconds = result.get(cp)
        assert isinstance(seconds, int)

        driver = self.driver

        new_seconds = seconds + 5
        if new_seconds > 30 or new_seconds < 15:
            new_seconds = 15

        # get a parameter
        result = driver.set({cp: new_seconds})
        print "set result = %s" % str(result)
        code = result.get(cp)
        InstErrorCode.is_ok(code)

        self._assert_auto_sample()

        result = self._get(params)
        seconds = result.get(cp)

        self.assertEqual(new_seconds, seconds)
        time.sleep(1)

        self._disconnect()

    def _assert_auto_sample(self):
        """asserts AUTOSAMPLE state"""
        curr_state = self.driver.get_current_state()
        self.assertEqual(DriverState.AUTOSAMPLE, curr_state)
        print "driver state = %s" % str(curr_state)
