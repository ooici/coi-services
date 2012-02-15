#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.driver0 import BarsInstrumentDriver
from ion.services.mi.drivers.uw_bars.bars_client import BarsClient
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsParameter

from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode

import time

from ion.services.mi.drivers.uw_bars.test import BarsTestCase
from nose.plugins.attrib import attr
import unittest
import os


# Does not work in conjunction with pyon internal preparations
@unittest.skipIf(None == os.getenv('run_it'), 'define run_it to run this. ')
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
        self.assertEqual(DriverState.AUTOSAMPLE, driver.get_current_state())
        print "driver state = %s" % str(driver.get_current_state())

        print "sleeping for a bit to see data streaming"
        time.sleep(4)

    def _disconnect(self):
        print "disconnecting"
        driver = self.driver
        result = driver.disconnect([BarsChannel.INSTRUMENT])
        self.assertEqual(DriverState.DISCONNECTED, driver.get_current_state())
        print "driver state = %s" % str(driver.get_current_state())

    def __test_connect_disconnect(self):
        self._connect()
        self._disconnect()

    def test_get(self):

        self._connect()
        driver = self.driver

        # get a parameter
        cp = (BarsChannel.INSTRUMENT, BarsParameter.TIME_BETWEEN_BURSTS)
        result = driver.get([cp])
        print "get result = %s" % str(result)

        # should be back in AUTOSAMPLE state:
        self.assertEqual(DriverState.AUTOSAMPLE, driver.get_current_state())
        print "driver state = %s" % str(driver.get_current_state())

        print "sleeping a bit more"
        time.sleep(4)

        self._disconnect()
