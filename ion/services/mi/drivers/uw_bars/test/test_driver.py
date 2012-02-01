#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.test import WithSimulatorTestCase
from ion.services.mi.drivers.uw_bars.driver import BarsInstrumentDriver
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsParameter

from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode

import time


class DriverTest(WithSimulatorTestCase):

    def test(self):
        """
        BARS driver tests
        """

        driver = BarsInstrumentDriver()

        self.assertEqual(DriverState.UNCONFIGURED, driver.get_current_state())

        # initialize
        success, result = driver.initialize()
        self.assertEqual(InstErrorCode.OK, success)
        self.assertEqual(DriverState.UNCONFIGURED, driver.get_current_state())

        # configure
        configs = {BarsChannel.INSTRUMENT: self.config}
        success, result = driver.configure(configs)
        self.assertEqual(InstErrorCode.OK, success)
        self.assertEqual(DriverState.DISCONNECTED, driver.get_current_state())

        # connect
        success, result = driver.connect([BarsChannel.INSTRUMENT])
        self.assertEqual(InstErrorCode.OK, success)
        print "connect result = %s" % str(result)
        self.assertEqual(DriverState.AUTOSAMPLE, driver.get_current_state())

        print "sleeping for a bit to see data streaming"
        time.sleep(4)

        # get a parameter
        cp = (BarsChannel.INSTRUMENT, BarsParameter.TIME_BETWEEN_BURSTS)
        success, result = driver.get([cp])
        self.assertEqual(InstErrorCode.OK, success)
        print "get result = %s" % str(result)

        # should be back in AUTOSAMPLE state:
        self.assertEqual(DriverState.AUTOSAMPLE, driver.get_current_state())

        print "sleeping a bit more"
        time.sleep(4)

        # disconnect
        print "disconnecting"
        success, result = driver.disconnect([BarsChannel.INSTRUMENT])
        self.assertEqual(InstErrorCode.OK, success)
        self.assertEqual(DriverState.DISCONNECTED, driver.get_current_state())
