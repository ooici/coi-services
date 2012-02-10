#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.test import BarsTestCase
from ion.services.mi.drivers.uw_bars.driver import BarsInstrumentDriver
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsParameter

from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode

import time

from nose.plugins.attrib import attr

from unittest import skip


@skip('not yet easy to test driver in isolation')
@attr('UNIT', group='mi')
class DriverTest(BarsTestCase):

    def test(self):
        """
        BARS driver tests
        """

        driver = BarsInstrumentDriver()

        self.assertEqual(DriverState.UNCONFIGURED, driver.get_current_state())

        # initialize
        result = driver.initialize()
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

        # get a parameter
        cp = (BarsChannel.INSTRUMENT, BarsParameter.TIME_BETWEEN_BURSTS)
        result = driver.get([cp])
        print "get result = %s" % str(result)

        # should be back in AUTOSAMPLE state:
        self.assertEqual(DriverState.AUTOSAMPLE, driver.get_current_state())
        print "driver state = %s" % str(driver.get_current_state())

        print "sleeping a bit more"
        time.sleep(4)

        # disconnect
        print "disconnecting"
        result = driver.disconnect([BarsChannel.INSTRUMENT])
        self.assertEqual(DriverState.DISCONNECTED, driver.get_current_state())
        print "driver state = %s" % str(driver.get_current_state())
