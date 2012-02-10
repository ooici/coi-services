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


@attr('UNIT', group='mi')
class DriverTest(BarsTestCase):

    def setUp(self):
        super(DriverTest, self).setUp()
        host = self.device_address
        port = self.device_port
        outfile = file('test_driver0.txt', 'w')
        self.bars_client = BarsClient(host, port, outfile)
        self.bars_client.connect()

    def tearDown(self):
        super(DriverTest, self).tearDown()
        print ":: bye"
        self.bars_client.end()

    def test(self):
        bars_client = self.bars_client

        print ":: is instrument collecting data?"
        if bars_client.is_collecting_data():
            print ":: Instrument is collecting data."
        else:
            print ":: Instrument in not in collecting data mode."
            return

        print ":: break data streaming to enter main menu"
        bars_client.enter_main_menu()

        print ":: select 6 to get system info"
        bars_client.send_option('6')
        bars_client.expect_generic_prompt()

        print ":: send enter to return to main menu"
        bars_client.send_enter()
        bars_client.expect_generic_prompt()

        print ":: resume data streaming"
        bars_client.send_option('1')

        print ":: sleeping for 10 secs to receive some data"
        time.sleep(10)

    def t__est(self):
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
