#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.bars_client import BarsClient

import time

from ion.services.mi.drivers.uw_bars.test import BarsTestCase
from nose.plugins.attrib import attr
import unittest
import os


# Does not work in conjunction with pyon internal preparations
@unittest.skipIf(None == os.getenv('run_it'), 'define run_it to run this.')
@attr('UNIT', group='mi')
class BarsClientTest(BarsTestCase):

    def setUp(self):
        super(BarsClientTest, self).setUp()
        host = self.device_address
        port = self.device_port
        outfile = file('test_bars_client.txt', 'w')
        self.bars_client = BarsClient(host, port, outfile)
        self.bars_client.connect()

    def tearDown(self):
        super(BarsClientTest, self).tearDown()
        self.bars_client.end()

    def test_simple(self):
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
