#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'


from ion.services.mi.drivers.uw_bars.test import WithSimulatorTestCase
from ion.services.mi.drivers.uw_bars.protocol import BarsInstrumentProtocol

import time


class ProtocolTest(WithSimulatorTestCase):

    def test_basic(self):
        """
        Tests protocol configuration, connection, disconnection
        """

        protocol = BarsInstrumentProtocol()
        protocol.configure(self.config)

        print "connecting"
        protocol.connect()

        print "sleeping for a bit"
        time.sleep(5)

        print "disconnecting"
        protocol.disconnect()
