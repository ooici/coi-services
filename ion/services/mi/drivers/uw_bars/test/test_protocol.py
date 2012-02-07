#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'


from unittest import TestCase
import time

from ion.services.mi.drivers.uw_bars.protocol import BarsInstrumentProtocol
from ion.services.mi.drivers.uw_bars.test.bars_simulator import BarsSimulator

from threading import Thread


class ProtocolTest(TestCase):

    def setUp(self):
        """Starts simulator"""

        self.simulator = BarsSimulator(accept_timeout=10.0)
        self.device_port = self.simulator.get_port()

        self.simulator_thread = Thread(target=self.simulator.run)
        print "starting simulator"
        self.simulator_thread.start()
        time.sleep(1)

    def tearDown(self):
        print "stopping simulator"
        self.simulator.stop()

        self.simulator_thread.join()

    def test_basic(self):
        """
        """

        config = {
            'method': 'ethernet',
            'device_addr': 'localhost',
            'device_port': self.device_port,
            'server_addr': 'localhost',
            'server_port': 8888
        }

        protocol = BarsInstrumentProtocol()
        protocol.configure(config)

        print "connecting"
        protocol.connect()

        print "sleeping for a bit"
        time.sleep(5)

        print "disconnecting"
        protocol.disconnect()
