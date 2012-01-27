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

        self.simulator = BarsSimulator()
        port = self.simulator.get_port()
        print "bound to port %s" % port

        self.device_port = port

        self.simulator_thread = Thread(target=self.simulator.run)
        print "starting simulator"
        self.simulator_thread.start()

    def tearDown(self):
        pass

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


        print "sleeping for a bit"
        time.sleep(2)

        protocol.get_status()

        print "terminating"
#        self.simulator.

        self.simulator_thread.join()
