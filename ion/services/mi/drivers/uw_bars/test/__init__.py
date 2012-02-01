#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.test.bars_simulator import BarsSimulator

from threading import Thread
from unittest import TestCase


class WithSimulatorTestCase(TestCase):
    """Base class for test cases needing a simulator"""

    def setUp(self):
        """Starts simulator"""

        self.simulator = BarsSimulator(accept_timeout=10.0)
        self.device_port = self.simulator.port

        self.config = {
            'method': 'ethernet',
            'device_addr': 'localhost',
            'device_port': self.device_port,
            'server_addr': 'localhost',
            'server_port': 8888
        }

        self.simulator_thread = Thread(target=self.simulator.run)
        print "starting simulator"
        self.simulator_thread.start()

    def tearDown(self):
        """
        Stops simulator and joins calling thread to that of the simulator.
        """
        print "stopping simulator"
        self.simulator.stop()

        self.simulator_thread.join()
