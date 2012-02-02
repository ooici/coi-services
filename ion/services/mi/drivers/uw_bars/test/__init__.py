#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.test.bars_simulator import BarsSimulator

from threading import Thread
from unittest import TestCase

import os


class WithSimulatorTestCase(TestCase):
    """
    Base class for test cases needing a simulator.
    Actually the simulator is launched only if the environment variable
    BARS is not defined. If it is defined then it is assumed that a BARS
    instrument is already running. The BARS value is expected to be in
    the format address:port"""

    def setUp(self):
        """Starts simulator"""

        bars = os.getenv('BARS', None)
        if bars is None:
            self.simulator = BarsSimulator(accept_timeout=10.0)
            self.device_port = self.simulator.port
            self.device_address = 'localhost'
        else:
            self.simulator = None
            a, p = bars.split(':')
            print "==ASSUMING BARS is listening on %s:%s==" % (a, p)
            self.device_address = a
            self.device_port = int(p)

        self.config = {
            'method': 'ethernet',
            'device_addr': self.device_address,
            'device_port': self.device_port,
            'server_addr': 'localhost',
            'server_port': 8888
        }

        if self.simulator is not None:
            self.simulator_thread = Thread(target=self.simulator.run)
            print "==starting simulator=="
            self.simulator_thread.start()

    def tearDown(self):
        """
        Stops simulator and joins calling thread to that of the simulator.
        """
        if self.simulator is not None:
            print "==stopping simulator=="
            self.simulator.stop()
            self.simulator_thread.join()
