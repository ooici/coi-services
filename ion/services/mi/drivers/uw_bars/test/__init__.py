#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.test.bars_simulator import BarsSimulator

from pyon.util.unit_test import PyonTestCase
from threading import Thread
import os
from unittest import skipIf


@skipIf(None == os.getenv('UW_BARS'), 'UW_BARS environment variable undefined')
class BarsTestCase(PyonTestCase):
    """
    Base class for BARS test cases.

    The whole test case is skipped if the environment variable UW_BARS is
    not defined.

    If UW_BARS is defined with the literal value "simulator", then a simulator
    is launched in setUp and terminated in tearDown.

    Otherwise, if UW_BARS is defined, it is assumed to be in the format
    address:port, then a connection to such service will be used.

    In both UW_BARS cases above, corresponding self.config object initialized
    accordingly.
    """

    bars = os.getenv('UW_BARS')

    def setUp(self):
        """
        Sets up the test case, launching a simulator if so specified and
        preparing self.config.
        """

        bars = BarsTestCase.bars

        if bars is None:
            # should not happen, but anyway just skip here:
            self.skipTest("Environment variable UW_BARS undefined")

        self.simulator = None

        if bars == "simulator":
            self.simulator = BarsSimulator(accept_timeout=10.0)
            self.device_port = self.simulator.port
            self.device_address = 'localhost'
        else:
            try:
                a, p = bars.split(':')
                port = int(p)
            except:
                self.skipTest("Malformed UW_BARS value")

            print "==Assuming BARS is listening on %s:%s==" % (a, p)
            self.device_address = a
            self.device_port = port

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
        Stops simulator if so specified and joins calling thread to that of the
        simulator.
        """
        if self.simulator is not None:
            print "==stopping simulator=="
            self.simulator.stop()
            self.simulator_thread.join()
