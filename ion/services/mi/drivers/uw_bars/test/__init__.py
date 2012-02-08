#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.test.bars_simulator import BarsSimulator

from pyon.util.unit_test import PyonTestCase

from threading import Thread

import os


class BarsTestCase(PyonTestCase):
    """
    Base class for BARS test cases.

    The whole test case is skipped if the environment variable BARS is
    not defined.

    If BARS is defined with the literal value "simulator", then a simulator is
    launched in setUp and terminated in tearDown.

    Otherwise, if BARS is defined, it is assumed to be in the format
    address:port, then a connection to such service will be used.

    In both BARS cases above, corresponding self.config object initialized
    accordingly.
    """

    bars = os.getenv('BARS', None)

    #
    # The following attempts to use some unittest internals to
    # conditionally skip the whole test case. A regular skipTest is used
    # in setUp if this internal mechanism is not valid anymore.
    # Interestingly, unittest does not provide a decorator for a
    # conditional skip similar to the unconditional unittest.skip(reason).
    #
    __unittest_skip__ = None == bars
    __unittest_skip_why__ = "Environment variable BARS not defined"


    def setUp(self):
        """
        Sets up the test case, launching a simulator if so specified and
        preparing self.config.
        """

        self.simulator = None

        bars = BarsTestCase.bars

        if bars is None:
            # should not happen (unless unittest has changed some of its
            # internals). Just skip explicitly here using regular API:
            self.skipTest("Environment variable BARS undefined")

        if bars == "simulator":
            self.simulator = BarsSimulator(accept_timeout=10.0)
            self.device_port = self.simulator.port
            self.device_address = 'localhost'
        else:
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
        Stops simulator if so specified and joins calling thread to that of the
        simulator.
        """
        if self.simulator is not None:
            print "==stopping simulator=="
            self.simulator.stop()
            self.simulator_thread.join()
