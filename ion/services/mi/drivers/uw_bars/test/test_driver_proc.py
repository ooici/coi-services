#!/usr/bin/env python

"""
@package
@file
@author Carlos Rueda
@brief
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from gevent import monkey
monkey.patch_all()

import time

from ion.services.mi.drivers.uw_bars.test import BarsTestCase

from ion.services.mi.instrument_driver import DriverState

from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess

from ion.services.mi.drivers.uw_bars.common import BarsChannel

from nose.plugins.attrib import attr


@attr('UNIT', group='mi')
class DriverAndProcsTest(BarsTestCase):
    """
    Tests involving ZMQ driver process and ZMQ client.
    """

    def setUp(self):
        super(DriverAndProcsTest, self).setUp()

        # Zmq parameters used by driver process and client.
        self.server_addr = 'localhost'
        self.cmd_port = 5556
        self.evt_port = 5557

        # Driver module parameters.
        self.dvr_mod = 'ion.services.mi.drivers.uw_bars.driver'
        self.dvr_cls = 'BarsInstrumentDriver'

        self._driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)

        self._driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        self._driver_client.start_messaging()
        time.sleep(1)

        self.addCleanup(self._clean_up)

    def _clean_up(self):
        if self._driver_process:
            try:
                self._driver_client.done()
                self._driver_process.wait()
            finally:
                self._driver_process = None

    def tearDown(self):
        super(DriverAndProcsTest, self).tearDown()
        self._clean_up()

    def test_config(self):
        """BARS tests with ZMQ driver process and ZMQ client"""

        driver_client = self._driver_client

        reply = driver_client.cmd_dvr('get_current_state')
        print("** get_current_state reply=%s" % str(reply))
        self.assertEqual(DriverState.UNCONFIGURED, reply)

        time.sleep(1)

        configs = {BarsChannel.INSTRUMENT: self.config}
        reply = driver_client.cmd_dvr('configure', configs)
        print("** configure reply=%s" % str(reply))

        time.sleep(1)

        reply = driver_client.cmd_dvr('get_current_state')
        print("** get_current_state reply=%s" % str(reply))

        reply = driver_client.cmd_dvr('connect', [BarsChannel.INSTRUMENT])
        print("** connect reply=%s" % str(reply))

        time.sleep(1)

        reply = driver_client.cmd_dvr('get_current_state')
        print("** get_current_state reply=%s" % str(reply))
        self.assertEqual(DriverState.AUTOSAMPLE, reply)

        time.sleep(1)

        reply = driver_client.cmd_dvr('get_status', [BarsChannel.INSTRUMENT])
        print("** get_status reply=%s" % str(reply))
        self.assertEqual(DriverState.AUTOSAMPLE, reply)

        time.sleep(1)

        # TODO the reply of the main operation should probably include
        # an item indicating the current state of the driver so there is no
        # need to explicitly query for it.
        reply = driver_client.cmd_dvr('disconnect', [BarsChannel.INSTRUMENT])
        print("** disconnect reply=%s" % str(reply))
        reply = driver_client.cmd_dvr('get_current_state')
        print("** get_current_state reply=%s" % str(reply))
        self.assertEqual(DriverState.DISCONNECTED, reply)
        time.sleep(1)

        reply = driver_client.cmd_dvr('initialize', [BarsChannel.INSTRUMENT])
        print("** initialize reply=%s" % str(reply))
        reply = driver_client.cmd_dvr('get_current_state')
        print("** get_current_state reply=%s" % str(reply))
        self.assertEqual(DriverState.UNCONFIGURED, reply)
        time.sleep(1)
