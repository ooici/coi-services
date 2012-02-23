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

from ion.services.mi.drivers.uw_bars.test.pyon_test import PyonBarsTestCase

from ion.services.mi.instrument_driver import DriverState

from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess

from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsParameter


import unittest
from nose.plugins.attrib import attr


@attr('UNIT', group='mi')
class BarsDriverTest(PyonBarsTestCase):
    """
    Tests involving ZMQ driver process and ZMQ client.
    """

    def setUp(self):
        super(BarsDriverTest, self).setUp()

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
        super(BarsDriverTest, self).tearDown()
        self._clean_up()

    def _initialize(self):
        driver_client = self._driver_client

        reply = driver_client.cmd_dvr('initialize', [BarsChannel.INSTRUMENT])
        print("** initialize reply=%s" % str(reply))
        reply = driver_client.cmd_dvr('get_current_state')
        print("** get_current_state reply=%s" % str(reply))
        self.assertEqual(DriverState.UNCONFIGURED, reply)
        time.sleep(1)

    def _connect(self):
        driver_client = self._driver_client

        reply = driver_client.cmd_dvr('get_current_state')
        print("** get_current_state reply=%s" % str(reply))
        self.assertEqual(DriverState.UNCONFIGURED, reply)

        self._initialize()

        configs = {BarsChannel.INSTRUMENT: self.config}
        reply = driver_client.cmd_dvr('configure', configs)
        print("** configure reply=%s" % str(reply))

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

    def _disconnect(self):
        driver_client = self._driver_client

        reply = driver_client.cmd_dvr('disconnect', [BarsChannel.INSTRUMENT])
        print("** disconnect reply=%s" % str(reply))
        reply = driver_client.cmd_dvr('get_current_state')
        print("** get_current_state reply=%s" % str(reply))
        self.assertEqual(DriverState.DISCONNECTED, reply)
        time.sleep(1)

        self._initialize()

    def test_connect_disconnect(self):
        """BARS connect and disconnect"""

        self._connect()
        self._disconnect()

    def test_get(self):
        """BARS get"""

        self._connect()

        driver_client = self._driver_client

        # get a parameter
        cp = (BarsChannel.INSTRUMENT, BarsParameter.TIME_BETWEEN_BURSTS)
        get_params = [cp]

        reply = driver_client.cmd_dvr('get', get_params)
        print "get reply = %s" % str(reply)
        seconds = reply.get(cp)
        assert isinstance(seconds, int)

        self._disconnect()
