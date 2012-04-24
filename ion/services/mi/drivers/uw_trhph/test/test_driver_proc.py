#!/usr/bin/env python

"""
@file ion/services/mi/drivers/uw_trhph/test/test_driver_proc.py
@author Carlos Rueda
@brief Tests to the TRHPH driver via ZMQ driver process and ZMQ client.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


#from gevent import monkey
#monkey.patch_all()

import time

# NOTE: not using the pyon-based PyonTrhphTestCase class because of issue with
# logging: log messages from the test case are not generated.
#from ion.services.mi.drivers.uw_trhph.test.pyon_test import PyonTrhphTestCase
from ion.services.mi.drivers.uw_trhph.test import TrhphTestCase

from ion.services.mi.instrument_driver import DriverState

from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess

from ion.services.mi.drivers.uw_trhph.common import TrhphChannel
from ion.services.mi.drivers.uw_trhph.common import TrhphParameter

from ion.services.mi.mi_logger import mi_logger
log = mi_logger

import unittest
from nose.plugins.attrib import attr

@unittest.skip('Need to align with new refactoring')
@attr('UNIT', group='mi')
class TrhphDriverTest(TrhphTestCase):
    """
    Tests involving ZMQ driver process and ZMQ client.
    """

    def setUp(self):
        super(TrhphDriverTest, self).setUp()

        # Zmq parameters used by driver process and client.
        self.server_addr = 'localhost'
        self.cmd_port = 5556
        self.evt_port = 5557

        # Driver module parameters.
        self.dvr_mod = 'ion.services.mi.drivers.uw_trhph.driver'
        self.dvr_cls = 'TrhphInstrumentDriver'

        self._driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)

        self._driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        self._driver_client.start_messaging()
        time.sleep(1)

        self.addCleanup(self._clean_up)

    def _clean_up(self):
        super(TrhphDriverTest, self).tearDown()
        if self._driver_process:
            try:
                log.info("conclude driver process...")
                self._driver_client.done()
                self._driver_process.wait()
                log.info("conclude driver process... done")
            finally:
                self._driver_process = None

    def tearDown(self):
        self._clean_up()

    def _get_current_state(self):
        driver_client = self._driver_client
        reply = driver_client.cmd_dvr('get_current_state')
        log.info("get_current_state reply=%s" % str(reply))
        return reply[TrhphChannel.INSTRUMENT]

    def _assert_state(self, state, curr_state):
        self.assertEqual(state, curr_state, "expected: %s, current=%s" %
                         (state, curr_state))

    def _initialize(self):
        driver_client = self._driver_client

        reply = driver_client.cmd_dvr('initialize', [TrhphChannel.INSTRUMENT])
        log.info("initialize reply=%s" % str(reply))

        # TODO review driver state vs. protocol state
        curr_state = self._get_current_state()
#        self._assert_state(DriverState.UNCONFIGURED, curr_state)

        time.sleep(1)

    def _connect(self):
        driver_client = self._driver_client

        self._assert_state(DriverState.UNCONFIGURED, self._get_current_state())

        self._initialize()

        configs = {TrhphChannel.INSTRUMENT: self.config}
        reply = driver_client.cmd_dvr('configure', configs)
        log.info("configure reply=%s" % str(reply))

        self._get_current_state()

        reply = driver_client.cmd_dvr('connect', [TrhphChannel.INSTRUMENT])
        log.info("connect reply=%s" % str(reply))

        time.sleep(1)

        self._assert_state(DriverState.CONNECTED, self._get_current_state())

        time.sleep(1)

    def _disconnect(self):
        driver_client = self._driver_client

        reply = driver_client.cmd_dvr('disconnect', [TrhphChannel.INSTRUMENT])
        log.info("disconnect reply=%s" % str(reply))

        # TODO review driver state vs. protocol state
        curr_state = self._get_current_state()
#        self._assert_state(DriverState.DISCONNECTED, curr_state)

        time.sleep(1)

        self._initialize()

    def test_connect_disconnect(self):
        """-- TRHPH connect/disconnect tests"""

        self._connect()
        self._disconnect()

    def test_get(self):
        """-- TRHPH get tests"""

        self._connect()

        driver_client = self._driver_client

        # get a parameter
        cp = (TrhphChannel.INSTRUMENT, TrhphParameter.TIME_BETWEEN_BURSTS)
        get_params = [cp]

        reply = driver_client.cmd_dvr('get', get_params)
        log.info("get reply = %s" % str(reply))
        seconds = reply.get(cp)
        assert isinstance(seconds, int)

        self._disconnect()

    def test_set(self):
        """-- TRHPH set test"""

        self._connect()

        driver_client = self._driver_client

        # get a parameter
        cp = (TrhphChannel.INSTRUMENT, TrhphParameter.TIME_BETWEEN_BURSTS)
        new_seconds = 33
        set_params = {cp: new_seconds}
        reply = driver_client.cmd_dvr('set', set_params)
        log.info("set reply = %s" % str(reply))

        time.sleep(1)

        self._disconnect()

    def test_get_set(self):
        """-- TRHPH get and set tests"""

        self._connect()

        driver_client = self._driver_client

        # get a parameter
        cp = (TrhphChannel.INSTRUMENT, TrhphParameter.TIME_BETWEEN_BURSTS)
        get_params = [cp]

        reply = driver_client.cmd_dvr('get', get_params)
        log.info("get reply = %s" % str(reply))
        seconds = reply.get(cp)
        assert isinstance(seconds, int)

        new_seconds = seconds + 5
        if new_seconds > 30 or new_seconds < 15:
            new_seconds = 15

        # set a parameter
        set_params = {cp: new_seconds}
        reply = driver_client.cmd_dvr('set', set_params)
        log.info("set reply = %s" % str(reply))

        # get again
        reply = driver_client.cmd_dvr('get', get_params)
        log.info("get reply = %s" % str(reply))
        seconds = reply.get(cp)
        assert isinstance(seconds, int)

        # compare
#        self.assertEqual(new_seconds, seconds)
        time.sleep(1)

        self._disconnect()
