#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.test.test_protocol
@file ion/services/mi/drivers/uw_trhph/test/test_protocol.py
@author Carlos Rueda
@brief Directly tests the TrhphInstrumentProtocol
"""

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_trhph.protocol import TrhphInstrumentProtocol
from ion.services.mi.drivers.uw_trhph.common import TrhphParameter

from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode

import random
import time
from ion.services.mi.mi_logger import mi_logger
log = mi_logger

from ion.services.mi.drivers.uw_trhph.test import TrhphTestCase
from nose.plugins.attrib import attr
import unittest
import os


# explicit run_it because of threading + gevent-monkey-patching issues
@unittest.skipIf(os.getenv('run_it') is None, 'define run_it to run this.')
@attr('UNIT', group='mi')
class ProtocolTest(TrhphTestCase):

    def setUp(self):
        super(ProtocolTest, self).setUp()
        self._connect()

    def tearDown(self):
        self._disconnect()
        super(ProtocolTest, self).tearDown()

    def _connect(self):
        """
        Inits, configures and connects.
        """
        def evt_callback(event):
            log.info("CALLBACK: %s" % str(event))
        self.protoc = TrhphInstrumentProtocol(evt_callback)
        protoc = self.protoc

        self.assertEqual(DriverState.UNCONFIGURED, protoc.get_current_state())

        # initialize
        log.debug("initializing")
        result = protoc.initialize(timeout=self._timeout)

        state = protoc.get_current_state()
        log.debug("state after init = %s" % str(state))
        self.assertEqual(DriverState.UNCONFIGURED, state)

        # configure
        log.debug("configuring")
        config = self.config
        result = protoc.configure(config, timeout=self._timeout)

        state = protoc.get_current_state()
        log.debug("state after config = %s" % str(state))
        self.assertEqual(DriverState.DISCONNECTED, state)

        # connect
        log.debug("connecting")
        result = protoc.connect(timeout=self._timeout)
        log.info("connect result = %s" % str(result))

        state = protoc.get_current_state()
        log.debug("state after connect = %s" % str(state))

    def _disconnect(self):
        """
        Disconnects
        """
        time.sleep(1)
        log.debug("disconnecting")
        protoc, self.protoc = self.protoc, None
        result = protoc.disconnect(timeout=self._timeout)
        log.info("disconnect result = %s" % str(result))

        state = protoc.get_current_state()
        log.debug("state after disconnect = %s" % str(state))
        self.assertEqual(DriverState.DISCONNECTED, state)

    def test_00_connect_disconnect(self):
        """-- PROTOCOL CONNECT/DISCONNECT test"""

        protoc = self.protoc

        state = protoc.get_current_state()
        log.info("current protocol state: %s" % str(state))

        timeout = self._timeout if DriverState.AUTOSAMPLE == state else 4
        time.sleep(timeout)

    def test_20_execute_start_autosample(self):
        """-- PROTOCOL START AUTOSAMPLE test"""
        protoc = self.protoc

        log.info("starting autosample")
        result = protoc.execute_start_autosample(timeout=self._timeout)
        log.info("execute_start_autosample result = %s" % str(result))
        self.assertTrue(InstErrorCode.is_ok(result))
        # just sleep for a few secs (note: not necessarily data will come
        # in this short interval).
        time.sleep(10)

    def test_25_execute_stop_autosample(self):
        """-- PROTOCOL STOP AUTOSAMPLE test"""
        protoc = self.protoc

        log.info("stopping autosample")
        result = protoc.execute_stop_autosample(timeout=self._timeout)
        log.info("execute_stop_autosample result = %s" % str(result))
        self.assertTrue(InstErrorCode.is_ok(result))

    def _get(self, params):
        protoc = self.protoc

        result = protoc.get(params, timeout=self._timeout)
        log.info("get result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

        return result

    def test_30_get_all_params(self):
        """-- PROTOCOL GET test"""

        # request all valid params:
        params = TrhphParameter.list()
        # plus some invalid params
        invalid_params = ["bad-param1", "bad-param2"]
        params += invalid_params

        result = self._get(params)

        for param in TrhphParameter.list():
            self.assertFalse(InstErrorCode.is_error(result.get(param)))

        for param in invalid_params:
            self.assertTrue(InstErrorCode.is_error(result.get(param)))

        seconds = result.get(TrhphParameter.TIME_BETWEEN_BURSTS)
        is_data_only = result.get(TrhphParameter.VERBOSE_MODE)

        self.assertTrue(isinstance(seconds, int))
        self.assertTrue(isinstance(is_data_only, bool))

    def test_35_set(self):
        """-- PROTOCOL SET test"""

        protoc = self.protoc

        p = TrhphParameter.TIME_BETWEEN_BURSTS
        new_seconds = random.randint(15, 60)

        # set a parameter
        result = protoc.set({p: new_seconds}, timeout=self._timeout)
        log.info("set result = %s" % str(result))
        code = result.get(p)
        self.assertTrue(InstErrorCode.is_ok(code))

    def test_40_get_set(self):
        """-- PROTOCOL GET/SET test"""

        p = TrhphParameter.TIME_BETWEEN_BURSTS
        params = [p]

        result = self._get(params)
        seconds = result.get(p)
        self.assertTrue(isinstance(seconds, int))

        protoc = self.protoc

        new_seconds = seconds + 5
        if new_seconds > 30 or new_seconds < 15:
            new_seconds = 15

        # set a parameter
        result = protoc.set({p: new_seconds}, timeout=self._timeout)
        log.info("set result = %s" % str(result))
        code = result.get(p)
        self.assertTrue(InstErrorCode.is_ok(code))

        result = self._get(params)
        seconds = result.get(p)

        self.assertEqual(new_seconds, seconds)

    def test_70_execute_get_metadata(self):
        """-- PROTOCOL EXECUTE GET METADATA test"""
        protoc = self.protoc

        log.info("getting metadata")
        # TODO NOTE meta_params arg is currently ignored
        meta_params = ["DUMMY"]
        result = protoc.execute_get_metadata(meta_params,
                                             timeout=self._timeout)
        log.info("metadata result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

    def test_80_execute_diagnostics(self):
        """-- PROTOCOL EXECUTE DIAGNOSTICS test"""
        protoc = self.protoc

        log.info("getting metadata")
        num_scans = 6
        result = protoc.execute_diagnostics(num_scans, timeout=self._timeout)
        log.info("execute_diagnostics result = %s" % str(result))
        self.assertTrue(isinstance(result, list))

    def test_90_execute_get_power_statuses(self):
        """-- PROTOCOL EXECUTE GET POWER STATUSES test"""
        protoc = self.protoc

        log.info("getting power statuses")
        result = protoc.execute_get_power_statuses(timeout=self._timeout)
        log.info("execute_get_power_statuses result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))
