#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.test.test_protocol
@file ion/services/mi/drivers/uw_trhph/test/test_protocol.py
@author Carlos Rueda
@brief Directly tests the TrhphInstrumentProtocol
"""

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_trhph.trhph_driver import TrhphDriverState
from ion.services.mi.drivers.uw_trhph.trhph_driver import TrhphInstrumentDriver
from ion.services.mi.drivers.uw_trhph.common import TrhphParameter

from ion.services.mi.exceptions import NotImplementedException

from ion.services.mi.drivers.uw_trhph.trhph_client import TrhphClient
from ion.services.mi.drivers.uw_trhph.common import TrhphParameter

from ion.services.mi.instrument_driver import DriverParameter

from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode

import random
import time
from ion.services.mi.mi_logger import mi_logger
log = mi_logger

from ion.services.mi.drivers.uw_trhph.test import TrhphTestCase
from nose.plugins.attrib import attr


@attr('UNIT', group='mi')
class DriverTest(TrhphTestCase):

    def _prepare_and_connect(self):
        """-- DRIVER BASIC TESTS"""
        self._create_driver()
        self._initialize()
        self._configure()
        self._connect()

    def _create_driver(self):
        def evt_callback(event):
            log.info("CALLBACK: %s" % str(event))

        self.driver = TrhphInstrumentDriver(evt_callback)
        state = self.driver.get_current_state()
        log.info("driver created -> %s" % str(state))
        self.assertEqual(TrhphDriverState.UNCONFIGURED, state)
        return self.driver

    def _initialize(self):
        self.driver.initialize()
        state = self.driver.get_current_state()
        log.info("intitialize -> %s" % str(state))
        self.assertEqual(TrhphDriverState.UNCONFIGURED, state)

    def _configure(self):
        config = {
            'addr': self.device_address,
            'port': self.device_port}
        self.driver.configure(config=config)
        state = self.driver.get_current_state()
        log.info("configure -> %s" % str(state))
        self.assertEqual(TrhphDriverState.DISCONNECTED, state)

    def _connect(self):
        self.driver.connect()
        state = self.driver.get_current_state()
        log.info("connect -> %s" % str(state))
        self.assertEqual(TrhphDriverState.CONNECTED, state)

    def _disconnect(self):
        self.driver.disconnect()
        state = self.driver.get_current_state()
        log.info("disconnect -> %s" % str(state))
        self.assertEqual(TrhphDriverState.DISCONNECTED, state)

    def _discover_not_implemented(self):
        self.assertRaises(NotImplementedException, self.driver.discover)
        log.info("discover not implemented ok.")

    def _get_params(self, valid_params, invalid_params=None):

        invalid_params = invalid_params or []

        if len(invalid_params) == 0:
            # use valid_params exactly as given
            params = valid_params
        else:
            if valid_params == DriverParameter.ALL:
                valid_params = TrhphParameter.list()

            params = valid_params + invalid_params

        result = self.driver.get(params=params, timeout=self._timeout)
        log.info("get result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

        if params == DriverParameter.ALL:
            all_requested_params = TrhphParameter.list()
        else:
            all_requested_params = params

        # check all requested params are in the result
        for p in all_requested_params:
            self.assertTrue(p in result)

        for p in valid_params:
            self.assertFalse(InstErrorCode.is_error(result.get(p)))
            if TrhphParameter.TIME_BETWEEN_BURSTS == p:
                seconds = result.get(p)
                self.assertTrue(isinstance(seconds, int))
            if TrhphParameter.VERBOSE_MODE == p:
                is_data_only = result.get(p)
                self.assertTrue(isinstance(is_data_only, bool))

        for p in invalid_params:
            self.assertTrue(InstErrorCode.is_error(result.get(p)))

        return result

    def test_00_basic(self):
        """-- DRIVER BASIC TESTS"""
        self._prepare_and_connect()
        self._discover_not_implemented()
        self._disconnect()

    def test_10_get_params(self):
        """-- DRIVER GET PARAMS TESTS"""
        self._prepare_and_connect()

        self._get_params(DriverParameter.ALL)

        self._get_params(DriverParameter.ALL,
                         ["bad-param1", "bad-param2"])

        self._get_params([TrhphParameter.TIME_BETWEEN_BURSTS],
                         ["bad-param1", "bad-param2"])

        self._disconnect()

    def _set_params(self, valid_params, invalid_params=None):

        invalid_params = invalid_params or {}
        params = dict(valid_params.items() + invalid_params.items())

        result = self.driver.set(params=params, timeout=self._timeout)
        log.info("set result = %s" % str(result))
        assert isinstance(result, dict)

        # check all requested params are in the result
        for (p, v) in params.items():
            self.assertTrue(p in result)
            if p in valid_params:
                self.assertTrue(InstErrorCode.is_ok(result.get(p)))
            else:
                self.assertFalse(InstErrorCode.is_ok(result.get(p)))

    def _get_verbose_flag_for_set_test(self):
        """
        Gets the value to use for the verbose flag in the "set" operations.
        If we are testing against the real instrument (self._is_real_instrument
        is true), this always returns False because the associated
        interfaces with verbose=True are not implemented yet.
        Otherwise it returns a random boolean value. Note, in this case, which
        means we are testing against the simulator, the actual verbose value
        does not have any effect of the other interface elements, so it is ok
        to set any value here.
        TODO align this when the verbose flag is handled completely,
        both in the driver and in the simulator.
        """
        if self._is_real_instrument:
            log.info("setting verbose=False because _is_real_instrument")
            return False
        else:
            return 0 == random.randint(0, 1)

    def test_20_set_params(self):
        """-- DRIVER SET TESTS"""
        self._prepare_and_connect()

        p1 = TrhphParameter.TIME_BETWEEN_BURSTS
        new_seconds = random.randint(15, 60)

        p2 = TrhphParameter.VERBOSE_MODE
        verbose = self._get_verbose_flag_for_set_test()

        valid_params = {p1: new_seconds, p2: verbose}
        invalid_params = {"bad-param": "dummy-value"}

        self._set_params(valid_params, invalid_params)

        self._disconnect()

    def test_25_get_set_params(self):
        """-- DRIVER GET/SET TESTS"""
        self._prepare_and_connect()

        p1 = TrhphParameter.TIME_BETWEEN_BURSTS
        result = self._get_params([p1])
        seconds = result[p1]
        new_seconds = seconds + 5
        if new_seconds > 30 or new_seconds < 15:
            new_seconds = 15

        p2 = TrhphParameter.VERBOSE_MODE
        new_verbose = self._get_verbose_flag_for_set_test()

        valid_params = {p1: new_seconds, p2: new_verbose}

        log.info("setting: %s" % str(valid_params))

        self._set_params(valid_params)

        result = self._get_params([p1, p2])

        seconds = result[p1]
        self.assertEqual(seconds, new_seconds)

        verbose = result[p2]
        self.assertEqual(verbose, new_verbose)

        self._disconnect()

    def test_35_execute_stop_autosample(self):
        """-- DRIVER STOP AUTOSAMPLE TEST"""
        self._prepare_and_connect()

        log.info("stopping autosample")
        result = self.driver.execute_stop_autosample(timeout=self._timeout)
        log.info("execute_stop_autosample result = %s" % str(result))
        self.assertTrue(InstErrorCode.is_ok(result))

        self._disconnect()

    def test_70_execute_get_metadata(self):
        """-- DRIVER EXECUTE GET METADATA TEST"""
        self._prepare_and_connect()

        log.info("getting metadata")
        # TODO specific metadata parameters may be desirable
        result = self.driver.execute_get_metadata(timeout=self._timeout)
        log.info("metadata result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

        self._disconnect()

    def test_80_execute_diagnostics(self):
        """-- DRIVER EXECUTE DIAGNOSTICS TEST"""
        self._prepare_and_connect()

        log.info("executing diagnotics")
        result = self.driver.execute_diagnostics(
                num_scans=6, timeout=self._timeout)
        log.info("execute_diagnostics result = %s" % str(result))
        self.assertTrue(isinstance(result, list))

        self._disconnect()

    def test_90_execute_get_power_statuses(self):
        """-- DRIVER EXECUTE GET POWER STATUSES TEST"""
        self._prepare_and_connect()

        log.info("getting power statuses")
        result = self.driver.execute_get_power_statuses(timeout=self._timeout)
        log.info("execute_get_power_statuses result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

        self._disconnect()

    def test_99_execute_start_autosample(self):
        """-- DRIVER START AUTOSAMPLE TEST"""
        self._prepare_and_connect()

        log.info("starting autosample")
        result = self.driver.execute_start_autosample(timeout=self._timeout)
        log.info("execute_start_autosample result = %s" % str(result))
        self.assertTrue(InstErrorCode.is_ok(result))
        # just sleep for a few secs (note: not necessarily data will come
        # in this short interval).
        time.sleep(10)

        self._disconnect()
