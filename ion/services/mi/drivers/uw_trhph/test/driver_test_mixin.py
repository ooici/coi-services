#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.test.driver_test_mixin
@file    ion/services/mi/drivers/uw_trhph/test/driver_test_mixin.py
@author Carlos Rueda
@brief A convenient mixin class for driver tests where the actual driver
       operations are implemented by a subclass.
"""

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'


from ion.services.mi.drivers.uw_trhph.trhph_driver import TrhphDriverState
from ion.services.mi.drivers.uw_trhph.common import TrhphParameter

from ion.services.mi.instrument_driver import DriverParameter
from ion.services.mi.exceptions import InstrumentParameterException
#from ion.services.mi.common import InstErrorCode

import random
import time
from ion.services.mi.mi_logger import mi_logger
log = mi_logger


class DriverTestMixin(object):
    """
    A very convenient mixin class for driver tests where the actual driver
    implementation is given by a subclass. This mixin greatly facilitates
    to perform a complete set of tests on various different mechanisms to
    access the actual driver implementation without having to duplicate lots
    of code.

    The only requirement for a subclass is to provide the concrete driver
    implementation object by assigning it to self.driver, and to provide
    a configuration object and assign it to self.comm_config, which is
    used for the self.driver.configure call.

    Actual driver implementations currently tested with this mixin are:

    1) As shown in test_trhph_driver_proc, a driver proxy that interacts with
    corresponding driver client (which itself interacts with the actual
    driver process running separately).

    2) As shown in test_trhph_driver, the driver object is directly an
    instance of TrhphInstrumentDriver.
    """

    def _prepare_and_connect(self):
        self._assert_driver_unconfigured()
        self._initialize()
        self._configure()
        self._connect()

    def _assert_driver_unconfigured(self):
        state = self.driver.get_current_state()
        log.info("driver connected -> %s" % str(state))
        self.assertEqual(TrhphDriverState.UNCONFIGURED, state)

    def _initialize(self):
        self.driver.initialize()
        state = self.driver.get_current_state()
        log.info("intitialize -> %s" % str(state))
        self.assertEqual(TrhphDriverState.UNCONFIGURED, state)

    def _configure(self):
        self.driver.configure(config=self.comms_config)
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

    def _get_params_OLD(self, valid_params, invalid_params=None):

        # TODO Remove this method

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
            res_p = result.get(p)
            print '::::: %s' % str(res_p)
            self.assertTrue(InstErrorCode.is_error(res_p))

        return result

    def _get_params(self, params):

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

            if TrhphParameter.TIME_BETWEEN_BURSTS == p:
                seconds = result.get(p)
                self.assertTrue(isinstance(seconds, int))
            elif TrhphParameter.VERBOSE_MODE == p:
                is_data_only = result.get(p)
                self.assertTrue(isinstance(is_data_only, bool))

        return result

    def test_00_basic(self):
        """
        -- TRHPH DRIVER: basic tests
        """
        self._prepare_and_connect()
        self._disconnect()

    def test_10_get_params_valid(self):
        """
        -- TRHPH DRIVER: get valid params
        """
        self._prepare_and_connect()

        self._get_params(DriverParameter.ALL)
        self._get_params([TrhphParameter.TIME_BETWEEN_BURSTS])

        self._disconnect()

    def test_15_get_params_invalid(self):
        """
        -- TRHPH DRIVER: get invalid params
        """
        self._prepare_and_connect()

        with self.assertRaises(InstrumentParameterException):
            self._get_params(["bad-param1", "bad-param2"])

        with self.assertRaises(InstrumentParameterException):
            self._get_params([TrhphParameter.TIME_BETWEEN_BURSTS,
                             "bad-param3"])

        self._disconnect()

    def _set_params_OLD(self, valid_params, invalid_params=None):
        """
        Method used when the status or result could be given to individual
        params, so there could be a mix of valid and invalid parameters.
        Now (May 6 2012) I see from the mainline that a exception is
        raised even when just an individual parameter is invalid.
        """
        # TODO Remove this method

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

    def _set_params(self, params):
        """
        Sets the given parameters, which are assumed to be all valid.
        """
        result = self.driver.set(params=params, timeout=self._timeout)
        log.info("set result = %s" % str(result))
        assert isinstance(result, dict)

        # check all requested params are in the result
        for (p, v) in params.items():
            self.assertTrue(p in result)

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

    def test_20_set_params_valid(self):
        """
        -- TRHPH DRIVER: set valid params
        """
        self._prepare_and_connect()

        p1 = TrhphParameter.TIME_BETWEEN_BURSTS
        new_seconds = random.randint(15, 60)

        p2 = TrhphParameter.VERBOSE_MODE
        verbose = self._get_verbose_flag_for_set_test()

        valid_params = {p1: new_seconds, p2: verbose}

        self._set_params(valid_params)

        self._disconnect()

    def test_21_set_params_invalid(self):
        """
        -- TRHPH DRIVER: set invalid params
        """
        self._prepare_and_connect()

        p1 = TrhphParameter.TIME_BETWEEN_BURSTS
        new_seconds = random.randint(15, 60)

        invalid_params = {p1: new_seconds, "bad-param": "dummy-value"}

        with self.assertRaises(InstrumentParameterException):
            self._set_params(invalid_params)

        self._disconnect()

    def test_25_get_set_params(self):
        """
        -- TRHPH DRIVER: get and set params
        """
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
        """
        -- TRHPH DRIVER: stop autosample
        """
        self._prepare_and_connect()

        log.info("stopping autosample")
        result = self.driver.execute_stop_autosample(timeout=self._timeout)
        log.info("execute_stop_autosample result = %s" % str(result))
#        self.assertTrue(InstErrorCode.is_ok(result))

        self._disconnect()

    def test_70_execute_get_metadata(self):
        """
        -- TRHPH DRIVER: get metadata
        """
        self._prepare_and_connect()

        log.info("getting metadata")
        # TODO specific metadata parameters may be desirable
        result = self.driver.execute_get_metadata(timeout=self._timeout)
        log.info("metadata result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

        self._disconnect()

    def test_80_execute_diagnostics(self):
        """
        -- TRHPH DRIVER: diagnostics
        """
        self._prepare_and_connect()

        log.info("executing diagnotics")
        result = self.driver.execute_diagnostics(
                num_scans=6, timeout=self._timeout)
        log.info("execute_diagnostics result = %s" % str(result))
        self.assertTrue(isinstance(result, list))

        self._disconnect()

    def test_90_execute_get_power_statuses(self):
        """
        -- TRHPH DRIVER: get power statuses
        """
        self._prepare_and_connect()

        log.info("getting power statuses")
        result = self.driver.execute_get_power_statuses(timeout=self._timeout)
        log.info("execute_get_power_statuses result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

        self._disconnect()

    def test_99_execute_start_autosample(self):
        """
        -- TRHPH DRIVER: start autosample
        """
        self._prepare_and_connect()

        log.info("starting autosample")
        result = self.driver.execute_start_autosample(timeout=self._timeout)
        log.info("execute_start_autosample result = %s" % str(result))
#        self.assertTrue(InstErrorCode.is_ok(result))
        # just sleep for a few secs (note: not necessarily data will come
        # in this short interval).
        time.sleep(10)

        self._disconnect()
