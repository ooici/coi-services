#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.test.test_driver
@file ion/services/mi/drivers/uw_trhph/test/test_driver.py
@author Carlos Rueda
@brief Directly tests the TrhphInstrumentDriver
"""

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_trhph.driver import TrhphInstrumentDriver
from ion.services.mi.drivers.uw_trhph.common import TrhphChannel
from ion.services.mi.drivers.uw_trhph.common import TrhphParameter

from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode

import random
import time
from ion.services.mi.mi_logger import mi_logger
log = mi_logger

from ion.services.mi.drivers.uw_trhph.test import TrhphTestCase
from mock import Mock
from nose.plugins.attrib import attr
import unittest
import os


@unittest.skip('Need to align with new refactoring')
@attr('UNIT', group='mi')
class DriverTest(TrhphTestCase):

    def setUp(self):
        super(DriverTest, self).setUp()
        self.callback = Mock()
        self.driver = TrhphInstrumentDriver(self.callback)
        self.driver.configure({TrhphChannel.INSTRUMENT: self.config},
                              timeout=self._timeout)
        log.info("configured")
        self.driver.connect([TrhphChannel.INSTRUMENT], timeout=self._timeout)
        log.info("connected")
        self.addCleanup(self._clean_up)

    def _clean_up(self):
        if self.driver:
            try:
                log.info("disconnecting")
                self.driver.disconnect([TrhphChannel.INSTRUMENT],
                                       timeout=self._timeout)
                super(DriverTest, self).tearDown()
            finally:
                self.driver = None

    def tearDown(self):
        self._clean_up()

    def test_00_connect_disconnect(self):
        """-- DRIVER CONNECT/DISCONNECT test"""

        result = self.driver.get_current_state()
        log.info("current driver state: %s" % str(result))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(TrhphChannel.INSTRUMENT in result)
        state = result[TrhphChannel.INSTRUMENT]

        self.assertTrue(DriverState.has(state))

    def _get(self, params):
        result = self.driver.get(params, timeout=self._timeout)
        log.info("get result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

        return result

    def test_20_execute_start_autosample(self):
        """-- DRIVER START AUTOSAMPLE test"""

        log.info("start autosample")
        result = self.driver.execute_start_autosample(timeout=self._timeout)
        log.info("execute_start_autosample result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

    def test_25_execute_stop_autosample(self):
        """-- DRIVER STOP AUTOSAMPLE test"""

        log.info("stop autosample")
        result = self.driver.execute_stop_autosample(timeout=self._timeout)
        log.info("execute_stop_autosample result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

    def test_30_get_all_params(self):
        """-- DRIVER GET test"""

        # request all valid params plus some invalid params:
        params = TrhphParameter.list()
        invalid_params = ["bad-param1", "bad-param2"]
        params.extend(invalid_params)
        cp_params = []
        for p in params:
            cp = (TrhphChannel.INSTRUMENT, p)
            cp_params.append(cp)

        result = self._get(cp_params)

        for cp in cp_params:
            self.assertTrue(cp in result)

        for p in TrhphParameter.list():
            cp = (TrhphChannel.INSTRUMENT, p)
            self.assertFalse(InstErrorCode.is_error(result.get(cp)))

        for p in invalid_params:
            cp = (TrhphChannel.INSTRUMENT, p)
            self.assertTrue(InstErrorCode.is_error(result.get(cp)))

        seconds = result.get((TrhphChannel.INSTRUMENT,
                              TrhphParameter.TIME_BETWEEN_BURSTS))
        is_data_only = result.get((TrhphChannel.INSTRUMENT,
                                   TrhphParameter.VERBOSE_MODE))

        self.assertTrue(isinstance(seconds, int))
        self.assertTrue(isinstance(is_data_only, bool))

    def test_35_set(self):
        """-- DRIVER SET test"""
        new_seconds = random.randint(15, 60)
        cp = (TrhphChannel.INSTRUMENT, TrhphParameter.TIME_BETWEEN_BURSTS)
        params = [cp]
        set_params = {cp: new_seconds}

        log.info("SET: %s" % str(set_params))

        set_result = self.driver.set(set_params, timeout=self._timeout)

        for cp in params:
            self.assertTrue(cp in set_result)

    def test_70_execute_get_metadata(self):
        """-- DRIVER EXECUTE GET METADATA test"""

        log.info("getting metadata")
        result = self.driver.execute_get_metadata(timeout=self._timeout)
        log.info("metadata result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

    def test_80_execute_diagnostics(self):
        """-- DRIVER EXECUTE DIAGNOSTICS test"""

        log.info("executing diagnostics")
        result = self.driver.execute_diagnostics(timeout=self._timeout)
        log.info("execute_diagnostics result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))

    def test_90_execute_get_power_statuses(self):
        """-- DRIVER EXECUTE GET POWER STATUSES test"""

        log.info("getting power statuses")
        result = self.driver.execute_get_power_statuses(timeout=self._timeout)
        log.info("execute_get_power_statuses result = %s" % str(result))
        self.assertTrue(isinstance(result, dict))
