#!/usr/bin/env python

"""
@file ion/services/mi/drivers/uw_trhph/test/test_trhph_client.py
@author Carlos Rueda
@brief TrhphClient tests
"""

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_trhph.trhph import CHANNEL_NAMES
from ion.services.mi.drivers.uw_trhph.trhph_client import TrhphClient
from ion.services.mi.drivers.uw_trhph.trhph_client import State

from ion.services.mi.mi_logger import mi_logger
log = mi_logger

import time
import datetime

from ion.services.mi.drivers.uw_trhph.test import TrhphTestCase
from nose.plugins.attrib import attr

import unittest
import os


@unittest.skipIf(os.getenv('run_it') is None,
'''Not run by default because of mixed monkey-patching issues. \
Define environment variable run_it to force execution.''')
@attr('UNIT', group='mi')
class TrhphClientTest(TrhphTestCase):

    # this class variable is to keep a single reference to the TrhphClient
    # object in the current test. setUp will first finalize such object in case
    # tearDown/cleanup does not get called. Note that any test with an error
    # will likely make subsequent tests immediately fail because of the
    # potential problem with a second connection.
    _trhph_client = None

    @classmethod
    def _end_client_if_any(cls):
        """Ends the current TrhphClient, if any."""
        if TrhphClientTest._trhph_client:
            log.info("releasing not finalized TrhphClient object")
            try:
                TrhphClientTest._trhph_client.end()
            finally:
                TrhphClientTest._trhph_client = None

    @classmethod
    def tearDownClass(cls):
        """Make sure we end the last TrhphClient object if still remaining."""
        try:
            cls._end_client_if_any()
        finally:
            super(TrhphClientTest, cls).tearDownClass()

    def setUp(self):
        """
        Sets up and connects the _client.
        """

        TrhphClientTest._end_client_if_any()

        super(TrhphClientTest, self).setUp()

        host = self.device_address
        port = self.device_port
        self._samples_recd = 0
        outfile = file('trhph_output.txt', 'w')
        prefix_state = True
        _client = TrhphClient(host, port, outfile, prefix_state)

        # set the class and instance variables to refer to this object:
        TrhphClientTest._trhph_client = self._client = _client

        # prepare client including going to the main menu
        _client.set_data_listener(self._data_listener)
        _client.set_generic_timeout(self._timeout)

        log.info("connecting")
        _client.connect()

    def tearDown(self):
        """
        Ends the _client.
        """
        client = TrhphClientTest._trhph_client
        TrhphClientTest._trhph_client = None
        try:
            if client:
                log.info("ending TrhphClient object")
                client.end()
        finally:
            super(TrhphClientTest, self).tearDown()

    def _data_listener(self, sample):
        """
        The data listener.
        """
        self._samples_recd += 1
        log.info("_data_listener: sample received:")
        for name in CHANNEL_NAMES:
            value = sample.get(name)
            log.info("\t %s = %s" % (name, str(value)))

    def test_00_connect_disconnect(self):
        """
        -- TRHPH CLIENT: Connect, get current state, sleep, disconnect
        """

        state = self._client.get_current_state()
        log.info("current instrument state: %s" % str(state))

        if State.COLLECTING_DATA == state:
            # wait for self._timeout to get some data (if in COLLECTING_DATA)
            log.info("Sleeping for %s secs..." % self._timeout)
            time.sleep(self._timeout)

    def test_10_get_system_info(self):
        """
        -- TRHPH CLIENT: Get system info
        """

        log.info("getting system info")
        system_info = self._client.get_system_info()
        log.info("system info = %s" % str(system_info))

        self.assertTrue(isinstance(system_info, dict))

    def _get_data_collection_params(self):
        """Get data collection params"""

        log.info("getting data collection params")
        seconds, is_data_only = self._client.get_data_collection_params()
        log.info("cycle_time = %s" % str(seconds))
        log.info("is_data_only = %s" % str(is_data_only))

        self.assertTrue(isinstance(seconds, int))
        self.assertTrue(isinstance(is_data_only, bool))
        return (seconds, is_data_only)

    def test_14_get_data_collection_params(self):
        """
        -- TRHPH CLIENT: Get data collection params
        """
        seconds, is_data_only = self._get_data_collection_params()

    def test_15_set_cycle_time(self):
        """
        -- TRHPH CLIENT: Set cycle time
        """
        seconds, _ = self._get_data_collection_params()

        new_seconds = seconds + 5
        if new_seconds > 30 or new_seconds < 15:
            new_seconds = 15

        log.info("setting cycle_time %d" % new_seconds)
        self._client.set_cycle_time(new_seconds)

        seconds, _ = self._get_data_collection_params()

        log.info("verifying")
        self.assertEqual(new_seconds, seconds)

    def _set_and_assert_is_data_only(self, new_is_data_only):
        """Sets the data-only flag and verifies the flag was set"""

        self._client.set_is_data_only(new_is_data_only)

        log.info("verifying data-only flag toggle")
        _, is_data_only = self._get_data_collection_params()

        self.assertEqual(new_is_data_only, is_data_only)

    def test_19_toggle_data_only(self):
        """
        -- TRHPH CLIENT: Toggle data-only flag twice
        """

        _, is_data_only = self._get_data_collection_params()

        new_is_data_only = not is_data_only

        log.info("toggling data-only flag to %s" % str(new_is_data_only))
        self._set_and_assert_is_data_only(new_is_data_only)

        log.info("toggling data-only flag back to %s" % str(is_data_only))
        self._set_and_assert_is_data_only(is_data_only)

    def test_20_diagnostics(self):
        """
        -- TRHPH CLIENT: Execute instrument diagnostics
        """

        num_scans = 12  # 12 is just arbitrary
        log.info("execute diagnostics, num_scans=%d" % num_scans)
        diagnostics = self._client.execute_diagnostics(num_scans)
        for row in diagnostics:
            log.info("diagnostics = %s" % str(row))

    def _get_power_statuses(self):
        log.info("get sensor power statuses")
        power_statuses = self._client.get_power_statuses()
        log.info("power_statuses: %s" % str(power_statuses))
        for (k, v) in power_statuses.items():
            self.assertTrue(isinstance(v, bool))
        return power_statuses

    def test_30_get_sensor_power_info(self):
        """
        -- TRHPH CLIENT: Get sensor power statuses
        """

        self._get_power_statuses()

    def _set_and_assert_power_statuses(self, new_power_statuses):
        """Sets sensor power statuses and verifies they are set as indicated"""

        self._client.set_power_statuses(new_power_statuses)

        power_statuses = self._get_power_statuses()

        log.info("verifying")
        self.assertEqual(len(new_power_statuses.keys()),
                         len(power_statuses.keys()))

        for (k, v) in power_statuses.items():
            self.assertEqual(new_power_statuses[k], v)

    def test_35_set_sensor_power_info(self):
        """
        -- TRHPH CLIENT: Toggle all sensor power statuses twice
        """

        power_statuses = self._get_power_statuses()
        new_power_statuses = {}
        for (k, v) in power_statuses.items():
            new_power_statuses[k] = not v

        self._set_and_assert_power_statuses(new_power_statuses)

        # revert to original values
        log.info("Toggling again to revert to %s" % str(power_statuses))
        self._set_and_assert_power_statuses(power_statuses)

    def test_99_resume_streaming(self):
        """
        -- TRHPH CLIENT: Go to main menu and resume streaming
        """

        log.info("going to main menu")
        self._client.go_to_main_menu(timeout=self._timeout)

        self._samples_recd = 0
        log.info("resuming data streaming")
        self._client.resume_data_streaming()

        # wait for a bit to get some data
        timeout_limit = time.time() + 30
        while self._samples_recd == 0 and time.time() < timeout_limit:
            time.sleep(2)
        log.info("%d samples received after resuming" % self._samples_recd)
