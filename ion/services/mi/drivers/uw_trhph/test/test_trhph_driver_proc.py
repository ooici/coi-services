#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.test.test_trhph_driver_proc
@file    ion/services/mi/drivers/uw_trhph/test/test_trhph_driver_proc.py
@author Carlos Rueda
@brief TrhphInstrumentDriver integration tests involving port agent
and driver process.
"""

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'


# Ensure the test class is monkey patched for gevent
from gevent import monkey; monkey.patch_all()
import gevent

# Standard lib imports
import time
import unittest
import logging
from subprocess import Popen
import os
import signal


from ion.services.mi.drivers.uw_trhph.trhph_driver import TrhphDriverState

from ion.services.mi.drivers.uw_trhph.test import TrhphTestCase
from ion.services.mi.driver_int_test_support import DriverIntegrationTestSupport
from nose.plugins.attrib import attr

from ion.services.mi.instrument_driver import DriverParameter


# MI logger
import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')


DVR_MOD = 'ion.services.mi.drivers.uw_trhph.trhph_driver'
DVR_CLS = 'TrhphInstrumentDriver'

WORK_DIR = '/tmp/'
DELIM = ['<<','>>']
COMMS_CONFIG = {'addr': 'localhost'}


@attr('INT', group='mi')
class DriverIntTest(TrhphTestCase):
    """
    Integration tests for the TRHPH driver.
    """    
    
    def setUp(self):
        """
        Setup test cases.
        """

        TrhphTestCase.setUp(self)
#        DriverTestCase.setUp(self)


        self._support = DriverIntegrationTestSupport(DVR_MOD, DVR_CLS,
                            self.device_address, self.device_port,
                            DELIM)

        self._dvr_client = None

        # Create and start the port agent.
        mi_logger.info('start')

        COMMS_CONFIG['port'] = self._support.start_pagent()
        self.addCleanup(self._support._top_pagent)


        # Create and start the driver.
        self._support.start_driver()
        self.addCleanup(self._support.stop_driver)

        self._dvr_client = self._support._dvr_client

    def _get_current_state(self):
        state = self._dvr_client.cmd_dvr('get_current_state')
        mi_logger.info("get_current_state = %s" % state)
        return state

    def test_basic(self):
        self._get_current_state()

    def test_get(self):
        """
        """

        state = self._get_current_state()
        self.assertEqual(state, TrhphDriverState.UNCONFIGURED)

        reply = self._dvr_client.cmd_dvr('configure', config=COMMS_CONFIG)

        state = self._get_current_state()
        self.assertEqual(state, TrhphDriverState.DISCONNECTED)

        reply = self._dvr_client.cmd_dvr('connect')

        state = self._get_current_state()
#        self.assertEqual(state, SBE37ProtocolState.UNKNOWN)

        reply = self._dvr_client.cmd_dvr('get', params=DriverParameter.ALL)
        mi_logger.info("get reply = %s" % reply)
