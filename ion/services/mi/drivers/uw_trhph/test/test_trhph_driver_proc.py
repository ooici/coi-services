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


from gevent import monkey; monkey.patch_all()

from ion.services.mi.drivers.uw_trhph.test import TrhphTestCase
from ion.services.mi.drivers.uw_trhph.test.driver_test_mixin import DriverTestMixin
from ion.services.mi.driver_int_test_support import DriverIntegrationTestSupport
from nose.plugins.attrib import attr

from ion.services.mi.instrument_driver import InstrumentDriver


from ion.services.mi.mi_logger import mi_logger
log = mi_logger


@attr('INT', group='mi')
class DriverTest(TrhphTestCase, DriverTestMixin):
    """
    TrhphInstrumentDriver integration tests involving port agent
    and driver process. The actual set of tests is provided by
    DriverTestMixin.
    """

    def setUp(self):
        """
        Calls TrhphTestCase.setUp(self), creates and assigns the
        TrhphDriverProxy, and assigns the comms_config object.
        """

        TrhphTestCase.setUp(self)

        # needed by DriverTestMixin
        self.driver = TrhphDriverProxy(self.device_address, self.device_port)
        self.comms_config = self.driver.comms_config

        def cleanup():
            self.driver._support.stop_pagent()
            self.driver._support.stop_driver()
        self.addCleanup(cleanup)


class TrhphDriverProxy(InstrumentDriver):
    """
    An InstrumentDriver serving as a proxy to the driver client
    connecting to the actual TrhphInstrumentDriver implementation.

    Extends InstrumentDriver (instead of TrhphInstrumentDriver) because
    that's the main driver interface in general whereas extending
    TrhphInstrumentDriver would bring unneeded implementation stuff.
    """

    def __init__(self, device_address, device_port):
        """
        Setup test cases.
        """

        driver_module = 'ion.services.mi.drivers.uw_trhph.trhph_driver'
        driver_class = 'TrhphInstrumentDriver'

        self._support = DriverIntegrationTestSupport(driver_module,
                                                     driver_class,
                                                     device_address,
                                                     device_port)

        # Create and start the port agent.
        mi_logger.info('starting port agent')
        self.comms_config = {'addr': 'localhost'}
        self.comms_config['port'] = self._support.start_pagent()

        # Create and start the driver.
        mi_logger.info('starting driver client')
        self._support.start_driver()

        self._dvr_client = self._support._dvr_client

    def get_current_state(self, *args, **kwargs):
        state = self._dvr_client.cmd_dvr('get_current_state',
                                         *args,
                                         **kwargs)
        mi_logger.info("get_current_state = %s" % state)
        return state

    def initialize(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('initialize')
        mi_logger.info("initialize reply = %s" % reply)
        return reply

    def configure(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('configure',
                                         *args,
                                         **kwargs)
        mi_logger.info("configure reply = %s" % reply)
        return reply

    def connect(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('connect',
                                         *args,
                                         **kwargs)
        mi_logger.info("connect reply = %s" % reply)
        return reply

    def disconnect(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('disconnect',
                                         *args,
                                         **kwargs)
        mi_logger.info("disconnect reply = %s" % reply)
        return reply

    def get(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('get',
                                         *args,
                                         **kwargs)
        mi_logger.info("get reply = %s" % reply)
        return reply

    def set(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('set',
                                         *args,
                                         **kwargs)
        mi_logger.info("set reply = %s" % reply)
        return reply

    def execute_stop_autosample(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('execute_stop_autosample',
                                         *args,
                                         **kwargs)
        mi_logger.info("execute_stop_autosample reply = %s" % reply)
        return reply

    def execute_get_metadata(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('execute_get_metadata',
                                         *args,
                                         **kwargs)
        mi_logger.info("execute_get_metadata reply = %s" % reply)
        return reply

    def execute_diagnostics(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('execute_diagnostics',
                                         *args,
                                         **kwargs)
        mi_logger.info("execute_diagnostics reply = %s" % reply)
        return reply

    def execute_get_power_statuses(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('execute_get_power_statuses',
                                         *args,
                                         **kwargs)
        mi_logger.info("execute_get_power_statuses reply = %s" % reply)
        return reply

    def execute_start_autosample(self, *args, **kwargs):
        reply = self._dvr_client.cmd_dvr('execute_start_autosample',
                                         *args,
                                         **kwargs)
        mi_logger.info("execute_start_autosample reply = %s" % reply)
        return reply
