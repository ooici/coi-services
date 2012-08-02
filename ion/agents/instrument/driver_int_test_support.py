#!/usr/bin/env python

"""
@package ion.agents.instrument.driver_int_test_support
@file    ion/agents.instrument/driver_int_test_support.py
@author Steve Foley, Carlos Rueda
@brief Utilities to be used in driver integration test cases
"""
from ion.agents.port.port_agent_process import PortAgentProcess

__author__ = 'Steve Foley, Carlos Rueda'
__license__ = 'Apache 2.0'

import gevent
import os
import logging
from ion.agents.port.logger_process import EthernetDeviceLogger

mi_logger = logging.getLogger('mi_logger')


class DriverIntegrationTestSupport(object):
    """
    Common functionality helpful for driver integration testing.
    """

    def __init__(self, driver_module, driver_class,
                 device_addr, device_port, delim=None,
                 work_dir='/tmp/'):

        """
        @param driver_module
        @param driver_class
        @param device_addr
        @param device_port
        @param delim 2-element delimiter to indicate traffic from the driver
               in the logfile. See EthernetDeviceLogger.launch_process for
               default value.
        @param work_dir by default, '/tmp/'
        """

        object.__init__(self)

        self.driver_module = driver_module
        self.driver_class = driver_class

        self.device_addr = device_addr
        self.device_port = device_port
        self.delim = delim
        self.work_dir = work_dir

        # Should clear this in setup
        self._events = []
        self._dvr_client = None

    def start_pagent(self):
        """
        Construct and start the port agent.
        @retval port Port that was used for connection to agent
        """
        # Create port agent object.
        config = { 'device_addr' : self.device_addr,
                   'device_port' : self.device_port,
                   'working_dir' : self.work_dir,
                   'delimiter' : self.delim }
        self._pagent = PortAgentProcess.launch_process(config, timeout = 60, test_mode = True)
        pid = self._pagent.get_pid()
        port = self._pagent.get_data_port()

        mi_logger.info('Started port agent pid %d listening at port %d', pid, port)
        return port

    def stop_pagent(self):
        """
        Stop the port agent.
        """
        if self._pagent:
            pid = self._pagent.get_pid()
            if pid:
                mi_logger.info('Stopping pagent pid %i', pid)
                self._pagent.stop()
            else:
                mi_logger.info('No port agent running.')

