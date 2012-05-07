#!/usr/bin/env python

"""
@package ion.services.mi.driver_int_test_support
@file    ion/services/mi/driver_int_test_support.py
@author Steve Foley, Carlos Rueda
@brief Utilities to be used in driver integration test cases
"""
__author__ = 'Steve Foley, Carlos Rueda'
__license__ = 'Apache 2.0'

import gevent
import os
import logging
from ion.services.mi.logger_process import EthernetDeviceLogger
from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess

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
        this_pid = os.getpid()
        self._pagent = EthernetDeviceLogger.launch_process(self.device_addr,
                                                           self.device_port,
                                                           self.work_dir,
                                                           self.delim,
                                                           this_pid)

        pid = self._pagent.get_pid()
        while not pid:
            gevent.sleep(.1)
            pid = self._pagent.get_pid()
        port = self._pagent.get_port()
        while not port:
            gevent.sleep(.1)
            port = self._pagent.get_port()

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

    def start_driver(self):
        """
        Start the driver process.
        """

        # Launch driver process based on test config.
        this_pid = os.getpid()
        (dvr_proc, cmd_port, evt_port) = ZmqDriverProcess.launch_process(self.driver_module,
                                                                         self.driver_class,
                                                                         self.work_dir,
                                                                         this_pid)
        self._dvr_proc = dvr_proc
        mi_logger.info('Started driver process for %d %d %s %s', cmd_port,
            evt_port, self.driver_module, self.driver_class)
        mi_logger.info('Driver process pid %d', self._dvr_proc.pid)

        # Create driver client.
        self._dvr_client = ZmqDriverClient('localhost', cmd_port,
            evt_port)
        mi_logger.info('Created driver client for %d %d %s %s', cmd_port,
            evt_port, self.driver_module, self.driver_class)

        # Start client messaging.
        self._dvr_client.start_messaging(self.evt_recd)
        mi_logger.info('Driver messaging started.')
        gevent.sleep(.5)

    def stop_driver(self):
        """
        Method to shut down the driver process. Attempt normal shutdown,
        and kill the process if unsuccessful.
        """

        if self._dvr_proc:
            mi_logger.info('Stopping driver process pid %d', self._dvr_proc.pid)
            if self._dvr_client:
                self._dvr_client.done()
                self._dvr_proc.wait()
                self._dvr_client = None

            else:
                try:
                    mi_logger.info('Killing driver process.')
                    self._dvr_proc.kill()
                except OSError:
                    pass
            self._dvr_proc = None

    def evt_recd(self, evt):
        """
        Simple callback to catch events from the driver for verification.
        """
        self._events.append(evt)
