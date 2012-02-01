#!/usr/bin/env python

"""
@package ion.services.mi.test.test_sbe37_driver
@file ion/services/mi/test_sbe37_driver.py
@author Edward Hunter
@brief Test cases for SBE37Driver
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from gevent import monkey; monkey.patch_all()

import time
import unittest
import logging
from subprocess import Popen
import os
import signal

from nose.plugins.attrib import attr

from pyon.util.unit_test import PyonTestCase

from ion.services.mi.zmq_driver_client import ZmqDriverClient
from ion.services.mi.zmq_driver_process import ZmqDriverProcess
from ion.services.mi.drivers.sbe37_driver import SBE37Channel
from ion.services.mi.drivers.sbe37_driver import SBE37Parameter
from ion.services.mi.drivers.sbe37_driver import SBE37Command
import ion.services.mi.mi_logger

mi_logger = logging.getLogger('mi_logger')

#from pyon.public import log

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py
# bin/nosetests -s -v ion/services/mi/drivers/test/test_sbe37_driver.py:TestSBE37Driver.test_get_set

@attr('UNIT', group='mi')
class TestSBE37Driver(PyonTestCase):    
    """
    Unit tests for ZMQ driver process.
    """
    
    def setUp(self):
        """
        Setup test cases.
        """
        # Zmq parameters used by driver process and client.
        self.server_addr = 'localhost'
        self.cmd_port = 5556
        self.evt_port = 5557
        
        # Driver module parameters.
        self.dvr_mod = 'ion.services.mi.drivers.sbe37_driver'
        self.dvr_cls = 'SBE37Driver'

        # Comms config.
        self.comms_config = {
            'method':'ethernet',
            'device_addr': '137.110.112.119',
            'device_port': 4001,
            'server_addr': 'localhost',
            'server_port': 8888            
        }

        # Add cleanup handler functions.
        # self.addCleanup()
        
        
    def test_process(self):
        """
        """
        pass
    
    def test_config(self):
        """
        """
        pass
    
    def test_connect(self):
        """
        """
        pass
    
    def test_get_set(self):
        """
        """
        driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)
        
        driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        driver_client.start_messaging()
        time.sleep(3)

        configs = {SBE37Channel.CTD:self.comms_config}
        reply = driver_client.cmd_dvr('configure', configs)
        time.sleep(2)

        reply = driver_client.cmd_dvr('connect', [SBE37Channel.CTD])
        time.sleep(2)
        
        get_params = [
            (SBE37Channel.CTD, SBE37Parameter.ALL)            
        ]
        reply = driver_client.cmd_dvr('get', get_params)
        time.sleep(2)
        
        get_params = [
            (SBE37Channel.CTD, SBE37Parameter.TA2 ),
            (SBE37Channel.CTD, SBE37Parameter.PTCA1),
            (SBE37Channel.CTD, SBE37Parameter.TCALDATE)
        ]
        reply = driver_client.cmd_dvr('get', get_params)
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('disconnect', [SBE37Channel.CTD])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('initialize', [SBE37Channel.CTD])
        time.sleep(2)
        
        driver_client.done()
        driver_process.wait()

    def test_poll(self):
        """
        """
        pass
    
    def test_autosample(self):
        """
        """
        pass
        
    def test_xxx(self):
        """
        Test driver configure.
        """

        driver_process = ZmqDriverProcess.launch_process(self.cmd_port,
            self.evt_port, self.dvr_mod,  self.dvr_cls)
        
        
        driver_client = ZmqDriverClient(self.server_addr, self.cmd_port,
                                        self.evt_port)
        driver_client.start_messaging()
        time.sleep(3)

        configs = {SBE37Channel.CTD:self.comms_config}
        reply = driver_client.cmd_dvr('configure', configs)
        time.sleep(2)

        reply = driver_client.cmd_dvr('connect', [SBE37Channel.CTD])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('execute', [SBE37Channel.CTD], [SBE37Command.ACQUIRE_SAMPLE])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('execute', [SBE37Channel.CTD], [SBE37Command.START_AUTO_SAMPLING])
        time.sleep(20)
        
        reply = driver_client.cmd_dvr('execute', [SBE37Channel.CTD], [SBE37Command.STOP_AUTO_SAMPLING])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('disconnect', [SBE37Channel.CTD])
        time.sleep(2)
        
        reply = driver_client.cmd_dvr('initialize', [SBE37Channel.CTD])
        time.sleep(2)
        
        driver_client.done()
        driver_process.wait()


    
