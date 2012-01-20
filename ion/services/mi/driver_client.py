#!/usr/bin/env python

"""
@package ion.services.mi.driver_client
@file ion/services/mi/driver_client.py
@author Edward Hunter
@brief Base class for driver process messaging client.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import time

class DriverClient(object):
    """
    Base class for driver clients, subclassed for specific messaging
    implementations.
    """
    
    def __init__(self):
        """
        Initialize members.
        """
        self.events = []
    
    def start_messaging(self):
        """
        Initialize and start messaging resources for the driver process client.
        Overridden for specific messaging implementations.
        """
        pass
    
    def stop_messaging(self):
        """
        Close messaging resources for the driver process client. Overridden for
        specific messaging implementations.
        """
        pass

    def cmd_dvr(self):
        """
        Command a driver by request-reply messaging. Overridden for
        specific messaging implementations.
        """
        pass
    
    def done(self):
        """
        Conclude driver process and stop client messaging resources.
        """
        self.cmd_dvr('stop_driver_process')
        self.stop_messaging()

    def test(self):
        """
        Simple test script to verify command request-response and event
        messaging.
        """
        self.start_messaging()
        time.sleep(3)
        reply = self.cmd_dvr('process_echo', data='test 1 2 3')
        time.sleep(3)
        reply = self.cmd_dvr('process_echo', data='zoom zoom boom boom')
        time.sleep(3)
        reply = self.cmd_dvr('test_events')
        time.sleep(3)
        self.done()
    
