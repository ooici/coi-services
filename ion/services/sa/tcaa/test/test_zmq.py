#!/usr/bin/env python
"""
@package 
@file 
@author 
@brief
"""

__author__ = 'Edward Hunter'


from pyon.public import log
from pyon.public import CFG

import gevent

import zmq
from zmq import ZMQError
from zmq import NOBLOCK

from pyon.util.int_test import IonIntegrationTestCase


# bin/nosetests -s -v --nologcapture ion/services/sa/tcaa/test/test_zmq.py
# bin/nosetests -s -v --nologcapture ion/services/sa/tcaa/test/test_zmq.py:TestZMQ.test_xx
# bin/nosetests -s -v --nologcapture ion/services/sa/tcaa/test/test_zmq.py:TestZMQ.test_yy
# bin/nosetests -s -v --nologcapture ion/services/sa/tcaa/test/test_zmq.py:TestZMQ.test_zz


class TestZMQ(IonIntegrationTestCase):
    """
    Simple testing scenario to expose the use of internet and unix domain
    socket resources by zeroMQ and determine proper cleanup steps.
    lsof -a -U -c python -r 1  == command to monitor unix sockets
    lsof -a -i -c python -r 1  == command to monitor internet sockets    
    """
    def setUp(self):
        """
        """
        log.info('')
        
    def create_context(self):
        """
        """
        log.info('Creating context.')
        self._context = zmq.Context(1)
        
    def destroy_context(self):
        """
        """
        log.info('Destroying context.')
        #self._context.destroy()
        self._context.term()
        self._context = None
        
    def test_xx(self):
        """
        """
        log.info('In text xx.')
        gevent.sleep(3)        
        self.create_context()
        gevent.sleep(3)
        self.destroy_context()
        
    def test_yy(self):
        """
        """
        log.info('In text yy.')
        gevent.sleep(3)        
        self.create_context()
        gevent.sleep(3)
        self.destroy_context()
        
    def test_zz(self):
        """
        """
        log.info('In text zz.')
        gevent.sleep(3)        
        self.create_context()
        gevent.sleep(3)
        self.destroy_context()

