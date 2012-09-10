#!/usr/bin/env python

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon log and config objects.
from pyon.public import log
from pyon.public import CFG

# Standard imports.
import time
import os
import signal
import time
import unittest
from datetime import datetime
import uuid
import socket
import re

# 3rd party imports.
import gevent
from gevent import spawn
from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from mock import patch

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase

from ion.services.sa.tcaa.r3pc import R3PCServer
from ion.services.sa.tcaa.r3pc import R3PCClient
from ion.services.sa.tcaa.r3pc import R3PCTestBehavior


# bin/nosetests -s -v ion/services/sa/tcaa/test/test_r3pc.py
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_r3pc.py:TestR3PCSocket.test_something
# bin/nosetests -s -v ion/services/sa/tcaa/test/test_r3pc.py:TestR3PCSocket.test_something_else

@unittest.skip('Socket unavailable on buildbot.')
@attr('INT', group='sa')
class TestR3PCSocket(IonIntegrationTestCase):
    """
    """
    def setUp(self):
        """
        """
        self._ack_recv_evt = AsyncResult()
        self._req_recv_evt = AsyncResult()
        self._client_close_evt = AsyncResult()
        self._server_close_evt = AsyncResult()
        self._ack_recv = []
        self._req_recv = {}
        self._req_sent = {}
        self._no_requests = 50
        self._requests = []
        for i in range(self._no_requests):
            request = 'I am request number %i!' % (i+1)
            id = uuid.uuid4()
            self._requests.append((id, request))
            
    def consume_ack(self):
        """
        """
        self._ack_recv.append('OK')
        log.debug('Client acks received: %i', len(self._ack_recv))
        if len(self._ack_recv) == self._no_requests and self._ack_recv_evt:
            self._ack_recv_evt.set()
    
    def consume_req(self, request):
        """
        """
        self._req_recv[request[0]] = request[1]
        log.debug('Server reqs received: %s', len(self._req_recv))
        if len(self._req_recv) == self._no_requests and self._req_recv_evt:
            self._req_recv_evt.set()

    def client_close(self):
        """
        """
        self._client_close_evt.set()
        
    def server_close(self):
        """
        """
        self._server_close_evt.set()

    def enqueue_all(self):
        """
        """
        for x in self._requests:
            self._req_sent[x[0]] = x[1]
            self._client.enqueue(x)
 
    def test_normal(self):
        """
        """
        self._server = R3PCServer(self.consume_req, self.server_close)
        self._client = R3PCClient(self.consume_ack, self.client_close)
        
        self._server.start()
        self._client.start()

        self.enqueue_all()
        
        self._req_recv_evt.get(timeout=15)
        self._ack_recv_evt.get(timeout=15)

        self.assertDictEqual(self._req_sent, self._req_recv)
        self.assertEqual(len(self._ack_recv), self._no_requests)
    
    def test_delay_momentary(self):
        """
        """
        self._server = R3PCServer(self.consume_req, self.server_close)
        self._client = R3PCClient(self.consume_ack, self.client_close)

        self._server.test_behaviors = {
            25 : R3PCTestBehavior(R3PCTestBehavior.delay, 5)
        }
        
        self._server.start()
        self._client.start()

        self.enqueue_all()
                
        self._req_recv_evt.get(timeout=15)
        self._ack_recv_evt.get(timeout=15)

        self.assertDictEqual(self._req_sent, self._req_recv)
        self.assertEqual(len(self._ack_recv), self._no_requests)


    def test_delay_long(self):
        """
        """        
        self._server = R3PCServer(self.consume_req, self.server_close)
        self._client = R3PCClient(self.consume_ack, self.client_close)

        self._server.test_behaviors = {
            25 : R3PCTestBehavior(R3PCTestBehavior.delay, 12)
        }
        
        self._server.start()
        self._client.start()

        self.enqueue_all()

        self._client_close_evt.get(timeout=15)
        self._client.start()

        self._req_recv_evt.get(timeout=15)
        self._ack_recv_evt.get(timeout=15)

        self.assertDictEqual(self._req_sent, self._req_recv)
        self.assertEqual(len(self._ack_recv), self._no_requests)

    def test_server_restart(self):
        """
        """        
        self._server = R3PCServer(self.consume_req, self.server_close)
        self._client = R3PCClient(self.consume_ack, self.client_close)

        self._server.test_behaviors = {
            25 : R3PCTestBehavior(R3PCTestBehavior.stop, 0)
        }
        
        self._server.start()
        self._client.start()

        self.enqueue_all()

        self._server_close_evt.get(timeout=15)
        gevent.sleep(3)
        self._server.start()

        self._req_recv_evt.get(timeout=15)
        self._ack_recv_evt.get(timeout=15)

        self.assertDictEqual(self._req_sent, self._req_recv)
        self.assertEqual(len(self._ack_recv), self._no_requests)

    def test_client_restart(self):
        """
        """        
        self._server = R3PCServer(self.consume_req, self.server_close)
        self._client = R3PCClient(self.consume_ack, self.client_close)

        self._client.test_behaviors = {
            25 : R3PCTestBehavior(R3PCTestBehavior.stop, 0)
        }
        
        self._server.start()
        self._client.start()

        self.enqueue_all()

        self._client_close_evt.get(timeout=15)
        gevent.sleep(3)
        self._client.start()

        self._req_recv_evt.get(timeout=15)
        self._ack_recv_evt.get(timeout=15)

        self.assertDictEqual(self._req_sent, self._req_recv)
        self.assertEqual(len(self._ack_recv), self._no_requests)

    def test_msg_lost(self):
        """
        """        
        self._server = R3PCServer(self.consume_req, self.server_close)
        self._client = R3PCClient(self.consume_ack, self.client_close)

        self._server.test_behaviors = {
            25 : R3PCTestBehavior(R3PCTestBehavior.restart, 0)
        }
        
        self._server.start()
        self._client.start()

        self.enqueue_all()

        self._req_recv_evt.get(timeout=15)
        self._ack_recv_evt.get(timeout=15)

        self.assertDictEqual(self._req_sent, self._req_recv)
        self.assertEqual(len(self._ack_recv), self._no_requests)
