#!/usr/bin/env python

"""
@package 
@file 
@author Edward Hunter
@brief 
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import gevent
from gevent import monkey
monkey.patch_all()

from random import randint
import time
import zmq


class R3PCSocketError(Exception): pass

class R3PCServer(object):
    """
    """
    def __init__(self, callback=None, host='*', port=7979):
        """
        """
        self._context = zmq.Context(1)
        self._host = host
        self._port = port
        self._endpoint = 'tcp://%s:%i' % (host, port)
        self._greenlet = None
        self._server = None
        self._callback = callback or self._default_callback
        
    def start(self):
        """
        """

        def server_loop():
            """
            """
            print 'Server greenlet spawned'
            
            self._start_sock()
            
            while True:
                print 'Awaiting request from client'
                request = self._server.recv()
                resp = self._callback(request)
                self._server.send(resp)
            
        print 'Spawning server greenlet'
        self._greenlet = gevent.spawn(server_loop)

    def _start_sock(self):
        """
        """
        print 'Constructing zmq rep server socket'
        self._server = self._context.socket(zmq.REP)
        print 'Binding server socket to endpoint'
        self._server.bind(self._endpoint)
    
    def _stop_sock(self):
        """
        """
        print 'Closing server socket'
        self._server.close()
        self._server = None

    def stop(self):
        """
        """
        self._greenlet.kill()
        self._greenlet.join()
        self._greenlet = None
        self._stop_sock()
    
    def done(self):
        """
        """
        self._context.term()
        self._context = None

    def _default_callback(self, msg):
        """
        """
        print 'Server received msg: %s' % msg
        return 'OK'

class R3PCClient(object):
    """
    """
    def __init__(self, host='localhost', port=7979):
        """
        """
        self._context = zmq.Context(1)
        self._host = host
        self._port = port
        self._endpoint = 'tcp://%s:%i' % (host, port)
        self._greenlet = None
        self._queue = []
        self._poller = zmq.Poller()
        self._retries = 3
        self._timeout = 2500
        self._sleep = 1
        self._client = None


    def start(self):
        """
        """


        def client_loop():
            """
            """
            print 'Client greenlet spawned'
            self._start_sock()
            no_retries = 0

            while True:            
                try:
                    msg = self._queue[0]
                    print 'Client sending msg: %s' % msg
                    self._client.send(msg)
                    print 'Client polling for reply'
                    socks = dict(self._poller.poll(self._timeout))
                    if socks.get(self._client) == zmq.POLLIN:
                        reply = self._client.recv()
                        if reply == 'OK':
                            # Reply success, pop msg and proceed.
                            print 'Client got OK from server'
                            self._queue.pop(0)
                            
                        else:
                            # Malformed reply. Log error and retry if possible.
                            print 'Malformed response from server: %s' % \
                                str(reply)
                            raise R3PCSocketError
                    else:
                        # Timeout.
                        print 'Client timeout waiting for reply'
                        raise R3PCSocketError
                    
                except IndexError:
                    # There was nothing in the send queue, sleep briefly.
                    print 'Client queue empty, sleeping'
                    gevent.sleep(self._sleep)
    
                except R3PCSocketError, ZMQError:
                    # Timeout, bad reply, zmqerror: retry if allowed.
                    self._stop_sock()
                    no_retries += 1
                    if no_retries > self._retries:
                        # Close socket and exit greenlet.
                        break
                    self._start_sock()
        
        self._greenlet = gevent.spawn(client_loop)
    
    def _start_sock(self):
        """
        """
        print 'Constructing zmq req client'
        self._client = self._context.socket(zmq.REQ)
        print 'Connecting client to endpoint %s' % self._endpoint
        self._client.connect(self._endpoint)
        print 'Registering client with poller'
        self._poller.register(self._client, zmq.POLLIN)
    
    def _stop_sock(self):
        """
        """
        print 'Closing client'
        self._client.setsockopt(zmq.LINGER, 0)
        self._client.close()
        print 'Unregistering client with poller'
        self._poller.unregister(self._client)
        self._client = None
    
    def stop(self):
        """
        """
        self._greenlet.kill()
        self._greenlet.join()
        self._greenlet = None
        self._stop_sock()
    
    def enqueue(self, msg):
        """
        """
        self._queue.append(msg)
    
    def done(self):
        """
        """
        self._context.term()
        self._conext = None


def test_server(host='*', port=7979):
    """
    """
    server = R3PCServer(None, host, port)
    server.start()
    while True:
        gevent.sleep(1)

def test_client(host='localhost', port=7979):
    """
    """
    client = R3PCClient(host, port)
    client.start()
    gevent.sleep(1)
    for i in range(10):
        msg = 'Message no. %i' % i
        client.enqueue(msg)
    gevent.sleep(2)
    client.stop()
    gevent.sleep(1)



