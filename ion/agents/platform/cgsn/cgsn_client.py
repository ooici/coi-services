#!/usr/bin/env python

"""
@package ion.agents.platform.cgsn.cgsn_client
@file    ion/agents/platform/cgsn/cgsn_client.py
@author  Carlos Rueda
@brief   CgsnClient
         See https://confluence.oceanobservatories.org/display/CICG/CI-CGSN+Interface+Development+Coordination
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging

from gevent.socket import *
from gevent import Greenlet, select

from ion.agents.platform.cgsn.defs import MessageIds, CIPOP, CICGINT, EOL


class CgsnClient(object):
    """
    The lower-level class dealing with the UDP connection to the CGSN
    services endpoint.
    """

    def __init__(self, address):
        """
        @param address   (host, port) of CG services endpoint
        """
        self._address = address
        self._sock = socket(AF_INET, SOCK_DGRAM)
        self._recv = _Recv(self._sock)
        log.debug("CgsnClient created.")

    def set_listener(self, listener):
        """
        Sets the listener for reception of complete lines from the endpoint.

        @param listener  listener(line) will be called for each line received
                         from the endpoint. The end-of-line (\n) is *not*
                         included in the line argument.
        """
        self._recv.set_listener(listener)

    def start(self):
        self._recv.start()

    def end_reception(self):
        self._recv.end()

    def _sendto(self, data):
        if log.isEnabledFor(logging.DEBUG):
            log.debug("calling sendto(%r)" % data)
        nobytes = self._sock.sendto(data, self._address)
        if log.isEnabledFor(logging.TRACE):
            log.trace("sendto returned: %i" % nobytes)
        return nobytes

    def send_command(self, dst, cmd):
        """
        """
        data = "%i,%i,%i,%i,%s%s" % (dst,
                                     CIPOP,
                                     CICGINT,
                                     len(cmd),
                                     cmd,
                                     EOL)
        self._sendto(data)


class _Recv(Greenlet):
    """
    Receives and handles messages from the CG services endpoint.
    """

    def __init__(self, sock):
        Greenlet.__init__(self)
        self._sock = sock
        self._listener = self._dummy_listener
        self._line = ''
        self._running = False

    def _dummy_listener(self, line):
        log.debug("DUMMY LISTENER: %r" % line)

    def set_listener(self, listener):
        self._listener = listener

    def run(self):
        if self._listener == self._dummy_listener:
            log.warn("No listener provided. Using a dummy one")

        if log.isEnabledFor(logging.DEBUG):
            log.debug("_Recv running")

        self._running = True
        while self._running:
            # some timeout to regularly check for end call
            rlist, wlist, elist = select.select([self._sock], [], [], 0.5)
            if rlist:
                recv_data = self._sock.recv(1024)
                self._handle_recv_data(recv_data)

        if log.isEnabledFor(logging.DEBUG):
            log.debug("_Recv.run done.")

    def end(self):
        self._running = False

    def _handle_recv_data(self, recv_data):
        if log.isEnabledFor(logging.TRACE):
            log.trace("_handle_recv_data: recv_data=%r" % recv_data)

        for c in recv_data:
            if c == '\n':
                self._handle_new_line(self._line)
                self._line = ''
            else:
                self._line += c

    def _handle_new_line(self, line):
        self._listener(line)