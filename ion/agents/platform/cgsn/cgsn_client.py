#!/usr/bin/env python

"""
@package ion.agents.platform.cgsn.cgsn_client
@file    ion/agents/platform/cgsn/cgsn_client.py
@author  Carlos Rueda
@brief   CGSNClient
         See https://confluence.oceanobservatories.org/display/CICG/CI-CGSN+Interface+Development+Coordination
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from gevent.socket import *
from gevent import Greenlet, select

from ion.agents.platform.cgsn.defs import get_cg_client_address, CIPOP, CICGINT
from ion.agents.platform.cgsn.util import basic_message_verification, MalformedMessage


class CGSNClient(object):
    """
    The lower-level class dealing with the UDP connection to the CGSN
    services endpoint.
    """

    def __init__(self, cg_address):
        """
        @param address   (host, port) of the CG services endpoint
        """
        self._cg_address = cg_address
        self._cg_sock = socket(AF_INET, SOCK_DGRAM)
        self._receiver = _Receiver()
        log.debug("CGSNClient created. cg_address: %s", cg_address)

    def set_listener(self, listener):
        """
        Sets the listener for reception of complete lines from the endpoint.

        @param listener  listener(src, msg_type, msg)
                         will be called for each message received from the endpoint.
        """
        self._receiver.set_listener(listener)

    def start(self):
        self._receiver.start()

    def end_reception(self):
        self._receiver.end()

    def _sendto(self, data):
        log.debug("calling sendto(%r)", data)
        nobytes = self._cg_sock.sendto(data, self._cg_address)
        log.trace("sendto returned: %i", nobytes)
        return nobytes

    def send_command(self, dst, cmd):
        """
        """
        data = "%i,%i,%i,%i,%s" % (dst,
                                   CIPOP,
                                   CICGINT,
                                   len(cmd),
                                   cmd)
        self._sendto(data)


class _Receiver(Greenlet):
    """
    Receives and handles messages from the CG services endpoint.
    """

    def __init__(self):
        Greenlet.__init__(self)

        self._sock = socket(AF_INET, SOCK_DGRAM)
        self._sock.bind(get_cg_client_address())
        self._listener = self._dummy_listener
        self._line = ''
        self._running = False

    def _dummy_listener(self, *args):
        log.debug("DUMMY LISTENER CALLED: %s", args)

    def set_listener(self, listener):
        self._listener = listener
        log.debug("set_listener: %r", self._listener)

    def run(self):
        if self._listener == self._dummy_listener:
            log.warn("No listener provided. Using a dummy one")

        log.debug("_Receiver running")

        self._running = True
        while self._running:
            # some timeout to regularly check for end() call
            rlist, wlist, elist = select.select([self._sock], [], [], 0.5)
            if rlist:
                recv_message = self._sock.recv(1024)
                self._parse_message_and_notify_listener(recv_message)

        self._sock.close()
        log.debug("_Receiver.run done.")

    def end(self):
        self._running = False

    def _parse_message_and_notify_listener(self, recv_message):
        """
        Converts the received payload to call the listener.
        """
        try:
            src, msg_type, msg = self._parse_message(recv_message)
        except MalformedMessage as e:
            log.warn("MalformedMessage: %s", e)
            return

        # OK, notify listener:
        self._listener(src, msg_type, msg)

    def _parse_message(self, recv_message):
        log.trace("_parse_message: recv_message=%r", recv_message)

        dst, src, msg_type, msg = basic_message_verification(recv_message)

        #
        # destination must be us, CIPOP:
        #
        if dst != CIPOP:
            raise MalformedMessage(
                "unexpected destination in received message: "
                "%d (we are %d)" % (dst, CIPOP))

        #
        # verify message type:
        # TODO how does this exactly work?
        #
        if msg_type != CICGINT:
            MalformedMessage(
                "unexpected msg_type in received message: "
                "%d (should be %d)" % (msg_type, CICGINT))

        #
        # TODO verification of source.
        # ...

        return src, msg_type, msg
