#!/usr/bin/env python

"""
@package ion.agents.platform.cgsn.simulator.cgsn_simulator
@file    ion/agents/platform/cgsn/simulator/cgsn_simulator.py
@author  Carlos Rueda
@brief   Very simple CGSN simulator - intended to be run outside of pyon:
         bin/python ion/agents/platform/cgsn/simulator/cgsn_simulator_server.py
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from socket import *
import select
import re

from ion.agents.platform.cgsn.defs import get_cg_client_address, CICGINT, CIPOP
from ion.agents.platform.cgsn.util import basic_message_verification, MalformedMessage


def _get_client_endpoints():
    """
    Gets a map from endpoints to corresponding (IP, port) pairs.
    Only CIPOP handled in this simple simulator.
    When a request is received having src=CIPOP, then the response is sent to
    the corresponding IP/port in this dict.
    """
    _client_endpoints = {CIPOP: get_cg_client_address()}
    print("_client_endpoints: %s" % str(_client_endpoints))
    return _client_endpoints


class CgsnSimulator(object):
    """
    CGSN simulator
    """

    def __init__(self, address):
        """
        @param address   (host, port) for this CG services endpoint
        """
        self._address = address
        self._server_socket = socket(AF_INET, SOCK_DGRAM)
        self._server_socket.bind(address)

        # client sockets:
        self._client_endpoints = _get_client_endpoints()

        self._running = False

    def serve_forever(self):
        self._running = True
        while self._running:
            # some timeout to regularly check for shutdown call
            rlist, wlist, elist = select.select([self._server_socket], [], [], 0.5)
            if rlist:
                recv_message, addr = self._server_socket.recvfrom(1024)
                print("<- Received from %-30s: %r" % (str(addr), recv_message))
                ca_rsp = self._handle_request(recv_message)
                if ca_rsp:
                    client_address, response = ca_rsp
                    self._server_socket.sendto(response, client_address)
                    print("-> Replied  to   %-30s: %r" % (str(client_address), response))
                    print

    def shutdown(self):
        self._running = False

    def _handle_request(self, recv_message):

        try:
            dst, src, msg_type, msg = self._parse_message(recv_message)
        except MalformedMessage as e:
            print("MalformedMessage: %s", e)
            return None

        if not src in self._client_endpoints:
            print "Unrecognized source in request: %r" % src
            return None

        client_address = self._client_endpoints[src]

        #
        # TODO more real implementation; this just ACKs whatever it comes
        #

        m = re.match(r"(.*)(\s+(.*))?", msg)
        if m:
            cmd = m.group(1)
            rmsg = "ACK %s" % cmd
            response = "%i,%i,%i,%i,%s" % (src,
                                           dst,
                                           CICGINT,
                                           len(rmsg),
                                           rmsg)
            return client_address, response

        else:
            # TODO handle remaining cases
            print "NOT responding anything to request: %r" % recv_message
            return None

    def _parse_message(self, recv_message):
        """
        Note that this is pretty symmetrical to the same method in CGSNClient.
        """
        dst, src, msg_type, msg = basic_message_verification(recv_message)

        #
        # source must be CIPOP:
        #
        if src != CIPOP:
            raise MalformedMessage(
                "unexpected source in received message: "
                "%d (expected %d)" % (src, CIPOP))

        #
        # verify message type:
        # TODO how does this exactly work?
        #
        if msg_type != CICGINT:
            MalformedMessage(
                "unexpected msg_type in received message: "
                "%d (expected %d)" % (msg_type, CICGINT))

        #
        # TODO verification of destination.
        # ...

        return dst, src, msg_type, msg
