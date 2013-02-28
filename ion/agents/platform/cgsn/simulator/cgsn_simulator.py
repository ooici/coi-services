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
                recv_data, addr = self._server_socket.recvfrom(1024)
                print("Received: %r from addr %s" % (recv_data, str(addr)))
                ca_rsp = self._handle_request(recv_data)
                if ca_rsp:
                    client_address, response = ca_rsp
                    print("Replying: %r to %s" % (response, str(client_address)))
                    self._server_socket.sendto(response, client_address)

    def shutdown(self):
        self._running = False

    def _handle_request(self, recv_data):

        toks = recv_data.split(',')

        print("toks = %s" % toks)

        (dst, src, nnn, lng) = tuple(int(toks[i]) for i in range(4))
        msg = toks[4]

        if not src in self._client_endpoints:
            print "Unrecognized source in request: %r" % src
            return None

        client_address = self._client_endpoints[src]

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
            print "NOT responding anything to request: %r" % recv_data
            return None
