#!/usr/bin/env python

"""
@package ion.agents.platform.cgsn.simulator.cgsn_simulator
@file    ion/agents/platform/cgsn/simulator/cgsn_simulator.py
@author  Carlos Rueda
@brief   CGSN simulator - intended to be run outside of pyon.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from socket import *
import select
import re

from ion.agents.platform.cgsn.defs import CICGINT


class CgsnSimulator(object):
    """
    CGSN simulator
    """

    def __init__(self, address):
        """
        @param address   (host, port) of CG services endpoint
        """
        self._address = address
        self._ss = socket(AF_INET, SOCK_DGRAM)
        self._ss.bind(address)
        self._running = False

    def serve_forever(self):
        self._running = True
        while self._running:
            # some timeout to regularly check for shutdown call
            rlist, wlist, elist = select.select([self._ss], [], [], 0.5)
            if rlist:
                recv_data, addr = self._ss.recvfrom(1024)
                print("Received: %r from addr %s" % (recv_data, str(addr)))
                response = self._handle_request(recv_data)
                if response:
                    print("Replying: %r" % response)
                    self._ss.sendto(response, addr)

    def shutdown(self):
        self._running = False

    def _handle_request(self, recv_data):
        response = None

        toks = recv_data.split(',')

        print("toks = %s" % toks)

        (dst, src, nnn, lng) = tuple(int(toks[i]) for i in range(4))
        msg = toks[4]

        m = re.match(r"(.*)(\s+(.*))?", msg)
        if m:
            cmd = m.group(1)
            rmsg = "ACK %s" % cmd
            response = "%i,%i,%i,%i,%s" % (src,
                                           dst,
                                           CICGINT,
                                           len(rmsg),
                                           rmsg)
        else:
            # TODO handle remaining cases
            print "NOT responding anything to request: %r" % recv_data

        return response
