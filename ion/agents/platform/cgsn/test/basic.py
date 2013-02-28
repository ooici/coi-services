#!/usr/bin/env python

"""
@brief  Basic test of messaging between my laptop (within MBARI network) and
        the 7370 box at WHOI set up my Michael Eder. With the CG services
        enpoint running, execute this test:
            bin/python ion/agents/platform/cgsn/test/basic.py
@author Carlos Rueda
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from socket import *


def client():
    # this is the CG service endpoint at WHOI
    server_address = ('128.128.24.43', 2221)
    server_socket = socket(AF_INET, SOCK_DGRAM)

    # this is my laptop within the MBARI network
    client_address = ('134.89.13.60', 10011)
    client_socket = socket(AF_INET, SOCK_DGRAM)
    client_socket.bind(client_address)

    #
    # simple test is to send a command to the CG services endpoint ...
    #
    cmd = "DCL_STATUS"
    data = "31,19,112,%i,%s" % (len(cmd), cmd)
    print "Sending %r" % data
    nobytes = server_socket.sendto(data, server_address)
    print "Sent %i bytes" % nobytes

    #
    # ... and get the corresponding response:
    #
    recv_data = client_socket.recv(1024)
    print "\t", "Received: ", recv_data


if "__main__" == __name__:   # pragma: no cover
    client()
