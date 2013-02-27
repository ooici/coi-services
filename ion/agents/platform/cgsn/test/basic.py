#!/usr/bin/env python

"""
@package 
@file 
@author Carlos Rueda
@brief
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from socket import *


def client():
    server_address = ('localhost', 2221)
    server_address = ('128.128.24.43', 2221)
    client_socket = socket(AF_INET, SOCK_DGRAM)

    cmd = "DCL_STATUS"
    data = "31,19,112,%i,%s" % (len(cmd), cmd)
    print "Sending %r" % data
    nobytes = client_socket.sendto(data, server_address)
    print "Sent %i bytes" % nobytes

    recv_data = client_socket.recv(1024)
    print "\t", "Received: ", recv_data


if "__main__" == __name__:   # pragma: no cover
    client()
