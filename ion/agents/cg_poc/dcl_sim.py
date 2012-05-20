#!/usr/bin/env python

import socket
import sys
import logging
from ion.services.mi.common import BaseEnum

class CgMsgTypes(BaseEnum):
    ACK               = 0
    NACK              = 1
    PING              = 2
    PING_RET          = 3
    QUIT              = 6
    DLOG_INST_DAQ_ON  = 37
    DLOG_INST_DAQ_OFF = 38
    DLOG_INST_PWR_ON  = 39
    DLOG_INST_PWR_OFF = 40
    INITIALIZE        = 105

if __name__ == '__main__':
    log = logging
    log.basicConfig(level=logging.DEBUG)
    
    log.info("Starting UDP client")
    HOST, PORT = "localhost", 9999
    
    # SOCK_DGRAM is the socket type to use for UDP sockets
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    while True:
        line = raw_input("enter start, stop, init, ping, quit, or end(to quit app) >")
        log.debug("line=" + line)
        if line == 'end':
            exit()
        # there is no connect() call; UDP has no connections.
        # Instead, data is directly sent to the recipient via sendto().
        if line == 'ping':
            msg_type = CgMsgTypes.PING
        else:
            print("unknown cmd " + line)
            continue
        msg = "%d,%d,%d,%d,%s" %(18,
                                 4,
                                 msg_type,
                                 0,
                                 "")
        sock.sendto(msg, (HOST, PORT))
        received = sock.recv(1024)
        
        print "Sent:     {}".format(msg)
        print "Received: {}".format(received)

