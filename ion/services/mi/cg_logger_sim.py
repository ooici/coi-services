#!/usr/bin/env python

import socket
import sys
import logging
import threading
import SocketServer
import time

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

class ThreadedUdpCmdServerHandler(SocketServer.BaseRequestHandler):
    """
    This class works similar to the TCP handler class, except that
    self.request consists of a pair of data and client socket, and since
    there is no connection the client address must be given explicitly
    when sending data back via sendto().
    """

    def handle(self):
        data = self.request[0].strip()
        log.info("cg_logger_sim.CmdServer: {} sent: ".format(self.client_address) + data)

class ThreadedUdpDataServerHandler(SocketServer.BaseRequestHandler):
    """
    This class works similar to the TCP handler class, except that
    self.request consists of a pair of data and client socket, and since
    there is no connection the client address must be given explicitly
    when sending data back via sendto().
    """

    def handle(self):
        data = self.request[0].strip()
        log.info("cg_logger_sim.DataServer: {} sent: ".format(self.client_address) + data)

class ThreadedUdpServer(SocketServer.ThreadingMixIn, SocketServer.UDPServer):
    pass

if __name__ == '__main__':
    log = logging
    log.basicConfig(level=logging.DEBUG)
    
    log.info("cg_logger_sim Starting UDP Cmd server on DLOGP1 port 4001")
    HOST, PORT = "localhost", 4001
    cmd_server = ThreadedUdpServer((HOST, PORT), ThreadedUdpCmdServerHandler)
    cmd_server_thread = threading.Thread(target=cmd_server.serve_forever)
    # Exit the server thread when the main thread terminates
    cmd_server_thread.daemon = True
    cmd_server_thread.start()
    
    log.info("cg_logger_sim Starting UDP Data server on DLOGP1 port 3001")
    HOST, PORT = "localhost", 3001
    data_server = ThreadedUdpServer((HOST, PORT), ThreadedUdpDataServerHandler)
    data_server_thread = threading.Thread(target=data_server.serve_forever)
    # Exit the server thread when the main thread terminates
    data_server_thread.daemon = True
    data_server_thread.start()
    
    log.info('cg_logger_sim UDP Cmd & Data servers started')

    try:
        while True:
            time.sleep(1)
    except:
        log.info('cg_logger_sim stopped')

