#!/usr/bin/env python

"""
@package ion.agents.platform.port_agent_helper
@file    ion/agents/platform/port_agent_helper.py
@author  Carlos Rueda
@brief   Support for port agent coordination. Preliminary.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

#
# See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+Port+Agent+Design
#

from pyon.public import log
import logging
from pyon.util.containers import DotDict

from ion.agents.platform.port_agent_packet import PortAgentPacket, HEADER_SIZE

from gevent import socket, select, Greenlet


class PortAgentHelper(object):
    """
    Preliminary sketch for supporting platform-port agent coordination.

    Initially, the connection is only to the cmd_port in the given
    comm_config. All packets received on that connection are logged out to a
    file under logs/ with a name indicating the platform ID.
    No commanding of the port agent included.
    """

    # TODO enable this module once it's more complete
    _disabled = True

    def __init__(self, platform_id, instrument_id, comms_config):
        """
        @param platform_id:
        @param instrument_id:
        @param comms_config:
        @return:
        """
        if self._disabled:
            return

        log.debug("%r/%r: PortAgentHelper called. comms_config=%s",
                  platform_id, instrument_id, comms_config)

        assert 'addr' in comms_config
        assert 'port' in comms_config
        assert 'cmd_port' in comms_config

        self._platform_id   = platform_id
        self._instrument_id = instrument_id
        self._comms_config  = DotDict(comms_config)

        self._outfilename = None
        self._outfile = None
        self._pp = None

    def _start_outfile(self):
        if log.isEnabledFor(logging.DEBUG):
            self._outfilename = "logs/port_agent_client_%s.txt" % self._platform_id
            try:
                self._outfile = file(self._outfilename, 'a')
                self._outfile.write("%r/%r: port agent client connected\n" % (
                    self._platform_id, self._instrument_id))
                self._outfile.flush()

                log.debug("%r/%r: will write packets to %s",
                          self._platform_id, self._instrument_id, self._outfilename)

            except Exception as ex:
                log.warn("%r/%r: could not open output file: %s: %s",
                         self._platform_id, self._instrument_id, self._outfilename, ex)

    def _end_outfile(self):
        if log.isEnabledFor(logging.DEBUG) and self._outfile:
            try:
                self._outfile.write("%r/%r: port agent client disconnected\n\n" % (
                    self._platform_id, self._instrument_id))
                self._outfile.close()

            except Exception as ex:
                log.warn("%r/%r: could not close output file: %s: %s",
                         self._platform_id, self._instrument_id, self._outfilename, ex)
            finally:
                self._outfile = None

    def _connect_socket(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        return sock

    def _handle_packet(self, pkt):
        if log.isEnabledFor(logging.DEBUG):
            self._outfile.write("%r/%r: %s: %s\n" % (
                self._platform_id, self._instrument_id,
                pkt.get_timestamp(), pkt.get_as_dict()))
            self._outfile.flush()

        # TODO do something concrete with received packet (notify platform agent, ...)

    def init_comms(self):
        if self._disabled:
            return

        log.debug("%r/%r: init_comms", self._platform_id, self._instrument_id)

        host     = self._comms_config.addr
        cmd_port = self._comms_config.cmd_port

        self._sock = self._connect_socket(host, cmd_port)

        self._start_outfile()

        self._receiver = _Receiver(self._platform_id, self._instrument_id,
                                   self._sock, self._handle_packet)
        self._receiver.start()
        log.info("init_comms(): started receiver")

    def stop_comms(self):
        if self._disabled:
            return

        log.debug("%r/%r: stop_comms", self._platform_id, self._instrument_id)

        self._receiver.stop()
        self._receiver.join()
        self._sock.close()
        self._sock = None

        self._end_outfile()


class _Receiver(Greenlet):
    """
    Dispatches all received responses from the connection.
    """

    def __init__(self, platform_id, instrument_id, sock, callback):
        """
        @param platform_id
        @param instrument_id
        @param callback       Function to be called with each received packet.
        """
        Greenlet.__init__(self)
        self._platform_id   = platform_id
        self._instrument_id = instrument_id
        self._sock = sock
        self._callback = callback
        self._done = False

    def stop(self):
        """
        Requests the receiver to stop.
        """
        self._done = True

    def _recv(self, bufsize):
        """
        Main read method. Uses select with timeout to detect request for
        termination, and return read buffer.
        """
        recv = ''
        while not self._done and not recv:
            # some timeout to regularly check for _done:
            rlist, wlist, elist = select.select([self._sock], [], [], 0.3)
            if not rlist:
                continue

            try:
                recv = self._sock.recv(bufsize)
            except:
                if not self._done:
                    self._done = True
                    log.exception("%r/%r: error in _sock.recv",
                                  self._platform_id, self._instrument_id)
                return ''

        return recv

    def run(self):
        """
        Adapted from Listener.run in the module
        mi.core.instrument.port_agent_client (MI repo commit 8d40898).
        """
        log.debug("%r/%r: _Receiver running", self._platform_id, self._instrument_id)

        while not self._done:
            received_header = False
            bytes_left = HEADER_SIZE
            while not received_header and not self._done:
                header = self._recv(bytes_left)
                bytes_left -= len(header)
                if bytes_left == 0:
                    received_header = True
                    paPacket = PortAgentPacket()
                    paPacket.unpack_header(header)
                    data_size = paPacket.get_data_size()
                    bytes_left = data_size
                elif len(header) == 0:
                    if not self._done:
                        errorString = 'Zero bytes received from port_agent socket'
                        log.error("%r/%r: %s", self._platform_id, self._instrument_id, errorString)
                        #self.callback_error(errorString)
                        self._done = True

            received_data = False
            while not received_data and not self._done:
                data = self._recv(bytes_left)
                bytes_left -= len(data)
                if bytes_left == 0:
                    received_data = True
                    paPacket.attach_data(data)
                elif len(data) == 0:
                    if not self._done:
                        errorString = 'Zero bytes received from port_agent socket'
                        log.error("%r/%r: %s", self._platform_id, self._instrument_id, errorString)
                        #self.callback_error(errorString)
                        self._done = True

            if not self._done:
                self._callback(paPacket)

        log.debug("%r/%r: _Receiver loop exited", self._platform_id, self._instrument_id)
