#!/usr/bin/env python

"""
@package ion.agents.platform.port_agent_packet
@file    ion/agents/platform/port_agent_packet.py
@brief   Copy of PortAgentPacket from mi.core.instrument.port_agent_client as
         of marine-integrations commit 8d40898, with just minimal adjustments.
@see     port_agent_helper.py
"""

__author__ = 'David Everett'
__license__ = 'Apache 2.0'


import time
import datetime
import struct
import ctypes

from pyon.public import log

HEADER_SIZE = 16 # BBBBHHLL = 1 + 1 + 1 + 1 + 2 + 2 + 4 + 4 = 16


OFFSET_P_CHECKSUM_LOW = 6
OFFSET_P_CHECKSUM_HIGH = 7

"""
Offsets into the unpacked header fields
"""
SYNC_BYTE1_INDEX = 0
SYNC_BYTE1_INDEX = 1
SYNC_BYTE1_INDEX = 2
TYPE_INDEX = 3
LENGTH_INDEX = 4 # packet size (including header)
CHECKSUM_INDEX = 5
TIMESTAMP_UPPER_INDEX = 6
TIMESTAMP_LOWER_INDEX = 6

SYSTEM_EPOCH = datetime.date(*time.gmtime(0)[0:3])
NTP_EPOCH = datetime.date(1900, 1, 1)
NTP_DELTA = (SYSTEM_EPOCH - NTP_EPOCH).days * 24 * 3600


class PortAgentPacket():
    """
    An object that encapsulates the details packets that are sent to and
    received from the port agent.
    https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+Port+Agent+Design
    """

    """
    Port Agent Packet Types
    """
    DATA_FROM_INSTRUMENT = 1
    DATA_FROM_DRIVER = 2
    PORT_AGENT_COMMAND = 3
    PORT_AGENT_STATUS = 4
    PORT_AGENT_FAULT = 5
    INSTRUMENT_COMMAND = 6
    HEARTBEAT = 7

    def __init__(self, packetType = None):
        self.__header = None
        self.__data = None
        self.__type = packetType
        self.__length = None
        self.__port_agent_timestamp = None
        self.__recv_checksum  = None
        self.__checksum = None
        self.__isValid = False

    def unpack_header(self, header):
        self.__header = header
        #@TODO may want to switch from big endian to network order '!' instead of '>' note network order is big endian.
        # B = unsigned char size 1 bytes
        # H = unsigned short size 2 bytes
        # L = unsigned long size 4 bytes
        # d = float size8 bytes
        variable_tuple = struct.unpack_from('>BBBBHHII', header)
        # change offset to index.
        self.__type = variable_tuple[TYPE_INDEX]
        self.__length = int(variable_tuple[LENGTH_INDEX]) - HEADER_SIZE
        self.__recv_checksum  = int(variable_tuple[CHECKSUM_INDEX])
        upper = variable_tuple[TIMESTAMP_UPPER_INDEX]
        lower = variable_tuple[TIMESTAMP_LOWER_INDEX]
        self.__port_agent_timestamp = float("%s.%s" % (upper, lower))

    def pack_header(self):
        """
        Given a type and length, pack a header to be sent to the port agent.
        """
        if self.__data == None:
            log.error('pack_header: no data!')
            """
            TODO: throw an exception here?
            """
        else:
            """
            Set the packet type if it was not passed in as parameter
            """
            if self.__type == None:
                self.__type = self.DATA_FROM_DRIVER
            self.__length = len(self.__data)
            self.__port_agent_timestamp = time.time() + NTP_DELTA


            variable_tuple = (0xa3, 0x9d, 0x7a, self.__type,
                              self.__length + HEADER_SIZE, 0x0000,
                              self.__port_agent_timestamp)

            # B = unsigned char size 1 bytes
            # H = unsigned short size 2 bytes
            # L = unsigned long size 4 bytes
            # d = float size 8 bytes
            format = '>BBBBHHd'
            size = struct.calcsize(format)
            temp_header = ctypes.create_string_buffer(size)
            struct.pack_into(format, temp_header, 0, *variable_tuple)
            self.__header = temp_header.raw
            #print "here it is: ", binascii.hexlify(self.__header)

            """
            do the checksum last, since the checksum needs to include the
            populated header fields.
            NOTE: This method is only used for test; messages TO the port_agent
            do not include a header (as I mistakenly believed when I wrote
            this)
            """
            self.__checksum = self.calculate_checksum()
            self.__recv_checksum  = self.__checksum

            """
            This was causing a problem, and since it is not used for our tests,
            commented out; if we need it we'll have to fix
            """
            #self.__header[OFFSET_P_CHECKSUM_HIGH] = self.__checksum & 0x00ff
            #self.__header[OFFSET_P_CHECKSUM_LOW] = (self.__checksum & 0xff00) >> 8


    def attach_data(self, data):
        self.__data = data

    def attach_timestamp(self, timestamp):
        self.__port_agent_timestamp = timestamp

    def calculate_checksum(self):
        checksum = 0
        for i in range(HEADER_SIZE):
            if i < OFFSET_P_CHECKSUM_LOW or i > OFFSET_P_CHECKSUM_HIGH:
                checksum += struct.unpack_from('B', str(self.__header[i]))[0]

        for i in range(self.__length):
            checksum += struct.unpack_from('B', str(self.__data[i]))[0]

        return checksum


    def verify_checksum(self):
        checksum = 0
        for i in range(HEADER_SIZE):
            if i < OFFSET_P_CHECKSUM_LOW or i > OFFSET_P_CHECKSUM_HIGH:
                checksum += struct.unpack_from('B', self.__header[i])[0]

        for i in range(self.__length):
            checksum += struct.unpack_from('B', self.__data[i])[0]

        if checksum == self.__recv_checksum:
            self.__isValid = True
        else:
            self.__isValid = False

        #log.debug('checksum: %i.' %(checksum))

    def get_data_size(self):
        return self.__length

    def get_header(self):
        return self.__header

    def get_data(self):
        return self.__data

    def get_timestamp(self):
        return self.__port_agent_timestamp

    def get_header_length(self):
        return self.__length

    def get_header_type(self):
        return self.__type

    def get_header_checksum(self):
        return self.__checksum

    def get_header_recv_checksum (self):
        return self.__recv_checksum

    def get_as_dict(self):
        """
        Return a dictionary representation of a port agent packet
        """
        return {
            'type': self.__type,
            'length': self.__length,
            'checksum': self.__checksum,
            'raw': self.__data
        }

    def is_valid(self):
        return self.__isValid
