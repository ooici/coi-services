#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from unittest import TestCase

from ion.services.mi.instrument_connection import SerialInstrumentConnection
from ion.services.mi.drivers.uw_bars.protocol import BarsInstrumentProtocol


class ProtocolTest(TestCase):

    def test_basic(self):
        """
        """

        connection = SerialInstrumentConnection()

        config = {
            'method':'ethernet',
            'device_addr': '10.180.80.173',  # arbitrary for the moment
            'device_port': 9999,  # arbitrary for the moment
            'server_addr': 'localhost',
            'server_port': 8888
        }
        
        protocol = BarsInstrumentProtocol(connection, config)

        protocol.connect()
