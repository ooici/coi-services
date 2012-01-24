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
        BarsInstrumentProtocol(connection)
