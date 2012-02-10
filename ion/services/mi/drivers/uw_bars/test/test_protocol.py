#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'


from ion.services.mi.drivers.uw_bars.test import BarsTestCase
from ion.services.mi.drivers.uw_bars.protocol import BarsInstrumentProtocol
from ion.services.mi.drivers.uw_bars.protocol import BarsProtocolState

import time

from nose.plugins.attrib import attr
from unittest import skip


@skip('not yet easy to test protocol in isolation')
@attr('UNIT', group='mi')
class ProtocolTest(BarsTestCase):

    def test(self):
        """
        BARS protocol tests
        """

        protocol = BarsInstrumentProtocol()
        self.assertEqual(BarsProtocolState.PRE_INIT,
                         protocol.get_current_state())

        protocol.initialize()
        protocol.configure(self.config)
        protocol.connect()

        self.assertEqual(BarsProtocolState.COLLECTING_DATA,
                         protocol.get_current_state())

        print "sleeping for a bit"
        time.sleep(5)

        print "disconnecting"
        protocol.disconnect()
