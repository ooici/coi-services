#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from unittest import TestCase

from ion.services.mi.drivers.uwash_bars import BarsInstrumentDriver
from ion.services.mi.drivers.uwash_bars import BarsChannel


class UWashBarsTest(TestCase):

    def setUp(self):
        """
        """

        pass

    def test_basic(self):
        """
        """

        dr = BarsInstrumentDriver()

        #TODO

        pass

class ChannelsTest(TestCase):

    def test_basic(self):
        """
        """

        channels = BarsChannel.list()

        print "channels = %s" % channels

