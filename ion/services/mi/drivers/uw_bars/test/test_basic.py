#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from unittest import TestCase

from ion.services.mi.drivers.uw_bars.common import BarsChannel


class ChannelsTest(TestCase):

    def test_basic(self):
        """
        """

        channels = BarsChannel.list()

        print "channels = %s" % channels
