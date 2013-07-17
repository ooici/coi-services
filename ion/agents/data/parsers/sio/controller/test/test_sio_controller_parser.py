#!/usr/bin/env python


from nose.plugins.attrib import attr

from ooi.logging import log

from pyon.util.unit_test import PyonTestCase

from ion.agents.data.parsers.sio.controller.parser import SIOControllerPackageParser


DATA_FILE = 'test_data/sio/controller/siopackage_20130525.xlog'


@attr('UNIT', group='eoi')
class TestSIOControllerParser(PyonTestCase):

    def test_read_package(self):
        """ assert can read and parse a file """
        parser = SIOControllerPackageParser(DATA_FILE)  # no error? then we parsed the file into profiles and records!

        records = parser.get_records()

        log.warn("Got %s particles", len(records))
