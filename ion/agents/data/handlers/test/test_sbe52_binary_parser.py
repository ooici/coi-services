#!/usr/bin/env python

"""
@package ion.agents.data.handlers.test.test_netcdf_data_handler
@file ion/agents/data/handlers/test/test_netcdf_data_handler
@author Christopher Mueller
@brief Test cases for netcdf_data_handler
"""

from ooi.logging import log
from ion.agents.data.handlers.sbe52_binary_handler import SBE52BinaryCTDParser
from nose.plugins.attrib import attr
from pyon.util.unit_test import PyonTestCase

CTD_FILE='test_data/C0000042.DAT'

@attr('UNIT', group='eoi')
class TestSBE52BinaryParser(PyonTestCase):
    def test_read_CTD(self):
        """ assert can read and parse a file """
        parser = SBE52BinaryCTDParser(CTD_FILE)  # no error? then we parsed the file into profiles and records!
        self.assertEqual(1, len(parser._profiles))
        records = parser.get_records()
        self.assertEqual(513, len(records))

    def test_read_CTD_count(self):
        """ assert can read fixed number of records """
        parser = SBE52BinaryCTDParser(CTD_FILE)  # no error? then we parsed the file into profiles and records!

        records = parser.get_records(5)
        self.assertEqual(5, len(records))

        self.assertTrue(records[0]['time']<records[1]['time'])
        self.assertEquals(records[0]['upload_time'], records[1]['upload_time'])

        # total 513 records; already read 5 above; read 500 more...
        records = parser.get_records(500)
        self.assertEqual(500, len(records))

        # now should only get last 3
        records = parser.get_records(20)
        self.assertEquals(8, len(records))

        # now should get none
        records = parser.get_records(8)
        self.assertTrue(records is None)

    def test_read_CTD(self):
        """ assert can read and parse a file """
        parser = SBE52BinaryCTDParser(CTD_FILE)  # no error? then we parsed the file into profiles and records!
        records = parser.get_records(1)
        record = records[0]

        self.assertAlmostEqual(0, record['oxygen'], delta=0.01)
        self.assertAlmostEqual(309.77, record['pressure'], delta=0.01)
        self.assertAlmostEqual(37.9848, record['conductivity'], delta=0.01)
        self.assertAlmostEqual(9.5163, record['temperature'], delta=0.01)
        self.assertAlmostEqual(1500353102, record['time'], delta=0.01)
