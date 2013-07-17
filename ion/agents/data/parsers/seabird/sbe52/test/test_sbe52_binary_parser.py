#!/usr/bin/env python


from nose.plugins.attrib import attr

from pyon.util.unit_test import PyonTestCase

from ion.agents.data.parsers.seabird.sbe52.binary_parser import SBE52BinaryCTDParser


CTD_FILE = 'test_data/seabird/sbe52/C0000042.DAT'


@attr('UNIT', group='eoi')
class TestSBE52BinaryParser(PyonTestCase):

    def test_read_CTD_simple(self):
        """ assert can read and parse a file """
        parser = SBE52BinaryCTDParser(CTD_FILE)  # no error? then we parsed the file into profiles and records!
        self.assertEqual(1, len(parser._profiles))
        records = parser.get_records()
        self.assertEqual(513, len(records))

    def _get_particle_value(self, values, value_id):
        if "values" in values:
            values = values["values"]
        for v in values:
            if v["value_id"] == value_id:
                return v["value"]
        raise Exception("Value ID %s not found in particle" % value_id)

    def test_read_CTD_count(self):
        """ assert can read fixed number of records """
        parser = SBE52BinaryCTDParser(CTD_FILE)  # no error? then we parsed the file into profiles and records!

        records = parser.get_records(5)
        self.assertEqual(5, len(records))

        self.assertTrue(records[0]['driver_timestamp']<records[1]['driver_timestamp'])
        self.assertEquals(self._get_particle_value(records[0],'upload_time'), self._get_particle_value(records[1],'upload_time'))

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
        particle = records[0]
        record = particle["values"]

        self.assertAlmostEqual(0, self._get_particle_value(record, 'oxygen'), delta=0.01)
        self.assertAlmostEqual(309.77, self._get_particle_value(record, 'pressure'), delta=0.01)
        self.assertAlmostEqual(37.9848, self._get_particle_value(record, 'conductivity'), delta=0.01)
        self.assertAlmostEqual(9.5163, self._get_particle_value(record, 'temp'), delta=0.01)
        self.assertAlmostEqual(3527207897, particle["driver_timestamp"], delta=0.01)
