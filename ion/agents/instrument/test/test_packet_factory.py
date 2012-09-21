#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_packet_factory
@file ion/agents.instrument/test_packet_factory.py
@author Bill French
@brief Test cases for generating granules from packet factories
"""

__author__ = 'Bill French, Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from nose.plugins.attrib import attr

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool

from ion.agents.instrument.packet_factory import PacketFactory, PacketFactoryType, LCAPacketFactory

from pyon.util.unit_test import PyonTestCase
from ion.util.parameter_yaml_IO import get_param_dict

@attr('UNIT', group='mi')
class TestPacketFactory(PyonTestCase):
    def setUp(self):
        """
        Setup test cases.
        """
        self._parsed_dict = get_param_dict('ctd_parsed_param_dict')
        self._raw_taxonomy = get_param_dict('ctd_raw_param_dict')

        self.packet_factory = PacketFactory.get_packet_factory( PacketFactoryType.R2LCAFormat)
        self.assertIsInstance(self.packet_factory, LCAPacketFactory)


class TestLCAPacketFactory(TestPacketFactory):


    def test_build_granule(self):
        """
        Test build granule with real names, not aliases
        """
        # use the nick names
        sample_data = {
            'conductivity': [10],
            'temp': [10],
            'pressure': [10],
            'lat': [10.112],
            'lon': [12.122],
            'time': [123122122],
            'depth': [33]
        }


        granule = self.packet_factory.build_packet(pdict=self._parsed_dict, data=sample_data)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        self.assertEquals(rdt['time'][0],123122122 )
        self.assertEquals(rdt['temp'][0], 10)
        self.assertEquals(rdt['depth'][0], 33)

