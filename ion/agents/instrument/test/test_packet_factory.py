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

from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule

from ion.agents.instrument.packet_factory import PacketFactory, PacketFactoryType, LCAPacketFactory

from pyon.util.unit_test import PyonTestCase


@attr('UNIT', group='mi')
class TestPacketFactory(PyonTestCase):
    def setUp(self):
        """
        Setup test cases.
        """
        parsed_tx = TaxyTool()
        parsed_tx.add_taxonomy_set('temp','long name for temp', 't')
        parsed_tx.add_taxonomy_set('cond','long name for cond', 'c')
        parsed_tx.add_taxonomy_set('pres','long name for pres', 'd')
        parsed_tx.add_taxonomy_set('lat','long name for latitude', 'lt')
        parsed_tx.add_taxonomy_set('lon','long name for longitude', 'ln')
        parsed_tx.add_taxonomy_set('time','long name for time', 'tm')

        # This is an example of using groups it is not a normative statement about how to use groups
        parsed_tx.add_taxonomy_set('coordinates','This group contains coordinates...')
        parsed_tx.add_taxonomy_set('data','This group contains data...')

        self._parsed_taxonomy = parsed_tx

        raw_tx = TaxyTool()
        raw_tx.add_taxonomy_set('raw_fixed','Fixed length bytes in an array of records')
        raw_tx.add_taxonomy_set('raw_blob','Unlimited length bytes in an array')

        self._raw_taxonomy = raw_tx

        log.debug("_parsed_taxonomy = %s" % self._parsed_taxonomy.dump())
        log.debug("_raw_taxonomy = %s" % self._raw_taxonomy.dump())

        self.packet_factory = PacketFactory.get_packet_factory( PacketFactoryType.R2LCAFormat)
        self.assertIsInstance(self.packet_factory, LCAPacketFactory)


class TestLCAPacketFactory(TestPacketFactory):

    def test_basic(self):
        tax = self._parsed_taxonomy
        self.assertEqual(0, tax.get_handle('temp'))
        self.assertEqual(1, tax.get_handle('cond'))
        self.assertEqual(set([1]), tax.get_handles('cond'))
        self.assertEqual(set([1]), tax.get_handles('c'))

    def test_build_granule(self):
        """
        Test build granule with real names, not aliases
        """
        # use the nick names
        sample_data = {
            'cond': [10],
            'temp': [10],
            'pres': [10],
            'lat': [10.112],
            'lon': [12.122],
            'time': [123122122],
            'height': [33]
        }

        tax = self._parsed_taxonomy
        granule = self.packet_factory.build_packet(data_producer_id='lca_parsed_granule', taxonomy=tax, data=sample_data)
        rd = granule.record_dictionary
        #todo: removed nested record dictionaries in packet_factory temporarily
#        data = rd[tax.get_handle('data')]
#        coordinates = rd[tax.get_handle('coordinates')]

#        print "granule: %s" % granule
#        print "granule.record_dictionary: %s" % rd
#        print "data: %s" % data
#        print "coordinates: %s" % coordinates

        self.assertEqual([10], rd[tax.get_handle('cond')])
        self.assertEqual([10], rd[tax.get_handle('temp')])
        self.assertEqual([10], rd[tax.get_handle('pres')])

        self.assertEqual([10.112], rd[tax.get_handle('lat')])
        self.assertEqual([12.122], rd[tax.get_handle('lon')])
        self.assertEqual([123122122], rd[tax.get_handle('time')])

    def test_build_granule_with_aliases(self):
        """
        Test build granule with aliases
        """

        # use aliases
        sample_data = {
            'c': [10],
            't': [10],
            'd': [10],
            'lt': [10.112],
            'ln': [12.122],
            'tm': [123122122],
            'h': [33]
        }

        tax = self._parsed_taxonomy
        granule = self.packet_factory.build_packet(data_producer_id='lca_parsed_granule', taxonomy=tax, data=sample_data)
        rd = granule.record_dictionary
        #todo: removed nested record dictionaries in packet_factory temporarily
#        data = rd[tax.get_handle('data')]
#        coordinates = rd[tax.get_handle('coordinates')]

#        print "granule: %s" % granule
#        print "granule.record_dictionary: %s" % rd
#        print "data: %s" % data
#        print "coordinates: %s" % coordinates

        self.assertEqual([sample_data['c']], rd[tax.get_handle('cond')])
        self.assertEqual([sample_data['t']], rd[tax.get_handle('temp')])
        self.assertEqual([sample_data['d']], rd[tax.get_handle('pres')])

        self.assertEqual([sample_data['lt']], rd[tax.get_handle('lat')])
        self.assertEqual([sample_data['ln']], rd[tax.get_handle('lon')])
        self.assertEqual([sample_data['tm']], rd[tax.get_handle('time')])
