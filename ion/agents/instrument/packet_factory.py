#!/usr/bin/env python

"""
@package ion.agents.instrument.packet_factory
@file ion/agents.instrument/packet_factory.py
@author Bill French
@brief Packet factory for generating granules from driver data
"""

__author__ = 'Bill French, Carlos Rueda'
__license__ = 'Apache 2.0'

import numpy

from pyon.util.log import log
from ion.agents.instrument.common import BaseEnum

from ion.agents.instrument.exceptions import PacketFactoryException
from ion.agents.instrument.exceptions import NotImplementedException
from ion.services.dm.utility.granule import RecordDictionaryTool, ParameterDictionary
from pyon.util.arg_check import validate_is_not_none


class PacketFactoryType(BaseEnum):
    """
    What type of packets is the driver sending us?
    """
    R2LCAFormat = 'R2_LCA_FORMAT',
    CommonSampleFormat = 'COMMON_SAMPLE_FORMAT',


class PacketFactory(object):
    """
    Base class for driver process launcher
    """
    @classmethod
    def get_packet_factory(cls, packet_factory_type):
        """
        Factory method to get the correct packet factory object based on the driver data type
        """

        if packet_factory_type == PacketFactoryType.CommonSampleFormat:
            return CommonSamplePacketFactory()

        if packet_factory_type == PacketFactoryType.R2LCAFormat:
            return LCAPacketFactory()

        else:
            raise PacketFactoryException("unknown driver process type: %s" % type)

    def build_packet(self, farg, **kwargs):
        raise NotImplementedException()


class LCAPacketFactory(PacketFactory):
    """
    Packet factory to build granules from sample dictionaries sent from the SBE37 driver at LCA.  This is simply a
    flat dict object with raw and parsed data.
    """
    def build_packet(self, pdict=None, data=None):
        """
        Build and return a granule of data.
        @param taxonomy the taxonomy of the granule
        @data dictionary containing sample data.
        @return granule suitable for publishing
        """
        validate_is_not_none(pdict, 'No Parameter Dictionary specified', PacketFactoryException)
        validate_is_not_none(data, 'No data supplied', PacketFactoryException)


        fields = pdict.keys()

        #
        # TODO in general, how are groups (and the individual values
        # belonging to the groups) to be determined?
        #

        # in this version, expect 'data' and 'coordinates' to be included in
        # the taxonomy -- TODO the idea would be to be more general here?

        ##############################################################
        # NOTE for the moment, using the flat data record dict 'rdt'
        ##############################################################



        rdt = RecordDictionaryTool(param_dictionary=pdict)
        for name, value in data.iteritems():

            if name in fields:
                assert isinstance(value, list)
                val = numpy.array(value)
                rdt[name] = val
            else:
                log.warning('%s was not part of the parameter dictionary')



        return rdt.to_granule()


class CommonSamplePacketFactory(PacketFactory):
    """
    Packet factory to build granules from Common Sample Objects from the driver
    """
    def build_packet(self):
        raise NotImplementedException()

