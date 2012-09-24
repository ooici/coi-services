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

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.taxonomy import TaxyTool, Taxonomy
from ion.services.dm.utility.granule.granule import build_granule


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
            raise PacketFactoryExeption("unknown driver process type: %s" % type)

    def build_packet(self, farg, **kwargs):
        raise NotImplementedException()

    def _get_taxy_tool(self, taxonomy):
        """
        helper to get the TaxyTool version of the given argument.

        @param taxonomy Either a TaxyTool, a Taxonomy, or some value that can
               be processed by TaxyTool.load to create the TaxyTool instance.
        """

        if isinstance(taxonomy, TaxyTool):
            tx = taxonomy
        elif isinstance(taxonomy, Taxonomy):
            tx = TaxyTool(taxonomy)
        else:
            tx = TaxyTool.load(taxonomy)

        return tx

    def _get_nick_names_from_taxonomy(self, taxonomy):

        # NOTE this operation should probably be provided by the
        # taxonomy object itself.

        return [v[0] for v in taxonomy._t.map.itervalues()]


class LCAPacketFactory(PacketFactory):
    """
    Packet factory to build granules from sample dictionaries sent from the SBE37 driver at LCA.  This is simply a
    flat dict object with raw and parsed data.
    """
    def build_packet(self, *args, **kwargs):
        """
        Build and return a granule of data.
        @param taxonomy the taxonomy of the granule
        @data dictionary containing sample data.
        @return granule suitable for publishing
        """
        taxonomy_str = kwargs.get('taxonomy')
        data = kwargs.get('data')
        data_producer_id = kwargs.get('data_producer_id')

        if not data_producer_id:
            raise PacketFactoryException("data_producer_id parameter missing")

        if not taxonomy_str:
            raise PacketFactoryException("taxonomy parameter missing")

        if not data:
            raise PacketFactoryException("data parameter missing")

        taxonomy = self._get_taxy_tool(taxonomy_str)

        # the nick_names in the taxonomy:
        nick_names = self._get_nick_names_from_taxonomy(taxonomy)

        #
        # TODO in general, how are groups (and the individual values
        # belonging to the groups) to be determined?
        #

        # in this version, expect 'data' and 'coordinates' to be included in
        # the taxonomy -- TODO the idea would be to be more general here?

        ##############################################################
        # NOTE for the moment, using the flat data record dict 'rdt'
        ##############################################################

#        if not 'data' in nick_names:
#            raise PacketFactoryException("expected name 'data' in taxonomy")
#        if not 'coordinates' in nick_names:
#            raise PacketFactoryException("expected name 'coordinates' in taxonomy")


        rdt = RecordDictionaryTool(taxonomy=taxonomy)
#        data_rdt = RecordDictionaryTool(taxonomy=taxonomy)
#        coordinates_rdt = RecordDictionaryTool(taxonomy=taxonomy)
#
#        rdt['data'] = data_rdt
#        rdt['coordinates'] = coordinates_rdt

#        def is_coordinate(nick_name):
#            # just an ad hoc check to determine which group the nick_names
#            # belong to
#            return nick_name in ['lat', 'lon', 'time', 'height']


        # now, assign the values to the corresp record dicts:
        for name, value in data.iteritems():
            handle = -1
            log.info("packetfactory: name: %s" % str(name))
            if name in nick_names:
                handle = taxonomy.get_handle(name)
                log.info("packetfactory: handle: %s" % str(handle))
            else:
                handles = taxonomy.get_handles(name)
                log.info("packetfactory: handles: %s" % str(handles))
                if len(handles) == 1:
                    handle = handles.pop()
                elif len(handles) > 1:
                    # TODO proper handling of this case
                    log.warn("Multiple handles found for '%s': %s" % (name %
                                                                 handles))

            if handle >= 0:
                # ok, the nick_name has been found, either directly as a
                # nick_name or via an alias; set value (using nick_name):
                nick_name = taxonomy.get_nick_name(handle)

                assert isinstance(value, list)
                val = numpy.array(value)

                # NOTE for the moment, using the flat data record dict 'rdt':
                rdt[nick_name] = val
#                if is_coordinate(nick_name):
#                    coordinates_rdt[nick_name] = val
#                else:
#                    data_rdt[nick_name] = val

            else:
                # name not found.
                # In the current tests this is happening with 'stream_id'
                log.warning("No handle found for '%s'" % name)

        log.debug("dictionary created: %s" % rdt.pretty_print())

        return build_granule(data_producer_id=data_producer_id, taxonomy=taxonomy, record_dictionary=rdt)


class CommonSamplePacketFactory(PacketFactory):
    """
    Packet factory to build granules from Common Sample Objects from the driver
    """
    def build_packet(self):
        raise NotImplementedException()

