#!/usr/bin/env python

"""
@package ion.agents.instrument.packet_factories
@file    ion/agents/instrument/packet_factories.py
@author  Carlos Rueda
@brief   Manager of packet factories according to given stream IDs.
         The main responsibility of this module is to provide the granule
         builder for a given stream.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.util.log import log

from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule

from ion.agents.instrument.packet_factory import PacketFactory, PacketFactoryType


# the following builders are hard-code here.
# TODO read in some of the parameters from some external resource

def _builder_for_ctd_parsed(data_producer_id, packet_factory_type):
    
    # taxonomy:
    tx = TaxyTool()
    tx.add_taxonomy_set('temp','long name for temp', 't')
    tx.add_taxonomy_set('cond','long name for cond', 'c')
    tx.add_taxonomy_set('pres','long name for pres', 'd', 'p')
    tx.add_taxonomy_set('lat','long name for latitude', 'lt')
    tx.add_taxonomy_set('lon','long name for longitude', 'ln')
    tx.add_taxonomy_set('time','long name for time', 'tm')
    tx.add_taxonomy_set('height','long name for height', 'h')

    # This is an example of using groups it is not a normative statement about how to use groups
    tx.add_taxonomy_set('coordinates','This group contains coordinates')
    tx.add_taxonomy_set('data','This group contains data')

    # packet factory:
    packet_factory = PacketFactory.get_packet_factory(packet_factory_type)
    
    def builder(**sample_data):
        granule = packet_factory.build_packet(data_producer_id=data_producer_id,
                                              taxonomy=tx,
                                              data=sample_data)

        log.debug("Granule created %s" % granule)
        return granule

    log.debug("packet builder for ctd_parsed created.")
    return builder


_builders = {
    'ctd_parsed' : _builder_for_ctd_parsed('lca_parsed_granule', PacketFactoryType.R2LCAFormat),

    # TODO other builders here

    }


def create_packet_builder(stream_name):
    """
    Gets the granule builder for the given stream.
    The driving idea here is that all necessary parameters for the builder
    can be retrieved from some appropriate configuration resource,
    by just looking up the given stream ID.

    @param stream_name Stream ID

    @retval a function b to be invoked as b.build(**value), which simply
            follows the way it is used in InstrumentAgent.evt_recv. But all
            of this can be adjusted if needed, of course.
    """

    builder = _builders[stream_name]
    return builder
