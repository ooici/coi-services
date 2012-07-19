#!/usr/bin/env python

"""
@package ion.agents.instrument.packet_factory_man
@file    ion/agents/instrument/packet_factory_man.py
@author  Carlos Rueda
@brief   Manager of packet factories according to given stream IDs.
         The main responsibility of this module is to provide the granule
         builder for a given stream.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.util.log import log

from ion.agents.instrument.packet_factory import PacketFactory, PacketFactoryType


def _create_builder(taxonomy, data_producer_id, packet_factory_type):
    """
    Creates a builder function corresponding to the given parameters.
    """

    packet_factory = PacketFactory.get_packet_factory(packet_factory_type)
    
    def builder(**sample_data):
        granule = packet_factory.build_packet(data_producer_id=data_producer_id,
                                              taxonomy=taxonomy,
                                              data=sample_data)

        log.debug("Granule created %s" % granule)
        return granule

    return builder


# _cache: A cache of created builders indexed by the stream ID.
# NOTE this assumes that the stream identified by its ID will always be
# associated with exactly the same configuration (taxonomy, etc.).
_cache = {}


def create_packet_builder(stream_name, stream_config):
    """
    Gets the granule builder for the given stream.

    @param stream_name Stream name. Only used for logging.
    @param stream_config stream configuration

    @retval a function b to be invoked as b.build(**value), which at the moment
            simply follows the way it is used in InstrumentAgent.evt_recv.
    """

    assert isinstance(stream_config, dict)
    assert 'id' in stream_config
    assert 'taxonomy' in stream_config
#    assert 'data_producer_id' in stream_config

    stream_id = stream_config['id']
    if stream_id in _cache:
        log.debug("packet builder for stream_name=%s (stream_id=%s) found in cache" %
                  (stream_name, stream_id))
        return _cache[stream_id]

    taxonomy = stream_config['taxonomy']

#    data_producer_id = stream_config['data_producer_id']
    # TODO determine correct data_producer_id; for now using stream_id
    data_producer_id = stream_id

    # TODO get packet_factory_type in an appropriate wayl for now, hard-coded.
    packet_factory_type = PacketFactoryType.R2LCAFormat

    builder = _create_builder(taxonomy, data_producer_id, packet_factory_type)
    _cache[stream_id] = builder

    log.debug("packet builder created for stream_name=%s (stream_id=%s)" %
              (stream_name, stream_id))

    return builder
