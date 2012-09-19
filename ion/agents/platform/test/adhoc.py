#!/usr/bin/env python

"""
@package
@file
@author  Carlos Rueda
@brief   ad hoc stuff while proper mechanisms are incorporated
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from ion.util.parameter_yaml_IO import get_param_dict


def adhoc_stream_definition():
# arbitrarily using L0_conductivity_stream_definition for the platform attributes
    #todo: complete guess if this is right
    sd = get_param_dict('ctd_param_parsed_dict')
    return sd



from ion.agents.instrument.packet_factory_man import create_packet_builder

# some of the attribute names in the simulated network (network.yml)
def adhoc_get_stream_names():
    return ['fooA', 'bazA', 'fooA1', 'bazA1b2']

# NOTE: the following actually follows the similar method in driver_process.py
# but we are not using driver processes here, so just adapt that method.
def adhoc_get_packet_factories(stream_names, stream_info):
    """
    Construct packet factories for the given stream names
    and the given stream_info dict.

    @param stream_names
    @param stream_info

    @retval a dict indexed by stream name of the packet factories defined.
    """

    log.trace("stream_names=%s; stream_info=%s" % (stream_names, stream_info))

    packet_factories = {}
    for name in stream_names:
        if not name in stream_info:
            log.error("Name '%s' not found in stream_info" % name)
            continue

        stream_config = stream_info[name]
        try:
            packet_builder = create_packet_builder(name, stream_config)
            packet_factories[name] = packet_builder
            log.debug('created packet builder for stream %s' % name)
        except Exception, e:
            log.error('error creating packet builder: %s' % e)

    return packet_factories
