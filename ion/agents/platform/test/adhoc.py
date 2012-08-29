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


from prototype.sci_data.stream_defs import L0_conductivity_stream_definition

def adhoc_stream_definition():
# arbitrarily using L0_conductivity_stream_definition for the platform attributes
    sd = L0_conductivity_stream_definition()
    sd.stream_resource_id = ''
    return sd



from ion.services.dm.utility.granule.taxonomy import TaxyTool

def adhoc_get_taxonomy(stream_name):
    """
    @param stream_name IGNORED in this adhoc function; it returns the same
                taxonomy definition always.
    @retval corresponding taxonomy.
    """

    taxy = TaxyTool()
    taxy.add_taxonomy_set('value')
    taxy.add_taxonomy_set('lat','long name for latitude')
    taxy.add_taxonomy_set('lon','long name for longitude')
    taxy.add_taxonomy_set('time','long name for time')
    taxy.add_taxonomy_set('height','long name for height')
    # This is an example of using groups it is not a normative statement about how to use groups
    taxy.add_taxonomy_set('coordinates','This group contains coordinates...')
    taxy.add_taxonomy_set('data','This group contains data...')

    return taxy



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

    log.info("stream_names=%s; stream_info=%s" % (stream_names, stream_info))

    packet_factories = {}
    for name in stream_names:
        if not name in stream_info:
            log.error("Name '%s' not found in stream_info" % name)
            continue

        stream_config = stream_info[name]
        try:
            packet_builder = create_packet_builder(name, stream_config)
            packet_factories[name] = packet_builder
            log.info('created packet builder for stream %s' % name)
        except Exception, e:
            log.error('error creating packet builder: %s' % e)

    return packet_factories
