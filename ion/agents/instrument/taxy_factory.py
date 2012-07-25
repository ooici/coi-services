#!/usr/bin/env python

"""
@package ion.agents.instrument.taxy_factory
@file    ion/agents/instrument/taxy_factory.py
@author  Carlos Rueda
@brief   Taxonomy factory. This is initially intended to facilitate testing but
         could be refined later on to retrieve the taxonomies from external
         sources (eg., configuration files or appropriate registry).
         It currently defines a couple of taxonomies in a hard-coded way.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.util.log import log
from ion.services.dm.utility.granule.taxonomy import TaxyTool


# _cache: A cache of taxonomies indexed by the stream name.
_cache = {}


def get_taxonomy(stream_name):
    """
    Gets the taxonomy for a given stream.

    @param stream_name Stream name.

    @retval corresponding taxonomy.
    """

    if stream_name in _cache:
        log.debug("taxonomy for stream_name=%s found in cache" % stream_name)
        return _cache[stream_name]

    taxy = None

    # TODO determine and use appropriate mechanism for definition of
    # taxonomies. The following hard-coded definitions are taken from
    # preliminary code in instrument_management_service.py as of 7/15/12.

    if "parsed" == stream_name:
        taxy = TaxyTool()
        taxy.add_taxonomy_set('temp', 't', 'long name for temp')
        taxy.add_taxonomy_set('cond', 'c', 'long name for cond')
        taxy.add_taxonomy_set('pres', 'p', 'long name for pres')
        taxy.add_taxonomy_set('lat','long name for latitude')
        taxy.add_taxonomy_set('lon','long name for longitude')
        taxy.add_taxonomy_set('time','long name for time')
        taxy.add_taxonomy_set('height','long name for height')
        # This is an example of using groups it is not a normative statement about how to use groups
        taxy.add_taxonomy_set('coordinates','This group contains coordinates...')
        taxy.add_taxonomy_set('data','This group contains data...')

    elif "raw" == stream_name:
        taxy = TaxyTool()
        taxy.add_taxonomy_set('blob','bytes in an array')
        taxy.add_taxonomy_set('lat','long name for latitude')
        taxy.add_taxonomy_set('lon','long name for longitude')
        taxy.add_taxonomy_set('time','long name for time')
        taxy.add_taxonomy_set('height','long name for height')
        taxy.add_taxonomy_set('coordinates','This group contains coordinates...')
        taxy.add_taxonomy_set('data','This group contains data...')

    else:
        log.warn("Undefined taxonomy for stream_name=%s" % stream_name)

    if taxy:
        _cache[stream_name] = taxy
        log.debug("created taxonomy for stream_name=%s " % stream_name)

    return taxy
