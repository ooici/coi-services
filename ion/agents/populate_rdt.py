#!/usr/bin/env python

"""
@package ion.agents.populate_rdt
@file ion/agents/populate_rdt.py
@author Edward Hunter
@brief Functions for populating RDTs with particle arrays.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import numpy
import base64
from pyon.public import log
from ion.agents.data.parsers.parser_utils import DataParticleKey

def populate_rdt(rdt, vals):
    """
    Populate a RecordDictionaryTool object with values from a data particle
    @param rdt An empty/fresh RecordDictionaryTool for the given stream
    @param A list of data particle dictionaries to insert into the RDT.
    They should look something like:
       [{u'quality_flag': u'ok',
         u'preferred_timestamp': u'port_timestamp',
         u'stream_name': u'raw',
         u'port_timestamp': 3578927113.3578925,
         u'pkt_format_id': u'JSON_Data',
         u'pkt_version': 1,
         u'values': [{u'binary': True,
                      u'value_id': u'raw',
                      u'value': u'ZAA='},
                     {u'value_id': u'length',
                      u'value': 2},
                     {u'value_id': u'type',
                      u'value': 1},
                     {u'value_id': u'checksum',
                      u'value': None}],
         u'driver_timestamp': 3578927113.75216}]
    @retval A valid, filled RDT structure
    """
    array_size = len(vals)
    data_arrays = {}    
        
    # Populate the temporal parameter.
    data_arrays[rdt.temporal_parameter] = [None] * array_size
        
    
    for i, particle in enumerate(vals):
        preferred_timestamp = DataParticleKey.DRIVER_TIMESTAMP
        if DataParticleKey.PREFERRED_TIMESTAMP in particle:
            preferred_timestamp = particle[DataParticleKey.PREFERRED_TIMESTAMP]
            
        for k,v in particle.iteritems():
            if k == DataParticleKey.VALUES:
                for value_dict in v:
                    value_id = value_dict[DataParticleKey.VALUE_ID]
                    value = value_dict[DataParticleKey.VALUE]
                    if value_id in rdt:
                        if value_id not in data_arrays:
                            data_arrays[value_id] = [None] * array_size
                        if 'binary' in value_dict:
                            value = base64.b64decode(value)
                        data_arrays[value_id][i] = value
            
            elif k == preferred_timestamp:
                data_arrays[rdt.temporal_parameter][i] = v
            
            elif k in rdt:
                if k not in data_arrays:
                    data_arrays[k] = [None] * array_size
                data_arrays[k][i] = v
                
    for k,v in data_arrays.iteritems():
        rdt[k] = numpy.array(v)
    
    return rdt

            
