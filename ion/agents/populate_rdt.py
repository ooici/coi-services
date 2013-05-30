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



def populate_rdt(rdt, vals):
    data_arrays = {}
    data_arrays[rdt.temporal_parameter] = [None] * len(vals)
    
    for i,tomato in enumerate(vals):
        if 'values' in tomato:
            for inner_dict in tomato['values']:
                field = inner_dict['value_id']
                value = inner_dict['value']
                if field not in rdt:
                    continue
                if field not in data_arrays:
                    data_arrays[field] = [None] * len(vals)
                data_arrays[field][i] = value if not inner_dict.get('binary',None) else base64.b64decode(value)
        for k,v in tomato.iteritems():
            if k == 'values' or k not in rdt:
                continue
            if k not in data_arrays:
                data_arrays[k] = [None] * len(vals)
            if k == 'driver_timestamp':
                data_arrays[rdt.temporal_parameter][i] = v
            data_arrays[k][i] = v
                
    for (k,v) in data_arrays.iteritems():
        if v and any(v):
            rdt[k] = numpy.array(v)

    return rdt