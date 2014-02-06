#!/usr/bin/env python
'''
@file ion/services/dm/utility/provenance.py

Contains an assortment of utilities for determining provenance
'''

from coverage_model import ParameterFunctionType

'''
An example of using graph()

Here's the output of the CTDMO data product's parameter dictionary

><> graph(pdict, 'seawater_density')
-->
{'cc_lat': {},
 'cc_lon': {},
 'sci_water_pracsal': {'seawater_conductivity': {'conductivity': {}},
  'seawater_pressure': {'cc_p_range': {}, 'pressure': {}},
  'seawater_temperature': {'temperature': {}}},
 'seawater_pressure': {'cc_p_range': {}, 'pressure': {}},
 'seawater_temperature': {'temperature': {}}}
'''

def graph(pdict, param_name):
    '''
    Essentially a depth-first-search of the dependency 
    tree for a particular parameter.

    Returns a dictionary where the key is the named parameter
    and the nested values are the dependencies. If a named 
    parameter does not contain a value, it is not a function.
    '''
    # if param_name is an integer, then just return it
    if not isinstance(param_name, basestring):
        return {}
    # get the parameter context
    ctx = pdict[param_name]
    # we only care about parameter functions
    if not isinstance(ctx.param_type, ParameterFunctionType):
        return {}

    # the parameter map describes what the function needs
    pmap = ctx.param_type.function.param_map
    retval = {}
    deps = pmap.values()
    # Recursively determine the graph for each dependency
    
    for d in deps:
        retval[d] = graph(pdict, d)

    return retval

