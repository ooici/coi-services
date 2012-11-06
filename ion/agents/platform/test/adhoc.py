#!/usr/bin/env python

"""
@package
@file
@author  Carlos Rueda
@brief   ad hoc stuff while proper mechanisms are incorporated
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

import numpy


def adhoc_get_parameter_dictionary(stream_name):
    """
    @param stream_name IGNORED in this adhoc function; it returns the same
                ParameterDictionary definition always.
    @retval corresponding ParameterDictionary.
    """

    #@TODO Luke - Maybe we can make this a bit more versatile, we could make this a standard pdict...

    pdict = ParameterDictionary()

#    ctxt = ParameterContext('value', param_type=QuantityType(value_encoding=numpy.float32))
    ctxt = ParameterContext('value', param_type=QuantityType(value_encoding=numpy.dtype('float64')))
    ctxt.uom = 'unknown'
    ctxt.fill_value = 0e0
    pdict.add_context(ctxt)

    ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=numpy.dtype('int64')))
    ctxt.axis = AxisTypeEnum.TIME
    ctxt.uom = 'seconds since 01-01-1970'
    pdict.add_context(ctxt)

    ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
    ctxt.axis = AxisTypeEnum.LON
    ctxt.uom = 'degree_east'
    pdict.add_context(ctxt)

    ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
    ctxt.axis = AxisTypeEnum.LAT
    ctxt.uom = 'degree_north'
    pdict.add_context(ctxt)

    ctxt = ParameterContext('height', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
    ctxt.axis = AxisTypeEnum.HEIGHT
    ctxt.uom = 'unknown'
    pdict.add_context(ctxt)

    return pdict


# some of the attribute names in the simulated network (network.yml)
def adhoc_get_stream_names():
    return ['input_voltage', 'Node1A_attr_2']
