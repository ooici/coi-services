#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell at ASAScience dot com>
@file ion/services/dm/utility/types.py
@date Thu Jan 17 15:51:16 EST 2013
'''
from pyon.core.exception import BadRequest, NotFound

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

from coverage_model.parameter_types import QuantityType, ArrayType 
from coverage_model.parameter_types import RecordType, CategoryType 
from coverage_model.parameter_types import ConstantType, ConstantRangeType
from coverage_model import ParameterFunctionType

from copy import deepcopy

import ast
import numpy as np
import re


function_lookups = {}
parameter_lookups = {}


def get_array_type(parameter_type=None):
    return ArrayType()

def get_boolean_type():
    return QuantityType(value_encoding = np.dtype('int8'))

def get_category_type(parameter_type, encoding, code_set):
    groups = re.match(r'(category)(<)(.*)(:)(.*)(>)', parameter_type).groups()
    dtype = np.dtype(groups[2])
    try:
        codestr = code_set.replace('\\', '')
        code_set = ast.literal_eval(codestr)
        for k in code_set.keys():
            v = code_set[k]
            del code_set[k]
            code_set[dtype.type(k)] = v
    except:
        raise TypeError('Invalid Code Set: %s' % code_set)
    return CategoryType(categories=code_set)

def get_constant_type(parameter_type, encoding, code_set):
    groups = re.match(r'(constant)(<)(.*)(>)', parameter_type).groups()
    if groups[2] == 'str' or groups[2] == 'string':
        if encoding[0] != 'S' or len(encoding) < 2:
            raise TypeError('Constant strings need to be specified with a valid length (e.g. S8): %s' % encoding)
        slen = encoding[1:]
        try:
            int(slen)
        except ValueError:
            raise TypeError('Improper String Length: %s' % slen)
        parameter_type = QuantityType(value_encoding = np.dtype('|%s' % encoding))
    else:
        parameter_type = get_parameter_type(groups[2], encoding, code_set)
    return ConstantType(parameter_type)

def get_fill_value(val, encoding, ptype=None):

    if isinstance(ptype,ConstantRangeType):
        matches = re.match(r'\((-?\d+(\.\d*)?), ?(-?\d+(\.\d*)?)\)', val)
        if matches:
            groups = matches.groups()
            return (get_fill_value(groups[0], encoding), get_fill_value(groups[2], encoding))
        else:
            retval = get_fill_value(val,encoding)
            if retval is not None:
                raise TypeError('Invalid range fill value: %s' % val)



    if val == '':
        return None
    if val.lower() == 'none':
        return None
    if val.lower() == 'empty':
        return ''
    if val.lower() == 'false':
        return 0
    if val.lower() == 'true':
        return 1
    if 'float' in encoding:
        return float(val)
    if 'int' in encoding:
        return int(val)
    if encoding.lower()[0] == 's':
        return val
    if encoding.lower() == 'opaque':
        raise TypeError('Fill value for opaque must be None, not: %s' % val)
    else:
        raise TypeError('Invalid Fill Value: %s' % val) # May never be called

def get_parameter_type(parameter_type, encoding, code_set=None, pfid=None, pmap=None):
    if parameter_type == 'quantity':
        return get_quantity_type(parameter_type,encoding)
    elif re.match(r'array<.*>', parameter_type):
        return get_array_type(parameter_type)
    elif re.match(r'category<.*>', parameter_type):
        return get_category_type(parameter_type, encoding, code_set)
    elif parameter_type == 'str':
        return get_string_type()
    elif re.match(r'constant<.*>', parameter_type):
        return get_constant_type(parameter_type, encoding, code_set)
    elif parameter_type == 'boolean':
        return get_boolean_type()
    elif re.match(r'range<.*>', parameter_type):
        return get_range_type(parameter_type, encoding)
    elif re.match(r'record<.*>', parameter_type):
        return get_record_type()
    elif parameter_type == 'function':
        return get_function_type(parameter_type, encoding, pfid, pmap)
    else:
        raise TypeError( 'Invalid Parameter Type: %s' % parameter_type)


def get_param_name(pdid):
    global parameter_lookups
    try:
        param_name = parameter_lookups[pdid]
    except KeyError:
        raise KeyError('Parameter %s was not loaded' % pdid)
    return param_name

def get_pfunc(pfid):
    global function_lookups
    try:
        resource_id = function_lookups[pfid]
    except KeyError:
        raise KeyError('Function %s was not loaded' % pfid) 
    try:
        pfunc = DatasetManagementService.get_parameter_function(resource_id)
    except NotFound:
        raise TypeError('Unable to locate functionf or PFID: %s' % pfid)
    except BadRequest:
        raise ValueError('Processing error trying to get PFID: %s' % pfid)
    except:
        raise ValueError('Error making service request')

    return pfunc

def evaluate_pmap(pfid, pmap):
    for k,v in pmap.iteritems():
        if isinstance(v, dict):
            pfid_,pmap_ = v.popitem()
            pmap[k] = evaluate_pmap(pfid_, pmap_)
        if isinstance(v, basestring) and 'PD' in v:
            pmap[k] = get_param_name(v)
    func = deepcopy(get_pfunc(pfid))
    func.param_map = pmap
    return func

def get_function_type(parameter_type, encoding, pfid, pmap):
    if pfid is None or pmap is None:
        raise TypeError('Function Types require proper function IDs and maps')
    try:
        pmap = ast.literal_eval(pmap)
    except:
        raise TypeError('Invalid Parameter Map Syntax')
    func = evaluate_pmap(pfid, pmap) # Parse out nested PFIDs and such
    param_type = ParameterFunctionType(func)
    return param_type

def get_quantity_type(parameter_type, encoding):
    if encoding[0] == 'S':
        slen = encoding[1:]
        try:
            int(slen)
        except ValueError:
            raise TypeError('Improper String Length: %s' % slen)
        param_type = QuantityType(value_encoding = np.dtype('|%s' % encoding))
    else:
        param_type = QuantityType(value_encoding = np.dtype(encoding))
    return param_type

def get_range_type(parameter_type, encoding):
    groups = re.match(r'(range)(<)(.*)(>)', parameter_type).groups()
    if groups[2] == 'quantity':
        return ConstantRangeType(get_quantity_type(groups[2], encoding))
    else:
        raise TypeError('Unsupported Constant Range Type: %s' % groups[2])


def get_record_type():
    return RecordType()

def get_string_type():
    return get_array_type()

