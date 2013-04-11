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
from coverage_model import ParameterFunctionType, ParameterContext, SparseConstantType, ConstantType

from copy import deepcopy
from udunitspy.udunits2 import Unit

import ast
import numpy as np
import re
from uuid import uuid4


class TypesManager(object):
    function_lookups = {}
    parameter_lookups = {}

    def __init__(self, dataset_management_client):
        self.dataset_management = dataset_management_client

    def get_array_type(self,parameter_type=None):
        return ArrayType()

    def get_boolean_type(self):
        return QuantityType(value_encoding = np.dtype('int8'))

    def get_category_type(self, parameter_type, encoding, code_set):
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

    def get_constant_type(self,parameter_type, encoding, code_set):
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
            parameter_type = self.get_parameter_type(groups[2], encoding, code_set)
        return ConstantType(parameter_type)

    def get_fill_value(self, val, encoding, ptype=None):

        if isinstance(ptype,ConstantRangeType):
            matches = re.match(r'\((-?\d+(\.\d*)?), ?(-?\d+(\.\d*)?)\)', val)
            if matches:
                groups = matches.groups()
                return (self.get_fill_value(groups[0], encoding), self.get_fill_value(groups[2], encoding))
            else:
                retval = self.get_fill_value(val,encoding)
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

    def get_parameter_type(self,parameter_type, encoding, code_set=None, pfid=None, pmap=None):
        if parameter_type == 'quantity':
            return self.get_quantity_type(parameter_type,encoding)
        elif re.match(r'array<.*>', parameter_type):
            return self.get_array_type(parameter_type)
        elif re.match(r'category<.*>', parameter_type):
            return self.get_category_type(parameter_type, encoding, code_set)
        elif parameter_type == 'str':
            return self.get_string_type()
        elif re.match(r'constant<.*>', parameter_type):
            return self.get_constant_type(parameter_type, encoding, code_set)
        elif parameter_type == 'boolean':
            return self.get_boolean_type()
        elif re.match(r'range<.*>', parameter_type):
            return self.get_range_type(parameter_type, encoding)
        elif re.match(r'record<.*>', parameter_type):
            return self.get_record_type()
        elif parameter_type == 'function':
            return self.get_function_type(parameter_type, encoding, pfid, pmap)
        else:
            raise TypeError( 'Invalid Parameter Type: %s' % parameter_type)


    def get_param_name(self, pdid):
        try:
            param_name = self.parameter_lookups[pdid]
        except KeyError:
            raise KeyError('Parameter %s was not loaded' % pdid)
        return param_name

    def get_pfunc(self,pfid):
        try:
            resource_id = self.function_lookups[pfid]
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

    def get_lookup_value(self,value):
        placeholder = value.replace('LV_','')
        document_key = ''
        if '||' in placeholder:
            document_key, placeholder = placeholder.split('||')
        document_val = placeholder
        placeholder = '%s_%s' % (placeholder, uuid4().hex)
        pc = ParameterContext(name=placeholder, param_type=SparseConstantType(base_type=ConstantType(value_encoding='float64'), fill_value=-9999.))
        pc.lookup_value = document_val
        pc.document_key = document_key
        ctxt_id = self.dataset_management.create_parameter_context(name=placeholder, parameter_context=pc.dump())
        self.parameter_lookups[placeholder] = ctxt_id
        return value, placeholder

    def has_lookup_value(self, context):
        if isinstance(context.param_type, ParameterFunctionType):
            if hasattr(context.function,'lookup_values'):
                return True
        else:
            return False

    def get_lookup_value_ids(self, context):
        if isinstance(context.param_type, ParameterFunctionType):
            if hasattr(context.function,'lookup_values'):
                lookup_values = context.function.lookup_values
                return [self.parameter_lookups[i] for i in lookup_values]
        return []

    def evaluate_pmap(self,pfid, pmap):
        lookup_values = []
        for k,v in pmap.iteritems():
            if isinstance(v, dict):
                pfid_,pmap_ = v.popitem()
                pmap[k] = self.evaluate_pmap(pfid_, pmap_)
            if isinstance(v, basestring) and 'PD' in v:
                pmap[k] = self.get_param_name(v)
            if isinstance(v, basestring) and 'LV' in v:
                value, placeholder = self.get_lookup_value(v)
                pmap[k] = placeholder
                lookup_values.append(placeholder)
        func = deepcopy(self.get_pfunc(pfid))
        func.param_map = pmap
        if lookup_values:
            func.lookup_values = lookup_values
        return func

    def get_function_type(self, parameter_type, encoding, pfid, pmap):
        if pfid is None or pmap is None:
            raise TypeError('Function Types require proper function IDs and maps')
        try:
            pmap = ast.literal_eval(pmap)
        except:
            raise TypeError('Invalid Parameter Map Syntax')
        func = self.evaluate_pmap(pfid, pmap) # Parse out nested PFIDs and such
        param_type = ParameterFunctionType(func)
        return param_type

    def get_quantity_type(self, parameter_type, encoding):
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

    def get_range_type(self, parameter_type, encoding):
        groups = re.match(r'(range)(<)(.*)(>)', parameter_type).groups()
        if groups[2] == 'quantity':
            return ConstantRangeType(self.get_quantity_type(groups[2], encoding))
        else:
            raise TypeError('Unsupported Constant Range Type: %s' % groups[2])


    def get_record_type(self):
        return RecordType()

    def get_string_type(self):
        return self.get_array_type()

    def get_unit(self, uom):
        return Unit(uom)


