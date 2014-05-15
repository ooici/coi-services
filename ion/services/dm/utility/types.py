#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell at ASAScience dot com>
@file ion/services/dm/utility/types.py
@date Thu Jan 17 15:51:16 EST 2013
'''
from pyon.container.cc import Container
from pyon.core.exception import BadRequest, NotFound
from pyon.public import CFG, RT
from pyon.util.memoize import memoize_lru
from pyon.util.log import log

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

from coverage_model.parameter_types import QuantityType, ArrayType, TextType
from coverage_model.parameter_types import RecordType, CategoryType 
from coverage_model.parameter_types import ConstantType, ConstantRangeType
from coverage_model.parameter_functions import AbstractFunction
from coverage_model import ParameterFunctionType, ParameterContext, SparseConstantType, ConstantType

from copy import deepcopy
from udunitspy.udunits2 import Unit, System
from interface.objects import ParameterContext as ParameterContextResource

import ast
import numpy as np
import re
from uuid import uuid4


class TypesManager(object):
    system = System(path=CFG.get_safe('units', 'res/config/units/udunits2.xml'))

    def __init__(self, dataset_management_client, resource_ids, resource_objs):
        self.dataset_management = dataset_management_client
        self.resource_ids = resource_ids
        self.resource_objs = resource_objs

    def get_array_type(self,parameter_type=None, encoding=None):
        if encoding in ('str', '', 'opaque'):
            encoding = None
        return ArrayType(inner_encoding=encoding)

    def get_boolean_type(self):
        return QuantityType(value_encoding = np.dtype('int8'))

    def get_category_type(self, parameter_type, encoding, code_set):
        groups = re.match(r'(category)(<)(.*)(:)(.*)(>)', parameter_type).groups()
        dtype = np.dtype(groups[2])
        try:
            if isinstance(code_set, basestring):
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

        if 'float' in encoding and val == '':
            return -9999999.0
        if 'int' in encoding and val == '':
            return -9999999
        if val == '':
            return None
        if isinstance(val, basestring) and val.lower() == 'none':
            return None
        if isinstance(val, basestring) and val.lower() == 'empty':
            return ''
        if isinstance(val, basestring) and val.lower() == 'false':
            return 0
        if isinstance(val, basestring) and val.lower() == 'true':
            return 1
        if 'float' in encoding:
            return float(val)
        if 'int' in encoding:
            return int(val)
        if isinstance(val, basestring) and encoding.lower()[0] == 's':
            return val
        if isinstance(val, basestring) and encoding.lower() == 'opaque':
            raise TypeError('Fill value for opaque must be None, not: %s' % val)
        if 'category' in encoding.lower() and isinstance(val, (int, float)):
            return val
        else:
            raise TypeError('Invalid Fill Value: %s' % val) # May never be called

    def get_parameter_type(self,parameter_type, encoding, code_set=None, pfid=None, pmap=None):
        if parameter_type == 'quantity':
            return self.get_quantity_type(parameter_type,encoding)
        elif re.match(r'array<?.*>?', parameter_type):
            return self.get_array_type(parameter_type, encoding)
        elif re.match(r'category<.*>', parameter_type):
            return self.get_category_type(parameter_type, encoding, code_set)
        elif parameter_type == 'str':
            return self.get_string_type()
        elif re.match(r'constant<.*>', parameter_type):
            return self.get_constant_type(parameter_type, encoding, code_set)
        elif parameter_type == 'boolean':
            return self.get_boolean_type()
        elif re.match(r'range<.*>', parameter_type):
            return self.get_array_type(parameter_type, encoding)
        elif re.match(r'record<.*>', parameter_type):
            return self.get_record_type()
        elif parameter_type == 'function':
            return self.get_function_type(parameter_type, encoding, pfid, pmap)
        elif parameter_type == 'sparse':
            return self.get_sparse_type(parameter_type, encoding)
        else:
            raise TypeError( 'Invalid Parameter Type: %s' % parameter_type)


    def get_param_name(self, pdid):
        try:
            param_name = self.resource_objs[pdid].name
        except KeyError:
            raise KeyError('Parameter %s was not loaded' % pdid)
        return param_name

    def get_pfunc(self,pfid):
        # Preload Case
        if not pfid:
            raise TypeError('No parameter function id specified')
        if pfid.startswith('PFID'):
            if pfid not in self.resource_objs: 
                raise KeyError('Function %s was not loaded' % pfid)

            pf = self.resource_objs[pfid]
            func = DatasetManagementService.get_coverage_function(pf)
            return func
        # System Case
        else:
            pf = Container.instance.resource_registry.read(pfid)
            func = DatasetManagementService.get_coverage_function(pf)
            return func

    def _placeholder(self, value, pc_callback):
        placeholder = value.replace('LV_','')
        document_key = ''
        if '||' in placeholder:
            document_key, placeholder = placeholder.split('||')
        document_val = placeholder
        placeholder = '%s_%s' % (placeholder, uuid4().hex)
        pc = pc_callback(placeholder)
        pc.lookup_value = document_val
        pc.document_key = document_key
        pc.uom = '1'
        pc.visible = False
        ctxt_id = self.dataset_management.create_parameter_context(name=placeholder, parameter_context=pc.dump())
        return ctxt_id, placeholder


    def get_lookup_value(self,value):
        return self._placeholder(value, lambda placeholder : ParameterContext(name=placeholder, param_type=SparseConstantType(base_type=ConstantType(value_encoding='float64'), fill_value=-9999.)))
    
    def get_array_lookup_value(self,value):
        return self._placeholder(value, lambda placeholder : ParameterContext(name=placeholder, param_type=SparseConstantType(base_type=ArrayType(inner_encoding='float64', inner_fill_value=-9999.))))

    def get_string_array_lookup_value(self, value):
        return self._placeholder(value, lambda placeholder : ParameterContext(name=placeholder, param_type=SparseConstantType(base_type=ArrayType())))

    def get_cc_value(self, value):
        placeholder = value.lower()
        # Check to see if this coefficient exists already

        hits, _ = Container.instance.resource_registry.find_resources(name=placeholder, restype=RT.ParameterContext, id_only=True)
        if hits:
            return hits[0], placeholder

        pc = ParameterContext(name=placeholder, param_type=SparseConstantType(value_encoding='float64'), fill_value=-9999.)
        pc.uom = '1'
        pc.display_name = 'Calibration Coefficient ' + placeholder
        ctxt_id = self.dataset_management.create_parameter_context(name=placeholder, parameter_context=pc.dump())
        return ctxt_id, placeholder


    def has_lookup_value(self, context):
        if isinstance(context.param_type, ParameterFunctionType) and hasattr(context.function,'lookup_values'):
            return True
        return False

    def get_lookup_value_ids(self, context):
        if isinstance(context.param_type, ParameterFunctionType):
            return getattr(context.function,'lookup_values',[])
        return []

    def get_cc_value_ids(self, context):
        if isinstance(context.param_type, ParameterFunctionType):
            return getattr(context.function,'coefficients',[])
        return []

    def evaluate_pmap(self,pfid, pmap):
        lookup_values = []
        coefficients = []
        for k,v in pmap.iteritems():
            if isinstance(v, dict):
                pfid_,pmap_ = v.popitem()
                pmap[k] = self.evaluate_pmap(pfid_, pmap_)
            if isinstance(v, basestring) and 'PD' in v:
                pmap[k] = self.get_param_name(v)
            if isinstance(v, basestring) and 'LV' in v:
                ctxt_id, placeholder = self.get_lookup_value(v)
                pmap[k] = placeholder
                lookup_values.append(ctxt_id)
            if isinstance(v, basestring) and 'CC' in v:
                ctxt_id, placeholder = self.get_cc_value(v)
                pmap[k] = placeholder
                coefficients.append(ctxt_id)

        func = self.get_pfunc(pfid)
        func.param_map = pmap
        if lookup_values:
            func.lookup_values = lookup_values
        if coefficients:
            func.coefficients = coefficients
        return func

    def evaluate_qc(self):
        pass

        
    @memoize_lru(maxsize=100)
    def find_function(self,name):
        res_obj, _ = Container.instance.resource_registry.find_resources(name=name, restype=RT.ParameterFunction, id_only=False)

        if res_obj:
            return res_obj[0]._id, DatasetManagementService.get_coverage_function(res_obj[0])
        else:
            raise KeyError('%s was never loaded' % name)

    def find_grt(self):
        return self.find_function('global_range_test')
    
    def find_spike(self):
        return self.find_function('dataqc_spiketest')
    
    def find_stuck_value(self):
        return self.find_function("dataqc_stuckvaluetest")

    def find_trend_test(self):
        return self.find_function("dataqc_polytrendtest")

    def find_propagate_test(self):
        return self.find_function("dataqc_propagateflags")

    def find_gradient_test(self):
        return self.find_function('dataqc_gradienttest')

    def find_localrange_test(self):
        return self.find_function('dataqc_localrangetest')

    def make_qc_functions(self, name, data_product, registration_function, qc_fields=None):
        contexts = []

        if qc_fields is None:
            qc_factories = [
                            self.make_grt_qc,
                            self.make_spike_qc,
                            self.make_stuckvalue_qc,
                            self.make_trendtest_qc, # was not supported
                            self.make_gradienttest_qc,
                            self.make_localrange_qc,
                            ]
        else:
            qc_factories = []
            if 'glblrng_qc' in qc_fields:
                qc_factories.append(self.make_grt_qc)
            if 'spketst_qc' in qc_fields:
                qc_factories.append(self.make_spike_qc)
            if 'stuckvl_qc' in qc_fields:
                qc_factories.append(self.make_stuckvalue_qc)
            if 'trndtst_qc' in qc_fields:
                qc_factories.append(self.make_trendtest_qc)
            if 'gradtst_qc' in qc_fields:
                qc_factories.append(self.make_gradienttest_qc)
            if 'loclrng_qc' in qc_fields:
                qc_factories.append(self.make_localrange_qc)

        for factory in qc_factories:
            try:
                ctxt_id, pc = factory(name,data_product)
            except KeyError as e:
                log.error(e.message)
                continue
            contexts.append(ctxt_id)
            registration_function(ctxt_id, ctxt_id, ParameterContextResource(name=pc.name, parameter_context=pc.dump()))

        return contexts

    @classmethod
    def dp_name(cls, data_product):
        data_product = re.sub(r'[^a-zA-Z0-9_]', '_', data_product)
        return re.sub(r'_L[0-9]+','',data_product)

    def make_grt_qc(self, name, data_product):
        pfunc_id, pfunc = self.find_grt() 
        grt_min_id, grt_min_name = self.get_lookup_value('LV_grt_$designator_%s||grt_min_value' % data_product)
        grt_max_id, grt_max_name = self.get_lookup_value('LV_grt_$designator_%s||grt_max_value' % data_product)

        pmap = {'dat':name, 'dat_min':grt_min_name,'dat_max':grt_max_name}
        pfunc.param_map = pmap
        pfunc.lookup_values = [grt_min_id, grt_max_id]
        dp_name = self.dp_name(data_product)
        pc = ParameterContext(name='%s_glblrng_qc' % dp_name.lower(), param_type=ParameterFunctionType(pfunc, value_encoding='|i1'))
        pc.uom = '1'
        pc.ooi_short_name = '%s_GLBLRNG_QC' % dp_name
        pc.display_name = '%s Global Range Test Quality Control Flag' % dp_name
        pc.description = "The OOI Global Range quality control algorithm generates a QC flag for the input data point indicating whether it falls within a given range."
        ctxt_id = self.dataset_management.create_parameter_context(name='%s_glblrng_qc' % dp_name.lower(), parameter_type='function', parameter_context=pc.dump(), parameter_function_id=pfunc_id, ooi_short_name=pc.ooi_short_name, units='1', value_encoding='int8', display_name=pc.display_name, description=pc.description)
        return ctxt_id, pc

    def make_spike_qc(self, name, data_product):
        pfunc_id, pfunc = self.find_spike()
        spike_acc_id, spike_acc_name = self.get_lookup_value('LV_spike_$designator_%s||acc' % data_product)
        spike_n_id, spike_n_name = self.get_lookup_value('LV_spike_$designator_%s||spike_n' % data_product)
        spike_l_id, spike_l_name = self.get_lookup_value('LV_spike_$designator_%s||spike_l' % data_product)

        pmap = {'dat':name, 'acc':spike_acc_name, 'N':spike_n_name, 'L':spike_l_name}
        pfunc.param_map = pmap
        pfunc.lookup_values = [spike_acc_id, spike_n_id, spike_l_id]
        dp_name = self.dp_name(data_product)
        pc = ParameterContext(name='%s_spketst_qc' % dp_name.lower(), param_type=ParameterFunctionType(pfunc, value_encoding='|i1'))
        pc.uom='1'
        pc.ooi_short_name = '%s_SPKETST_QC' % dp_name
        pc.display_name = '%s Spike Test Quality Control Flag' % dp_name

        pc.description = "The OOI Spike Test quality control algorithm generates a flag for individual data values that deviate significantly from surrounding data values."

        ctxt_id = self.dataset_management.create_parameter_context(name='%s_spketst_qc' % dp_name.lower(), parameter_type='function', parameter_context=pc.dump(), parameter_function_id=pfunc_id, ooi_short_name=pc.ooi_short_name, units='1', value_encoding='int8', display_name=pc.display_name, description=pc.description)
        return ctxt_id, pc

    def make_stuckvalue_qc(self, name, data_product):
        pfunc_id, pfunc = self.find_stuck_value()

        reso_id, reso_name = self.get_lookup_value('LV_svt_$designator_%s||svt_resolution' % data_product)
        n_id, n_name = self.get_lookup_value('LV_svt_$designator_%s||svt_n' % data_product)

        pmap = {'x' : name, 'reso': reso_name, 'num': n_name}
        pfunc.param_map = pmap
        pfunc.lookup_values = [reso_id, n_id]
        dp_name = self.dp_name(data_product)
        pc = ParameterContext(name='%s_stuckvl_qc' % dp_name.lower(), param_type=ParameterFunctionType(pfunc, value_encoding='|i1'))
        pc.uom = '1'
        pc.ooi_short_name = '%s_STUCKVL_QC' % dp_name
        pc.display_name = '%s Stuck Value Test Quality Control Flag' % dp_name
        pc.description =  'The OOI Stuck Value Test quality control algorithm generates a flag for repeated occurrence of one value in a time series.'

        ctxt_id = self.dataset_management.create_parameter_context(name='%s_stuckvl_qc' % dp_name.lower(), parameter_type='function', parameter_context=pc.dump(), parameter_function_id=pfunc_id, ooi_short_name=pc.ooi_short_name, units='1', value_encoding='int8', display_name=pc.display_name, description=pc.description)
        return ctxt_id, pc

    def make_trendtest_qc(self, name, data_product):

        pfunc_id, pfunc = self.find_trend_test()

        order_id, order_name = self.get_lookup_value('LV_trend_$designator_%s||polynomial_order' % data_product)
        dev_id, dev_name = self.get_lookup_value('LV_trend_$designator_%s||standard_deviation' % data_product)

        pmap = {"dat":name ,"t":'time',"ord_n":order_name,"ntsd":dev_name}

        pfunc.param_map = pmap
        pfunc.lookup_values = [order_id, dev_id]
        dp_name = self.dp_name(data_product)
        pc = ParameterContext(name='%s_trndtst_qc' % dp_name.lower(), param_type=ParameterFunctionType(pfunc,value_encoding='|i1'))
        pc.uom = '1'
        pc.ooi_short_name = '%s_TRNDTST_QC' % dp_name
        pc.display_name = '%s Trend Test Test Quality Control Flag' % dp_name
        pc.description = 'The OOI Trend Test quality control algorithm generates flags on data values within a time series where a significant fraction of the variability in the time series can be explained by a drift, where the drift is assumed to be a polynomial of specified order.'
        ctxt_id = self.dataset_management.create_parameter_context(name='%s_trndtst_qc' % dp_name.lower(), parameter_type='function', parameter_context=pc.dump(), parameter_function_id=pfunc_id, ooi_short_name=pc.ooi_short_name, units='1', value_encoding='int8', display_name=pc.display_name, description=pc.description)
        return ctxt_id, pc

    def make_gradienttest_qc(self, name, data_product):

        pfunc_id, pfunc = self.find_gradient_test()

        ddatdx_id, ddatdx = self.get_lookup_value('LV_grad_$designator_%s_time||d_dat_dx' % data_product)
        mindx_id, mindx = self.get_lookup_value('LV_grad_$designator_%s_time||min_dx' % data_product)
        startdat_id, startdat = self.get_lookup_value('LV_grad_$designator_%s_time||start_dat' % data_product)
        toldat_id, toldat = self.get_lookup_value('LV_grad_$designator_%s_time||tol_dat' % data_product)

        pmap = {"dat":name, "x": 'time', 'ddatdx': ddatdx, 'mindx':mindx, 'startdat': startdat, 'toldat':toldat}
        pfunc.param_map = pmap
        pfunc.lookup_values = [ddatdx_id, mindx_id, startdat_id, toldat_id]
        dp_name = self.dp_name(data_product)

        pc = ParameterContext(name='%s_gradtst_qc' % dp_name.lower(), param_type=ParameterFunctionType(pfunc, value_encoding='|i1'))
        pc.uom = '1'
        pc.ooi_short_name = '%s_GRADTST_QC' % dp_name
        pc.display_name = '%s Gradient Test Quality Control Flag' % dp_name
        pc.description = 'The OOI Gradient Test is an automated quality control algorithm used on various OOI data products. This automated algorithm generates flags for data points according to whether changes between successive points are within a pre-determined range.'
        ctxt_id = self.dataset_management.create_parameter_context(name='%s_gradtst_qc' % dp_name.lower(), parameter_type='function', parameter_context=pc.dump(), parameter_function_id=pfunc_id, ooi_short_name=pc.ooi_short_name, units='1', value_encoding='int8', display_name=pc.display_name, description=pc.description)
        return ctxt_id, pc
    
    def make_localrange_qc(self, name, data_product):

        pfunc_id, pfunc = self.find_localrange_test()

        datlim_id, datlim = self.get_array_lookup_value('LV_lrt_$designator_%s||datlim' % data_product)
        datlimz_id, datlimz = self.get_array_lookup_value('LV_lrt_$designator_%s||datlimz' % data_product)
        dims_id, dims = self.get_string_array_lookup_value('LV_lrt_$designator_%s||dims' % data_product)

        pmap = {"dat":name, "dims*":dims, "datlim*":datlim, "datlimz*":datlimz}
        pfunc.param_map = pmap
        pfunc.lookup_values = [datlim_id, datlimz_id, dims_id]
        dp_name = self.dp_name(data_product)

        pc = ParameterContext(name='%s_loclrng_qc' % dp_name.lower(), param_type=ParameterFunctionType(pfunc, value_encoding='|i1'))
        pc.uom = '1'
        pc.ooi_short_name = '%s_LOCLRNG_QC' % dp_name
        pc.display_name = '%s Local Range Test Quality Control Flag' % dp_name
        pc.description = 'The OOI Local Range Test is the computation to test whether a given data point falls within pre-defined ranges.'
        ctxt_id = self.dataset_management.create_parameter_context(name='%s_loclrng_qc' % dp_name.lower(), parameter_type='function', parameter_context=pc.dump(), parameter_function_id=pfunc_id, ooi_short_name=pc.ooi_short_name, units='1', value_encoding='int8', display_name=pc.display_name, description=pc.description)
        return ctxt_id, pc

    def make_propagate_qc(self,inputs):

        pfunc_id, pfunc = self.find_propagate_test()
        pmap = {"strict_validation":False}
        arg_list = ['strict_validation']
        for i,val in enumerate(inputs):
            if i >= 100:
                break
            pmap['array%s' % i] = val
            arg_list.append('array%s' % i)
        pfunc.param_map = pmap
        pfunc.arg_list = arg_list
        pc = ParameterContext(name='cmbnflg_qc', param_type=ParameterFunctionType(pfunc, value_encoding='|i1'))
        pc.uom = '1'
        pc.ooi_short_name = 'CMBNFLG_QC' 
        pc.display_name = 'Combined Data Quality Control Flag' 
        pc.description = 'The purpose of this computation is to produce a single merged QC flag from a set of potentially many flags.'
        ctxt_id = self.dataset_management.create_parameter_context(name='cmbnflg_qc', parameter_type='function', parameter_context=pc.dump(), parameter_function_id=pfunc_id, ooi_short_name=pc.ooi_short_name, units='1', value_encoding='int8', display_name=pc.display_name, description=pc.description)
        return ctxt_id, pc


    def get_function_type(self, parameter_type, encoding, pfid, pmap):
        if pfid is None or pmap is None:
            raise TypeError('Function Types require proper function IDs and maps')
        if isinstance(pmap, basestring):
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

    def get_sparse_type(self, parameter_type, encoding):
        return SparseConstantType(value_encoding=encoding)


    def get_record_type(self):
        return RecordType()

    def get_string_type(self):
        return TextType()

    def get_unit(self, uom):
        return Unit(uom, system=self.system)


