#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/utility/test/parameter_helper.py
@brief Helpers for Parameters
'''
from coverage_model import ParameterContext, QuantityType, AxisTypeEnum, ArrayType, CategoryType, ConstantType, NumexprFunction, ParameterFunctionType, VariabilityEnum, PythonFunction
import numpy as np

class ParameterHelper(object):
    def __init__(self, dataset_management, addCleanup):
        self.dataset_management = dataset_management
        self.addCleanup = addCleanup

    def create_parsed(self):
        contexts, funcs = self.create_parsed_params()
        context_ids = [i[1] for i in contexts.itervalues()]

        parsed_param_dict_id = self.dataset_management.create_parameter_dictionary('parsed', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary,parsed_param_dict_id)

    def create_parsed_params(self):
        
        contexts = {}
        funcs = {}
        

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('float64')))
        t_ctxt.uom = 'seconds since 1900-01-01'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump())
        contexts['time'] = (t_ctxt, t_ctxt_id)

        lat_ctxt = ParameterContext('lat', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='lat', parameter_context=lat_ctxt.dump())
        contexts['lat'] = lat_ctxt, lat_ctxt_id

        lon_ctxt = ParameterContext('lon', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt_id = self.dataset_management.create_parameter_context(name='lon', parameter_context=lon_ctxt.dump())
        contexts['lon'] = lon_ctxt, lon_ctxt_id

        # Independent Parameters

        # Temperature - values expected to be the decimal results of conversion from hex
        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        temp_ctxt.uom = 'deg_C'
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='temp', parameter_context=temp_ctxt.dump())
        contexts['temp'] = temp_ctxt, temp_ctxt_id

        # Conductivity - values expected to be the decimal results of conversion from hex
        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        cond_ctxt.uom = 'S m-1'
        cond_ctxt_id = self.dataset_management.create_parameter_context(name='conductivity', parameter_context=cond_ctxt.dump())
        contexts['conductivity'] = cond_ctxt, cond_ctxt_id

        # Pressure - values expected to be the decimal results of conversion from hex
        press_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        press_ctxt.uom = 'dbar'
        press_ctxt_id = self.dataset_management.create_parameter_context(name='pressure', parameter_context=press_ctxt.dump())
        contexts['pressure'] = press_ctxt, press_ctxt_id

        preffered_ctxt = ParameterContext('preferred_timestamp', param_type=CategoryType(categories={0:'port_timestamp', 1:'driver_timestamp', 2:'internal_timestamp', 3:'time', -99:'empty'}), fill_value=-99)
        preffered_ctxt.uom = ''
        preffered_ctxt_id = self.dataset_management.create_parameter_context(name='preferred_timestamp', parameter_context=preffered_ctxt.dump())
        contexts['preferred_timestamp'] = preffered_ctxt, preffered_ctxt_id
        
        port_ctxt = ParameterContext('port_timestamp', param_type=QuantityType(value_encoding=np.dtype('float64')), fill_value=-9999)
        port_ctxt.uom = 'seconds since 1900-01-01'
        port_ctxt_id = self.dataset_management.create_parameter_context(name='port_timestamp', parameter_context=port_ctxt.dump())
        contexts['port_timestamp'] = port_ctxt, port_ctxt_id
        
        driver_ctxt = ParameterContext('driver_timestamp', param_type=QuantityType(value_encoding=np.dtype('float64')), fill_value=-9999)
        driver_ctxt.uom = 'seconds since 1900-01-01'
        driver_ctxt_id = self.dataset_management.create_parameter_context(name='driver_timestamp', parameter_context=driver_ctxt.dump())
        contexts['driver_timestamp'] = driver_ctxt, driver_ctxt_id
        
        internal_ctxt = ParameterContext('internal_timestamp', param_type=QuantityType(value_encoding=np.dtype('float64')), fill_value=-9999)
        internal_ctxt.uom = 'seconds since 1900-01-01'
        internal_ctxt_id = self.dataset_management.create_parameter_context(name='internal_timestamp', parameter_context=internal_ctxt.dump())
        contexts['internal_timestamp'] = internal_ctxt, internal_ctxt_id
        
        press_ctxt = ParameterContext('quality_flag', param_type=ArrayType())
        press_ctxt.uom = ''
        press_ctxt_id = self.dataset_management.create_parameter_context(name='pressure', parameter_context=press_ctxt.dump())
        contexts['quality_flag'] = press_ctxt, press_ctxt_id

        # Dependent Parameters

        # TEMPWAT_L1 = (TEMPWAT_L0 / 10000) - 10
        tl1_func = '(temperature / 10000.0) - 10'
        expr = NumexprFunction('temp_L1', tl1_func, ['temperature'])
        expr_id = self.dataset_management.create_parameter_function(name='temp_L1', parameter_function=expr.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)
        funcs['temp_L1'] = expr, expr_id

        tl1_pmap = {'temperature':'temp'}
        expr.param_map = tl1_pmap
        tempL1_ctxt = ParameterContext('temp_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        tempL1_ctxt.uom = 'deg_C'
        tempL1_ctxt_id = self.dataset_management.create_parameter_context(name='temp_L1', parameter_context=tempL1_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, tempL1_ctxt_id)
        contexts['temp_L1'] = tempL1_ctxt, tempL1_ctxt_id

        # CONDWAT_L1 = (CONDWAT_L0 / 100000) - 0.5
        cl1_func = '(conductivity / 100000.0) - 0.5'
        expr = NumexprFunction('conductivity_L1', cl1_func, ['conductivity'])
        expr_id = self.dataset_management.create_parameter_function(name='conductivity_L1', parameter_function=expr.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)
        funcs['conductivity_L1'] = expr, expr_id

        cl1_pmap = {'conductivity':'conductivity'}
        expr.param_map = cl1_pmap
        condL1_ctxt = ParameterContext('conductivity_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        condL1_ctxt.uom = 'S m-1'
        condL1_ctxt_id = self.dataset_management.create_parameter_context(name='conductivity_L1', parameter_context=condL1_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, condL1_ctxt_id)
        contexts['conductivity_L1'] = condL1_ctxt, condL1_ctxt_id

        # Equation uses p_range, which is a calibration coefficient - Fixing to 679.34040721
        #   PRESWAT_L1 = (PRESWAT_L0 * p_range / (0.85 * 65536)) - (0.05 * p_range)
        pl1_func = '(pressure / 100.0) + 0.5'
        expr = NumexprFunction('pressure_L1', pl1_func, ['pressure'])
        expr_id = self.dataset_management.create_parameter_function(name='pressure_L1', parameter_function=expr.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)
        funcs['pressure_L1'] = expr, expr_id
        
        pl1_pmap = {'pressure':'pressure'}
        expr.param_map = pl1_pmap
        presL1_ctxt = ParameterContext('pressure_L1', param_type=ParameterFunctionType(function=expr), variability=VariabilityEnum.TEMPORAL)
        presL1_ctxt.uom = 'S m-1'
        presL1_ctxt_id = self.dataset_management.create_parameter_context(name='pressure_L1', parameter_context=presL1_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, presL1_ctxt_id)
        contexts['pressure_L1'] = presL1_ctxt, presL1_ctxt_id

        # Density & practical salinity calucluated using the Gibbs Seawater library - available via python-gsw project:
        #       https://code.google.com/p/python-gsw/ & http://pypi.python.org/pypi/gsw/3.0.1

        # PRACSAL = gsw.SP_from_C((CONDWAT_L1 * 10), TEMPWAT_L1, PRESWAT_L1)
        owner = 'ion_functions.workflow_tests.fake_data'
        sal_func = 'data_l2_salinity'
        sal_arglist = ['conductivity', 'temp', 'pressure']
        expr = PythonFunction('salinity_L2', owner, sal_func, sal_arglist)
        expr_id = self.dataset_management.create_parameter_function(name='salinity_L2', parameter_function=expr.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)
        funcs['salinity_L2'] = expr, expr_id
        
        # A magic function that may or may not exist actually forms the line below at runtime.
        sal_pmap = {'conductivity':'conductivity_L1', 'temp':'temp_L1', 'pressure':'pressure_L1'}
        expr.param_map = sal_pmap
        sal_ctxt = ParameterContext('salinity', param_type=ParameterFunctionType(expr), variability=VariabilityEnum.TEMPORAL)
        sal_ctxt.uom = 'g kg-1'
        sal_ctxt_id = self.dataset_management.create_parameter_context(name='salinity', parameter_context=sal_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, sal_ctxt_id)
        contexts['salinity'] = sal_ctxt, sal_ctxt_id

        # absolute_salinity = gsw.SA_from_SP(PRACSAL, PRESWAT_L1, longitude, latitude)
        # conservative_temperature = gsw.CT_from_t(absolute_salinity, TEMPWAT_L1, PRESWAT_L1)
        # DENSITY = gsw.rho(absolute_salinity, conservative_temperature, PRESWAT_L1)
        owner = 'ion_functions.workflow_tests.fake_data'
        dens_func = 'data_l2_density'
        dens_arglist =['conductivity', 'temp', 'pressure', 'lat', 'lon'] 
        expr = PythonFunction('density_L2', owner, dens_func, dens_arglist)
        expr_id = self.dataset_management.create_parameter_function(name='density_L2', parameter_function=expr.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)
        funcs['density_L2'] = expr, expr_id


        dens_pmap = {'conductivity':'conductivity_L1', 'temp':'temp_L1', 'pressure':'pressure_L1', 'lat':'lat', 'lon':'lon'}
        expr.param_map = dens_pmap
        dens_ctxt = ParameterContext('density', param_type=ParameterFunctionType(expr), variability=VariabilityEnum.TEMPORAL)
        dens_ctxt.uom = 'kg m-3'
        dens_ctxt_id = self.dataset_management.create_parameter_context(name='density', parameter_context=dens_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, dens_ctxt_id)
        contexts['density'] = dens_ctxt, dens_ctxt_id

        return contexts, funcs
    
    def create_lookups(self):
        contexts = self.create_lookup_contexts()
        context_ids = [i[1] for i in contexts.itervalues()]

        lookup_pdict_id = self.dataset_management.create_parameter_dictionary('lookups', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary,lookup_pdict_id)
        return lookup_pdict_id


    def create_lookup_contexts(self):
        contexts = {}
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('float64')))
        t_ctxt.uom = 'seconds since 1900-01-01'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump())
        contexts['time'] = (t_ctxt, t_ctxt_id)
        
        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        temp_ctxt.uom = 'deg_C'
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='temp', parameter_context=temp_ctxt.dump())
        contexts['temp'] = temp_ctxt, temp_ctxt_id

        offset_ctxt = ParameterContext('offset_a', param_type=QuantityType(value_encoding='float32'), fill_value=-9999)
        offset_ctxt.uom = ''
        offset_ctxt.lookup_value = True
        offset_ctxt_id = self.dataset_management.create_parameter_context(name='offset_a', parameter_context=offset_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, offset_ctxt_id)
        contexts['offset_a'] = offset_ctxt, offset_ctxt_id

        func = NumexprFunction('calibrated', 'temp + offset', ['temp','offset'], param_map={'temp':'temp', 'offset':'offset_a'})
        func.lookup_values = ['LV_offset']
        calibrated = ParameterContext('calibrated', param_type=ParameterFunctionType(func, value_encoding='float32'), fill_value=-9999)
        calibrated.uom = 'deg_C'
        calibrated_id = self.dataset_management.create_parameter_context(name='calibrated', parameter_context=calibrated.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, calibrated_id)
        contexts['calibrated'] = calibrated, calibrated_id

        return contexts
