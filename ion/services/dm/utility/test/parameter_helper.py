#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/utility/test/parameter_helper.py
@brief Helpers for Parameters
'''
from coverage_model import ParameterContext, QuantityType, AxisTypeEnum, ArrayType, CategoryType, ConstantType, NumexprFunction, ParameterFunctionType, VariabilityEnum, PythonFunction, SparseConstantType
from ion.services.dm.utility.types import TypesManager
from ion.services.dm.utility.granule import RecordDictionaryTool
from pyon.container.cc import Container
from pyon.ion.stream import StandaloneStreamPublisher
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
import time
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
        return parsed_param_dict_id

    def get_rdt(self, stream_def_id):
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        return rdt

    def fill_parsed_rdt(self, rdt):
        now = time.time()
        ntp_now = now + 2208988800 # Do not use in production, this is a loose translation

        rdt['internal_timestamp'] = [ntp_now]
        rdt['temp'] = [300000]
        rdt['preferred_timestamp'] = ['driver_timestamp']
        rdt['time'] = [ntp_now]
        rdt['port_timestamp'] = [ntp_now]
        rdt['quality_flag'] = [None]
        rdt['lat'] = [45]
        rdt['conductivity'] = [4341400]
        rdt['driver_timestamp'] = [ntp_now]
        rdt['lon'] = [-71]
        rdt['pressure'] = [256.8]
        return rdt

    def fill_rdt(self, rdt, t, offset=0):
        rdt[rdt.temporal_parameter] = np.arange(offset,t+offset)
        for field in rdt.fields:
            if field == rdt.temporal_parameter:
                continue
            self.fill_parameter(rdt,field,t)

    def publish_rdt_to_data_product(self,data_product_id, rdt, connection_id='', connection_index=''):
        resource_registry       = Container.instance.resource_registry
        pubsub_management       = PubsubManagementServiceClient()
        stream_ids, _ = resource_registry.find_objects(data_product_id,'hasStream',id_only=True)
        stream_id = stream_ids[0]
        route = pubsub_management.read_stream_route(stream_id)
        publisher = StandaloneStreamPublisher(stream_id,route)
        publisher.publish(rdt.to_granule(connection_id=connection_id, connection_index=connection_index))


    def fill_parameter(self,rdt,parameter,t):
        tn = np.arange(t)
        context = rdt.context(parameter)
        if isinstance(context.param_type, QuantityType):
            if parameter == 'temp':
                # t' = temp
                # t = (t' / 1e5)-10 
                # 0 < t < 30 =>
                # 1e5 < t' < 4e5
                rdt[parameter] = self.float_range(1e5,4e5,tn)
            elif parameter == 'conductivity':
                # c' = conductivity
                # c = (c' / 1e5) - 0.5 
                # 4.6 < c < 5.0 =>
                # 5.1e5 < c' 5.5e5
                rdt[parameter] = self.float_range(510000, 550000,tn)
            elif parameter == 'pressure':
                # p' = pressure
                # p = (p' / 100) + 0.5
                # 0 < p < 303.3 =>
                # 50 < p' < 30380
                rdt[parameter] = self.float_range(50,30380, tn)
            elif parameter == 'lat':
                rdt[parameter] = [45] * t
            elif parameter == 'lon':
                rdt[parameter] = [-71] * t
            else:
                rdt[parameter] = np.sin(np.pi * 2 * tn / 60)
        elif isinstance(context.param_type, ArrayType):
            rdt[parameter] = np.array([range(10)] * t)
        elif isinstance(context.param_type, CategoryType):
            rdt[parameter] = [context.categories.keys()[0]] * t
        elif isinstance(context.param_type, ConstantType):
            rdt[parameter] = np.dtype(context.param_type.value_encoding).type(1)
        
    def float_range(self,minvar, maxvar,t):
        '''
        Produces a signal with values between minvar and maxvar 
        at a frequency of 1/60 Hz centered at the midpoint 
        between minvar and maxvar.


        This method provides a deterministic function that 
        varies over time and is sinusoidal when graphed.
        '''
        a = (maxvar-minvar)/2
        return np.sin(np.pi * 2 * t /60) * a + (minvar + a)

    def create_sparse(self):
        contexts, funcs = self.create_sparse_params()
        context_ids = [i[1] for i in contexts.itervalues()]

        parsed_param_dict_id = self.dataset_management.create_parameter_dictionary('sparse', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary,parsed_param_dict_id)
        return parsed_param_dict_id
    
    def create_sparse_params(self):
        contexts = {}
        funcs = {}
        

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('float64')))
        t_ctxt.uom = 'seconds since 1900-01-01'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump())
        contexts['time'] = (t_ctxt, t_ctxt_id)

        lat_ctxt = ParameterContext('lat', param_type=SparseConstantType(base_type=ConstantType(value_encoding='float64'), fill_value=-9999.), fill_value=-9999)
        lat_ctxt.uom = 'degree_north'
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='lat', parameter_context=lat_ctxt.dump())
        contexts['lat'] = lat_ctxt, lat_ctxt_id

        lon_ctxt = ParameterContext('lon', param_type=SparseConstantType(base_type=ConstantType(value_encoding='float64'), fill_value=-9999.), fill_value=-9999)
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
        
        quality_ctxt = ParameterContext('quality_flag', param_type=ArrayType())
        quality_ctxt.uom = ''
        quality_ctxt_id = self.dataset_management.create_parameter_context(name='quality_flag', parameter_context=quality_ctxt.dump())
        contexts['quality_flag'] = quality_ctxt, quality_ctxt_id

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

    def create_parsers(self):
        resource_registry       = Container.instance.resource_registry
        from interface.objects import Parser
        resource_registry.create(Parser(name='Global Range Test',module="ion.util.parsers.global_range_test", method="grt_parser"))
        resource_registry.create(Parser(name='Stuck Value Test',module="ion.util.parsers.stuck_value_test", method="stuck_value_test_parser"))
        resource_registry.create(Parser(name="Gradient Test",module="ion.util.parsers.gradient_test", method="gradient_test_parser"))
        resource_registry.create(Parser(name="Spike Test",module="ion.util.parsers.spike_test", method="spike_parser"))




    def create_parsed_params(self):
        
        contexts = {}
        funcs = {}
        

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('float64')))
        t_ctxt.uom = 'seconds since 1900-01-01'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump())
        contexts['time'] = (t_ctxt, t_ctxt_id)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        lat_ctxt.uom = 'degree_north'
        lat_ctxt_id = self.dataset_management.create_parameter_context(name='lat', parameter_context=lat_ctxt.dump())
        contexts['lat'] = lat_ctxt, lat_ctxt_id

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
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
        
        quality_ctxt = ParameterContext('quality_flag', param_type=ArrayType())
        quality_ctxt.uom = ''
        quality_ctxt_id = self.dataset_management.create_parameter_context(name='quality_flag', parameter_context=quality_ctxt.dump())
        contexts['quality_flag'] = quality_ctxt, quality_ctxt_id

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
    
    def create_extended_parsed_contexts(self):
        contexts, funcs = self.create_parsed_params()
        expr, expr_id = funcs['density_L2']
        
        density_lookup_map = {'conductivity':'conductivity_L1', 'temp':'temp_L1', 'pressure':'pressure_L1', 'lat':'lat_lookup', 'lon':'lon_lookup'}
        expr.param_map = density_lookup_map
        density_lookup_ctxt = ParameterContext('density_lookup', param_type=ParameterFunctionType(expr), variability=VariabilityEnum.TEMPORAL)
        density_lookup_ctxt.uom = 'kg m-3'
        density_lookup_ctxt_id = self.dataset_management.create_parameter_context(name='density_lookup', parameter_context=density_lookup_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, density_lookup_ctxt_id)
        contexts['density_lookup'] = density_lookup_ctxt, density_lookup_ctxt_id


        lat_lookup_ctxt = ParameterContext('lat_lookup', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lat_lookup_ctxt.uom = 'degree_north'
        lat_lookup_ctxt.lookup_value = 'lat'
        lat_lookup_ctxt.document_key = ''
        lat_lookup_ctxt_id = self.dataset_management.create_parameter_context(name='lat_lookup', parameter_context=lat_lookup_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, lat_lookup_ctxt_id)
        contexts['lat_lookup'] = lat_lookup_ctxt, lat_lookup_ctxt_id
        

        lon_lookup_ctxt = ParameterContext('lon_lookup', param_type=ConstantType(QuantityType(value_encoding=np.dtype('float32'))), fill_value=-9999)
        lon_lookup_ctxt.uom = 'degree_east'
        lon_lookup_ctxt.lookup_value = 'lon'
        lon_lookup_ctxt.document_key = ''
        lon_lookup_ctxt_id = self.dataset_management.create_parameter_context(name='lon_lookup', parameter_context=lon_lookup_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, lon_lookup_ctxt_id)
        contexts['lon_lookup'] = lon_lookup_ctxt, lon_lookup_ctxt_id

        return contexts, funcs

    def create_qc_contexts(self):
        contexts = {}
        qc_whatever_ctxt = ParameterContext('qc_whatever', param_type=ArrayType())
        qc_whatever_ctxt.uom = '1'
        qc_whatever_ctxt_id = self.dataset_management.create_parameter_context(name='qc_whatever', parameter_context=qc_whatever_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, qc_whatever_ctxt_id)
        contexts['qc_whatever'] = qc_whatever_ctxt, qc_whatever_ctxt_id


        nexpr = NumexprFunction('range_qc', 'min < var > max', ['min','max','var'])
        expr_id = self.dataset_management.create_parameter_function(name='range_qc', parameter_function=nexpr.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)

        pmap = {'min':0, 'max':20, 'var':'temp'}
        nexpr.param_map = pmap
        temp_qc_ctxt = ParameterContext('temp_qc', param_type=ParameterFunctionType(function=nexpr), variability=VariabilityEnum.TEMPORAL)
        temp_qc_ctxt.uom = '1'
        temp_qc_ctxt_id = self.dataset_management.create_parameter_context(name='temp_qc', parameter_context=temp_qc_ctxt.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, temp_qc_ctxt_id)
        contexts['temp_qc'] = temp_qc_ctxt, temp_qc_ctxt_id

        return contexts

    def create_qc_pdict(self):
        contexts, funcs = self.create_parsed_params()
        context_ids = [i[1] for i in contexts.itervalues()]
        
        contexts = self.create_qc_contexts()
        context_ids.extend([i[1] for i in contexts.itervalues()])

        qc_what_pdict_id = self.dataset_management.create_parameter_dictionary('qc_what', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, qc_what_pdict_id)

        return qc_what_pdict_id



    def create_extended_and_platform(self):
        contexts,funcs = self.create_extended_parsed_contexts()
        context_ids = [i[1] for i in contexts.itervalues()]

        extended_pdict_id = self.dataset_management.create_parameter_dictionary('extended_parsed', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, extended_pdict_id)

        context_ids = [i[1] for i in contexts.itervalues() if i[0].name in ['time', 'lat', 'lon', 'internal_timestamp', 'driver_timestamp', 'port_timestamp', 'preferred_timestamp']]
        platform_pdict_id = self.dataset_management.create_parameter_dictionary('platform_eng', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, platform_pdict_id)

        return extended_pdict_id


    def create_extended_parsed(self):
        contexts,funcs = self.create_extended_parsed_contexts()
        context_ids = [i[1] for i in contexts.itervalues()]

        extended_pdict_id = self.dataset_management.create_parameter_dictionary('extended_parsed', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, extended_pdict_id)

        return extended_pdict_id

    def create_lookups(self):
        contexts = self.create_lookup_contexts()
        context_ids = [i[1] for i in contexts.itervalues()]

        lookup_pdict_id = self.dataset_management.create_parameter_dictionary('lookups', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary,lookup_pdict_id)
        return lookup_pdict_id

    def create_global_range_function(self):
        func = PythonFunction('global_range_test','ion_functions.qc.qc_functions','dataqc_globalrangetest_minmax',['dat','dat_min','dat_max'])
        func_id = self.dataset_management.create_parameter_function(name='global_range_test', parameter_function=func.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, func_id)
        return func

    def create_spike_test_function(self):
        func = PythonFunction('dataqc_spiketest','ion_functions.qc.qc_functions','dataqc_spiketest',['dat','acc','N','L'])
        func_id = self.dataset_management.create_parameter_function(name='dataqc_spiketest', parameter_function=func.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, func_id)
        return func

    def create_stuck_value_test_function(self):
        func = PythonFunction('dataqc_stuckvaluetest','ion_functions.qc.qc_functions','dataqc_stuckvaluetest',["x","reso","num"])
        func_id = self.dataset_management.create_parameter_function(name='dataqc_stuckvaluetest', parameter_function=func.dump())
        self.addCleanup(self.dataset_management.delete_parameter_function, func_id)
        return func

    def create_simple_qc(self):
        contexts = {}
        types_manager = TypesManager(self.dataset_management,None,None)
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('float64')))
        t_ctxt.uom = 'seconds since 1900-01-01'
        t_ctxt_id = self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, t_ctxt_id)
        contexts['time'] = (t_ctxt, t_ctxt_id)
        
        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.dtype('float32')), fill_value=-9999)
        temp_ctxt.uom = 'deg_C'
        temp_ctxt.ooi_short_name = 'TEMPWAT'
        temp_ctxt.qc_contexts = types_manager.make_qc_functions('temp','TEMPWAT',lambda *args, **kwargs : None)
        temp_ctxt_id = self.dataset_management.create_parameter_context(name='temp', parameter_context=temp_ctxt.dump(), ooi_short_name='TEMPWAT')
        self.addCleanup(self.dataset_management.delete_parameter_context, temp_ctxt_id)
        contexts['temp'] = temp_ctxt, temp_ctxt_id

        return contexts

    def create_simple_qc_pdict(self):
        types_manager = TypesManager(self.dataset_management,None,None)
        self.create_global_range_function()
        self.create_spike_test_function()
        self.create_stuck_value_test_function()
        contexts = self.create_simple_qc()
        context_ids = [i[1] for i in contexts.itervalues()]
        context_ids.extend(contexts['temp'][0].qc_contexts)
        for qc_context in contexts['temp'][0].qc_contexts:
            context_ids.extend(types_manager.get_lookup_value_ids(DatasetManagementService.get_parameter_context(qc_context)))
        pdict_id = self.dataset_management.create_parameter_dictionary('simple_qc', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)

        return pdict_id


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

        offset_ctxt = ParameterContext(name='offset_a', param_type=SparseConstantType(base_type=ConstantType(value_encoding='float64'), fill_value=-9999.))
        offset_ctxt.uom = ''
        offset_ctxt.lookup_value = 'offset_a'
        offset_ctxt.document_key = ''
        offset_ctxt_id = self.dataset_management.create_parameter_context(name='offset_a', parameter_context=offset_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, offset_ctxt_id)
        contexts['offset_a'] = offset_ctxt, offset_ctxt_id

        offsetb_ctxt = ParameterContext('offset_b', param_type=SparseConstantType(base_type=ConstantType(value_encoding='float64'), fill_value=-9999))
        offsetb_ctxt.uom = ''
        offsetb_ctxt.lookup_value = 'offset_b'
        offsetb_ctxt.document_key = 'coefficient_document'
        offsetb_ctxt_id = self.dataset_management.create_parameter_context(name='offset_b', parameter_context=offsetb_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, offsetb_ctxt_id)
        contexts['offset_b'] = offsetb_ctxt, offsetb_ctxt_id
        
        offsetc_ctxt = ParameterContext('offset_c', param_type=SparseConstantType(base_type=ConstantType(value_encoding='float64'), fill_value=-9999))
        offsetc_ctxt.uom = ''
        offsetc_ctxt.lookup_value = 'offset_c'
        offsetc_ctxt.document_key = '$designator_OFFSETC'
        offsetc_ctxt_id = self.dataset_management.create_parameter_context(name='offset_c', parameter_context=offsetc_ctxt.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, offsetc_ctxt_id)
        contexts['offset_c'] = offsetc_ctxt, offsetc_ctxt_id

        func = NumexprFunction('calibrated', 'temp + offset', ['temp','offset'], param_map={'temp':'temp', 'offset':'offset_a'})
        func.lookup_values = ['LV_offset']
        calibrated = ParameterContext('calibrated', param_type=ParameterFunctionType(func, value_encoding='float32'), fill_value=-9999)
        calibrated.uom = 'deg_C'
        calibrated_id = self.dataset_management.create_parameter_context(name='calibrated', parameter_context=calibrated.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, calibrated_id)
        contexts['calibrated'] = calibrated, calibrated_id

        func = NumexprFunction('calibrated_b', 'temp + offset_a + offset_b', ['temp','offset_a', 'offset_b'], param_map={'temp':'temp', 'offset_a':'offset_a', 'offset_b':'offset_b'})
        func.lookup_values = ['LV_offset_a', 'LV_offset_b']
        calibrated_b = ParameterContext('calibrated_b', param_type=ParameterFunctionType(func, value_encoding='float32'), fill_value=-9999)
        calibrated_b.uom = 'deg_C'
        calibrated_b_id = self.dataset_management.create_parameter_context(name='calibrated_b', parameter_context=calibrated_b.dump())
        self.addCleanup(self.dataset_management.delete_parameter_context, calibrated_b_id)
        contexts['calibrated_b'] = calibrated_b, calibrated_b_id

        return contexts
