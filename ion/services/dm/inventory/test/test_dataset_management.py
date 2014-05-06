'''
@author Luke Campbell <luke.s.campbell@gmail.com>
@file ion/services/dm/inventory/test/dataset_management_test.py
@description Unit and Integration test implementations for the data set management service class.
'''
from pyon.core.exception import NotFound
from pyon.util.int_test import IonIntegrationTestCase

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule_utils import time_series_domain

from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import ParameterFunction, ParameterContext, ParameterFunctionType as PFT
from coverage_model import NumexprFunction, ParameterDictionary, ParameterContext as CoverageParameterContext

from nose.plugins.attrib import attr



@attr('INT', group='dm')
class DatasetManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.resource_registry  = ResourceRegistryServiceClient()
        self.dataset_management = DatasetManagementServiceClient()

    def test_dataset_crud(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        tdom, sdom = time_series_domain()
        dataset_id = self.dataset_management.create_dataset(name='ctd_dataset', parameter_dictionary_id=pdict_id)

        ds_obj = self.dataset_management.read_dataset(dataset_id)
        self.assertEquals(ds_obj.name, 'ctd_dataset')
        
        ds_obj.name = 'something different'
        self.dataset_management.update_dataset(ds_obj)
        ds_obj2 = self.dataset_management.read_dataset(dataset_id)
        self.assertEquals(ds_obj.name, ds_obj2.name)
    
    def test_context_crud(self):
        context_ids = self.create_contexts()
        context_id = context_ids.pop()

        ctxt = self.dataset_management.read_parameter_context(context_id)
        context = DatasetManagementService.get_coverage_parameter(ctxt)
        self.assertIsInstance(context, CoverageParameterContext)

        self.dataset_management.delete_parameter_context(context_id)

        with self.assertRaises(NotFound):
            self.dataset_management.read_parameter_context(context_id)



    def test_pfunc_crud(self):
        contexts, funcs = self.create_pfuncs()
        context_ids = [context_id for context_id in contexts.itervalues()]

        pdict_id = self.dataset_management.create_parameter_dictionary(name='functional_pdict', parameter_context_ids=context_ids, temporal_context='time')
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)

        expr_id = funcs['CONDWAT_L1']
        expr = self.dataset_management.read_parameter_function(expr_id)
        func_class = DatasetManagementService.get_coverage_function(expr)
        self.assertIsInstance(func_class, NumexprFunction)

    def test_pdict_crud(self):
        context_ids = self.create_contexts()
        pdict_res_id = self.dataset_management.create_parameter_dictionary(name='pdict1', parameter_context_ids=context_ids, temporal_context='time')

        pdict_contexts = self.dataset_management.read_parameter_contexts(parameter_dictionary_id=pdict_res_id, id_only=True)

        pdict = DatasetManagementService.get_parameter_dictionary(pdict_res_id)
        self.assertIsInstance(pdict, ParameterDictionary)
        self.assertTrue('time_test' in pdict)
        self.assertEquals(pdict.identifier, pdict_res_id)

        self.assertEquals(set(pdict_contexts), set(context_ids))

        self.dataset_management.delete_parameter_dictionary(parameter_dictionary_id=pdict_res_id)
        with self.assertRaises(NotFound):
            self.dataset_management.read_parameter_dictionary(parameter_dictionary_id=pdict_res_id)

    def create_contexts(self):
        context_ids = []
        cond = ParameterContext(name='condictivity_test',
                                parameter_type='quantity',
                                value_encoding='float32',
                                units='1',
                                fill_value=0)
        context_ids.append(self.dataset_management.create_parameter(cond))

        pres = ParameterContext(name='pressure_test',
                                parameter_type='quantity',
                                value_encoding='float32',
                                units='Pa',
                                fill_value=0)
        context_ids.append(self.dataset_management.create_parameter(pres))

        sal = ParameterContext(name='salinity_test', 
                               parameter_type='quantity',
                               value_encoding='float32',
                               units='psu',
                               fill_value=0)
        context_ids.append(self.dataset_management.create_parameter(sal))

        temp = ParameterContext(name='temp_test', 
                                parameter_type='quantity',
                                value_encoding='float32',
                                units='degree_C',
                                fill_value=0)
        context_ids.append(self.dataset_management.create_parameter(temp))

        time_test = ParameterContext(name='time_test', 
                                     parameter_type='quantity',
                                     value_encoding='float32',
                                     units='seconds since 1970-01-01',
                                     fill_value=0)
        context_ids.append(self.dataset_management.create_parameter(time_test))

        return context_ids

    def create_pfuncs(self):
        contexts = {}
        funcs = {}

        time_ = ParameterContext(name='TIME', 
                                 parameter_type='quantity',
                                 value_encoding='float32',
                                 units='seconds since 1900-01-01',
                                 fill_value=0)

        t_ctxt_id = self.dataset_management.create_parameter(time_)
        contexts['TIME'] = t_ctxt_id

        lat = ParameterContext(name='LAT', 
                               parameter_type='sparse',
                               value_encoding='float32',
                               units='degrees_north',
                               fill_value=-9999.)
        lat_ctxt_id = self.dataset_management.create_parameter(lat)
        contexts['LAT'] = lat_ctxt_id

        lon = ParameterContext(name='LON', 
                               parameter_type="sparse",
                               value_encoding='float32',
                               units='degrees_east',
                               fill_value=-9999)
        lon_ctxt_id = self.dataset_management.create_parameter(lon)
        contexts['LON'] = lon_ctxt_id

        # Independent Parameters

        # Temperature - values expected to be the decimal results of conversion from hex
        temp = ParameterContext(name='TEMPWAT_L0', 
                                parameter_type='quantity',
                                value_encoding='float32',
                                units='deg_C')
        temp_ctxt_id = self.dataset_management.create_parameter(temp)
        contexts['TEMPWAT_L0'] = temp_ctxt_id

        # Conductivity - values expected to be the decimal results of conversion from hex
        cond = ParameterContext(name='CONDWAT_L0', 
                                parameter_type='quantity',
                                value_encoding='float32',
                                units='S m-1')
        cond_ctxt_id = self.dataset_management.create_parameter(cond)
        contexts['CONDWAT_L0'] = cond_ctxt_id

        # Pressure - values expected to be the decimal results of conversion from hex
        press = ParameterContext(name='PRESWAT_L0', 
                                 parameter_type='quantity',
                                 value_encoding='float32',
                                 units='dbar')
        press_ctxt_id = self.dataset_management.create_parameter(press)
        contexts['PRESWAT_L0'] = press_ctxt_id


        # Dependent Parameters

        # TEMPWAT_L1 = (TEMPWAT_L0 / 10000) - 10
        tl1_func = '(T / 10000) - 10'
        tempwat_f = ParameterFunction(name='TEMPWAT_L1',
                                      function_type=PFT.NUMEXPR,
                                      function=tl1_func,
                                      args=['T'])
        expr_id = self.dataset_management.create_parameter_function(tempwat_f)
        funcs['TEMPWAT_L1'] = expr_id

        tl1_pmap = {'T': 'TEMPWAT_L0'}
        tempL1 = ParameterContext(name='TEMPWAT_L1', 
                                  parameter_type='function',
                                  parameter_function_id=expr_id,
                                  parameter_function_map=tl1_pmap,
                                  value_encoding='float32',
                                  units='deg_C')
        tempL1_ctxt_id = self.dataset_management.create_parameter(tempL1)
        contexts['TEMPWAT_L1'] = tempL1_ctxt_id

        # CONDWAT_L1 = (CONDWAT_L0 / 100000) - 0.5
        cl1_func = '(C / 100000) - 0.5'
        condwat_f = ParameterFunction(name='CONDWAT_L1',
                                      function_type=PFT.NUMEXPR,
                                      function=cl1_func,
                                      args=['C'])
        expr_id = self.dataset_management.create_parameter_function(condwat_f)
        funcs['CONDWAT_L1'] = expr_id

        cl1_pmap = {'C': 'CONDWAT_L0'}
        condL1 = ParameterContext(name='CONDWAT_L1', 
                                       parameter_type='function',
                                       parameter_function_id=expr_id,
                                       parameter_function_map=cl1_pmap,
                                       value_encoding='float32',
                                       units='S m-1')
        condL1_ctxt_id = self.dataset_management.create_parameter(condL1)
        contexts['CONDWAT_L1'] = condL1_ctxt_id

        # Equation uses p_range, which is a calibration coefficient - Fixing to 679.34040721
        #   PRESWAT_L1 = (PRESWAT_L0 * p_range / (0.85 * 65536)) - (0.05 * p_range)
        pl1_func = '(P * p_range / (0.85 * 65536)) - (0.05 * p_range)'
        preswat_f = ParameterFunction(name='PRESWAT_L1',
                                      function_type=PFT.NUMEXPR,
                                      function=pl1_func,
                                      args=['P', 'p_range'])
        expr_id = self.dataset_management.create_parameter_function(preswat_f)
        funcs['PRESWAT_L1'] = expr_id
        
        pl1_pmap = {'P': 'PRESWAT_L0', 'p_range': 679.34040721}
        presL1 = ParameterContext(name='PRESWAT_L1', 
                                  parameter_type='function',
                                  parameter_function_id=expr_id,
                                  parameter_function_map=pl1_pmap,
                                  value_encoding='float32',
                                  units='dbar')
        presL1_ctxt_id = self.dataset_management.create_parameter(presL1)
        contexts['PRESWAT_L1'] = presL1_ctxt_id

        
        # A magic function that may or may not exist actually forms the line below at runtime.
        cond_f = ParameterFunction(name='condwat10',
                                   function_type=PFT.NUMEXPR,
                                   function='C*10',
                                   args=['C'])
        expr_id = self.dataset_management.create_parameter_function(cond_f)
        cond10 = ParameterContext(name='c10',
                                  parameter_type='function',
                                  parameter_function_id=expr_id,
                                  parameter_function_map={'C':'CONDWAT_L1'},
                                  value_encoding='float32',
                                  units='1')
        cond10_id = self.dataset_management.create_parameter(cond10)
        contexts['C10'] = cond10_id
        
        # Density & practical salinity calucluated using the Gibbs Seawater library - available via python-gsw project:
        #       https://code.google.com/p/python-gsw/ & http://pypi.python.org/pypi/gsw/3.0.1

        # PRACSAL = gsw.SP_from_C((CONDWAT_L1 * 10), TEMPWAT_L1, PRESWAT_L1)
        owner = 'gsw'
        sal_func = 'SP_from_C'
        sal_arglist = ['C', 't', 'p']
        pracsal_f = ParameterFunction(name='PRACSAL',
                                      function_type=PFT.PYTHON,
                                      owner=owner,
                                      function=sal_func,
                                      args=sal_arglist)
        expr_id = self.dataset_management.create_parameter_function(pracsal_f)
        funcs['PRACSAL'] = expr_id

        sal_pmap = {'C': 'c10', 't': 'TEMPWAT_L1', 'p': 'PRESWAT_L1'}
        sal_ctxt = ParameterContext(name='PRACSAL', 
                                    parameter_type='function',
                                    parameter_function_id=expr_id,
                                    parameter_function_map=sal_pmap,
                                    value_encoding='float32',
                                    units='g kg-1')
        sal_ctxt_id = self.dataset_management.create_parameter(sal_ctxt)
        contexts['PRACSAL'] = sal_ctxt_id

        # absolute_salinity = gsw.SA_from_SP(PRACSAL, PRESWAT_L1, longitude, latitude)
        # conservative_temperature = gsw.CT_from_t(absolute_salinity, TEMPWAT_L1, PRESWAT_L1)
        # DENSITY = gsw.rho(absolute_salinity, conservative_temperature, PRESWAT_L1)
        return contexts, funcs

    def test_verify_contexts(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name(name='ctd_parsed_param_dict', id_only=True)
        pcontexts = self.dataset_management.read_parameter_contexts(parameter_dictionary_id=pdict_id)
        for pcontext in pcontexts:
            self.assertTrue('fill_value' in pcontext)
            self.assertTrue('reference_urls' in pcontext)
            self.assertTrue('internal_name' in pcontext)
            self.assertTrue('display_name' in pcontext)
            self.assertTrue('standard_name' in pcontext)
            self.assertTrue('ooi_short_name' in pcontext)
            self.assertTrue('description' in pcontext)
            self.assertTrue('precision' in pcontext)


