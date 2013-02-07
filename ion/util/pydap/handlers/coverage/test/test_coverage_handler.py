from nose.plugins.attrib import attr
from pyon.util.unit_test import IonUnitTestCase
from pyon.util.int_test import IonIntegrationTestCase
from coverage_model.coverage import SimplexCoverage
from coverage_model.parameter_types import QuantityType,ConstantRangeType,ArrayType, ConstantType, RecordType, CategoryType, BooleanType
from coverage_model.coverage import GridDomain, CRS, AxisTypeEnum, MutabilityEnum, GridShape
from coverage_model.parameter import ParameterContext, ParameterDictionary 
import numpy as np
import tempfile
import os
from coverage_model.utils import create_guid
from ion.util.pydap.handlers.coverage.coverage_handler import Handler
from pydap.client import open_url
from pyon.public import CFG
from pyon.util.file_sys import FileSystem

@attr('UNIT', group='dm')
class TestPydapCoverageHandlerUnit(IonUnitTestCase):
    
    def setUp(self):
        test_dir = os.path.join(tempfile.gettempdir(), 'cov_tests')
        try:
            os.mkdir(test_dir)
        except Exception:
            pass
        (cov,filename) = _make_coverage(test_dir)
        self.cov = cov
        self._handler = Handler(filename)
        self.nt=5
        self.cov.insert_timesteps(self.nt)
    
    
    def test_parse_constraints_meta(self):
        environ = {'pydap.headers': [], 'pydap.ce': (None, [])}
        dataset = self._handler.parse_constraints(environ)
        cols = dataset['data'].keys()
        params = self.cov.list_parameters()
        expanded_params = self._expand_parameters(params)
        #order matters
        self.assertEquals(cols, expanded_params)
    
    def _expand_parameters(self, params):    
        result = []
        for param_name in params:
            pc = self.cov.get_parameter_context(param_name)
            if isinstance(pc.param_type, ConstantRangeType):
                result.append(param_name+"_low")
                result.append(param_name+"_high")
            elif isinstance(pc.param_type,RecordType):
                result.append(param_name+"_keys")
                result.append(param_name+"_values")
            else:
                result.append(param_name)
        return result

@attr('INT', group='dm')
class TestPydapCoverageHandlerInt(IonIntegrationTestCase):
    
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        path = CFG.get_safe('server.pydap.data_path', "RESOURCE:ext/pydap")
        ext_path = FileSystem.get_extended_url(path)
        self.cov,self.filename = _make_coverage(ext_path)
        self.nt = 5
        self.cov.insert_timesteps(self.nt) 
        self.time_data = [i+1 for i in range(self.nt)]
        self.cov.set_parameter_values("time", value=self.time_data)
        host = CFG.get_safe('container.pydap_gateway.web_server.host', 'localhost')
        port = CFG.get_safe('container.pydap_gateway.web_server.port', '8001')
        self.request_url = "http://"+host+":"+str(port)+os.sep+os.path.basename(self.filename)
    
    def test_parse_constraints_array_list(self):
        input_data = [["larry", "mike", "bob", "harry", "sally"], ["larry", "mike", "bob", "harry", "sally"], ["larry", "mike", "bob", "harry", "sally"], ["larry", "mike", "bob", "harry", "sally"], ["larry", "mike", "bob", "harry", "sally"]]
        #input_data = [["larry"], ["bob"], ["sally"], ["jennifer"], ["fred"]]
        self.cov.set_parameter_values('array',value=input_data)
        dataset = open_url(self.request_url)
        for i,data in enumerate(input_data):
            result = []
            for d in dataset['data']['array_'+str(i)]:
                result.append(d)
            self.assertEqual(result, data)
    
    def test_parse_constraints_array_string(self):
        input_data = ["larry", "bob", "sally", "jennifer", "fred"]
        self.cov.set_parameter_values('array',value=input_data)
        dataset = open_url(self.request_url)
        result = []
        for d in dataset['data']['array']:
            result.append(d)
        self.assertEqual(result, input_data)
    
    def test_parse_constraints_time(self):
        dataset = open_url(self.request_url)
        result = []
        for d in dataset['data']['time']:
            result.append(d)
        self.assertEqual(result, self.time_data)
    
    def test_parse_constraints_quantity(self):
        test_data = [4, 7, 2, 3, 9]
        self.cov.set_parameter_values('quantity', value=test_data)
        dataset = open_url(self.request_url)
        result = []
        for d in dataset['data']['quantity']:
            result.append(d)
        self.assertEqual(result, test_data)
    
    def test_parse_constraints_constant(self):
        test_data = [30] * self.nt
        self.cov.set_parameter_values('constant', value=test_data)
        dataset = open_url(self.request_url)
        result = []
        for d in dataset['data']['constant']:
            result.append(d)
        self.assertEqual(result, test_data)
    
    def test_parse_constraints_boolean(self):
        test_data = [True,False,True,True,False]
        self.cov.set_parameter_values('boolean',value=test_data)
        dataset = open_url(self.request_url)
        result = []
        for d in dataset['data']['boolean']:
            result.append(d)
        self.assertEqual(result, test_data)
    
    def test_parse_constraints_category(self):
        test_data = ["apple","lemon","apple","banana", "lemon"]
        self.cov.set_parameter_values('category',value=test_data)
        dataset = open_url(self.request_url)
        result = []
        for d in dataset['data']['category']:
            result.append(d)
        self.assertEqual(result, test_data)
    
    def test_parse_constraints_range(self):
        input_data = (45.0,60.0)
        test_data_one = [input_data[0]] * self.nt
        test_data_two = [input_data[1]] * self.nt
        self.cov.set_parameter_values('range',value=input_data)
        dataset = open_url(self.request_url)
        result = []
        for d in dataset['data']['range_low']:
            result.append(d)
        self.assertEqual(result, test_data_one)
        result = []
        for d in dataset['data']['range_high']:
            result.append(d)
        self.assertEqual(result, test_data_two)
    
    
    def test_parse_constraints_record(self):
        input_data = [{'key1':'value1'}, {'key2':'value2'}, {'key3':'value3'}, {'key4':'value4'}, {'key5':'value5'}]
        keys = []
        for d in input_data:
            for k in d.keys():
                keys.append(k)
        values = []
        for d in input_data:
            for k in d.values():
                values.append(k)
        self.cov.set_parameter_values('record',value=input_data)
        dataset = open_url(self.request_url)
        result = []
        for d in dataset['data']['record_keys']:
            result.append(d)
        self.assertEqual(result, keys)
        result = []
        for d in dataset['data']['record_values']:
            result.append(d)
        self.assertEqual(result, values)
    
    def tearDown(self):
        self.cov.close()

def _make_coverage(path):
    tcrs = CRS([AxisTypeEnum.TIME])
    scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT, AxisTypeEnum.HEIGHT])

    tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE)
    sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # Dimensionality is excluded for now
        
    pdict = ParameterDictionary()
    t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.int64))
    t_ctxt.axis = AxisTypeEnum.TIME
    t_ctxt.uom = 'seconds since 1970-01-01'
    t_ctxt.fill_value = 0x0
    pdict.add_context(t_ctxt)
    
    lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.float32))
    lat_ctxt.axis = AxisTypeEnum.LAT
    lat_ctxt.uom = 'degree_north'
    lat_ctxt.fill_value = 0e0
    pdict.add_context(lat_ctxt)

    lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.float32))
    lon_ctxt.axis = AxisTypeEnum.LON
    lon_ctxt.uom = 'degree_east'
    lon_ctxt.fill_value = 0e0
    pdict.add_context(lon_ctxt)
    
    cat = {0:'lemon',1:'apple',2:'banana',99:'None'}
    cat_ctxt = ParameterContext('category', param_type=CategoryType(categories=cat))
    cat_ctxt.long_name = "example of category"
    pdict.add_context(cat_ctxt)
    

    dens_ctxt = ParameterContext('quantity', param_type=QuantityType(value_encoding=np.float32))
    dens_ctxt.uom = 'unknown'
    dens_ctxt.fill_value = 0x0
    pdict.add_context(dens_ctxt)
    
    
    const_ctxt = ParameterContext('constant', param_type=ConstantType())
    const_ctxt.long_name = 'example of a parameter of type ConstantType'
    pdict.add_context(const_ctxt)
    
    rec_ctxt = ParameterContext('boolean', param_type=BooleanType())
    rec_ctxt.long_name = 'example of a parameter of type BooleanType'
    pdict.add_context(rec_ctxt)
    
    
    rec_ctxt = ParameterContext('range', param_type=ConstantRangeType())
    rec_ctxt.long_name = 'Range example'
    rec_ctxt.fill_value = 0x0
    pdict.add_context(rec_ctxt)
    
    rec_ctxt = ParameterContext('record', param_type=RecordType())
    rec_ctxt.long_name = 'example of a parameter of type RecordType, will be filled with dictionaries'
    pdict.add_context(rec_ctxt)
    
    serial_ctxt = ParameterContext('array', param_type=ArrayType())
    serial_ctxt.uom = 'unknown'
    serial_ctxt.fill_value = 0x0
    pdict.add_context(serial_ctxt)
    
    guid = create_guid()
    cov = SimplexCoverage(path, guid, name="sample_cov", parameter_dictionary=pdict, temporal_domain=tdom, spatial_domain=sdom)
    
    return (cov,path+os.sep+guid)

