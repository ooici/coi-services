#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell at ASAScience dot com>
@file ion/services/dm/utility/test/test_types.py
@date Fri Jan 18 10:45:05 EST 2013
'''

from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

from ion.services.dm.utility.types import get_fill_value, get_parameter_type
from ion.services.dm.utility.granule import RecordDictionaryTool
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_values import get_value_class
from coverage_model.coverage import SimpleDomainSet

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from ion.services.dm.utility.granule_utils import time_series_domain
from pyon.ion.stream import StandaloneStreamPublisher

from interface.objects import DataProduct
from coverage_model import ArrayType, QuantityType, ConstantRangeType, RecordType, ConstantType, CategoryType

import numpy as np
import gevent


@attr('UNIT')
class TestTypes(PyonTestCase):
    def setUp(self):
        PyonTestCase.setUp(self)
    
    def get_context(self, ptype, encoding, fill_value, codeset=None):
        ptype = get_parameter_type(ptype, encoding, codeset)
        context = ParameterContext(name='test', param_type=ptype)
        context.fill_value = get_fill_value(fill_value, encoding, ptype)
        return context

    def get_pval(self, context):
        return get_value_class(context.param_type, SimpleDomainSet((20,)))

    def rdt_to_granule(self, context, value_array, comp_val=None):
        
        pdict = ParameterDictionary()
        pdict.add_context(context)

        rdt = RecordDictionaryTool(param_dictionary=pdict)
        rdt['test'] = value_array

        granule = rdt.to_granule()
        rdt2 = RecordDictionaryTool.load_from_granule(granule)

        testval = comp_val or value_array
        actual = rdt2['test']


        if isinstance(testval, basestring):
            self.assertEquals(testval, actual)
        else:
            self.assertTrue(np.array_equal(testval, actual))

    def test_quantity_type(self):
        ptype      = 'quantity'
        encodings  = ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'float32', 'float64']
        fill_value = '9'

        for encoding in encodings:
            context = self.get_context(ptype, encoding, fill_value)
            paramval = self.get_pval(context)
            paramval[:] = np.arange(20)
            self.assertTrue((paramval[:] == np.arange(20)).all())
        self.rdt_to_granule(context, np.arange(20))


    def test_string_type(self):
        ptype      = 'quantity'
        encoding   = 'S8'
        fill_value = 'empty'


        context = self.get_context(ptype, encoding, fill_value)
        paramval = self.get_pval(context)
        
        paramval[:] = [context.fill_value] * 20
        [self.assertEquals(paramval[i], context.fill_value) for i in xrange(20)]
        paramval[:] = ['hi'] * 20
        [self.assertEquals(paramval[i], 'hi') for i in xrange(20)]
        
        ptype      = 'str'
        encoding   = 'vlen'
        fill_value = 'empty'


        context = self.get_context(ptype, encoding, fill_value)
        paramval = self.get_pval(context)
        
        paramval[:] = [context.fill_value] * 20
        [self.assertEquals(paramval[i], context.fill_value) for i in xrange(20)]
        paramval[:] = ['hi'] * 20
        [self.assertEquals(paramval[i], 'hi') for i in xrange(20)]

        self.rdt_to_granule(context, ['hi'] * 20)

    def test_string_arrays(self):
        ptype = 'array<quantity>'
        encoding = 'str'
        fill_value = 'none'
    
        context = self.get_context(ptype, encoding, fill_value)
        paramval = self.get_pval(context)
        
        paramval[:] = [context.fill_value] * 20
        [self.assertEquals(paramval[i], context.fill_value) for i in xrange(20)]
        paramval[:] = ['hi'] * 20
        [self.assertEquals(paramval[i], 'hi') for i in xrange(20)]

        self.rdt_to_granule(context,['hi'] * 20)


    def test_category_type(self):
        ptype      = 'category<int8:str>'
        encoding   = 'int8'
        codeset    = '{0: "off", 1: "on"}'
        fill_value = '0'
    
        context = self.get_context(ptype, encoding, fill_value, codeset)
        paramval = self.get_pval(context)
        
        for key in context.param_type.categories:
            self.assertIsInstance(key,np.int8)

        paramval[:] = [context.fill_value] * 20
        [self.assertEquals(paramval[i], 'off') for i in xrange(20)]

        paramval[:] = [1] * 20
        [self.assertEquals(paramval[i], 'on') for i in xrange(20)]

        self.rdt_to_granule(context, [1] * 20, ['on'] * 20)


    def test_const_str(self):
        ptype      = 'constant<str>'
        encoding   = 'S12'
        fill_value = 'empty'

        context = self.get_context(ptype, encoding, fill_value)
        paramval = self.get_pval(context)
        
        paramval[:] = context.fill_value
        [self.assertEquals(paramval[i], context.fill_value) for i in xrange(20)]

        paramval[:] = 'hi'
        [self.assertEquals(paramval[i], 'hi') for i in xrange(20)]

        long_str = 'this string is too long'

        paramval[0] = long_str
        [self.assertEquals(paramval[i], long_str[:12]) for i in xrange(20)]

        self.rdt_to_granule(context, 'hi')

    def test_const(self):
        ptype      = 'constant<quantity>'
        encoding   = 'uint8'
        fill_value = '12'

        context = self.get_context(ptype, encoding, fill_value)
        paramval = self.get_pval(context)

        [self.assertEquals(paramval[i], context.fill_value) for i in xrange(20)]

        paramval[:] = 2
        [self.assertEquals(paramval[i], 2) for i in xrange(20)]

        self.rdt_to_granule(context, 2)

    def test_boolean(self):
        ptype      = 'boolean'
        encoding   = 'int8'
        fill_value = 'false'

        context = self.get_context(ptype, encoding, fill_value)
        paramval = self.get_pval(context)
        
        paramval[:] = context.fill_value
        [self.assertEquals(paramval[i], context.fill_value) for i in xrange(20)]

        paramval[:] = [1] * 20
        [self.assertTrue(paramval[i]) for i in xrange(20)]

        self.assertEquals(get_fill_value('true',encoding), 1, ptype)

        self.rdt_to_granule(context, [1] * 20, [True] * 20)

    
    def test_range_type(self):
        ptype      = 'range<quantity>'
        encoding   = 'int32'
        fill_value = '(-9999, -9998)'

        context = self.get_context(ptype, encoding, fill_value)
        paramval = self.get_pval(context)

        print '>>>>>>'
        print context.fill_value
        print type(context.fill_value)
        print type(get_fill_value(fill_value, encoding, context.param_type))

        [self.assertEquals(paramval[i], context.fill_value) for i in xrange(20)]
        paramval[:] = (0,1000)
        [self.assertEquals(paramval[i], (0,1000)) for i in xrange(20)]

        self.rdt_to_granule(context, (0,1000), (0,1000))


    def test_bad_codeset(self):
        ptype      = 'category<int8:str>'
        encoding   = 'int8'
        codeset    = '{{0:"hi"}'

        self.assertRaises(TypeError, get_parameter_type, ptype, encoding, codeset)

    def test_invalid_str_len(self):
        ptype = 'constant<str>'
        encoding = 'Sfour'
        
        self.assertRaises(TypeError, get_parameter_type, ptype, encoding)
        
        ptype = 'constant<str>'
        encoding = 'int8'
        
        self.assertRaises(TypeError, get_parameter_type, ptype, encoding)
        
        ptype = 'quantity'
        encoding = 'Sfour'
        
        self.assertRaises(TypeError, get_parameter_type, ptype, encoding)

    def test_invalid_range(self):
        ptype    = 'range<str>'
        encoding = ''

        self.assertRaises(TypeError, get_parameter_type, ptype, encoding)


    def test_str_fill(self):
        ptype      = 'quantity'
        encoding   = 'S8'
        fill_value = 'now'

        context = self.get_context(ptype,encoding,fill_value)
        paramval = self.get_pval(context)
        
        [self.assertEquals(paramval[i], 'now') for i in xrange(20)]

    def test_invalid_fill(self):
        encoding = 'opaque'
        fill_value = 'a'

        self.assertRaises(TypeError, get_fill_value,fill_value, encoding)

    def test_invalid_range_fill(self):
        ptype = 'range<quantity>'
        encoding = 'int32'
        fill_value = '-9999'

        ptype = get_parameter_type(ptype,encoding)

        self.assertRaises(TypeError, get_fill_value,fill_value, encoding, ptype)



    def test_record_type(self):
        ptype = 'record<>'
        encoding = ''
        fill_value = ''

        context = self.get_context(ptype, encoding, fill_value)
        paramval = self.get_pval(context)

        [self.assertEquals(paramval[i], None) for i in xrange(20)]

        paramval[:] = [{0:'a'}] * 20
        [self.assertEquals(paramval[i], {0:'a'}) for i in xrange(20)]

        self.rdt_to_granule(context, [{0:'a'}] * 20)

    def test_bad_ptype(self):
        self.assertRaises(TypeError, get_parameter_type, 'flimsy','','')


@attr('EXHAUSTIVE')
class ExhaustiveParameterTest(IonIntegrationTestCase):
    def setUp(self):
        self.i=0
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2params.yml')

        self.dataset_management      = DatasetManagementServiceClient()
        self.pubsub_management       = PubsubManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()
        self.resource_registry       = self.container.resource_registry
        self.data_retriever          = DataRetrieverServiceClient()

        pdicts, _ = self.resource_registry.find_resources(restype='ParameterDictionary', id_only=False)
        self.dp_ids = []
        for pdict in pdicts:
            stream_def_id = self.pubsub_management.create_stream_definition(pdict.name, parameter_dictionary_id=pdict._id)
            dp_id = self.make_dp(stream_def_id)
            if dp_id: self.dp_ids.append(dp_id)

    def make_dp(self, stream_def_id):
        tdom, sdom = time_series_domain()
        tdom = tdom.dump()
        sdom = sdom.dump()
        stream_def = self.resource_registry.read(stream_def_id)
        dp_obj = DataProduct(
                name=stream_def.name,
                description=stream_def.name,
                processing_level_code='Parsed_Canonical',
                temporal_domain = tdom,
                spatial_domain = sdom)


        data_product_id = self.data_product_management.create_data_product(dp_obj, stream_definition_id=stream_def_id)
        self.data_product_management.activate_data_product_persistence(data_product_id)
        return data_product_id

    def fill_values(self, ptype, size):
        if isinstance(ptype, ArrayType):
            return ['blah'] * size
        elif isinstance(ptype, QuantityType):
            return np.sin(np.arange(size, dtype=ptype.value_encoding) * 2 * np.pi / 3)
        elif isinstance(ptype, RecordType):
            return [{'record': 'ok'}] * size
        elif isinstance(ptype, ConstantRangeType):
            return (1,1000)
        elif isinstance(ptype, ConstantType):
            return np.dtype(ptype.value_encoding).type(1)
        elif isinstance(ptype, CategoryType):
            return ptype.categories.keys()[0]
        else:
            return


    def wait_until_we_have_enough_granules(self, dataset_id='',data_size=40):
        '''
        Loops until there is a sufficient amount of data in the dataset
        '''
        done = False
        with gevent.Timeout(40):
            while not done:
                granule = self.data_retriever.retrieve_last_data_points(dataset_id, 1)
                rdt     = RecordDictionaryTool.load_from_granule(granule)
                extents = self.dataset_management.dataset_extents(dataset_id, rdt._pdict.temporal_parameter_name)[0]
                if rdt[rdt._pdict.temporal_parameter_name] and rdt[rdt._pdict.temporal_parameter_name][0] != rdt._pdict.get_context(rdt._pdict.temporal_parameter_name).fill_value and extents >= data_size:
                    done = True
                else:
                    gevent.sleep(0.2)

    def write_to_data_product(self,data_product_id):

        dataset_ids, _ = self.resource_registry.find_objects(data_product_id, 'hasDataset', id_only=True)
        dataset_id = dataset_ids.pop()

        stream_ids , _ = self.resource_registry.find_objects(data_product_id, 'hasStream', id_only=True)
        stream_id = stream_ids.pop()
        stream_def_ids, _ = self.resource_registry.find_objects(stream_id, 'hasStreamDefinition', id_only=True)
        stream_def_id = stream_def_ids.pop()

        route = self.pubsub_management.read_stream_route(stream_id)

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)

        time_param = rdt._pdict.temporal_parameter_name
        if time_param is None:
            print '%s has no temporal parameter' % self.resource_registry.read(data_product_id).name 
            return
        rdt[time_param] = np.arange(40)


        for field in rdt.fields:
            if field == rdt._pdict.temporal_parameter_name:
                continue
            rdt[field] = self.fill_values(rdt._pdict.get_context(field).param_type,40)

        publisher = StandaloneStreamPublisher(stream_id, route)
        publisher.publish(rdt.to_granule())

        self.wait_until_we_have_enough_granules(dataset_id,40)


        granule = self.data_retriever.retrieve(dataset_id)
        rdt_out = RecordDictionaryTool.load_from_granule(granule)

        bad = []

        for field in rdt.fields:
            if not np.array_equal(rdt[field], rdt_out[field]):
                print '%s' % field
                print '%s != %s' % (rdt[field], rdt_out[field])
                bad.append(field)

        return bad

        
    def test_data_products(self):
        bad_data_products = {}
        for dp_id in self.dp_ids:
            try:
                bad_fields = self.write_to_data_product(dp_id)
                if bad_fields:
                    bad_data_products[dp_id] = "Couldn't write and retrieve %s." % bad_fields
            except:
                import traceback
                bad_data_products[dp_id] = traceback.format_exc()


        for dp_id, tb in bad_data_products.iteritems():
            print '----------'
            print 'Problem with %s' % self.resource_registry.read(dp_id).name
            print tb
            print '----------'


        if bad_data_products:
            raise AssertionError('There are bad parameter dictionaries.')


