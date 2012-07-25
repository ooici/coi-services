#!/usr/bin/env python

'''
@package pyon.ion.granule.test.test_granule
@file pyon/ion/granule/test/test_granule.py
@author David Stuebe
@author Tim Giguere
@brief https://confluence.oceanobservatories.org/display/CIDev/R2+Construction+Data+Model
'''


import unittest
import numpy as np
from nose.plugins.attrib import attr
from ion.services.dm.utility.granule.taxonomy import TaxyTool
from ion.services.dm.utility.granule.granule import build_granule, combine_granules
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

@attr('UNIT', group='dm')
class GranuleTestCase(unittest.TestCase):

    def test_build_granule_and_load_from_granule_with_taxonomy(self):

        #Define a taxonomy and add sets. add_taxonomy_set takes one or more names and assigns them to one handle
        tx = TaxyTool()
        tx.add_taxonomy_set('temp', 'long_temp_name')
        tx.add_taxonomy_set('cond', 'long_cond_name')
        tx.add_taxonomy_set('pres', 'long_pres_name')
        tx.add_taxonomy_set('rdt')
        # map is {<local name>: <granule name or path>}

        #Use RecordDictionaryTool to create a record dictionary. Send in the taxonomy so the Tool knows what to expect
        rdt = RecordDictionaryTool(taxonomy=tx)

        #Create some arrays and fill them with random values
        temp_array = np.random.standard_normal(100)
        cond_array = np.random.standard_normal(100)
        pres_array = np.random.standard_normal(100)

        #Use the RecordDictionaryTool to add the values. This also would work if you used long_temp_name, etc.
        rdt['temp'] = temp_array
        rdt['cond'] = cond_array
        rdt['pres'] = pres_array

        #You can also add in another RecordDictionaryTool, providing the taxonomies are the same.
        rdt2 = RecordDictionaryTool(taxonomy=tx)
        rdt2['temp'] = temp_array
        rdt['rdt'] = rdt2


        g = build_granule(data_producer_id='john', taxonomy=tx, record_dictionary=rdt)

        l_tx = TaxyTool.load_from_granule(g)

        l_rd = RecordDictionaryTool.load_from_granule(g)

        # Make sure we got back the same Taxonomy Object
        self.assertEquals(l_tx._t, tx._t)
        self.assertEquals(l_tx.get_handles('temp'), tx.get_handles('temp'))
        self.assertEquals(l_tx.get_handles('testing_2'), tx.get_handles('testing_2'))


        # Now test the record dictionary object
        self.assertEquals(l_rd._rd, rdt._rd)
        self.assertEquals(l_rd._tx._t, rdt._tx._t)


        for k, v in l_rd.iteritems():
            self.assertIn(k, rdt)

            if isinstance(v, np.ndarray):
                self.assertTrue( (v == rdt[k]).all())

            else:
                self.assertEquals(v._rd, rdt[k]._rd)

    def test_build_granule_and_load_from_granule(self):
        pdict = ParameterDictionary()

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 01-01-1970'
        pdict.add_context(t_ctxt)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=np.dtype('float32')))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=np.dtype('float32')))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        pdict.add_context(lon_ctxt)

        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.dtype('float32')))
        temp_ctxt.uom = 'degree_Celsius'
        pdict.add_context(temp_ctxt)

        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.dtype('float32')))
        cond_ctxt.uom = 'unknown'
        pdict.add_context(cond_ctxt)

        pres_ctxt = ParameterContext('pres', param_type=QuantityType(value_encoding=np.dtype('float32')))
        pres_ctxt.uom = 'unknown'
        pdict.add_context(pres_ctxt)

        rdt = RecordDictionaryTool(param_dictionary=pdict)

        #Create some arrays and fill them with random values
        temp_array = np.random.standard_normal(100)
        cond_array = np.random.standard_normal(100)
        pres_array = np.random.standard_normal(100)
        time_array = np.random.standard_normal(100)
        lat_array = np.random.standard_normal(100)
        lon_array = np.random.standard_normal(100)

        #Use the RecordDictionaryTool to add the values. This also would work if you used long_temp_name, etc.
        rdt['temp'] = temp_array
        rdt['conductivity'] = cond_array
        rdt['pres'] = pres_array
        rdt['time'] = time_array
        rdt['lat'] = lat_array
        rdt['lon'] = lon_array

        g = build_granule(data_producer_id='john', record_dictionary=rdt, param_dictionary=pdict)

        l_pd = ParameterDictionary.load(g.param_dictionary)

        #l_tx = TaxyTool.load_from_granule(g)

        l_rd = RecordDictionaryTool.load_from_granule(g)

        # Make sure we got back the same Taxonomy Object
        #self.assertEquals(l_pd, pdict)
        self.assertEquals(l_pd.ord_from_key('temp'), pdict.ord_from_key('temp'))
        self.assertEquals(l_pd.ord_from_key('conductivity'), pdict.ord_from_key('conductivity'))


        # Now test the record dictionary object
        self.assertEquals(l_rd._rd, rdt._rd)
        #self.assertEquals(l_rd._param_dict, rdt._param_dict)


        for k, v in l_rd.iteritems():
            self.assertIn(k, rdt)

            if isinstance(v, np.ndarray):
                self.assertTrue( (v == rdt[k]).all())

            else:
                self.assertEquals(v._rd, rdt[k]._rd)

    def test_combine_granule(self):
        tt = TaxyTool()
        tt.add_taxonomy_set('a')

        rdt = RecordDictionaryTool(tt)
        rdt['a'] = np.array([1,2,3])

        granule1 = build_granule('test',tt,rdt)

        rdt = RecordDictionaryTool(tt)
        rdt['a'] = np.array([4,5,6])
        
        granule2 = build_granule('test',tt,rdt)

        granule3 = combine_granules(granule1,granule2)

        rdt = RecordDictionaryTool.load_from_granule(granule3)

        self.assertTrue(np.allclose(rdt['a'],np.array([1,2,3,4,5,6])))
