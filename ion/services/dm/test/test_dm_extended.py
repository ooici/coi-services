#!/usr/bin/env python
'''
@author Luke Campbell <Use the force Luke>
@file ion/services/dm/test/test_dm_extended.py
@description Resting place for tests that validate the extended functionalities of DM
'''

from ion.services.dm.test.dm_test_case import DMTestCase
from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDTAlgorithm
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from nose.plugins.attrib import attr
from pyon.util.breakpoint import breakpoint
import numpy as np

@attr('INT',group='dm')
class TestDMExtended(DMTestCase):
    '''
    ion/services/dm/test/test_dm_extended.py:TestDMExtended
    '''
    def setUp(self):
        DMTestCase.setUp(self)
        self.ph = ParameterHelper(self.dataset_management, self.addCleanup)

    def make_array_data_product(self):
        pdict_id = self.ph.crete_simple_array_pdict()
        stream_def_id = self.create_stream_definition('test_array_flow_paths', parameter_dictionary_id=pdict_id)

        data_product_id = self.create_data_product('test_array_flow_paths', stream_def_id)
        self.activate_data_product(data_product_id)
        return data_product_id, stream_def_id
    
    def test_array_visualization(self):
        data_product_id, stream_def_id = self.make_array_data_product()

        # Make a granule with an array type, give it a few values
        # Send it to google_dt transform, verify output

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(10)
        rdt['temp_sample'] = np.arange(10*4).reshape(10,4)
        granule = rdt.to_granule()

        gdt_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('google_dt',id_only=True)
        gdt_stream_def = self.create_stream_definition('gdt', parameter_dictionary_id=gdt_pdict_id)

        gdt_data_granule = VizTransformGoogleDTAlgorithm.execute(granule, params=gdt_stream_def)



    def test_array_flow_paths(self):
        data_product_id, stream_def_id = self.make_array_data_product()

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dm = DatasetMonitor(dataset_id)


        # I need to make sure that we can fill the RDT with its values
        # Test for one timestep
        # Test for multiple timesteps
        # Publishes 
        # Ingests correctly
        # Retrieves correctly

        #--------------------------------------------------------------------------------
        # Ensure that the RDT can be filled with ArrayType values
        #--------------------------------------------------------------------------------
        
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = [0]
        rdt['temp_sample'] = [[0,1,2,3,4]]
        np.testing.assert_array_equal(rdt['temp_sample'], np.array([[0,1,2,3,4]]))

        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dm.event.wait(10))
        dm.event.clear()

        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['temp_sample'], np.array([[0,1,2,3,4]]))

        #--------------------------------------------------------------------------------
        # Ensure that it deals with multiple values
        #--------------------------------------------------------------------------------

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = [1,2,3]
        rdt['temp_sample'] = [[0,1,2,3,4],[1],[5,5,5,5,5]]

        m = rdt.fill_value('temp_sample') or np.finfo(np.float32).max
        np.testing.assert_equal(m,np.finfo(np.float32).max)
        np.testing.assert_array_equal(rdt['temp_sample'], [[0,1,2,3,4],[1,m,m,m,m],[5,5,5,5,5]])
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dm.event.wait(10))
        dm.event.clear()

        #--------------------------------------------------------------------------------
        # Retrieve and Verify
        #--------------------------------------------------------------------------------

        retrieved_granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(retrieved_granule)
        np.testing.assert_array_equal(rdt['time'], np.array([0,1,2,3]))
        np.testing.assert_array_equal(rdt['temp_sample'], np.array([[0,1,2,3,4],[0,1,2,3,4],[1,m,m,m,m],[5,5,5,5,5]]))


