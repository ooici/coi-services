#!/usr/bin/env python
'''
@author Luke Campbell
@file ion/services/dm/test/test_dynamic_parameters.py
@description Integration Tests for Dynamic Parameters
'''


from ion.services.dm.test.dm_test_case import DMTestCase
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from nose.plugins.attrib import attr
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
import numpy as np

@attr('INT', group='dm')
class TestDynamicParameters(DMTestCase):
    def setUp(self):
        DMTestCase.setUp(self)

        self.ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = self.ph.create_simple_cc_pdict()

        self.stream_def_id = self.pubsub_management.create_stream_definition('Calibration Coefficients', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, self.stream_def_id)

    def test_coefficient_compatibility(self):
        data_product_id = self.create_data_product(name='Calibration Coefficient Test Data product', stream_def_id=self.stream_def_id)

        self.data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, data_product_id)

        rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
        rdt['time'] = np.arange(10)
        rdt['temp'] = [10] * 10
        rdt['cc_coefficient'] = [2] * 10
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        dataset_monitor.event.wait(10)

        rdt2 = RecordDictionaryTool.load_from_granule(self.data_retriever.retrieve(dataset_id))
        np.testing.assert_array_equal(rdt2['offset'],[12]*10)



