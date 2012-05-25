#!/usr/bin/env python

"""
@package ion.agents.data.handlers.test.test_ruv_data_handler
@file ion/agents/data/handlers/test/test_ruv_data_handler
@author Christopher Mueller
@brief Test cases for ruv_data_handler
"""

from pyon.public import log
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from mock import patch, Mock, call
import unittest

from ion.agents.data.handlers.ruv_data_handler import RuvDataHandler, RuvParser
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, ExternalDataProvider, DataProduct, DataSourceModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution, DataSource

@attr('UNIT', group='eoi')
class TestRuvDataHandlerUnit(PyonTestCase):

    def setUp(self):
        self.__rr_cli = Mock()
        pass

    def test__init_acquisition_cycle_no_ext_ds_res(self):
        config = {}
        RuvDataHandler._init_acquisition_cycle(config)
        self.assertEqual(config,config)

    @patch('ion.agents.data.handlers.ruv_data_handler.RuvParser')
    def test__init_acquisition_cycle_ext_ds_res(self, RuvParser_mock):
        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        edres.dataset_description.parameters['dataset_path'] = 'test_data/RDLi_SEAB_2011_08_24_1600.ruv'
        config = {'external_dataset_res':edres}
        RuvDataHandler._init_acquisition_cycle(config)

        RuvParser_mock.assert_called_once_with(edres.dataset_description.parameters['dataset_path'])
        self.assertIn('parser', config)
        self.assertTrue(config['parser'], RuvParser_mock())

    def test__new_data_constraints(self):
        ret = RuvDataHandler._new_data_constraints({})
        self.assertIsInstance(ret, dict)

    def test__get_data(self):
        pass

