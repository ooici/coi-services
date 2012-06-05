#!/usr/bin/env python

"""
@package ion.agents.data.handlers.test.test_slocum_data_handler
@file ion/agents/data/handlers/test/test_slocum_data_handler
@author Christopher Mueller
@brief Test cases for slocum_data_handler
"""

from pyon.public import log
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from mock import patch, Mock, call, sentinel
import unittest

from ion.agents.data.handlers.slocum_data_handler import SlocumDataHandler, SlocumParser
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, ExternalDataProvider, DataProduct, DataSourceModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution, DataSource

@attr('UNIT', group='eoi')
class TestSlocumDataHandlerUnit(PyonTestCase):

    def setUp(self):
        self.__rr_cli = Mock()
        pass

    def test__init_acquisition_cycle_no_ext_ds_res(self):
        config = {}
        SlocumDataHandler._init_acquisition_cycle(config)
        self.assertEqual(config,config)

    @patch('ion.agents.data.handlers.slocum_data_handler.SlocumParser')
    def test__init_acquisition_cycle_ext_ds_res(self, SlocumParser_mock):
        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        edres.dataset_description.parameters['dataset_path'] = 'test_data/ru05-2012-021-0-0-sbd.dat'
        edres.dataset_description.parameters['header_count'] = 17
        config = {'external_dataset_res':edres}
        SlocumDataHandler._init_acquisition_cycle(config)

        SlocumParser_mock.assert_called_once_with(edres.dataset_description.parameters['dataset_path'])
        self.assertIn('parser', config)
        self.assertEquals(config['parser'],SlocumParser_mock())

    def test__new_data_constraints(self):
        ret = SlocumDataHandler._new_data_constraints({})
        self.assertIsInstance(ret, dict)

        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        edres.dataset_description.parameters['dataset_path'] = 'test_data/ru05-2012-021-0-0-sbd.dat'
        edres.dataset_description.parameters['header_count'] = 17
        edres.dataset_description.parameters['filename_pattern'] = 'ru05-*-sbd.dat' # Shell-style pattern - NOT regex: see glob documentation
        config = {
            'external_dataset_res':edres,
#            'new_data_check':None,
            'new_data_check':['test_data/ru05-2012-021-0-0-sbd.dat', 'test_data/ru05-2012-022-0-0-sbd.dat',],
        }
        ret = SlocumDataHandler._new_data_constraints(config)
        log.warn(ret)

    def test__get_data(self):
        pass

