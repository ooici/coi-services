#!/usr/bin/env python

"""
@package ion.agents.data.handlers.test.test_netcdf_data_handler
@file ion/agents/data/handlers/test/test_netcdf_data_handler
@author Christopher Mueller
@brief Test cases for netcdf_data_handler
"""

from pyon.public import log
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from mock import patch, Mock, call
import unittest

from ion.agents.data.handlers.netcdf_data_handler import NetcdfDataHandler
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, ExternalDataProvider, DataProduct, DataSourceModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution, DataSource
from netCDF4 import Dataset

@attr('UNIT', group='eoi')
class TestNetcdfDataHandlerUnit(PyonTestCase):

    def setUp(self):
        self.__rr_cli = Mock()
        pass

    def test__init_acquisition_cycle_no_ext_ds_res(self):
        config = {}
        NetcdfDataHandler._init_acquisition_cycle(config)
        self.assertEqual(config,config)

    def test__init_acquisition_cycle_ext_ds_res(self):
        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        edres.dataset_description.parameters['dataset_path'] = 'test_data/usgs.nc'
        config = {'external_dataset_res':edres}
        NetcdfDataHandler._init_acquisition_cycle(config)
        self.assertIn('dataset_object', config)
        self.assertTrue(isinstance(config['dataset_object'], Dataset))

    def test__new_data_constraints(self):
        pass

    def test__get_data(self):
        pass

