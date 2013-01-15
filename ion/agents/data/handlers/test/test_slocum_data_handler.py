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
from mock import patch, Mock, MagicMock, sentinel
from ion.agents.data.handlers.handler_utils import list_file_info
from ion.agents.data.handlers.slocum_data_handler import SlocumDataHandler
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from interface.objects import ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Granule


@attr('UNIT', group='eoi')
class TestSlocumDataHandlerUnit(PyonTestCase):

    def setUp(self):
        self.__rr_cli = Mock()
        pass

    def test__init_acquisition_cycle_no_ext_ds_res(self):
        config = {}
        self.assertRaises(SystemError, SlocumDataHandler._init_acquisition_cycle, config)

    def test__init_acquisition_cycle_ext_ds_res(self):
        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        edres.dataset_description.parameters['base_url'] = 'test_data/dir'
        edres.dataset_description.parameters['header_count'] = 12
        edres.dataset_description.parameters['pattern'] = 'test_filter'
        config = {'external_dataset_res': edres}
        SlocumDataHandler._init_acquisition_cycle(config)

        self.assertIn('ds_params', config)
        ds_params = config['ds_params']

        self.assertIn('header_count', ds_params)
        self.assertEquals(ds_params['header_count'], 12)
        self.assertIn('base_url', ds_params)
        self.assertEquals(ds_params['base_url'], 'test_data/dir')
        self.assertIn('pattern', ds_params)
        self.assertEquals(ds_params['pattern'], 'test_filter')

    def test__constraints_for_new_request(self):
    #        ret = SlocumDataHandler._constraints_for_new_request({})
    #        self.assertIsInstance(ret, dict)

        old_list = [
            ('test_data/slocum/ru05-2012-021-0-0-sbd.dat', 1337261358.0, 521081),
            ('test_data/slocum/ru05-2012-022-0-0-sbd.dat', 1337261358.0, 521081),
            ]

        #        old_list = None

        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        config = {
            'external_dataset_res': edres,
            'new_data_check': old_list,
            'ds_params': {
                # These would be extracted from the dataset_description.parameters during _init_acquisition_cycle, but since that isn't called, just add them here
                'base_url': 'test_data/slocum/',
                'list_pattern': 'ru05-*-sbd.dat',  # Appended to base to filter files; Either a shell style pattern (for filesystem) or regex (for http/ftp)
                'date_pattern': '%Y %j',
                'date_extraction_pattern': 'ru05-([\d]{4})-([\d]{3})-\d-\d-sbd.dat'
            }
        }
        ret = SlocumDataHandler._constraints_for_new_request(config)
        log.debug('test__constraints_for_new_request: {0}'.format(ret['new_files']))
        self.assertEqual(ret['new_files'], list_file_info(config['ds_params']['base_url'], config['ds_params']['list_pattern']))

    @patch('ion.agents.data.handlers.slocum_data_handler.RecordDictionaryTool')
    def test__get_data(self, RecordDictionaryTool_mock):
        config = {
            'constraints': {
                'new_files': [
                    ('test_data/slocum/ru05-2012-021-0-0-sbd.dat', 1337261358.0, 521081), ]
            },
            'param_dictionary': sentinel.pdict,
            'data_producer_id': sentinel.dprod_id,
            'stream_def': sentinel.stream_def_id
        }

        my_dict = {}

        def setitem(name, val):
            my_dict[name] = val

        retval = MagicMock(spec=RecordDictionaryTool)
        retval.__setitem__ = Mock(side_effect=setitem)
        retval.to_granule.return_value = MagicMock(spec=Granule)
        RecordDictionaryTool_mock.return_value = retval

        for x in SlocumDataHandler._get_data(config):
            self.assertTrue(isinstance(x, Granule))
            retval.to_granule.assert_any_call()
            log.debug(x)

    def test__constraints_for_historical_request(self):
        config = {
            'ds_params': {
                # These would be extracted from the dataset_description.parameters during _init_acquisition_cycle, but since that isn't called, just add them here
                #            'base_url':'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/',
                #            'pattern':'<a href="([^"]*\.ruv)">.*(\d{2}-[a-zA-Z]{3}-\d{4} \d{2}:\d{2})\s*(\d{3,5}\w)',# Appended to base to filter files; Either a shell style pattern (for filesystem) or regex (for http/ftp)
                'base_url': 'test_data/slocum',
                'list_pattern': 'ru05-*-sbd.dat',  # Appended to base to filter files; Either a shell style pattern (for filesystem) or regex (for http/ftp)
                'date_pattern': '%Y %j',
                'date_extraction_pattern': 'ru05-([\d]{4})-([\d]{3})-\d-\d-sbd.dat'},
            'constraints': {
                'start_time': 1327122000,
                'end_time': 1327294800
            }
        }
        ret = SlocumDataHandler._constraints_for_historical_request(config)
        log.debug('test_constraints_for_historical_request: {0}'.format(config))
        self.assertEqual(ret['new_files'], list_file_info(config['ds_params']['base_url'], config['ds_params']['list_pattern']))
