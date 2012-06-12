#!/usr/bin/env python

"""
@package ion.agents.data.handlers.test.test_ruv_data_handler
@file ion/agents/data/handlers/test/test_ruv_data_handler
@author Christopher Mueller
@brief Test cases for ruv_data_handler
"""

from pyon.public import log
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import get_safe
from nose.plugins.attrib import attr
from mock import patch, Mock, call
import unittest

from ion.agents.data.handlers.handler_utils import list_file_info
from ion.agents.data.handlers.ruv_data_handler import RuvDataHandler, RuvParser
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, ExternalDataProvider, DataProduct, DataSourceModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution, DataSource

@attr('UNIT', group='eoi')
class TestRuvDataHandlerUnit(PyonTestCase):

    def setUp(self):
        self.__rr_cli = Mock()
        pass

    def test__init_acquisition_cycle_no_ext_ds_res(self):
        config = {}
        self.assertRaises(SystemError, RuvDataHandler._init_acquisition_cycle, config)

    def test__init_acquisition_cycle_ext_ds_res(self):
        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        edres.dataset_description.parameters['base_url'] = 'test_data/dir'
        edres.dataset_description.parameters['list_pattern'] = 'test_filter'
        config = {'external_dataset_res':edres}
        RuvDataHandler._init_acquisition_cycle(config)

        self.assertIn('ds_params', config)
        ds_params = config['ds_params']

        self.assertIn('base_url',ds_params)
        self.assertEquals(ds_params['base_url'],'test_data/dir')
        self.assertIn('list_pattern',ds_params)
        self.assertEquals(ds_params['list_pattern'], 'test_filter')

    def test__constraints_for_new_request(self):
        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

#        old_list = [
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_0000.ruv',
#             '04-Jun-2012 20:43',
#             '136K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_0100.ruv',
#             '04-Jun-2012 21:43',
#             '135K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_0200.ruv',
#             '04-Jun-2012 22:42',
#             '137K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_0300.ruv',
#             '04-Jun-2012 23:41',
#             '136K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_0400.ruv',
#             '05-Jun-2012 00:41',
#             '150K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_0500.ruv',
#             '05-Jun-2012 01:41',
#             '142K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_0600.ruv',
#             '05-Jun-2012 02:41',
#             '138K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_0700.ruv',
#             '05-Jun-2012 03:41',
#             '136K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_0800.ruv',
#             '05-Jun-2012 04:41',
#             '138K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_0900.ruv',
#             '05-Jun-2012 05:40',
#             '147K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_1000.ruv',
#             '05-Jun-2012 06:40',
#             '143K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_1100.ruv',
#             '05-Jun-2012 07:40',
#             '148K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_1200.ruv',
#             '05-Jun-2012 08:40',
#             '147K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_1300.ruv',
#             '05-Jun-2012 09:39',
#             '148K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_1400.ruv',
#             '05-Jun-2012 10:38',
#             '143K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_1500.ruv',
#             '05-Jun-2012 11:43',
#             '143K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_1600.ruv',
#             '05-Jun-2012 12:43',
#             '146K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_1700.ruv',
#             '05-Jun-2012 13:42',
#             '134K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_1800.ruv',
#             '05-Jun-2012 14:42',
#             '143K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_1900.ruv',
#             '05-Jun-2012 15:42',
#             '148K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_2000.ruv',
#             '05-Jun-2012 16:41',
#             '157K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_2100.ruv',
#             '05-Jun-2012 17:41',
#             '160K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_2200.ruv',
#             '05-Jun-2012 18:41',
#             '158K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_05_2300.ruv',
#             '05-Jun-2012 19:41',
#             '148K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_0000.ruv',
#             '05-Jun-2012 20:40',
#             '140K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_0100.ruv',
#             '05-Jun-2012 21:40',
#             '133K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_0200.ruv',
#             '05-Jun-2012 22:40',
#             '143K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_0300.ruv',
#             '05-Jun-2012 23:39',
#             '156K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_0400.ruv',
#             '06-Jun-2012 00:39',
#             '146K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_0500.ruv',
#             '06-Jun-2012 01:39',
#             '147K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_0600.ruv',
#             '06-Jun-2012 02:39',
#             '147K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_0700.ruv',
#             '06-Jun-2012 03:43',
#             '148K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_0800.ruv',
#             '06-Jun-2012 04:42',
#             '137K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_0900.ruv',
#             '06-Jun-2012 05:42',
#             '130K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1000.ruv',
#             '06-Jun-2012 06:42',
#             '129K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1100.ruv',
#             '06-Jun-2012 07:42',
#             '136K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1200.ruv',
#             '06-Jun-2012 08:42',
#             '137K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1300.ruv',
#             '06-Jun-2012 09:41',
#             '151K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1400.ruv',
#             '06-Jun-2012 10:41',
#             '153K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1500.ruv',
#             '06-Jun-2012 11:41',
#             '156K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1600.ruv',
#             '06-Jun-2012 12:41',
#             '157K'),
#            ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1700.ruv',
#             '06-Jun-2012 13:40',
#             '161K'),]

        old_list = [
            ('test_data/ruv/RDLm_SEAB_2012_06_06_1200.ruv', 1339006638.0, 119066),
            ('test_data/ruv/RDLm_SEAB_2012_06_06_1300.ruv', 1339006629.0, 109316),
            ('test_data/ruv/RDLm_SEAB_2012_06_06_1400.ruv', 1339006521.0, 113411),
        ]

#        old_list = None

        config = {
            'external_dataset_res':edres,
            #            'new_data_check':None,
            'new_data_check':old_list,
            'ds_params':{
                # These would be extracted from the dataset_description.parameters during _init_acquisition_cycle, but since that isn't called, just add them here
    #            'base_url':'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/',
                #CBM: What's the difference between RDLi and RDLm???
    #            'pattern':'<a href="(RDLm.*\.ruv)">.*(\d{2}-[a-zA-Z]{3}-\d{4} \d{2}:\d{2})\s*(\d{1,5}\w)',# Appended to base to filter files; Either a shell style pattern (for filesystem) or regex (for http/ftp)
                'base_url':'test_data/ruv',
                'list_pattern':'RDLm_SEAB_*.ruv',
                'date_pattern':'%Y %m %d %H %M',
                'date_extraction_pattern': 'RDLm_SEAB_([\d]{4})_([\d]{2})_([\d]{2})_([\d]{2})([\d]{2}).ruv'
            }
        }
        ret = RuvDataHandler._constraints_for_new_request(config)
        log.warn('test__new_data_constraints: {0}'.format(config))
        self.assertEqual(ret['new_files'], list_file_info(config['ds_params']['base_url'], config['ds_params']['list_pattern']))

    def test__get_data(self):
        config = {
            'constraints':{
                'new_files':[
#                    'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1300.ruv',
#                    'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1400.ruv',
                    ('test_data/ruv/RDLm_SEAB_2012_06_06_1500.ruv', 1339006491.0, 113996),
                    ('test_data/ruv/RDLm_SEAB_2012_06_06_1600.ruv', 1339006513.0, 122576),
                ]
            }
        }

        for x in RuvDataHandler._get_data(config):
            log.debug('test__get_data: {0}'.format(x))

#    def test__get_data_with_exception(self):
#        config = {
#            'constraints':{
#                'new_files':[
#                    #                    'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1300.ruv',
#                    #                    'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLi_BELM_2012_06_06_1400.ruv',
#                    ('test_data/ruv/RDLm_SEAB_2012_06_06_1500.ruv', 1339006491.0, 113996),
#                    ('test_data/ruv/RDLm_SEAB_2012_06_06_1600.ruv', 1339006513.0, 122576),
#                    ]
#            }
#        }
#
#        for x in RuvDataHandler._get_data(config):
#            log.debug(x)

    def test__get_archive_constraints(self):
        config = {
            'ds_params':{
                # These would be extracted from the dataset_description.parameters during _init_acquisition_cycle, but since that isn't called, just add them here
                #            'base_url':'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/',
                #            'pattern':'<a href="([^"]*\.ruv)">.*(\d{2}-[a-zA-Z]{3}-\d{4} \d{2}:\d{2})\s*(\d{3,5}\w)',# Appended to base to filter files; Either a shell style pattern (for filesystem) or regex (for http/ftp)
                'base_url':'test_data/ruv',
                'list_pattern':'RDLm_SEAB_*.ruv',
                'date_pattern':'%Y %m %d %H %M',
                'date_extraction_pattern': 'RDLm_SEAB_([\d]{4})_([\d]{2})_([\d]{2})_([\d]{2})([\d]{2}).ruv'
                },
            'constraints' : {
                'start_time': 1338998400,
                'end_time': 1339012800
            }
        }
        ret = RuvDataHandler._constraints_for_historical_request(config)
        log.warn('test__get_archive_constraints: {0}'.format(ret['new_files']))
        self.assertEqual(ret['new_files'], list_file_info(config['ds_params']['base_url'], config['ds_params']['list_pattern']))





