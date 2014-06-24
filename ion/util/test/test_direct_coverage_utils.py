#!/usr/bin/env python

"""
@package ion.util.test.test_direct_coverage_utils
@file ion/util/test/test_direct_coverage_utils.py
@author Christopher Mueller
@brief 
"""

import os
import mock
import time
import unittest
import numpy as np
from gevent.event import Event
from nose.plugins.attrib import attr
from collections import OrderedDict

from coverage_model import AbstractCoverage, ParameterContext, SparseConstantType, BooleanType
from ion.services.dm.test.dm_test_case import DMTestCase, Streamer
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.util.direct_coverage_utils import DirectCoverageAccess, SimpleDelimitedParser
from pyon.ion.resource import PRED
from pyon.util.unit_test import PyonTestCase


from subprocess import call
import re
not_have_h5stat = call('which h5stat'.split(), stdout=open('/dev/null','w'))
if not not_have_h5stat:
    from subprocess import check_output
    from distutils.version import StrictVersion
    output = check_output('h5stat -V'.split())
    version_str = re.match(r'.*(\d+\.\d+\.\d+).*', output).groups()[0]
    h5stat_correct_version = StrictVersion(version_str) >= StrictVersion('1.8.9')


@attr('INT', group='dm')
class TestDirectCoverageAccess(DMTestCase):

    def use_monitor(self, dataset_id, samples=10, wait=10):
        for x in xrange(samples):
            monitor = DatasetMonitor(dataset_id)
            monitor.event.wait(wait)
            monitor.stop()

    def make_cal_data_product(self):
        # Get a precompiled parameter dictionary with basic ctd fields
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',id_only=True)
        context_ids = self.dataset_management.read_parameter_contexts(pdict_id, id_only=True)

        # Add a handful of Calibration Coefficient parameters
        for cc in ['cc_ta0', 'cc_ta1', 'cc_ta2', 'cc_ta3', 'cc_toffset']:
            c = ParameterContext(cc, param_type=SparseConstantType(value_encoding='float32', fill_value=-9999))
            c.uom = '1'
            context_ids.append(self.dataset_management.create_parameter_context(cc, c.dump()))

        pdict_id = self.dataset_management.create_parameter_dictionary('calcoeff_dict', context_ids, temporal_context='time')

        data_product_id = self.create_data_product('calcoeff_dp', pdict_id=pdict_id)
        self.activate_data_product(data_product_id)
        dataset_id, _ = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)

        return data_product_id, dataset_id[0]

    def make_manual_upload_data_product(self):
        # Get a precompiled parameter dictionary with basic ctd fields
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',id_only=True)
        context_ids = self.dataset_management.read_parameter_contexts(pdict_id, id_only=True)

        # Add a handful of Calibration Coefficient parameters
        for cc in ['temp_hitl_qc', 'cond_hitl_qc']:
            c = ParameterContext(cc, param_type=BooleanType())
            c.uom = '1'
            context_ids.append(self.dataset_management.create_parameter_context(cc, c.dump()))

        pdict_id = self.dataset_management.create_parameter_dictionary('manup_dict', context_ids, temporal_context='time')

        data_product_id = self.create_data_product('manup_dp', pdict_id=pdict_id)
        self.activate_data_product(data_product_id)
        dataset_id, _ = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)

        return data_product_id, dataset_id[0]

    def make_ctd_data_product(self):
        data_product_id = self.create_data_product('test_prod', param_dict_name='ctd_parsed_param_dict')
        self.activate_data_product(data_product_id)
        dataset_id, _ = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)

        return data_product_id, dataset_id[0]

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_dca_ingestion_pause_resume(self):
        data_product_id, dataset_id = self.make_ctd_data_product()

        streamer = Streamer(data_product_id, interval=1)
        self.addCleanup(streamer.stop)

        # Let a couple samples accumulate
        self.use_monitor(dataset_id, samples=2)

        # Go into DCA and get an editable handle to the coverage
        with DirectCoverageAccess() as dca:
            with dca.get_editable_coverage(dataset_id) as cov: # <-- This pauses ingestion
                monitor = DatasetMonitor(dataset_id)
                monitor.event.wait(7) # <-- ~7 Samples should accumulate on the ingestion queue
                self.assertFalse(monitor.event.is_set()) # Verifies that nothing was processed (i.e. ingestion is actually paused)
                monitor.stop()

        # Stop the streamer
        streamer.stop()

        cont = True
        while cont:
            monitor = DatasetMonitor(dataset_id)
            if not monitor.event.wait(10):
                cont = False
            monitor.stop()

        with DirectCoverageAccess() as dca:
            with dca.get_read_only_coverage(dataset_id) as cov:
                self.assertGreaterEqual(cov.num_timesteps, 8)

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_dca_coverage_reuse(self):
        data_product_id, dataset_id = self.make_ctd_data_product()

        streamer = Streamer(data_product_id, interval=1)
        self.addCleanup(streamer.stop)

        # Let a couple samples accumulate
        self.use_monitor(dataset_id, samples=2)

        with DirectCoverageAccess() as dca:
            import os
            cpth = dca.get_coverage_path(dataset_id)
            self.assertTrue(os.path.exists(cpth), msg='Path does not exist: %s' % cpth)

            with dca.get_read_only_coverage(dataset_id) as cov:
                self.assertFalse(cov.closed)

            self.assertTrue(cov.closed)

            with dca.get_editable_coverage(dataset_id) as cov:
                self.assertFalse(cov.closed)

            self.assertTrue(cov.closed)

            with dca.get_read_only_coverage(dataset_id) as cov:
                self.assertFalse(cov.closed)

            self.assertTrue(cov.closed)

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_dca_not_managed_warnings(self):
        data_product_id, dataset_id = self.make_ctd_data_product()

        dca = DirectCoverageAccess()

        with mock.patch('ion.util.direct_coverage_utils.warn_user') as warn_user_mock:
            dca.pause_ingestion(dataset_id)
            self.assertEqual(warn_user_mock.call_args_list[0],
                             mock.call('Warning: Pausing ingestion when not using a context manager is potentially unsafe - '
                                       'be sure to resume ingestion for all streams by calling self.clean_up(streams=True)'))

        with mock.patch('ion.util.direct_coverage_utils.warn_user') as warn_user_mock:
            cov = dca.get_read_only_coverage(dataset_id)
            self.assertEqual(warn_user_mock.call_args_list[0],
                             mock.call('Warning: Coverages will remain open until they are closed or go out of scope - '
                                       'be sure to close coverage instances when you are finished working with them or call self.clean_up(ro_covs=True)'))

        with mock.patch('ion.util.direct_coverage_utils.warn_user') as warn_user_mock:
            cov = dca.get_editable_coverage(dataset_id)
            self.assertEqual(warn_user_mock.call_args_list[0],
                             mock.call('Warning: Pausing ingestion when not using a context manager is potentially unsafe - '
                                       'be sure to resume ingestion for all streams by calling self.clean_up(streams=True)'))
            self.assertEqual(warn_user_mock.call_args_list[1],
                             mock.call('Warning: Coverages will remain open until they are closed or go out of scope - '
                                       'be sure to close coverage instances when you are finished working with them or call self.clean_up(w_covs=True)'))

        dca.clean_up()


@attr('UNIT', group='dm')
class TestSimpleDelimitedParser(PyonTestCase):

    def test_parse_with_config(self):
        parser = SimpleDelimitedParser.get_parser('test_data/testmanualupload.csv', 'test_data/testmanualupload.yml')
        props = parser.properties
        want_props = {
            'data_url': 'test_data/testmanualupload.csv',
            'header_size': 0,
            'delimiter': ',',
            'num_columns': 3,
            'use_column_names': True,
            'dtype': 'int8',
            'fill_val': 0,
            'column_map': OrderedDict([(0, OrderedDict([('name', 'time'), ('dtype', 'int64'), ('fill_val', -9)]))])
        }
        for p in props.keys():
            self.assertEqual(props[p], want_props[p], '%s: %s != %s' % (p, props[p], want_props[p]))

        dat = parser.parse()

        self.assertEqual(dat.dtype.names, ('time', 'temp_hitl_qc', 'cond_hitl_qc'))

        want_vals = {
            'time': np.arange(10, dtype='int64'),
            'temp_hitl_qc': np.array([0, 0, 0, 0, 1, 0, 0, 1, 0, 0], dtype=bool),
            'cond_hitl_qc': np.array([1, 0, 1, 0, 0, 0, 1, 1, 0, 0], dtype=bool)
        }

        for x in dat.dtype.names:
            np.testing.assert_array_equal(dat[x], want_vals[x])

    def test_parse_no_config(self):
        parser = SimpleDelimitedParser.get_parser('test_data/testmanualupload.csv')
        props = parser.properties
        want_props = {
            'data_url': 'test_data/testmanualupload.csv',
            'header_size': 0,
            'delimiter': ',',
            'num_columns': None,
            'use_column_names': True,
            'dtype': 'float32',
            'fill_val': -999,
            'column_map': None
        }
        for p in props.keys():
            self.assertEqual(props[p], want_props[p])

        dat = parser.parse()

        self.assertEqual(dat.dtype.names, ('time', 'temp_hitl_qc', 'cond_hitl_qc'))

        want_vals = {
            'time': np.arange(10, dtype='int64'),
            'temp_hitl_qc': np.array([0, 0, 0, 0, 1, 0, 0, 1, 0, 0], dtype=bool),
            'cond_hitl_qc': np.array([1, 0, 1, 0, 0, 0, 1, 1, 0, 0], dtype=bool)
        }

        for x in dat.dtype.names:
            np.testing.assert_array_equal(dat[x], want_vals[x])

    def test_parse_no_names(self):
        parser = SimpleDelimitedParser.get_parser('test_data/testmanualupload.csv', 'test_data/testmanualupload.yml')
        parser.use_column_names = False
        parser.header_size = 1
        props = parser.properties
        want_props = {
            'data_url': 'test_data/testmanualupload.csv',
            'header_size': 1,
            'delimiter': ',',
            'num_columns': 3,
            'use_column_names': False,
            'dtype': 'int8',
            'fill_val': 0,
            'column_map': OrderedDict([(0, OrderedDict([('name', 'time'), ('dtype', 'int64'), ('fill_val', -9)]))])
        }
        for p in props.keys():
            self.assertEqual(props[p], want_props[p])

        dat = parser.parse()

        self.assertEqual(dat.dtype.names, ('time', 'f1', 'f2'))

        want_vals = {
            'time': np.arange(10, dtype='int64'),
            'f1': np.array([0, 0, 0, 0, 1, 0, 0, 1, 0, 0], dtype=bool),
            'f2': np.array([1, 0, 1, 0, 0, 0, 1, 1, 0, 0], dtype=bool)
        }

        for x in dat.dtype.names:
            np.testing.assert_array_equal(dat[x], want_vals[x])

    def test_write_default_config(self):
        import tempfile
        tdir = tempfile.mkdtemp()
        outpth = os.path.join(tdir, 'defconfig.yml')

        SimpleDelimitedParser.write_default_parser_config(outpth)

        parser = SimpleDelimitedParser.get_parser('test_data/testmanualupload.csv', outpth)
        props = parser.properties
        want_props = {
            'data_url': 'test_data/testmanualupload.csv',
            'header_size': 0,
            'delimiter': ',',
            'num_columns': None,
            'use_column_names': True,
            'dtype': 'float32',
            'fill_val': -999,
            'column_map': None
        }
        for p in props.keys():
            self.assertEqual(props[p], want_props[p])

        import shutil
        shutil.rmtree(outpth, ignore_errors=True)
