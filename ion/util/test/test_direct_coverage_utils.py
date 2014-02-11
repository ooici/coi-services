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

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_upload_calibration_coefficients(self):
        data_product_id, dataset_id = self.make_cal_data_product()

        streamer = Streamer(data_product_id, interval=0.5)
        self.addCleanup(streamer.stop)

        # Let at least 10 samples accumulate
        self.use_monitor(dataset_id, samples=10)

        # Verify that the CC parameters are fill value
        with DirectCoverageAccess() as dca:
            with dca.get_read_only_coverage(dataset_id) as cov:
                for p in [p for p in cov.list_parameters() if p.startswith('cc_')]:
                    np.testing.assert_equal(cov.get_parameter_values(p, -1), -9999.)

        # Upload the calibration coefficients - this pauses ingestion, performs the upload, and resumes ingestion
        with DirectCoverageAccess() as dca:
            dca.upload_calibration_coefficients(dataset_id, 'test_data/testcalcoeff.csv', 'test_data/testcalcoeff.yml')

        # Let a little more data accumulate
        self.use_monitor(dataset_id, samples=2)

        # Verify that the CC parameters now have the correct values
        want_vals = {
            'cc_ta0': np.float32(1.155787e-03),
            'cc_ta1': np.float32(2.725208e-04),
            'cc_ta2': np.float32(-7.526811e-07),
            'cc_ta3': np.float32(1.716270e-07),
            'cc_toffset': np.float32(0.000000e+00)
        }
        with DirectCoverageAccess() as dca:
            with dca.get_read_only_coverage(dataset_id) as cov:
                for p in [p for p in cov.list_parameters() if p.startswith('cc_')]:
                    np.testing.assert_equal(cov.get_parameter_values(p, -1), want_vals[p])

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_manual_data_upload(self):
        data_product_id, dataset_id = self.make_manual_upload_data_product()

        streamer = Streamer(data_product_id, interval=0.5, simple_time=True)
        self.addCleanup(streamer.stop)

        # Let at least 10 samples accumulate
        self.use_monitor(dataset_id, samples=10)

        # Verify that the HITL parameters are fill value
        with DirectCoverageAccess() as dca:
            with dca.get_read_only_coverage(dataset_id) as cov:
                fillarr = np.array([False]*10)
                for p in [p for p in cov.list_parameters() if p.endswith('_hitl_qc')]:
                    np.testing.assert_equal(cov.get_parameter_values(p, slice(None, 10)), fillarr)

        # Upload the data - this pauses ingestion, performs the upload, and resumes ingestion
        with DirectCoverageAccess() as dca:
            dca.manual_upload(dataset_id, 'test_data/testmanualupload.csv', 'test_data/testmanualupload.yml')

        streamer.stop()

        # Wait a moment for ingestion to catch up
        self.use_monitor(dataset_id, samples=2)

        # Verify that the HITL parameters now have the correct values
        want_vals = {
            'temp_hitl_qc': np.array([0, 0, 0, 0, 1, 0, 0, 1, 0, 0], dtype=bool),
            'cond_hitl_qc': np.array([1, 0, 1, 0, 0, 0, 1, 1, 0, 0], dtype=bool)
        }
        with DirectCoverageAccess() as dca:
            with dca.get_read_only_coverage(dataset_id) as cov:
                for p in [p for p in cov.list_parameters() if p.endswith('_hitl_qc')]:
                    np.testing.assert_equal(cov.get_parameter_values(p, slice(None, 10)), want_vals[p])

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    @unittest.skipIf(not_have_h5stat, 'h5stat is not accessible in current PATH')
    @unittest.skipIf(not not_have_h5stat and not h5stat_correct_version, 'HDF is the incorrect version: %s' % version_str)
    def test_run_coverage_doctor(self):
        data_product_id, dataset_id = self.make_ctd_data_product()

        # Run coverage doctor on an empty coverage
        with DirectCoverageAccess() as dca:
            # it's not corrupt yet , so it shouldn't need repair
            self.assertEqual(dca.run_coverage_doctor(dataset_id, data_product_id=data_product_id), 'Repair Not Necessary')

            # Get the path to the master file so we can mess it up!
            with dca.get_editable_coverage(dataset_id) as cov:
                mpth = cov._persistence_layer.master_manager.file_path

            # Mess up the master file
            with open(mpth, 'wb') as f:
                f.write('mess you up!')

            # Repair the coverage
            self.assertEqual(dca.run_coverage_doctor(dataset_id, data_product_id=data_product_id), 'Repair Successful')

        # Stream some data to the coverage
        streamer = Streamer(data_product_id, interval=0.5)
        self.addCleanup(streamer.stop)

        # Let at least 10 samples accumulate
        self.use_monitor(dataset_id, samples=10)

        # Run coverage doctor on a coverage with data
        with DirectCoverageAccess() as dca:
            # it's not corrupt yet , so it shouldn't need repair
            self.assertEqual(dca.run_coverage_doctor(dataset_id, data_product_id=data_product_id), 'Repair Not Necessary')
            with dca.get_read_only_coverage(dataset_id) as cov:
                self.assertIsInstance(cov, AbstractCoverage)

            # Mess up the master file
            with open(mpth, 'wb') as f:
                f.write('mess you up!')

            # Repair the coverage
            self.assertEqual(dca.run_coverage_doctor(dataset_id, data_product_id=data_product_id), 'Repair Successful')

        # Let at least 1 sample arrive
        self.use_monitor(dataset_id, samples=1)

        with DirectCoverageAccess() as dca:
            with dca.get_read_only_coverage(dataset_id) as cov:
                self.assertIsInstance(cov, AbstractCoverage)

    @attr('LOCOINT')
    @unittest.skip('Complex Coverages not supported in R2')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_fill_temporal_gap(self):
        from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

        data_product_id, dataset_id = self.make_ctd_data_product()
        pdict = DatasetManagementService.get_parameter_dictionary_by_name('ctd_parsed_param_dict')

        streamer = Streamer(data_product_id, interval=0.5)
        self.addCleanup(streamer.stop)

        self.use_monitor(dataset_id, samples=10)

        streamer.stop()

        gap_times = []
        waiter = Event()
        while not waiter.wait(1):
            gap_times.append(time.time() + 2208988800)
            if len(gap_times) == 10:
                waiter.set()

        # Simulate a gap by appending a new SimplexCoverage with times after the above gap
        with DirectCoverageAccess() as dca:
            dca.pause_ingestion(dataset_id)

            with dca.get_read_only_coverage(dataset_id) as cov:
                beforecovtimes = cov.get_time_values()

            with DatasetManagementService._create_simplex_coverage(dataset_id, pdict, None, None) as scov:
                scov.insert_timesteps(3)
                now = time.time() + 2208988800
                ts = [now, now + 1, now + 2]
                scov.set_time_values(ts)
                aftercovtimes = scov.get_time_values()

            DatasetManagementService._splice_coverage(dataset_id, scov)

        # Start streaming data again
        streamer.start()

        # Create the gap-fill coverage
        with DatasetManagementService._create_simplex_coverage(dataset_id, pdict, None, None) as scov:
            scov.insert_timesteps(len(gap_times))
            scov.set_time_values(gap_times)
            gap_cov_path = scov.persistence_dir
            gapcovtimes = scov.get_time_values()

        # Fill the gap and capture times to do some assertions
        with DirectCoverageAccess() as dca:
            with dca.get_read_only_coverage(dataset_id) as cov:
                otimes = cov.get_time_values()

            dca.fill_temporal_gap(dataset_id, gap_coverage_path=gap_cov_path)

            with dca.get_read_only_coverage(dataset_id) as cov:
                agtimes = cov.get_time_values()

        self.use_monitor(dataset_id, samples=5)

        with DirectCoverageAccess() as dca:
            with dca.get_read_only_coverage(dataset_id) as cov:
                ntimes = cov.get_time_values()

        self.assertLess(len(otimes), len(agtimes))
        self.assertLess(len(agtimes), len(ntimes))

        bctl = len(beforecovtimes)
        gctl = len(gapcovtimes)
        actl = len(aftercovtimes)
        np.testing.assert_array_equal(beforecovtimes, ntimes[:bctl])
        np.testing.assert_array_equal(gapcovtimes, ntimes[bctl+1:bctl+gctl+1])
        np.testing.assert_array_equal(aftercovtimes, ntimes[bctl+gctl+1:bctl+gctl+actl+1])
        np.testing.assert_array_equal(agtimes, ntimes[:len(agtimes)])

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Host requires file-system access to coverage files, CEI mode does not support.')
    def test_repair_temporal_geometry(self):
        data_product_id, dataset_id = self.make_ctd_data_product()

        streamer = Streamer(data_product_id, interval=0.5, simple_time=True)
        self.addCleanup(streamer.stop)

        # Let at least 10 samples accumulate
        self.use_monitor(dataset_id, samples=10)

        # Stop the streamer, reset i, restart the streamer - this simulates duplicate data
        streamer.stop()
        streamer.i = 0
        streamer.start()

        # Let at least 20 more samples accumulate
        self.use_monitor(dataset_id, samples=20)

        #Stop the streamer
        streamer.stop()

        # Open the coverage and mess with the times
        with DirectCoverageAccess() as dca:
            with dca.get_read_only_coverage(dataset_id) as cov:
                self.assertEqual(cov.num_timesteps, 30)
                t = cov.get_time_values()
                self.assertEqual(len(t), 30)
                self.assertFalse(np.array_equal(np.sort(t), t))

            dca.repair_temporal_geometry(dataset_id)

            with dca.get_read_only_coverage(dataset_id) as cov:
                self.assertGreaterEqual(cov.num_timesteps, 19)
                t = cov.get_time_values()
                self.assertGreaterEqual(len(t), 19)
                np.testing.assert_array_equal(np.sort(t), t)


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
