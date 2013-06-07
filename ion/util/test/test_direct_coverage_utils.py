#!/usr/bin/env python

"""
@package ion.util.test.test_direct_coverage_utils
@file ion/util/test/test_direct_coverage_utils.py
@author Christopher Mueller
@brief 
"""

import os
import unittest
import numpy as np
from nose.plugins.attrib import attr

from coverage_model import ParameterContext, SparseConstantType, BooleanType
from ion.services.dm.test.dm_test_case import DMTestCase, Streamer
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.util.direct_coverage_utils import DirectCoverageAccess
from pyon.ion.resource import PRED


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
                self.assertGreaterEqual(cov.num_timesteps, 9)

    def test_dca_coverage_reuse(self):
        data_product_id, dataset_id = self.make_ctd_data_product()

        streamer = Streamer(data_product_id, interval=1)
        self.addCleanup(streamer.stop)

        # Let a couple samples accumulate
        self.use_monitor(dataset_id, samples=2)

        with DirectCoverageAccess() as dca:
            with dca.get_read_only_coverage(dataset_id) as cov:
                self.assertFalse(cov.closed)

            self.assertTrue(cov.closed)

            with dca.get_editable_coverage(dataset_id) as cov:
                self.assertFalse(cov.closed)

            self.assertTrue(cov.closed)

            with dca.get_read_only_coverage(dataset_id) as cov:
                self.assertFalse(cov.closed)

            self.assertTrue(cov.closed)

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

    def test_run_coverage_doctor(self):
        data_product_id, dataset_id = self.make_ctd_data_product()

        with DirectCoverageAccess() as dca:
            # Run coverage doctor on an empty coverage - it's not corrupt yet , so it shouldn't need repair
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
        streamer = Streamer(data_product_id, interval=0.5, simple_time=True)
        self.addCleanup(streamer.stop)

        # Let at least 10 samples accumulate
        self.use_monitor(dataset_id, samples=10)

        with DirectCoverageAccess() as dca:
            # Run coverage doctor on the coverage - it's not corrupt yet , so it shouldn't need repair
            self.assertEqual(dca.run_coverage_doctor(dataset_id, data_product_id=data_product_id), 'Repair Not Necessary')
            with dca.get_read_only_coverage(dataset_id) as cov:
                ont = cov.num_timesteps

            # Mess up the master file
            with open(mpth, 'wb') as f:
                f.write('mess you up!')

            # Repair the coverage
            self.assertEqual(dca.run_coverage_doctor(dataset_id, data_product_id=data_product_id), 'Repair Successful')

        # Let at least 1 sample arrive
        self.use_monitor(dataset_id, samples=1)

        with dca.get_read_only_coverage(dataset_id) as cov:
            self.assertGreaterEqual(cov.num_timesteps, ont)


