#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file test_science_granule_ingestion_worker
@date 06/27/12 10:16
@description DESCRIPTION
'''
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.taxonomy import TaxyTool
from ion.services.dm.utility.granule.granule import build_granule as bg
from ion.processes.data.ingestion.science_granule_ingestion_worker import ScienceGranuleIngestionWorker
from interface.objects import Granule

from mock import Mock, patch
from nose.plugins.attrib import attr
import numpy as np
import unittest


@attr('UNIT',group='dm')
class ScienceGranuleIngestionWorkerUnitTest(PyonTestCase):
    def setUp(self):
        self.worker = ScienceGranuleIngestionWorker()

    @staticmethod
    def build_granule():
        tt = TaxyTool()
        tt.add_taxonomy_set('c')
        tt.add_taxonomy_set('t')
        tt.add_taxonomy_set('d')

        rdt = RecordDictionaryTool(taxonomy=tt)
        rdt['c'] = np.array([0,1])
        rdt['t'] = np.array([0,1])
        rdt['d'] = np.array([0,1])

        granule = bg(data_producer_id='test_identifier', taxonomy=tt, record_dictionary=rdt)
        return granule

    def test_consume(self):
        self.worker.ingest = Mock()

        message = {}
        headers = {'routing_key' : 'stream_id.data'}

        self.worker.consume(message,headers)
        self.worker.ingest.assert_called_once_with({},'stream_id')
    
    def test_get_dataset(self):
        self.worker.datasets = {}
        self.worker._new_dataset = Mock()
        self.worker._new_dataset.return_value = 'dataset_id'
        retval = self.worker.get_dataset('stream_id')
        self.assertEquals(retval,'dataset_id')

    @patch('ion.processes.data.ingestion.science_granule_ingestion_worker.DatasetManagementService')
    def test_get_coverage(self, dsm):
        dsm._get_coverage = Mock()
        dsm._get_coverage.return_value = {'test':True}

        self.worker.coverages = {}
        self.worker.get_dataset = Mock()
        self.worker.get_dataset.return_value = 'dataset_id'

        retval = self.worker.get_coverage('stream_id')
        self.assertEquals(retval,{'test':True})

    def test_consume(self):
        self.worker.ingest = Mock()
       
        self.worker.consume({},{'routing_key':'testval.data'})

        self.worker.ingest.assert_called_once_with({},'testval')

    def test_ingest(self):
        self.worker.add_granule = Mock()
        self.worker.persist_meta = Mock()
        
        granule = Granule()

        self.worker.ingest(granule,'stream_id')

        self.worker.add_granule.assert_called_once_with('stream_id',granule)
        self.worker.persist_meta.assert_called_once_with('stream_id',granule)

    @patch('ion.processes.data.ingestion.science_granule_ingestion_worker.RecordDictionaryTool')
    def test_persist_meta(self, rdt):
        self.worker.get_dataset = Mock()
        self.worker.get_dataset.return_value = 'dataset_id'
        self.worker.persist = Mock()

        mock_rdt = DotDict()
        mock_rdt.time = np.arange(10)

        rdt.load_from_granule = Mock()
        rdt.load_from_granule.return_value = mock_rdt

        correct_val = {
           'stream_id'      : 'stream_id',
           'dataset_id'     : 'dataset_id',
           'persisted_sha1' : 'dataset_id', 
           'encoding_type'  : 'coverage',
           'ts_create'      : '0'
        }

        self.worker.persist_meta('stream_id',{})

        self.worker.persist.assert_called_once_with(correct_val)


    @patch('ion.processes.data.ingestion.science_granule_ingestion_worker.CoverageCraft')
    @patch('ion.processes.data.ingestion.science_granule_ingestion_worker.DatasetManagementService')
    def test_add_granule(self, dsm, craft):
        craft().sync_with_granule = Mock()
        dsm._persist_coverage = Mock()

        self.worker.get_dataset = Mock()
        self.worker.get_dataset.return_value = 'dataset_id'

        self.worker.get_coverage = Mock()
        self.worker.get_coverage.return_value = {}

        self.worker.add_granule('stream_id',{})


