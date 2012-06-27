#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file test_science_granule_ingestion_worker
@date 06/27/12 10:16
@description DESCRIPTION
'''
from pyon.util.unit_test import PyonTestCase
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from ion.processes.data.ingestion.science_granule_ingestion_worker import ScienceGranuleIngestionWorker

from mock import Mock
from nose.plugins.attrib import attr
import numpy as np


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

        granule = build_granule(data_producer_id='test_identifier', taxonomy=tt, record_dictionary=rdt)
        return granule

    def test_consume(self):
        self.worker.ingest = Mock()

        message = {}
        headers = {'routing_key' : 'stream_id.data'}

        self.worker.consume(message,headers)
        self.worker.ingest.assert_called_once_with({},'stream_id')
    
    def test_ingest(self):
        retval = self.worker.ingest({}, '')
        self.assertTrue(retval is None)
        granule = self.build_granule()

        self.worker.write   = Mock()
        self.worker.persist = Mock()

        def check_persist(dataset_granule):
            self.assertTrue(dataset_granule['stream_id'] == 'stream_id')
            self.assertTrue(dataset_granule['dataset_id'] == 'test_identifier')
        self.worker.persist.side_effect = check_persist

        self.worker.ingest(granule,'stream_id')



        


