#!/usr/bin/env python
'''
@author Luke Campbell
@date Fri Apr  5 13:20:45 EDT 2013
@file ion/processes/data/ingestion/test/test_ingestion.py
'''

from pyon.util.unit_test import PyonTestCase
from ion.processes.data.ingestion.science_granule_ingestion_worker import ScienceGranuleIngestionWorker
from nose.plugins.attrib import attr


@attr('UNIT',group='dm')
class IngestionTest(PyonTestCase):
    def test_ingestion_gap(self):

        ingestion = ScienceGranuleIngestionWorker()
        ingestion.connection_id = ''
        ingestion.connection_index = None

        self.assertFalse(ingestion.has_gap('c1','0'))
        self.assertTrue(ingestion.has_gap('c1','0'))
        self.assertFalse(ingestion.has_gap('c1','1'))
        self.assertTrue(ingestion.has_gap('c2','1'))
        self.assertTrue(ingestion.has_gap('c2','0'))

        ingestion.update_connection_index('c2','12')
        self.assertFalse(ingestion.has_gap('c2','13'))
        self.assertTrue(ingestion.has_gap('c2','12'))
        self.assertTrue(ingestion.has_gap('c2','14'))

    def test_ingestion_default(self):

        ingestion = ScienceGranuleIngestionWorker()
        ingestion.connection_id = ''
        ingestion.connection_index = None

        self.assertFalse(ingestion.has_gap('',''))
        self.assertFalse(ingestion.has_gap('',''))
        self.assertFalse(ingestion.has_gap('',''))



