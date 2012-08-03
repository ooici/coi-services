#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/ingestion/test/test_science_granule_ingestion.py
@date 06/27/2012
@description Testing for ScienceGranuleIngestionWorker
'''

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.containers import DotDict
from pyon.core.bootstrap import get_sys_name
from pyon.core.exception import Timeout
from pyon.net.endpoint import Publisher
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from ion.processes.data.ingestion.test.test_science_granule_ingestion_worker import ScienceGranuleIngestionWorkerUnitTest
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import IngestionQueue
from nose.plugins.attrib import attr

import time
import unittest
import os

@attr('INT',group='dm')
class ScienceGranuleIngestionIntTest(IonIntegrationTestCase):
    def setUp(self):
        self.datastore_name = 'datasets'
        self.exchange_point = 'science_data'
        self.exchange_space = 'science_granule_ingestion'
        self.queue_name     = self.exchange_space
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.ingestion_management = IngestionManagementServiceClient()
        self.pubsub               = PubsubManagementServiceClient()


    def build_granule(self):
        return ScienceGranuleIngestionWorkerUnitTest.build_granule()

    def launch_worker(self):
        cfg = DotDict()

        cfg.process.datastore_name = self.datastore_name
        cfg.process.queue_name = self.queue_name

        #@todo: replace with CEI friendly calls

        pid = self.container.spawn_process('ingest_worker', 'ion.processes.data.ingestion.science_granule_ingestion_worker','ScienceGranuleIngestionWorker',cfg)
        return pid

    def create_ingestion_config(self):
        ingest_queue = IngestionQueue(name=self.exchange_space, type='science_granule')
        config_id = self.ingestion_management.create_ingestion_configuration(name='standard_ingest', exchange_point_id=self.exchange_point, queues=[ingest_queue])
        return config_id
            
    def create_stream(self):
        stream_id = self.pubsub.create_stream()
        return stream_id

    def poll(self, evaluation_callback,  *args, **kwargs):
        now = time.time()
        cutoff = now + 5
        done = False
        while not done:
            if evaluation_callback(*args,**kwargs):
                done = True
            if now >= cutoff:
                raise Timeout('No results found within the allotted time')
            now = time.time()
        return True


