#!/usr/bin/env python
'''
@author Tim Giguere
@file ion/processes/data/ingestion/test/test_stream_ingestion_worker.py
'''

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.processes.data.ingestion.stream_ingestion_worker import retrieve_stream

import numpy

from pyon.ion.stream import StandaloneStreamPublisher
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.file_sys import FileSystem, FS
from pyon.event.event import EventSubscriber
from pyon.public import OT, PRED
from pyon.util.containers import DotDict
from pyon.core.object import IonObjectDeserializer
from pyon.core.bootstrap import get_obj_registry

from nose.plugins.attrib import attr

from ion.services.dm.utility.granule_utils import SimplexCoverage, ParameterDictionary, GridDomain, ParameterContext
from coverage_model.parameter_types import QuantityType, ArrayType

from ion.services.dm.utility.granule_utils import time_series_domain
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from coverage_model.coverage import AbstractCoverage

from gevent.event import Event

import unittest
import os

@attr('INT', group='dm')
class TestStreamIngestionWorker(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management_client = DatasetManagementServiceClient(node=self.container.node)
        self.pubsub_client = PubsubManagementServiceClient(node=self.container.node)

        self.time_dom, self.spatial_dom = time_series_domain()

        self.parameter_dict_id = self.dataset_management_client.read_parameter_dictionary_by_name(name='ctd_parsed_param_dict', id_only=True)

        self.stream_def_id = self.pubsub_client.create_stream_definition(name='stream_def', parameter_dictionary_id=self.parameter_dict_id)
        self.addCleanup(self.pubsub_client.delete_stream_definition, self.stream_def_id)

        self.stream_id, self.route_id = self.pubsub_client.create_stream(name='parsed_stream', stream_definition_id=self.stream_def_id, exchange_point='science_data')
        self.addCleanup(self.pubsub_client.delete_stream, self.stream_id)

        self.subscription_id = self.pubsub_client.create_subscription(name='parsed_subscription', stream_ids=[self.stream_id], exchange_name='parsed_subscription')
        self.addCleanup(self.pubsub_client.delete_subscription, self.subscription_id)

        self.pubsub_client.activate_subscription(self.subscription_id)
        self.addCleanup(self.pubsub_client.deactivate_subscription, self.subscription_id)

        self.publisher = StandaloneStreamPublisher(self.stream_id, self.route_id)

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_stream_ingestion_worker(self):
        self.start_ingestion_worker()

        context_ids, time_ctxt = self._create_param_contexts()
        pdict_id = self.dataset_management_client.create_parameter_dictionary(name='stream_ingestion_pdict', parameter_context_ids=context_ids, temporal_context='ingestion_timestamp')
        self.addCleanup(self.dataset_management_client.delete_parameter_dictionary, pdict_id)

        dataset_id = self.dataset_management_client.create_dataset(name='fake_dataset', description='fake_dataset', stream_id=self.stream_id, parameter_dictionary_id=pdict_id)
        self.addCleanup(self.dataset_management_client.delete_dataset, dataset_id)

        self.cov = self._create_coverage(dataset_id=dataset_id, parameter_dict_id=pdict_id, time_dom=self.time_dom, spatial_dom=self.spatial_dom)
        self.addCleanup(self.cov.close)

        rdt = RecordDictionaryTool(stream_definition_id=self.stream_def_id)
        rdt['conductivity'] = 1
        rdt['pressure'] = 2
        rdt['salinity'] = 3

        self.start_listener(dataset_id)

        self.publisher.publish(rdt.to_granule())
        self.data_modified = Event()
        self.data_modified.wait(30)

        cov = self.get_coverage(dataset_id)
        self.assertIsNotNone(cov.get_parameter_values('raw'))

        deserializer = IonObjectDeserializer(obj_registry=get_obj_registry())

        granule = retrieve_stream(dataset_id)
        rdt_complex = RecordDictionaryTool.load_from_granule(granule)
        rdt_complex['raw'] = [deserializer.deserialize(i) for i in rdt_complex['raw']]
        for gran in rdt_complex['raw']:
            rdt_new = RecordDictionaryTool.load_from_granule(gran)
            self.assertIn(1, rdt_new['conductivity'])
            self.assertIn(2, rdt_new['pressure'])
            self.assertIn(3, rdt_new['salinity'])
            
        cov.close()


    def start_ingestion_worker(self):
        config = DotDict()
        config.process.queue_name = 'parsed_subscription'

        self.container.spawn_process(
            name='stream_ingestion_worker',
            module='ion.processes.data.ingestion.stream_ingestion_worker',
            cls='StreamIngestionWorker',
            config=config
        )

    def start_listener(self, dataset_id):
        def cb(*args, **kwargs):
            self.data_modified.set()

        es = EventSubscriber(event_type=OT.DatasetModified, callback=cb, origin=dataset_id)
        es.start()

        self.addCleanup(es.stop)

    def _create_param_contexts(self):
        context_ids = []
        t_ctxt = ParameterContext('ingestion_timestamp', param_type=QuantityType(value_encoding=numpy.dtype('float64')))
        t_ctxt.uom = 'seconds since 1900-01-01'
        t_ctxt.fill_value = -9999
        t_ctxt_id = self.dataset_management_client.create_parameter_context(name='ingestion_timestamp', parameter_context=t_ctxt.dump())
        context_ids.append(t_ctxt_id)

        raw_ctxt = ParameterContext('raw', param_type=ArrayType())
        raw_ctxt.uom = ''
        context_ids.append(self.dataset_management_client.create_parameter_context(name='raw', parameter_context=raw_ctxt.dump()))

        return context_ids, t_ctxt_id

    def _create_coverage(self, dataset_id, parameter_dict_id, time_dom, spatial_dom):
        pd = self.dataset_management_client.read_parameter_dictionary(parameter_dict_id)
        pdict = ParameterDictionary.load(pd)
        sdom = GridDomain.load(spatial_dom.dump())
        tdom = GridDomain.load(time_dom.dump())
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        scov = SimplexCoverage(file_root, dataset_id, dataset_id, parameter_dictionary=pdict, temporal_domain=tdom, spatial_domain=sdom)
        return scov

    def get_coverage(self, dataset_id,mode='w'):
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        coverage = AbstractCoverage.load(file_root, dataset_id, mode=mode)
        return coverage
