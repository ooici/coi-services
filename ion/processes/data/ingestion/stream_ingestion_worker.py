#!/usr/bin/env python
'''
@author Tim Giguere
@file ion/processes/data/ingestion/stream_ingestion_worker.py
@description Ingestion Process
'''
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService
from interface.objects import Granule
from ion.core.process.transform import TransformStreamListener
from ion.util.time_utils import TimeUtils

from pyon.public import log, RT, PRED, CFG, OT
from pyon.util.arg_check import validate_is_instance
from pyon.event.event import EventPublisher
from pyon.core.object import IonObjectSerializer

import collections
import time

class StreamIngestionWorker(TransformStreamListener):
    CACHE_LIMIT=CFG.get_safe('container.ingestion_cache',5)

    def __init__(self, *args,**kwargs):
        super(StreamIngestionWorker, self).__init__(*args, **kwargs)
        #--------------------------------------------------------------------------------
        # Ingestion Cache
        # - Datasets
        # - Coverage instances
        #--------------------------------------------------------------------------------
        self._datasets = collections.OrderedDict()
        self._coverages = collections.OrderedDict()


    def on_start(self): #pragma no cover
        super(StreamIngestionWorker,self).on_start()
        self.event_publisher = EventPublisher(OT.DatasetModified)

    def recv_packet(self, msg, stream_route, stream_id):
        validate_is_instance(msg, Granule, 'Incoming packet must be of type granule')

        cov = self.get_coverage(stream_id)
        if cov:
            cov.insert_timesteps(1)

            if 'raw' in cov.list_parameters():
                gran = IonObjectSerializer().serialize(msg)
                cov.set_parameter_values(param_name='raw', value=[gran])

            if 'ingestion_timestamp' in cov.list_parameters():
                t_now = time.time()
                ntp_time = TimeUtils.ts_to_units(cov.get_parameter_context('ingestion_timestamp').uom, t_now)
                cov.set_parameter_values(param_name='ingestion_timestamp', value=ntp_time)

            self.dataset_changed(self.get_dataset(stream_id), cov.num_timesteps)

    def on_quit(self): #pragma no cover
        super(StreamIngestionWorker, self).on_quit()
        for stream, coverage in self._coverages.iteritems():
            try:
                coverage.close(timeout=5)
            except:
                log.exception('Problems closing the coverage')

    def _new_dataset(self, stream_id):
        '''
        Adds a new dataset to the internal cache of the ingestion worker
        '''
        rr_client = ResourceRegistryServiceClient()
        datasets, _ = rr_client.find_subjects(subject_type=RT.Dataset,predicate=PRED.hasStream,object=stream_id,id_only=True)
        if datasets:
            return datasets[0]
        return None

    def get_dataset(self,stream_id):
        '''
        Memoization (LRU) of _new_dataset
        '''
        try:
            result = self._datasets.pop(stream_id)
        except KeyError:
            result = self._new_dataset(stream_id)
            if result is None:
                return None
            if len(self._datasets) >= self.CACHE_LIMIT:
                self._datasets.popitem(0)
        self._datasets[stream_id] = result
        return result

    def get_coverage(self, stream_id):
        '''
        Memoization (LRU) of _get_coverage
        '''
        try:
            result = self._coverages.pop(stream_id)
        except KeyError:
            dataset_id = self.get_dataset(stream_id)
            if dataset_id is None:
                return None
            result = DatasetManagementService._get_simplex_coverage(dataset_id, mode='a')
            if result is None:
                return None
            if len(self._coverages) >= self.CACHE_LIMIT:
                k, coverage = self._coverages.popitem(0)
                coverage.close(timeout=5)
        self._coverages[stream_id] = result
        return result

    def dataset_changed(self, dataset_id, extents):
        self.event_publisher.publish_event(origin=dataset_id, author=self.id, extents=extents)

def retrieve_stream(dataset_id='', query=None):
    return DataRetrieverService.retrieve_oob(dataset_id, query)