#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/ingestion/science_granule_ingestion_worker.py
@date 06/26/12 11:38
@description Ingestion Process
'''
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.core.exception import CorruptionError, NotFound
from pyon.ion.event import handle_stream_exception, EventPublisher
from pyon.ion.event import EventSubscriber
from pyon.public import log, RT, PRED, CFG, OT
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from interface.objects import Granule
from ion.core.process.transform import TransformStreamListener
from ion.util.time_utils import TimeUtils
from ion.util.stored_values import StoredValueManager

from coverage_model.parameter_values import SparseConstantValue

from ooi.timer import Timer, Accumulator
from ooi.logging import TRACE
from logging import DEBUG

import collections
import gevent
import time
import uuid
import numpy as np
from gevent.queue import Queue

REPORT_FREQUENCY=100
MAX_RETRY_TIME=3600

class ScienceGranuleIngestionWorker(TransformStreamListener):
    CACHE_LIMIT=CFG.get_safe('container.ingestion_cache',5)

    def __init__(self, *args,**kwargs):
        super(ScienceGranuleIngestionWorker, self).__init__(*args, **kwargs)
        #--------------------------------------------------------------------------------
        # Ingestion Cache
        # - Datasets
        # - Coverage instances
        #--------------------------------------------------------------------------------
        self._datasets  = collections.OrderedDict()
        self._coverages = collections.OrderedDict()

        self._bad_coverages = {}

        self.time_stats = Accumulator(format='%3f')
        # unique ID to identify this worker in log msgs
        self._id = uuid.uuid1()

    def on_start(self): #pragma no cover
        super(ScienceGranuleIngestionWorker,self).on_start()
        self.event_publisher = EventPublisher(OT.DatasetModified)
        self.stored_value_manager = StoredValueManager(self.container)

        self.lookup_docs = self.CFG.get_safe('process.lookup_docs',[])
        self.input_product = self.CFG.get_safe('process.input_product','')
        self.new_lookups = Queue()
        self.lookup_monitor = EventSubscriber(event_type=OT.ExternalReferencesUpdatedEvent, callback=self._add_lookups, auto_delete=True)
        self.lookup_monitor.start()


    def on_quit(self): #pragma no cover
        super(ScienceGranuleIngestionWorker, self).on_quit()
        for stream, coverage in self._coverages.iteritems():
            try:
                coverage.close(timeout=5)
            except:
                log.exception('Problems closing the coverage')
    
    def _add_lookups(self, event, *args, **kwargs):
        if event.origin == self.input_product:
            if isinstance(event.reference_keys, list):
                self.new_lookups.put(event.reference_keys)

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

    def dataset_changed(self, dataset_id, extents, window):
        self.event_publisher.publish_event(origin=dataset_id, author=self.id, extents=extents, window=window)

    @handle_stream_exception()
    def recv_packet(self, msg, stream_route, stream_id):
        ''' receive packet for ingestion '''
        log.trace('received granule for stream %s', stream_id)

        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        # Message validation
        if not isinstance(msg, Granule):
            log.error('Ingestion received a message that is not a granule: %s', msg)
            return

        rdt = RecordDictionaryTool.load_from_granule(msg)
        if rdt is None:
            log.error('Invalid granule (no RDT) for stream %s', stream_id)
            return
        if not len(rdt):
            log.debug('Empty granule for stream %s', stream_id)
            return

        self.persist_or_timeout(stream_id, rdt)

    def persist_or_timeout(self, stream_id, rdt):
        """ retry writing coverage multiple times and eventually time out """
        done = False
        timeout = 2
        start = time.time()
        while not done:
            try:
                self.add_granule(stream_id, rdt)
                done = True
            except:
                log.exception('An issue with coverage, retrying after a bit')
                if (time.time() - start) > MAX_RETRY_TIME: # After an hour just give up
                    dataset_id = self.get_dataset(stream_id)
                    log.error("We're giving up, the coverage needs to be inspected %s", DatasetManagementService._get_coverage_path(dataset_id))
                    raise

                if stream_id in self._coverages:
                    log.info('Popping coverage for stream %s', stream_id)
                    self._coverages.pop(stream_id)

                gevent.sleep(timeout)
                if timeout > (60 * 5):
                    timeout = 60 * 5
                else:
                    timeout *= 2


    def expand_coverage(self, coverage, elements, stream_id):
        try:
            coverage.insert_timesteps(elements, oob=False)
        except IOError as e:
            log.error("Couldn't insert time steps for coverage: %s",
                      coverage.persistence_dir, exc_info=True)
            try:
                coverage.close()
            finally:
                self._bad_coverages[stream_id] = 1
                raise CorruptionError(e.message)
    
    def get_stored_values(self, lookup_value):
        if not self.new_lookups.empty():
            new_values = self.new_lookups.get()
            self.lookup_docs = new_values + self.lookup_docs
        lookup_value_document_keys = self.lookup_docs
        for key in lookup_value_document_keys:
            try:
                document = self.stored_value_manager.read_value(key)
                if lookup_value in document:
                    return document[lookup_value] 
            except NotFound:
                log.warning('Specified lookup document does not exist')
        return None


    def fill_lookup_values(self, rdt):
        rdt.fetch_lookup_values()
        for field in rdt.lookup_values():
            value = self.get_stored_values(rdt.context(field).lookup_value)
            if value:
                rdt[field] = value

    def insert_sparse_values(self, coverage, rdt, stream_id):

        self.fill_lookup_values(rdt)
        for field in rdt._lookup_values():
            if rdt[field] is None:
                continue
            value = rdt[field]
            try:
                coverage.set_parameter_values(param_name=field, value=value)
            except IOError as e:
                log.error("Couldn't insert values for coverage: %s",
                          coverage.persistence_dir, exc_info=True)
                try:
                    coverage.close()
                finally:
                    self._bad_coverages[stream_id] = 1
                    raise CorruptionError(e.message)

    def insert_values(self, coverage, rdt, stream_id):
        elements = len(rdt)

        start_index = coverage.num_timesteps - elements

        for k,v in rdt.iteritems():
            if isinstance(v, SparseConstantValue):
                continue
            slice_ = slice(start_index, None)
            try:
                coverage.set_parameter_values(param_name=k, tdoa=slice_, value=v)
            except IOError as e:
                log.error("Couldn't insert values for coverage: %s",
                          coverage.persistence_dir, exc_info=True)
                try:
                    coverage.close()
                finally:
                    self._bad_coverages[stream_id] = 1
                    raise CorruptionError(e.message)
    
        if 'ingestion_timestamp' in coverage.list_parameters():
            t_now = time.time()
            ntp_time = TimeUtils.ts_to_units(coverage.get_parameter_context('ingestion_timestamp').uom, t_now)
            coverage.set_parameter_values(param_name='ingestion_timestamp', tdoa=slice_, value=ntp_time)
    
    def add_granule(self,stream_id, rdt):
        ''' Appends the granule's data to the coverage and persists it. '''
        debugging = log.isEnabledFor(DEBUG)
        timer = Timer() if debugging else None
        if stream_id in self._bad_coverages:
            log.info('Message attempting to be inserted into bad coverage: %s',
                     DatasetManagementService._get_coverage_path(self.get_dataset(stream_id)))
            

        #--------------------------------------------------------------------------------
        # Coverage determiniation and appending
        #--------------------------------------------------------------------------------
        dataset_id = self.get_dataset(stream_id)
        if not dataset_id:
            log.error('No dataset could be determined on this stream: %s', stream_id)
            return
        try:
            coverage = self.get_coverage(stream_id)
        except IOError as e:
            log.error("Couldn't open coverage: %s",
                      DatasetManagementService._get_coverage_path(self.get_dataset(stream_id)))
            raise CorruptionError(e.message)

        if debugging:
            path = DatasetManagementService._get_coverage_path(dataset_id)
            log.debug('%s: add_granule stream %s dataset %s coverage %r file %s',
                      self._id, stream_id, dataset_id, coverage, path)

        if not coverage:
            log.error('Could not persist coverage from granule, coverage is None')
            return
        #--------------------------------------------------------------------------------
        # Actual persistence
        #--------------------------------------------------------------------------------

        elements = len(rdt)

        self.insert_sparse_values(coverage,rdt,stream_id)
        
        if debugging:
            timer.complete_step('checks') # lightweight ops, should be zero
        
        self.expand_coverage(coverage, elements, stream_id)
        
        if debugging:
            timer.complete_step('insert')

        self.insert_values(coverage, rdt, stream_id)
        
        if debugging:
            timer.complete_step('keys')
        
        DatasetManagementService._save_coverage(coverage)
        
        if debugging:
            timer.complete_step('save')
        
        start_index = coverage.num_timesteps - elements
        self.dataset_changed(dataset_id,coverage.num_timesteps,(start_index,start_index+elements))
        
        if debugging:
            timer.complete_step('notify')
            self._add_timing_stats(timer)

    def _add_timing_stats(self, timer):
        """ add stats from latest coverage operation to Accumulator and periodically log results """
        self.time_stats.add(timer)
        if self.time_stats.get_count() % REPORT_FREQUENCY>0:
            return

        if log.isEnabledFor(TRACE):
            # report per step
            for step in 'checks', 'insert', 'keys', 'save', 'notify':
                log.debug('%s step %s times: %s', self._id, step, self.time_stats.to_string(step))
        # report totals
        log.debug('%s total times: %s', self._id, self.time_stats)


