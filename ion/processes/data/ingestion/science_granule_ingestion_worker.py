#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/ingestion/science_granule_ingestion_worker.py
@date 06/26/12 11:38
@description Ingestion Process
'''
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.public import log, RT, PRED, CFG
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from interface.objects import Granule
from ion.core.process.transform import TransformStreamListener
import collections
import gevent
import time



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
        self._queues    = {}
    def on_start(self): #pragma no cover
        super(ScienceGranuleIngestionWorker,self).on_start()
        self.buffer_limit   = self.CFG.get_safe('process.buffer_limit',10)
        self.time_limit     = self.CFG.get_safe('process.time_limit', 10)
        self.flushing       = gevent.coros.RLock()
        self.done_flushing = gevent.event.Event()
        self.flusher_g = gevent.spawn(self.flusher)


    def on_quit(self): #pragma no cover
        self.subscriber.stop()
        self.flush_all()
        self.done_flushing.set()
        self.flusher_g.join(10)
        self.flusher_g.kill()

        for stream, coverage in self._coverages.iteritems():
            try:
                coverage.close(timeout=5)
            except:
                log.exception('Problems closing the coverage')
    
    def flusher(self):
        then = time.time()
        while not self.done_flushing.wait(1):
            if (time.time() - then) >= self.time_limit:
                then = time.time()
                self.flush_all()




    def _new_dataset(self, stream_id):
        '''
        Adds a new dataset to the internal cache of the ingestion worker
        '''
        rr_client = ResourceRegistryServiceClient()
        datasets, _ = rr_client.find_subjects(subject_type=RT.DataSet,predicate=PRED.hasStream,object=stream_id,id_only=True)
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
            result = DatasetManagementService._get_coverage(dataset_id)
            if result is None:
                return None
            if len(self._coverages) >= self.CACHE_LIMIT:
                k, coverage = self._coverages.popitem(0)
                coverage.close(timeout=5)
        self._coverages[stream_id] = result
        return result

    @classmethod
    def _new_queue(cls):
        rlock = gevent.coros.RLock()
        queue = gevent.queue.Queue()
        return (rlock, queue)

    def get_queue(self, stream_id):
        '''
        Memoization (LRU) for retrieving a queue based on the stream id.
        '''
        if not stream_id in self._queues:
            self._queues[stream_id] = self._new_queue()
        return self._queues[stream_id]


    def recv_packet(self, msg, stream_route, stream_id):
        '''
        Actual ingestion mechanism
        '''
        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        # Message validation
        if not isinstance(msg, Granule):
            log.error('Ingestion received a message that is not a granule. %s' % msg)
            return
        log.trace('Received incoming granule from route: %s and stream_id: %s', stream_route, stream_id)
        log.trace('Granule contents: %s', msg.__dict__)
        granule = msg
        lock, queue = self.get_queue(stream_id)
        queue.put(granule)
        if queue.qsize() >= self.buffer_limit:
            self.flush_queue(stream_id)

    def flush_queue(self,stream_id):
        lock, queue = self.get_queue(stream_id)
        lock.acquire()
        rdt = None
        while not queue.empty():
            g = queue.get()
            if rdt is not None:
                rdt = RecordDictionaryTool.append(rdt, RecordDictionaryTool.load_from_granule(g))
            else:
                rdt = RecordDictionaryTool.load_from_granule(g)
        if rdt is not None:
            self.add_granule(stream_id, rdt)
        lock.release()

    def flush_all(self):
        for stream_id in self._queues.iterkeys():
            self.flush_queue(stream_id)
        

    def add_granule(self,stream_id, granule):
        '''
        Appends the granule's data to the coverage and persists it.
        '''
        #--------------------------------------------------------------------------------
        # Coverage determiniation and appending
        #--------------------------------------------------------------------------------
        dataset_id = self.get_dataset(stream_id)
        if not dataset_id:
            log.error('No dataset could be determined on this stream: %s', stream_id)
            return
        coverage = self.get_coverage(stream_id)
        if not coverage:
            log.error('Could not persist coverage from granule, coverage is None')
            return
        #--------------------------------------------------------------------------------
        # Actual persistence
        #-------------------------------------------------------------------------------- 
        log.trace('Loaded coverage for %s' , dataset_id)

        rdt = granule
        log.trace('%s', {i:rdt[i] for i in rdt.fields})
        elements = len(rdt)
        if not elements:
            return
        coverage.insert_timesteps(elements)
        start_index = coverage.num_timesteps - elements

        for k,v in rdt.iteritems():
            if k == 'image_obj':
                log.trace( '%s:', k)
            else:
                log.trace( '%s: %s', k, v)

            slice_ = slice(start_index, None)
            coverage.set_parameter_values(param_name=k, tdoa=slice_, value=v)
            DatasetManagementService._save_coverage(coverage)
            #coverage.flush()

