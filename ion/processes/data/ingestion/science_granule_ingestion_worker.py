#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/ingestion/science_granule_ingestion_worker.py
@date 06/26/12 11:38
@description Ingestion Process
'''
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.datastore.datastore import DataStore
from pyon.util.arg_check import validate_is_instance
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.util.containers import get_ion_ts, get_safe
from pyon.public import log, RT, PRED, CFG
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from interface.objects import Granule
from couchdb import ResourceNotFound
from ion.core.process.transform import TransformStreamListener
import collections
import numpy
import gevent



class ScienceGranuleIngestionWorker(TransformStreamListener):
    CACHE_LIMIT=CFG.get_safe('container.ingestion_cache',5)

    def __init__(self, *args,**kwargs):
        super(ScienceGranuleIngestionWorker, self).__init__(*args, **kwargs)
        self.iqueue = gevent.queue.Queue()
        #--------------------------------------------------------------------------------
        # Ingestion Cache
        # - Datasets
        # - Coverage instances
        #--------------------------------------------------------------------------------
        self._datasets  = collections.OrderedDict()
        self._coverages = collections.OrderedDict()
    def on_start(self): #pragma no cover
        super(ScienceGranuleIngestionWorker,self).on_start()
        self.datastore_name = self.CFG.get_safe('process.datastore_name', 'datasets')
        self.db = self.container.datastore_manager.get_datastore(self.datastore_name, DataStore.DS_PROFILE.SCIDATA)
        log.debug('Created datastore %s', self.datastore_name)



    def on_quit(self): #pragma no cover
        self.subscriber.stop()
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
        self.iqueue.put(granule)
        if self.iqueue.qsize() == 10:
            g = self.iqueue.get()
            rdt = RecordDictionaryTool.load_from_granule(g)
            while not self.iqueue.empty():
                g = self.iqueue.get()
                rdt = RecordDictionaryTool.append(rdt, RecordDictionaryTool.load_from_granule(g))
            self.add_granule(stream_id, rdt)
            self.persist_meta(stream_id, rdt)
        

    def persist_meta(self, stream_id, granule):
        #--------------------------------------------------------------------------------
        # Metadata persistence
        #--------------------------------------------------------------------------------
        # Determine the `time` in the granule
        dataset_id = self.get_dataset(stream_id)
        rdt = granule
        time = get_safe(rdt,'time')
        if time is not None and len(time) and isinstance(time,numpy.ndarray):
            time = time[0]
        else:
            time = None
            
        dataset_granule = {
           'stream_id'      : stream_id,
           'dataset_id'     : dataset_id,
           'persisted_sha1' : dataset_id, 
           'encoding_type'  : 'coverage',
           'ts_create'      : get_ion_ts()
        }
        if time is not None:
            dataset_granule['ts_create'] = '%s' % time
        self.persist(dataset_granule)
        #--------------------------------------------------------------------------------


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


    def persist(self, dataset_granule): #pragma no cover
        '''
        Persists the dataset metadata
        '''
        #--------------------------------------------------------------------------------
        # Theres a potential that the datastore could have been deleted while ingestion
        # is still running.  Essentially this refreshes the state
        #--------------------------------------------------------------------------------
        try:
            self.db.create_doc(dataset_granule)
            return
        except ResourceNotFound as e:
            log.error('The datastore was removed while ingesting (retrying)')
            self.db = self.container.datastore_manager.get_datastore(self.datastore_name, DataStore.DS_PROFILE.SCIDATA)

        #--------------------------------------------------------------------------------
        # The first call to create_doc attached an _id to the dictionary which causes an
        # error to be raised, to make this more resilient, we investigate to ensure
        # the dictionary does not have any of these excess keys
        #--------------------------------------------------------------------------------
        try:
            if '_id' in dataset_granule:
                del dataset_granule['_id']
            if '_rev' in dataset_granule:
                del dataset_granule['_rev']
            self.db.create_doc(dataset_granule)
        except ResourceNotFound as e:
            log.error(e.message) # Oh well I tried

