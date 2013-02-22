#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/ingestion/science_granule_ingestion_worker.py
@date 06/26/12 11:38
@description Ingestion Process
'''
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.core.exception import CorruptionError
from pyon.event.event import handle_stream_exception, EventPublisher
from pyon.public import log, RT, PRED, CFG
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from interface.objects import Granule
from ion.core.process.transform import TransformStreamListener
import collections



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
    def on_start(self): #pragma no cover
        super(ScienceGranuleIngestionWorker,self).on_start()
        self.event_publisher = EventPublisher('DatasetModified')


    def on_quit(self): #pragma no cover
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
            result = DatasetManagementService._get_coverage(dataset_id, mode='a')
            if result is None:
                return None
            if len(self._coverages) >= self.CACHE_LIMIT:
                k, coverage = self._coverages.popitem(0)
                coverage.close(timeout=5)
        self._coverages[stream_id] = result
        return result


    @handle_stream_exception()
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
        rdt = RecordDictionaryTool.load_from_granule(msg)
        if rdt is not None:
            self.add_granule(stream_id, rdt)
        else:
            log.error('Invalid granule')

    def dataset_changed(self, dataset_id, extents, window):
        self.event_publisher.publish_event(origin=dataset_id, dataset_id=dataset_id, extents=extents, window=window)

    def add_granule(self,stream_id, granule):
        '''
        Appends the granule's data to the coverage and persists it.
        '''
        if stream_id in self._bad_coverages:
            log.info('Message attempting to be inserted into bad coverage: %s' % DatasetManagementService._get_coverage_path(self.get_dataset(stream_id)))
            

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
            log.error("Couldn't open coverage: %s" % DatasetManagementService._get_coverage_path(self.get_dataset(stream_id)))
            log.exception('IOError')
            raise CorruptionError(e.message)
        
        if not coverage:
            log.error('Could not persist coverage from granule, coverage is None')
            return
        #--------------------------------------------------------------------------------
        # Actual persistence
        #-------------------------------------------------------------------------------- 

        rdt = granule
        elements = len(rdt)
        if not elements:
            return
        try:
            coverage.insert_timesteps(elements, oob=False)
        except IOError as e:
            log.error("Couldn't insert time steps for coverage: %s" % DatasetManagementService._get_coverage_path(self.get_dataset(stream_id)))
            log.exception('IOError')
            try:
                coverage.close()
            finally:
                self._bad_coverages[stream_id] = 1
                raise CorruptionError(e.message)

        start_index = coverage.num_timesteps - elements
        self.dataset_changed(dataset_id,coverage.num_timesteps,(start_index,start_index+elements))

        for k,v in rdt.iteritems():
            slice_ = slice(start_index, None)
            try:
                coverage.set_parameter_values(param_name=k, tdoa=slice_, value=v)
            except IOError as e:
                log.error("Couldn't insert values for coverage: %s" % DatasetManagementService._get_coverage_path(self.get_dataset(stream_id)))
                log.exception('IOError')
                try:
                    coverage.close()
                finally:
                    self._bad_coverages[stream_id] = 1
                    raise CorruptionError(e.message)
            DatasetManagementService._save_coverage(coverage)
            #coverage.flush()


