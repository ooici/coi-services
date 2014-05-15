'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/inventory/data_retriever_service.py
@description Data Retriever Service
'''

from ion.core.function.transform_function import TransformFunction
from ion.processes.data.replay.replay_process import ReplayProcess
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule import RecordDictionaryTool

from pyon.core.exception import BadRequest 
from pyon.container.cc import Container
from pyon.public import PRED, RT
from pyon.util.arg_check import validate_is_instance, validate_true
from pyon.util.containers import for_name
from pyon.util.log import log
from pyon.event.event import EventSubscriber

from interface.objects import Replay, CoverageTypeEnum
from interface.services.dm.idata_retriever_service import BaseDataRetrieverService

import collections
import time
import gevent

class DataRetrieverService(BaseDataRetrieverService):
    REPLAY_PROCESS = 'replay_process'

    _refresh_interval = 10
    _cache_limit      = 5
    _retrieve_cache   = collections.OrderedDict()
    _cache_lock       = gevent.coros.RLock()

    def on_start(self):
        self.event_subscriber = EventSubscriber(event_type='DatasetModified', callback=lambda event,m : self._eject_cache(event.origin), auto_delete=True)
        self.add_endpoint(self.event_subscriber)

    @classmethod
    def _eject_cache(cls, dataset_id):
        with cls._cache_lock:
            try:
                cls._retrieve_cache.pop(dataset_id)
            except KeyError:
                pass
    
    def define_replay(self, dataset_id='', query=None, delivery_format='', stream_id=''):
        ''' Define the stream that will contain the data from data store by streaming to an exchange name.
        query: 
          start_time: 0    The beginning timestamp
          end_time:   N    The ending timestamp
          parameters: []   The list of parameters which match the coverages parameters
          tdoa: slice()    The slice for the desired indices to be replayed
        '''

        if not dataset_id:
            raise BadRequest('(Data Retriever Service %s): No dataset provided.' % self.name)
        validate_true(stream_id, 'No stream_id provided')


        res, _  = self.clients.resource_registry.find_resources(restype=RT.ProcessDefinition,name=self.REPLAY_PROCESS,id_only=True)
        if not len(res):
            raise BadRequest('No replay process defined.')
        process_definition_id = res[0]

        replay_stream_id = stream_id
        pid = self.clients.process_dispatcher.create_process(process_definition_id=process_definition_id)

        #--------------------------------------------------------------------------------
        # Begin the Decision tree for the various types of replay
        #--------------------------------------------------------------------------------
        replay=self.replay_data_process(dataset_id, query, delivery_format, replay_stream_id)

        replay.process_id = pid

        self.clients.resource_registry.update(replay)
        self.clients.resource_registry.create_association(replay._id, PRED.hasStream, replay_stream_id)
        return replay._id, pid

    def delete_replay(self,replay_id=''):
        assocs = self.clients.resource_registry.find_associations(subject=replay_id,predicate=PRED.hasStream)

        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)

        self.clients.resource_registry.delete(replay_id)

    def read_process_id(self, replay_id=''):
        replay = self.clients.resource_registry.read(replay_id)
        validate_is_instance(replay,Replay)

        return replay.process_id

    def start_replay_agent(self, replay_id=''):
        """
        """
        res, _  = self.clients.resource_registry.find_resources(restype=RT.ProcessDefinition,name=self.REPLAY_PROCESS,id_only=True)
        if not len(res):
            raise BadRequest('No replay process defined.')
        process_definition_id = res[0]
        replay = self.clients.resource_registry.read(replay_id)
        validate_is_instance(replay,Replay)
        
        config = replay.config
        pid = replay.process_id

        self.clients.process_dispatcher.schedule_process(process_definition_id=process_definition_id, process_id=pid, configuration=config)


    def cancel_replay_agent(self, replay_id=''):
        replay = self.clients.resource_registry.read(replay_id)
        pid = replay.process_id
        self.clients.process_dispatcher.cancel_process(pid)

        for pred in [PRED.hasStream]:
            assocs = self.clients.resource_registry.find_associations(replay_id, pred, id_only=True)
            for assoc in assocs:
                self.clients.resource_registry.delete_association(assoc)

        self.clients.resource_registry.delete(replay_id)

    @classmethod
    def _get_coverage(cls,dataset_id):
        '''
        Memoized coverage instantiation and management
        '''
        # Cached get
        retval = None
        with cls._cache_lock:
            try:
                retval, age = cls._retrieve_cache.pop(dataset_id)
                if (time.time() - age) > cls._refresh_interval:
                    raise KeyError(dataset_id)
            except KeyError: # Cache miss
                #@TODO: Add in LRU logic (maybe some mem checking too!)
                if len(cls._retrieve_cache) > cls._cache_limit:
                    cls._retrieve_cache.popitem(0)
                retval = DatasetManagementService._get_nonview_coverage(dataset_id, mode='r') 
            age = time.time()
            cls._retrieve_cache[dataset_id] = (retval, age)
        return retval

    @classmethod
    def retrieve_oob(cls, dataset_id='', query=None, delivery_format=''):
        query = query or {}
        coverage = None
        try:
            coverage = cls._get_coverage(dataset_id)
            if coverage is None:
                raise BadRequest('no such coverage')
            if coverage.is_empty():
                log.info('Reading from an empty coverage')
                rdt = RecordDictionaryTool(param_dictionary=coverage.parameter_dictionary)
            else:
                rdt = ReplayProcess._cov2granule(coverage=coverage, start_time=query.get('start_time', None), end_time=query.get('end_time',None), stride_time=query.get('stride_time',None), parameters=query.get('parameters',None), stream_def_id=delivery_format, tdoa=query.get('tdoa',None))
        except Exception as e:
            cls._eject_cache(dataset_id)
            data_products, _ = Container.instance.resource_registry.find_subjects(object=dataset_id, predicate=PRED.hasDataset, subject_type=RT.DataProduct)
            for data_product in data_products:
                log.error("Data Product %s (%s) had issues reading from the coverage model\nretrieve_oob(dataset_id='%s', query=%s, delivery_format=%s)", data_product.name, data_product._id, dataset_id, query, delivery_format)
            log.error("Problems reading from the coverage", exc_info=True)
            raise BadRequest('Problems reading from the coverage')
        return rdt.to_granule()

  
    def retrieve(self, dataset_id='', query=None, delivery_format='', module='', cls='', kwargs=None):
        '''
        Retrieves a dataset.
        @param dataset_id      Dataset identifier
        @param query           Query parameters (start_time, end_time, stride_time, parameters, tdoa)
        @param delivery_format The stream definition identifier for the outgoing granule (stream_defintinition_id)
        @param module          Module to chain a transform into
        @param cls             Class of the transform
        @param kwargs          Keyword Arguments to pass into the transform.

        '''
        dataset = self.clients.dataset_management.read_dataset(dataset_id)
        if dataset.coverage_type == CoverageTypeEnum.COMPLEX:
            raise BadRequest("Can't retrieve from complex coverage stub")

        retrieve_data = self.retrieve_oob(dataset_id=dataset_id,query=query,delivery_format=delivery_format)

        if module and cls:
            return self._transform_data(retrieve_data, module, cls, kwargs or {})

        return retrieve_data

    def retrieve_last_data_points(self, dataset_id='', number_of_points=100, delivery_format=''):
        rdt = ReplayProcess.get_last_values(dataset_id, number_of_points, delivery_format)
        return rdt.to_granule()

    def retrieve_last_granule(self, dataset_id='', delivery_format=''):
        return self.retrieve_last_data_points(dataset_id,10,delivery_format)

    def replay_data_process(self, dataset_id, query, delivery_format, replay_stream_id):
        dataset = self.clients.dataset_management.read_dataset(dataset_id=dataset_id)
        delivery_format = delivery_format or {}


        replay = Replay()
        replay.delivery_format = delivery_format

        replay.process_id = 'null'

        replay_id, rev = self.clients.resource_registry.create(replay)
        replay._id = replay_id
        replay._rev = rev
        config = {'process':{
            'query':query,
            'dataset_id':dataset_id,
            'delivery_format':delivery_format,
            'publish_streams':{'output':replay_stream_id}
            }
        }
        replay.config = config
        return replay

    @classmethod
    def _transform_data(binding, data, module, cls, kwargs={}):
        transform = for_name(module,cls)
        validate_is_instance(transform,TransformFunction,'%s.%s is not a TransformFunction' % (module,cls))
        return transform.execute(data,**kwargs)

