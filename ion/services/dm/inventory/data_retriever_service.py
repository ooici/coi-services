'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/inventory/data_retriever_service.py
@description Data Retriever Service
'''

from interface.services.dm.idata_retriever_service import BaseDataRetrieverService
from interface.services.dm.ireplay_process import ReplayProcessClient
from interface.objects import Replay, ProcessDefinition, StreamDefinitionContainer
from prototype.sci_data.constructor_apis import DefinitionTree, StreamDefinitionConstructor
from pyon.core.exception import BadRequest, NotFound
from pyon.ion.transforma import TransformAlgorithm
from pyon.util.arg_check import validate_is_instance, validate_true
from ion.processes.data.replay.replay_process import ReplayProcess
from pyon.public import PRED, RT
from pyon.util.containers import for_name
from pyon.util.log import log


class DataRetrieverService(BaseDataRetrieverService):
    SCIENCE_REPLAY = 'SCIDATA'
    BINARY_REPLAY  = 'BINARY'

    REPLAY_TYPES = {
        SCIENCE_REPLAY : 'data_replay_process',
        BINARY_REPLAY  : 'binary_replay_process'
    }


    def on_quit(self): #pragma no cover
        #self.clients.process_dispatcher.delete_process_definition(process_definition_id=self.process_definition_id)
        super(DataRetrieverService,self).on_quit()


    def define_replay(self, dataset_id='', query=None, delivery_format=None, replay_type='', stream_id=''):
        ''' Define the stream that will contain the data from data store by streaming to an exchange name.
        query: 
          start_time: 0    The beginning timestamp
          end_time:   N    The ending timestamp
          parameters: []   The list of parameters which match the coverages parameters
        '''

        if not dataset_id and replay_type != self.BINARY_REPLAY:
            raise BadRequest('(Data Retriever Service %s): No dataset provided.' % self.name)
        validate_true(stream_id, 'No stream_id provided')

        if not replay_type:
            replay_type = self.SCIENCE_REPLAY
        if replay_type not in self.REPLAY_TYPES:
            replay_type = self.SCIENCE_REPLAY

        res, _  = self.clients.resource_registry.find_resources(restype=RT.ProcessDefinition,name=self.REPLAY_TYPES[replay_type],id_only=True)
        if not len(res):
            log.error('Failed to find replay process for replay_type: %s', replay_type)
            raise BadRequest('No replay process defined.')
        process_definition_id = res[0]

        replay_stream_id = stream_id

        #--------------------------------------------------------------------------------
        # Begin the Decision tree for the various types of replay
        #--------------------------------------------------------------------------------
        if replay_type == self.SCIENCE_REPLAY:
            replay, config=self.replay_data_process(dataset_id, query, delivery_format, replay_stream_id)
        elif replay_type == self.BINARY_REPLAY:
            replay, config=self.replay_binary_process(query,delivery_format,replay_stream_id)


        pid = self.clients.process_dispatcher.create_process(process_definition_id=process_definition_id)
        
        self.clients.process_dispatcher.schedule_process(process_definition_id=process_definition_id, process_id=pid, configuration=config)

        replay.process_id = pid

        self.clients.resource_registry.update(replay)
        self.clients.resource_registry.create_association(replay._id, PRED.hasStream, replay_stream_id)
        return replay._id

    def delete_replay(self,replay_id=''):
        assocs = self.clients.resource_registry.find_associations(subject=replay_id,predicate=PRED.hasStream)

        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)

        self.clients.resource_registry.delete(replay_id)

    def read_process_id(self, replay_id=''):
        replay = self.clients.resource_registry.read(replay_id)
        validate_is_instance(replay,Replay)

        return replay.process_id

    def start_replay(self, replay_id=''):
        """
        Problem: start_replay does not return until execute replay is complete - it is all chained rpc.
        Execute replay should be a command which is fired, not RPC???
        """

        pid = self.read_process_id(replay_id)
        cli = ReplayProcessClient(to_name=pid)
        cli.execute_replay()


    def cancel_replay(self, replay_id=''):
        replay = self.clients.resource_registry.read(replay_id)
        pid = replay.process_id
        self.clients.process_dispatcher.cancel_process(pid)

        for pred in [PRED.hasStream]:
            assocs = self.clients.resource_registry.find_associations(replay_id, pred, id_only=True)
            for assoc in assocs:
                self.clients.resource_registry.delete_association(assoc)

        self.clients.resource_registry.delete(replay_id)

    def retrieve(self, dataset_id='', query=None, delivery_format=None, module='', cls='', kwargs=None):
        '''
        Query can have the following parameters:
          start_time:  Beginning time value
          end_time:    Ending time value
          stride_time: The stride time
        '''
        if query is None:
            query = {}
        if delivery_format is None:
            delivery_format = {}

        validate_is_instance(query,dict,'Query was improperly formatted.')
        validate_true(dataset_id, 'No dataset provided')
        

        replay_instance = ReplayProcess()

        replay_instance.dataset = self.clients.dataset_management.read_dataset(dataset_id)
        replay_instance.dataset_id = dataset_id
        replay_instance.start_time = query.get('start_time', None)
        replay_instance.end_time = query.get('end_time', None)
        replay_instance.stride_time = query.get('stride_time', None)
        replay_instance.parameters = query.get('parameters',None)
        replay_instance.container = self.container

        retrieve_data = replay_instance.execute_retrieve()

        if module and cls:
            return self._transform_data(retrieve_data, module, cls, kwargs or {})

        return retrieve_data

    def retrieve_last_granule(self, dataset_id=''):
        return ReplayProcess.get_last_granule(self.container,dataset_id)

    def retrieve_last_data_point(self, dataset_id=''):
        return ReplayProcess.get_last_values(dataset_id)


    def replay_data_process(self, dataset_id, query, delivery_format, replay_stream_id):
        dataset = self.clients.dataset_management.read_dataset(dataset_id=dataset_id)
        datastore_name = dataset.datastore_name
        delivery_format = delivery_format or {}

        view_name = dataset.view_name
        key_id = dataset.primary_view_key
        # Make a new definition container


        replay = Replay()
        replay.delivery_format = delivery_format
        replay.type = self.SCIENCE_REPLAY

        replay.process_id = 'null'

        replay_id, rev = self.clients.resource_registry.create(replay)
        replay._id = replay_id
        replay._rev = rev
        config = {'process':{
            'query':query,
            'datastore_name':datastore_name,
            'dataset_id':dataset_id,
            'view_name':view_name,
            'key_id':key_id,
            'delivery_format':delivery_format,
            'publish_streams':{'output':replay_stream_id}
            }
        }
        return replay, config

    def replay_binary_process(self, query, delivery_format, replay_stream_id):
        delivery_format = delivery_format or {}
        replay = Replay()
        replay.delivery_format = delivery_format
        replay.type = self.BINARY_REPLAY

        replay.process_id = 'null'

        replay_id, rev = self.clients.resource_registry.create(replay)
        replay._id = replay_id
        replay._rev = rev
        config = {'process':{
            'query':query,
            'delivery_format': delivery_format,
            'publish_streams':{'output':replay_stream_id}
            }
        }
        return replay, config
    
    @classmethod
    def _transform_data(binding, data, module, cls, kwargs={}):
        transform = for_name(module,cls)
        validate_is_instance(transform,TransformAlgorithm,'%s.%s is not a TransformAlgorithm' % (module,cls))
        return transform.execute(data,**kwargs)

