'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/inventory/data_retriever_service.py
@description Data Retriever Service
'''
from interface.services.dm.ireplay_agent import ReplayAgentClient
from interface.services.dm.idata_retriever_service import BaseDataRetrieverService
from interface.objects import Replay
from pyon.public import PRED
from pyon.service.service import BaseService


class DataRetrieverService(BaseDataRetrieverService):

    def __init__(self, *args, **kwargs):
        super(DataRetrieverService,self).__init__(*args,**kwargs)

    def on_start(self):
        super(DataRetrieverService,self).on_start()

    def define_replay(self, dataset_id='', query={}, delivery_format={}):
        ''' Define the stream that will contain the data from data store by streaming to an exchange name.
        '''
        # first things first, let's get a stream
        replay_stream_id = self.clients.pubsub_management.create_stream(original=True)
        replay = Replay()
        replay.delivery_format = delivery_format
        replay.query = query
        #@todo: make an actual process id

        replay.process_id = 0

        replay_id, rev = self.clients.resource_registry.create(replay)
        replay._id = replay_id
        replay._rev = rev
        config = {'process':{'query':query, 'delivery_format':delivery_format,'publish_streams':{'output':replay_stream_id}}}
        pid = self.container.spawn_process(name=replay_id+'agent',
            module='ion.services.dm.inventory.replay_agent',
            cls='ReplayAgent',
            config=config,
            process_type='agent')

        pid = self.container.id + '.' + pid
        replay.process_id = pid

        self.clients.resource_registry.update(replay)



        self.clients.resource_registry.create_association(replay_id, PRED.hasStream, replay_stream_id)
        return (replay_id, replay_stream_id)



    def start_replay(self, replay_id=''):
        replay = self.clients.resource_registry.read(replay_id)
        pid = replay.process_id
        cli = ReplayAgentClient(name=pid, node=self.container.node)
        cli.execute_replay()

    def cancel_replay(self, replay_id=''):
        replay = self.clients.resource_registry.read(replay_id)
        pid = replay.process_id
        self.container.proc_manager.terminate_process(pid)

        for pred in [PRED.hasStream]:
            assocs = self.clients.resource_registry.find_associations(replay_id, pred, id_only=True)
            for assoc in assocs:
                self.clients.resource_registry.delete_association(assoc)

        self.clients.resource_registry.delete(replay_id)

