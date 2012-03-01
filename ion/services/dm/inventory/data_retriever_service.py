'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/inventory/data_retriever_service.py
@description Data Retriever Service
'''

from interface.services.dm.idata_retriever_service import BaseDataRetrieverService
from interface.services.dm.ireplay_process import ReplayProcessClient
from interface.objects import Replay, ProcessDefinition, StreamDefinitionContainer
from ion.processes.data.replay_process import llog
from prototype.sci_data.constructor_apis import DefinitionTree, StationDataStreamDefinitionConstructor
from prototype.sci_data.ctd_stream import ctd_stream_definition
from pyon.core.exception import BadRequest
from pyon.public import PRED



class DataRetrieverService(BaseDataRetrieverService):

    def __init__(self, *args, **kwargs):
        super(DataRetrieverService,self).__init__(*args,**kwargs)

    def on_start(self):
        super(DataRetrieverService,self).on_start()
        self.process_definition = ProcessDefinition()
        self.process_definition.executable['module']='ion.processes.data.replay_process'
        self.process_definition.executable['class'] = 'ReplayProcess'
        self.process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=self.process_definition)


    def on_quit(self):
        self.clients.process_dispatcher.delete_process_definition(process_definition_id=self.process_definition_id)
        super(DataRetrieverService,self).on_quit()





    def define_replay(self, dataset_id='', query=None, delivery_format=None):
        ''' Define the stream that will contain the data from data store by streaming to an exchange name.

        '''
        # Get the datastore name from the dataset object, use dm_datastore by default.
        """
        delivery_format
            - fields
        """
        if not dataset_id:
            raise BadRequest('(Data Retriever Service %s): No dataset provided.' % self.name)

        dataset = self.clients.dataset_management.read_dataset(dataset_id=dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name)
        delivery_format = delivery_format or {}
        fields = delivery_format.get('fields',None)
        view_name = dataset.view_name
        key_id = dataset.primary_view_key
        # Make a new definition container


        if fields:
            """
            break down
            Create a new definition without fields
            forvery field in fields append the important stuff to the new definition
            """

            def traverse(identifiables,node):
                '''
                Recursively traverse the definition for keys with _id and return all the values in a list to append later
                '''
                retval = []
                for key in dir(node):
                    if key.endswith('_id'):
                        val = getattr(node,key)
                        retval.append(val)
                        if val in identifiables:
                            r = traverse(identifiables,identifiables[getattr(node,key)])
                            for i in r:
                                if i:
                                    retval.append(i)

                return retval


            definition = datastore.query_view('datasets/dataset_by_id',opts={'key':[dataset.primary_view_key,0],'include_docs':True})[0]['doc']
            definition_constructor = StationDataStreamDefinitionConstructor()
            definition_container = definition_constructor.stream_definition
            definition_container.identifiables['data_record'].domain_ids = definition.identifiables['data_record'].domain_ids

            llog('Before')
            llog('%s' % definition_container.identifiables.keys())

            for field_id in fields:
                # I need to traverse each node looking for _id
                identifiables = definition.identifiables
                field = identifiables[field_id]
                for id in traverse(identifiables, field):
                    if id not in definition_container.identifiables:
                        llog('Copying %s' % id)
                        definition_container.identifiables[id] = identifiables[id]
                llog('Copying coverage %s' % field_id)
                definition_container.identifiables[field_id] = field
            llog('After')
            llog('%s' % definition_container.identifiables.keys())

        else:
            definition_container = ctd_stream_definition()





        # Tell pubsub about our definition that we want to use and setup the association so clients can figure out
        # What belongs on the stream
        definition_id = self.clients.pubsub_management.create_stream_definition(container=definition_container)
        # Make a stream
        replay_stream_id = self.clients.pubsub_management.create_stream(stream_definition_id=definition_id)
        replay = Replay()
        replay.delivery_format = delivery_format





        replay.process_id = 0

        replay_id, rev = self.clients.resource_registry.create(replay)
        replay._id = replay_id
        replay._rev = rev
        config = {'process':{
            'query':query,
            'datastore_name':datastore_name,
            'view_name':view_name,
            'key_id':key_id,
            'delivery_format':dict({'container':definition_container}, **delivery_format),
            'publish_streams':{'output':replay_stream_id}
            }
        }


        pid = self.clients.process_dispatcher.schedule_process(
            process_definition_id=self.process_definition_id,
            configuration=config
        )

        replay.process_id = pid

        self.clients.resource_registry.update(replay)
        self.clients.resource_registry.create_association(replay_id, PRED.hasStream, replay_stream_id)
        return (replay_id, replay_stream_id)



    def start_replay(self, replay_id=''):
        """
        Problem: start_replay does not return until execute replay is complete - it is all chained rpc.
        Execute replay should be a command which is fired, not RPC???
        """

        replay = self.clients.resource_registry.read(replay_id)
        pid = replay.process_id
        cli = ReplayProcessClient(name=pid)
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

