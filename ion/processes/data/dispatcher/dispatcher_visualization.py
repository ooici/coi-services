'''
@author Luke Campbell
@file ion/processes/data/dispatcher_process.py
@description Dispatcher Visualization handling set manipulations for the DataModel
'''
from pyon.datastore.datastore import DataStore
from pyon.public import log
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.processes.data.replay_process import ReplayProcess
from ion.processes.data.dispatcher.dispatcher_cache import DISPATCH_DATASTORE
from prototype.sci_data.constructor_apis import PointSupplementConstructor
from pyon.util.containers import DotDict

class DispatcherVisualization(ReplayProcess):
    def __init__(self, datastore_manager, stream_id, stream_definition_id, datastore_name=DISPATCH_DATASTORE, view_name='datasets/dataset_by_id' ):
        super(ReplayProcess, self).__init__()
        rr_cli = ResourceRegistryServiceClient()

        self.query = dict()

        self.db = datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA)

        self.delivery_format = dict()
        self.datastore_name = datastore_name

        definition = rr_cli.read(stream_definition_id)
        self.definition = definition.container
        self.fields = None

        self.view_name = view_name
        self.key_id = stream_id
        self.stream_id = stream_id

        self.data_stream_id = self.definition.data_stream_id
        self.encoding_id = self.definition.identifiables[self.data_stream_id].encoding_id
        self.element_type_id = self.definition.identifiables[self.data_stream_id].element_type_id
        self.element_count_id = self.definition.identifiables[self.data_stream_id].element_count_id
        self.data_record_id = self.definition.identifiables[self.element_type_id].data_record_id
        self.field_ids = self.definition.identifiables[self.data_record_id].field_ids
        self.domain_ids = self.definition.identifiables[self.data_record_id].domain_ids
        self.time_id = self.definition.identifiables[self.domain_ids[0]].temporal_coordinate_vector_id
        setattr(self, 'output', DotDict({'publish':self.send}))
        self.callback = lambda x: x


    def send(self, msg):
        log.debug('Calling callback')
        self.callback(msg)

    def execute_replay(self):
        datastore_name = self.datastore_name
        key_id = self.key_id
        self.result = None
        view_name = self.view_name

        opts = {
            'start_key':[key_id,0],
            'end_key':[key_id,2],
            'include_docs':True
        }

        self._query(datastore_name=datastore_name, view_name=view_name, opts=opts,
            callback=lambda results: self._publish_query(results))

        return self.result

    def _publish_query(self, results):
        '''
        @brief Publishes the appropriate data based on the delivery format and data returned from query
        @param results The query results from the couch query
        '''
        if results is None:
            log.info('No Results')
            return

        publish_queue = self._parse_results(results)
        for item in publish_queue:
            log.debug('Item in queue: %s' % type(item))
        granule = self._merge(publish_queue)
        if not granule:
            return # no dataset
        total_records = granule.identifiables[self.element_count_id].value
        granule.identifiables[self.element_count_id].constraint.intervals = [[0, total_records-1],]
        self.result = granule

    def _query(self,datastore_name='dm_datastore', view_name='posts/posts_by_id', opts={}, callback=None):
        ret = self.db.query_view(view_name, opts=opts)
        self._publish_query(ret)