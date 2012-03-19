'''
@author Luke Campbell
@file ion/processes/data/dispatcher_process.py
@description Dispatcher Visualization handling set manipulations for the DataModel
'''
from pyon.public import log
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.processes.data.replay_process import ReplayProcess
from ion.processes.data.dispatcher.dispatcher_cache import DISPATCH_DATASTORE
from prototype.sci_data.constructor_apis import PointSupplementConstructor
from pyon.util.containers import DotDict

class DispatcherVisualization(ReplayProcess):
    def on_start(self):
        super(ReplayProcess, self).on_start()
        self.query = self.CFG.get_safe('process.query',{})

        self.delivery_format = self.CFG.get_safe('process.delivery_format',{})
        self.datastore_name = self.CFG.get_safe('process.datastore_name',DISPATCH_DATASTORE)

        definition_id = self.delivery_format.get('definition_id')
        rr_cli = ResourceRegistryServiceClient()
        definition = rr_cli.read(definition_id)
        self.definition = definition.container
        self.fields = self.delivery_format.get('fields',None)

        self.view_name = self.CFG.get_safe('process.view_name','datasets/dataset_by_id')
        self.key_id = self.CFG.get_safe('process.key_id')
        self.stream_id = self.key_id

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