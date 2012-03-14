'''
@author Luke Campbell
@file ion/processes/data/ingestion/ingestion_aggregation
@description Ingestion Process for aggregating data
'''
from prototype.sci_data.stream_parser import PointSupplementStreamParser
from pyon.core.exception import NotFound
from pyon.public import log
from pyon.ion.transform import TransformDataProcess
from pyon.datastore.datastore import DataStore
from pyon.util.file_sys import FileSystem
from interface.objects import StreamGranuleContainer, Variable, LastUpdate
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from prototype.hdf.hdf_array_iterator import acquire_data



CACHE_DATASTORE_NAME = 'last_update_datastore'

class LastUpdateCache(TransformDataProcess):
    def __init__(self, *args, **kwargs):
        super(LastUpdateCache, self)
        self.def_cache = {}


    def on_start(self):

        self.couch_config = self.CFG.get('couch_storage')

        #self.datastore_name = self.couch_config.get('datastore_name','dm_cache')
        self.datastore_name = CACHE_DATASTORE_NAME


        try:
            self.datastore_profile = getattr(DataStore.DS_PROFILE,self.couch_config.get('datastore_profile','SCIDATA'))
        except AttributeError:
            self.datastore_profile = DataStore.DS_PROFILE.SCIDATA

        self.db = self.container.datastore_manager.get_datastore(ds_name=self.datastore_name,
            profile=self.datastore_profile)

        self.ps_cli = PubsubManagementServiceProcessClient(process=self)



    def process(self, packet):

        if isinstance(packet,StreamGranuleContainer):
            granule = packet
            lu = self.db._ion_object_to_persistence_dict(self.get_last_value(granule))
            try:
                doc = self.db.read_doc(granule.stream_resource_id)
                doc_id = doc.id
                doc_rev = doc.rev

            except NotFound:
                log.debug('Creating...')
                doc_id, doc_rev = self.db.create_doc(lu,object_id=granule.stream_resource_id)
            lu['_id'] = doc_id
            lu['_rev'] = doc_rev

            self.db.update_doc(lu)


        else:
            log.warning('Unknown packet type %s' % str(type(packet)))

        return

    def get_last_value(self,granule):

        stream_resource_id = granule.stream_resource_id
        if not self.def_cache.has_key(stream_resource_id):
            stream_def = self.ps_cli.find_stream_definition(stream_id=stream_resource_id, id_only=False)
            self.def_cache[stream_resource_id] = stream_def.container

        definition = self.def_cache[stream_resource_id]

        psp = PointSupplementStreamParser(stream_definition=definition, stream_granule=granule)
        fields = psp.list_field_names()


        lu = LastUpdate()
        lu.timestamp = granule.identifiables[granule.data_stream_id].timestamp.value
        for field in fields:
            range_id = definition.identifiables[field].range_id
            lu.variables[field] = Variable()
            if definition.identifiables.has_key(field):
                lu.variables[field].definition = definition.identifiables[field].definition
            if definition.identifiables.has_key(range_id):
                lu.variables[field].units = definition.identifiables[range_id].unit_of_measure.code
            lu.variables[field].value = float(psp.get_values(field_name=field)[-1])
        return lu