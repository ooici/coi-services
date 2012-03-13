'''
@author Luke Campbell
@file ion/processes/data/ingestion/ingestion_aggregation
@description Ingestion Process for aggregating data
'''
from interface.objects import StreamGranuleContainer, StreamDefinitionContainer, CountElement, QuantityRangeElement, RangeSet
from ion.processes.data.replay_process import ReplayProcess
from prototype.sci_data.constructor_apis import DefinitionTree
from pyon.core.exception import NotFound
from pyon.public import log
from pyon.ion.transform import TransformDataProcess
from pyon.datastore.datastore import DataStore


class IngestionAggregation(TransformDataProcess):
    def on_start(self):

        self.couch_config = self.CFG.get('couch_storage')

        self.datastore_name = self.couch_config.get('datastore_name','dm_aggregation')

        try:
            self.datastore_profile = getattr(DataStore.DS_PROFILE,self.couch_config.get('datastore_profile','SCIDATA'))
        except AttributeError:
            self.datastore_profile = DataStore.DS_PROFILE.SCIDATA



        self.db = self.container.datastore_manager.get_datastore(ds_name=self.datastore_name,
            profile=self.datastore_profile)

    def process(self, packet):

        if isinstance(packet,StreamDefinitionContainer):
            definition = self.db._ion_object_to_persistence_dict(packet)
            self.db.create_doc(definition, object_id=packet.stream_resource_id)
            return

        elif isinstance(packet,StreamGranuleContainer):
            granule = packet
            doc = None
            try:
                doc = self.db.read(granule.stream_resource_id)
            except NotFound:
                log.error('Packet could not be aggregated due to query not found. Stream Resource ID: %s' % granule.stream_resource_id)
                return

            outgoing = self.merge(doc,doc, granule)
            self.db.update(outgoing)
            return
        else:
            log.warning('Unknown packet type %s' % str(type(packet)))

        return

    def _list_data(self,definition, granule):
        '''
        @brief Lists all the fields in the granule based on the Stream Definition
        @param definition Stream Definition
        @param granule Stream Granule
        @return dict of field_id : values_path for each field_id that exists
        '''
        from interface.objects import StreamDefinitionContainer, StreamGranuleContainer, RangeSet, CoordinateAxis
        assert isinstance(definition, StreamDefinitionContainer), 'object is not a definition.'
        assert isinstance(granule, StreamGranuleContainer) or isinstance(granule,StreamDefinitionContainer), 'object is not a granule.'
        retval = {}
        for key, value in granule.identifiables.iteritems():
            if isinstance(value, RangeSet):
                values_path = value.values_path or definition.identifiables[key].values_path
                retval[key] = values_path

            elif isinstance(value, CoordinateAxis):
                values_path = value.values_path or definition.identifiables[key].values_path
                retval[key] = values_path

        return retval

    def merge(self, definition, granule1, granule2):
        '''
        @brief Merges two granules based on the definition
        @param definition Stream Definition
        @param granule1 First Granule
        @param granule2 Second Granule
        @return Returns granule1 which is then merged with granule2 and the file pair for indexing

        @description granule1 := granule1 U granule2
        '''
        import numpy as np

        encoding_id = DefinitionTree.get(definition,'%s.encoding_id' % definition.data_stream_id)

        #-------------------------------------------------------------------------------------
        # First step is figure out where each granule belongs on the timeline
        # We do this with a tuple consisting of the point in the timeline and the filename
        # These will get stable sorted later
        #-------------------------------------------------------------------------------------



        element_count_id = DefinitionTree.get(definition,'%s.element_count_id' % definition.data_stream_id)
        record_count = 0
        if element_count_id in granule1.identifiables:
            record_count += granule1.identifiables[element_count_id].value
        if element_count_id in granule2.identifiables:
            record_count += granule2.identifiables[element_count_id].value

        if not element_count_id in granule1.identifiables:
            granule1.identifiables[element_count_id] = CountElement()
            granule1.identifiables[element_count_id].value = record_count
        else:
            granule1.identifiables[element_count_id].value = record_count

        fields1 = self._list_data(definition, granule1)
        fields2 = self._list_data(definition, granule2)
        #@todo albeit counterintuitive an intersection is the only thing I can support




        for k,v in granule2.identifiables.iteritems():
            # Switch(value):

            # Case Bounds:
            if isinstance(v, QuantityRangeElement):
                # If its not in granule1 just throw it in there
                if k not in granule1.identifiables:
                    granule1.identifiables[k] = v
                else:
                    bounds1 = granule1.identifiables[k].value_pair
                    bounds2 = granule2.identifiables[k].value_pair
                    bounds = np.append(bounds1,bounds2)
                    granule1.identifiables[k].value_pair = [np.nanmin(bounds), np.nanmax(bounds)]


            if isinstance(v, RangeSet): #Including coordinate axis
                if not granule1.identifiables.has_key(k):
                    granule1.identifiables[k] = v

                else:
                    g1 = granule1.identifiables[k]
                    updatable = g1.updatable and v.updatable
                    granule1.identifiables[k].updatable = updatable

                    optional = g1.optional and v.optional
                    granule1.identifiables[k].optional = optional

        return granule1

