#!/usr/bin/env python
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.core.object import IonObjectBase

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.public import  log, IonObject
from interface.services.sa.idata_product_management_service import BaseDataProductManagementService

from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from interface.objects import DataProduct, DataProductVersion
from interface.objects import ComputedValueAvailability

from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT, OT, PRED, LCS, CFG
from pyon.util.ion_time import IonTime
from pyon.ion.resource import ExtendedResourceContainer
from pyon.util.arg_check import validate_is_instance, validate_is_not_none, validate_false
import string
from lxml import etree
from datetime import datetime
from ion.util.time_utils import TimeUtils

import numpy as np

class DataProductManagementService(BaseDataProductManagementService):
    """ @author     Bill Bollenbacher
        @file       ion/services/sa/product/data_product_management_service.py
        @brief      Implementation of the data product management service
    """

    def on_init(self):
        self.RR2 = EnhancedResourceRegistryClient(self.clients.resource_registry)

    def create_data_product(self, data_product=None, stream_definition_id='', exchange_point=''):
        """
        @param      data_product IonObject which defines the general data product resource
        @param      source_resource_id IonObject id which defines the source for the data
        @retval     data_product_id
        """


        # Create will validate and register a new data product within the system
        # If the stream definition has a parameter dictionary, use that
        validate_is_not_none(stream_definition_id, 'A stream definition id must be passed to register a data product')
        stream_def_obj = self.clients.pubsub_management.read_stream_definition(stream_definition_id) # Validates and checks for param_dict
        parameter_dictionary = stream_def_obj.parameter_dictionary 
        validate_is_not_none(parameter_dictionary , 'A parameter dictionary must be passed to register a data product')
        validate_is_not_none(data_product, 'A data product (ion object) must be passed to register a data product')
        exchange_point = exchange_point or 'science_data'

        #--------------------------------------------------------------------------------
        # Register - create and store a new DataProduct resource using provided metadata
        #--------------------------------------------------------------------------------
        data_product_id = self.RR2.create(data_product, RT.DataProduct)


        #-----------------------------------------------------------------------------------------------
        #Create the stream and a dataset if a stream definition is provided
        #-----------------------------------------------------------------------------------------------

        #if stream_definition_id:
        #@todo: What about topics?

        stream_id,route = self.clients.pubsub_management.create_stream(name=data_product.name,
                                                                exchange_point=exchange_point,
                                                                description=data_product.description,
                                                                stream_definition_id=stream_definition_id)

        # Associate the Stream with the main Data Product and with the default data product version
        self.RR2.assign_stream_to_data_product(stream_id, data_product_id)



        # Return the id of the new data product
        return data_product_id

    def read_data_product(self, data_product_id=''):
        """
        method docstring
        """
        # Retrieve all metadata for a specific data product
        # Return data product resource

        data_product = self.RR2.read(data_product_id, RT.DataProduct)

        return data_product


    def update_data_product(self, data_product=None):
        """
        @todo document this interface!!!

        @param data_product    DataProduct
        @throws NotFound    object with specified id does not exist
        """

        self.RR2.update(data_product, RT.DataProduct)

        #TODO: any changes to producer? Call DataAcquisitionMgmtSvc?

    def delete_data_product(self, data_product_id=''):

        #--------------------------------------------------------------------------------
        # suspend persistence
        #--------------------------------------------------------------------------------
        if self.is_persisted(data_product_id):
            self.suspend_data_product_persistence(data_product_id)
        #--------------------------------------------------------------------------------
        # remove stream associations
        #--------------------------------------------------------------------------------
        #self.remove_streams(data_product_id)


        self.RR2.retire(data_product_id, RT.DataProduct)


    def force_delete_data_product(self, data_product_id=''):


        #get the assoc producers before deleteing the links
        producer_ids = self.RR2.find_data_producer_ids_of_data_product(data_product_id)

        self.RR2.pluck(data_product_id)
        for producer_id in producer_ids:
            self.RR2.delete(producer_id)

        self.RR2.pluck_delete(data_product_id, RT.DataProduct)

    def remove_streams(self, data_product_id=''):
        streams, assocs = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        datasets, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataset, id_only=True)
        for dataset in datasets:
            for stream, assoc in zip(streams,assocs):
                self.clients.resource_registry.delete_association(assoc)
                self.clients.pubsub_management.delete_stream(stream)
                self.clients.dataset_management.remove_stream(dataset, stream)

        return streams


    def find_data_products(self, filters=None):
        """
        method docstring
        """
        # Validate the input filter and augment context as required

        # Define set of resource attributes to filter on, change parameter from "filter" to include attributes and filter values.
        #     potentially: title, keywords, date_created, creator_name, project, geospatial coords, time range

        # Call DM DiscoveryService to query the catalog for matches

        # Organize and return the list of matches with summary metadata (title, summary, keywords)

        ret, _ = self.clients.resource_registry.find_resources(RT.DataProduct, None, None, False)
        return ret



    def activate_data_product_persistence(self, data_product_id=''):
        """Persist data product data into a data set

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """
        #--------------------------------------------------------------------------------
        # retrieve the data_process object
        #--------------------------------------------------------------------------------
        data_product_obj = self.RR2.read(data_product_id)

        validate_is_not_none(data_product_obj, "The data product id should correspond to a valid registered data product.")

        stream_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        if not stream_ids:
            raise BadRequest('Specified DataProduct has no streams associated with it')
        stream_id = stream_ids[0]

        stream_defs, _ = self.clients.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition,id_only=True)
        if not stream_defs:
            raise BadRequest("Data Product stream is without a stream definition")
        stream_def_id = stream_defs[0]

        stream_def = self.clients.pubsub_management.read_stream_definition(stream_def_id) # additional read necessary to fill in the pdict

        
        

        dataset_id = self.clients.dataset_management.create_dataset(   name= 'data_set_%s' % stream_id,
                                                                        stream_id=stream_id,
                                                                        parameter_dict=stream_def.parameter_dictionary,
                                                                        temporal_domain=data_product_obj.temporal_domain,
                                                                        spatial_domain=data_product_obj.spatial_domain)

        # link dataset with data product. This creates the association in the resource registry
        self.RR2.assign_dataset_to_data_product(dataset_id, data_product_id)
        
        log.debug("Activating data product persistence for stream_id: %s"  % str(stream_id))



        #-----------------------------------------------------------------------------------------
        # grab the ingestion configuration id from the data_product in order to use to persist it
        #-----------------------------------------------------------------------------------------
        if data_product_obj.dataset_configuration_id:
            ingestion_configuration_id = data_product_obj.dataset_configuration_id
        else:
            ingestion_configuration_id = self.clients.ingestion_management.list_ingestion_configurations(id_only=True)[0]

        #--------------------------------------------------------------------------------
        # persist the data stream using the ingestion config id and stream id
        #--------------------------------------------------------------------------------

        # find datasets for the data product
        dataset_id = self.clients.ingestion_management.persist_data_stream(stream_id=stream_id,
                                                ingestion_configuration_id=ingestion_configuration_id,
                                                dataset_id=dataset_id)

        # register the dataset for externalization
        self.clients.dataset_management.register_dataset(dataset_id, external_data_product_name=data_product_obj.description or data_product_obj.name)


        #--------------------------------------------------------------------------------
        # todo: dataset_configuration_obj contains the ingest config for now...
        # Update the data product object
        #--------------------------------------------------------------------------------
        data_product_obj.dataset_configuration_id = ingestion_configuration_id
        self.update_data_product(data_product_obj)



    def is_persisted(self, data_product_id=''):
        # Is the data product currently persisted into a data set?
        if data_product_id:
            stream_id = self._get_stream_id(data_product_id)
            if stream_id:
                return self.clients.ingestion_management.is_persisted(stream_id)
        return False



    def suspend_data_product_persistence(self, data_product_id=''):
        """Suspend data product data persistence into a data set, multiple options

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """

        #--------------------------------------------------------------------------------
        # retrieve the data_process object
        #--------------------------------------------------------------------------------
        data_product_obj = self.clients.resource_registry.read(data_product_id)

        validate_is_not_none(data_product_obj, 'Should not have been empty')
        validate_is_instance(data_product_obj, DataProduct)

        if data_product_obj.dataset_configuration_id is None:
            raise NotFound("Data Product %s dataset configuration does not exist" % data_product_id)

        #--------------------------------------------------------------------------------
        # get the Stream associated with this data product; if no stream then create one, if multiple streams then Throw
        #streams = self.data_product.find_stemming_stream(data_product_id)
        #--------------------------------------------------------------------------------
        stream_id = self._get_stream_id(data_product_id)
        validate_is_not_none(stream_id, 'Data Product %s must have one stream associated' % str(data_product_id))

        ret = self.clients.ingestion_management.unpersist_data_stream(stream_id=stream_id, ingestion_configuration_id=data_product_obj.dataset_configuration_id)

        #--------------------------------------------------------------------------------
        # detach the dataset from this data product
        #--------------------------------------------------------------------------------
#        dataset_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataset, id_only=True)
#        for dataset_id in dataset_ids:
#            self.data_product.unlink_data_set(data_product_id, dataset_id)


    def get_data_product_stream_definition(self, data_product_id=''):
        self.read_data_product(data_product_id)
        streams, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        for stream in streams:
            stream_defs, _ = self.clients.resource_registry.find_objects(subject=stream, predicate=PRED.hasStreamDefinition, id_only=True)
            if stream_defs:
                return stream_defs[0]
    
    def get_data_product_provenance(self, data_product_id=''):

        # Retrieve information that characterizes how this data was produced
        # Return in a dictionary

        self.provenance_results = {}

        data_product = self.RR2.read(data_product_id)
        validate_is_not_none(data_product, "Should have got a non empty data product")

        # todo: get the start time of this data product
        self._find_producers(data_product_id, self.provenance_results)

        return self.provenance_results

    def get_data_product_provenance_report(self, data_product_id=''):

        # Retrieve information that characterizes how this data was produced
        # Return in a dictionary

        self.provenance_results = self.get_data_product_provenance(data_product_id)

        results = ''

        results = self._write_product_provenance_report(data_product_id, self.provenance_results)

        return results

    ############################
    #
    #  Data Product Collections
    #
    ############################


    def create_data_product_collection(self, data_product_id='', collection_name='', collection_description=''):
        """Define a  set of an existing data products that represent an improvement in the quality or
        understanding of the information.
        """
        validate_is_not_none(data_product_id, 'A data product identifier must be passed to create a data product version')

        dpv = DataProductVersion()
        dpv.name = 'base'
        dpv.description = 'the base version on which subsequent versions are built'
        dpv.data_product_id = data_product_id

        dp_collection_obj = IonObject(RT.DataProductCollection, name=collection_name, description=collection_description, version_list=[dpv])

        data_product_collection_id, rev = self.clients.resource_registry.create(dp_collection_obj)
        self.clients.resource_registry.create_association( subject=data_product_collection_id, predicate=PRED.hasVersion, object=data_product_id)

        return data_product_collection_id



    def update_data_product_collection(self, data_product_collection=None):
        """@todo document this interface!!!

        @param data_product_collection    DataProductCollection
        @throws NotFound    object with specified id does not exist
        """


        self.RR2.update(data_product_collection, RT.DataProductCollection)

        #TODO: any changes to producer? Call DataAcquisitionMgmtSvc?

        return

    def read_data_product_collection(self, data_product_collection_id=''):
        """Retrieve data product information

        @param data_product_collection_id    str
        @retval data_product    DataProductVersion
        """
        result = self.RR2.read(data_product_collection_id, RT.DataProductCollection)

        return result

    def delete_data_product_collection(self, data_product_collection_id=''):
        """Remove a version of an data product.

        @param data_product_collection_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """

        #check that all assoc data products are deleted
        dataproduct_objs, _ = self.clients.resource_registry.find_objects(subject=data_product_collection_id, predicate=PRED.hasVersion, object_type=RT.DataProduct, id_only=False)
        for dataproduct_obj in dataproduct_objs:
            if dataproduct_obj.lcstate != LCS.RETIRED:
                raise BadRequest("All Data Products in a collection must be deleted before the collection is deleted.")

        self.RR2.retire(data_product_collection_id, RT.DataProductCollection)

    def force_delete_data_product_collection(self, data_product_collection_id=''):

        # if not yet deleted, the first execute delete logic
        dp_obj = self.read_data_product_collection(data_product_collection_id)
        if dp_obj.lcstate != LCS.RETIRED:
            self.delete_data_product_collection(data_product_collection_id)

        self.RR2.pluck_delete(data_product_collection_id, RT.DataProductCollection)

    def add_data_product_version_to_collection(self, data_product_id='', data_product_collection_id='', version_name='', version_description=''):


        dp_collection_obj =self.clients.resource_registry.read(data_product_collection_id)

        #retrieve the stream definition for both the new data product to add to this collection and the base data product for this collection
        new_data_product_obj = self.clients.resource_registry.read(data_product_id)
        new_data_product_streams, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        validate_is_not_none(new_data_product_streams, 'The data product to add to the collection must have an associated stream')
        new_data_product_streamdefs, _ = self.clients.resource_registry.find_objects(subject=new_data_product_streams[0], predicate=PRED.hasStreamDefinition, object_type=RT.StreamDefinition, id_only=True)

        base_data_product_id = dp_collection_obj.version_list[0].data_product_id
        base_data_product_obj = self.clients.resource_registry.read(base_data_product_id)
        base_data_product_streams, _ = self.clients.resource_registry.find_objects(subject=base_data_product_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        validate_is_not_none(base_data_product_streams, 'The base data product in the collection must have an associated stream')
        base_data_product_streamdefs, _ = self.clients.resource_registry.find_objects(subject=base_data_product_streams[0], predicate=PRED.hasStreamDefinition, object_type=RT.StreamDefinition, id_only=True)
        if not self.clients.pubsub_management.compare_stream_definition(stream_definition1_id=new_data_product_streamdefs[0], stream_definition2_id=base_data_product_streamdefs[0]):
            raise BadRequest("All Data Products in a collection must have equivelent stream definitions.")

        #todo: validate that the spatial/temporal domain match the base data product


        dpv = DataProductVersion()
        dpv.name = version_name
        dpv.description = version_description
        dpv.data_product_id = data_product_id

        dp_collection_obj.version_list.append(dpv)
        self.clients.resource_registry.update(dp_collection_obj)

        self.clients.resource_registry.create_association( subject=data_product_collection_id, predicate=PRED.hasVersion, object=data_product_id)

        return

    def get_current_version(self, data_product_collection_id=''):

        data_product_collection_obj = self.clients.resource_registry.read(data_product_collection_id)

        count = len (data_product_collection_obj.version_list)


        dpv_obj = data_product_collection_obj.version_list[count - 1]

        return dpv_obj.data_product_id

    def get_base_version(self, data_product_collection_id=''):

        data_product_collection_obj = self.clients.resource_registry.read(data_product_collection_id)

        dpv_obj = data_product_collection_obj.version_list[0]

        return dpv_obj.data_product_id


    def execute_data_product_lifecycle(self, data_product_id="", lifecycle_event=""):
        """
        declare a data_product to be in a given state
        @param data_product_id the resource id
        """
        return self.RR2.advance_lcs(data_product_id, lifecycle_event)

    def get_last_update(self, data_product_id=''):
        """@todo document this interface!!!

        @param data_product_id    str
        @retval last_update    LastUpdate
        @throws NotFound    Data product not found or cache for data product not found.
        """
        from ion.processes.data.last_update_cache import CACHE_DATASTORE_NAME
        datastore_name = CACHE_DATASTORE_NAME
        db = self.container.datastore_manager.get_datastore(datastore_name)
        stream_ids,other = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        retval = {}
        for stream_id in stream_ids:
            try:
                lu = db.read(stream_id)
                retval[stream_id] = lu
            except NotFound:
                continue
        return retval

    def _get_dataset_id(self, data_product_id=''):
        # find datasets for the data product
        dataset_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.Dataset, id_only=True)
        if not dataset_ids:
            raise NotFound('No Dataset is associated with DataProduct %s' % data_product_id)
        return dataset_ids[0]

    def _get_stream_id(self, data_product_id=''):
        # find datasets for the data product
        stream_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStream, RT.Stream, id_only=True)
        return stream_ids[0]

    ############################
    #
    #  EXTENDED RESOURCES
    #
    ############################



    def get_data_product_extension(self, data_product_id='', ext_associations=None, ext_exclude=None, user_id=''):
        #Returns an DataProductExtension object containing additional related information

        if not data_product_id:
            raise BadRequest("The data_product_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_product = extended_resource_handler.create_extended_resource_container(
            extended_resource_type=OT.DataProductExtension,
            resource_id=data_product_id,
            computed_resource_type=OT.DataProductComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id)

        #Loop through any attachments and remove the actual content since we don't need
        #   to send it to the front end this way
        #TODO - see if there is a better way to do this in the extended resource frame work.
        if hasattr(extended_product, 'attachments'):
            for att in extended_product.attachments:
                if hasattr(att, 'content'):
                    delattr(att, 'content')

        #extract the list of upstream data products from the provenance results
        dp_list = []
        for key, value in extended_product.computed.provenance.value.iteritems():
            for producer_id, dataprodlist in value['inputs'].iteritems():
                for dataprod in dataprodlist:
                    dp_list.append( self.clients.resource_registry.read(dataprod) )
        extended_product.provenance_product_list = set(dp_list)  #remove dups in list

        #set the data_ingestion_datetime from get_data_datetime
        if extended_product.computed.data_datetime.status == ComputedValueAvailability.PROVIDED :
            extended_product.data_ingestion_datetime =  extended_product.computed.data_datetime.value[1]

        # divide up the active and past user subscriptions
        active = []
        nonactive = []
        for notification_obj in extended_product.computed.active_user_subscriptions.value:
            if notification_obj.lcstate == LCS.RETIRED:
                nonactive.append(notification_obj)
            else:
                active.append(notification_obj)

        extended_product.computed.active_user_subscriptions.value = active
        extended_product.computed.past_user_subscriptions.value = nonactive
        extended_product.computed.past_user_subscriptions.status = ComputedValueAvailability.PROVIDED
        extended_product.computed.number_active_subscriptions.value = len(active)
        extended_product.computed.number_active_subscriptions.status = ComputedValueAvailability.PROVIDED

        # replace list of lists with single list
        replacement_data_products = []
        for inner_list in extended_product.process_input_data_products:
            if inner_list:
                for actual_data_product in inner_list:
                    if actual_data_product:
                        replacement_data_products.append(actual_data_product)
        extended_product.process_input_data_products = replacement_data_products

        return extended_product


    def get_data_datetime(self, data_product_id=''):
        # Returns a temporal bounds object of the span of data product life span (may exist without getting a granule)
        ret = IonObject(OT.ComputedListValue)
        ret.value = []
        ret.status = ComputedValueAvailability.NOTAVAILABLE

        try:
            dataset_id = self._get_dataset_id(data_product_id)
            bounds = self.clients.dataset_management.dataset_bounds(dataset_id)
            if 'time' in bounds and len(bounds['time']) == 2 :
                log.debug("get_data_datetime bounds['time']: %s"  % str(dataset_id))
                timeStart = IonTime(bounds['time'][0]  -  IonTime.JAN_1970)
                timeEnd = IonTime(bounds['time'][1]  -  IonTime.JAN_1970)
                ret.value = [str(timeStart), str(timeEnd)]
                ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Dataset for this Data Product could not be located"
        except Exception as e:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Could not calculate time range for this data product"

        return ret


#    def get_data_ingestion_datetime(self, data_product_id=''):
#        # Returns a temporal bounds object of the earliest/most recent values ingested into in the data product
#        ret = IonObject(OT.ComputedStringValue)
#        ret.value = ""
#        ret.status = ComputedValueAvailability.NOTAVAILABLE
#        ret.reason = "FIXME. also, should datetime be stored as a string?"
#
#        return ret


    def get_product_download_size_estimated(self, data_product_id=''):
        # Returns the size of the full data product if downloaded/presented in a given presentation form
        ret = IonObject(OT.ComputedIntValue)
        ret.value = 0
        try:
            dataset_id = self._get_dataset_id(data_product_id)
            size_in_bytes = self.clients.dataset_management.dataset_size(dataset_id, in_bytes=False)
            ret.status = ComputedValueAvailability.PROVIDED
            ret.value = size_in_bytes
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Dataset for this Data Product could not be located"
        except Exception as e:
            raise e

        return ret


    def get_stored_data_size(self, data_product_id=''):
        # Returns the storage size occupied by the data content of the resource, in bytes.
        ret = IonObject(OT.ComputedIntValue)
        ret.value = 0
        try:
            dataset_id = self._get_dataset_id(data_product_id)
            size_in_bytes = self.clients.dataset_management.dataset_size(dataset_id, in_bytes=True)
            ret.status = ComputedValueAvailability.PROVIDED
            ret.value = size_in_bytes
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Dataset for this Data Product could not be located"
        except Exception as e:
            raise e

        return ret


    def get_data_contents_updated(self, data_product_id=''):
        # the datetime when the contents of the data were last modified in any way.
        # This is distinct from modifications to the data product attributes
        ret = IonObject(OT.ComputedStringValue)
        ret.value = ""
        ret.status = ComputedValueAvailability.NOTAVAILABLE
        ret.reason = "FIXME. also, should datetime be stored as a string?"

        return ret


    def get_parameters(self, data_product_id=''):
        # The set of Parameter objects describing each variable in this data product
        ret = IonObject(OT.ComputedListValue)
        ret.value = []
        try:
            stream_id = self._get_stream_id(data_product_id)
            if not stream_id:
                ret.status = ComputedValueAvailability.NOTAVAILABLE
                ret.reason = "There is no Stream associated with this DataProduct"
            else:
                stream_def_ids, _ = self.clients.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition, id_only=True)
                if not stream_def_ids:
                    ret.status = ComputedValueAvailability.NOTAVAILABLE
                    ret.reason = "There is no StreamDefinition associated with this DataProduct"
                else:
                    param_dict_ids, _ = self.clients.resource_registry.find_objects(subject=stream_def_ids[0], predicate=PRED.hasParameterDictionary, id_only=True)
                    if not param_dict_ids:
                        ret.status = ComputedValueAvailability.NOTAVAILABLE
                        ret.reason = "There is no ParameterDictionary associated with this DataProduct"
                    else:
                        ret.status = ComputedValueAvailability.PROVIDED
                        ret.value = self.clients.dataset_management.read_parameter_contexts(param_dict_ids[0])
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "FIXME: this message should say why the calculation couldn't be done"
        except Exception as e:
            raise e

        return ret

    def get_data_url(self, data_product_id=''):
        # The unique pointer to this set of data
        ret = IonObject(OT.ComputedStringValue)
        ret.value  = ""

        erddap_host = CFG.get_safe('server.erddap.host','localhost')
        errdap_port = CFG.get_safe('server.erddap.port','8080')
        try:
            dataset_id = self._get_dataset_id(data_product_id)
            ret.value  = string.join( ["http://", erddap_host, ":", str(errdap_port),"/erddap/griddap/", str(dataset_id), "_0.html"],'')
            ret.status = ComputedValueAvailability.PROVIDED
            log.debug("get_data_url: data_url: %s", ret.value)
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Dataset for this Data Product could not be located"

        return ret

    def get_provenance(self, data_product_id=''):
        # Provides an audit trail for modifications to the original data

        ret = IonObject(OT.ComputedDictValue)

        try:
            ret.value = self.get_data_product_provenance(data_product_id)
            ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Error in DataProuctMgmtService:get_data_product_provenance"
        except Exception as e:
            raise e

        return ret


    def get_number_active_subscriptions(self, data_product_id=''):
        # The number of current subscriptions to the data
        # Returns the storage size occupied by the data content of the resource, in bytes.
        ret = IonObject(OT.ComputedIntValue)
        ret.value = 0
        try:
            ret.status = ComputedValueAvailability.PROVIDED
            raise NotFound #todo: ret.value = ???
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "FIXME: this message should say why the calculation couldn't be done"
        except Exception as e:
            raise e

        return ret


    def get_active_user_subscriptions(self, data_product_id=''):
        # The UserSubscription objects for this data product
        ret = IonObject(OT.ComputedListValue)
        ret.value = []
        try:
            ret.value = self.clients.user_notification.get_subscriptions(resource_id=data_product_id, include_nonactive=True)
            ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Product subscription infromation not provided by UserNotificationService"
        except Exception as e:
            raise e

        return ret

#    def get_past_user_subscriptions(self, data_product_id=''):
#        # Provides information for users who have in the past acquired this data product, but for which that acquisition was terminated
#        ret = IonObject(OT.ComputedListValue)
#        ret.value = []
#        try:
#            ret.status = ComputedValueAvailability.PROVIDED
#            raise NotFound #todo: ret.value = ???
#        except NotFound:
#            ret.status = ComputedValueAvailability.NOTAVAILABLE
#            ret.reason = "FIXME: this message should say why the calculation couldn't be done"
#        except Exception as e:
#            raise e
#
#        return ret


    def get_last_granule(self, data_product_id=''):
        # Provides information for users who have in the past acquired this data product, but for which that acquisition was terminated
        ret = IonObject(OT.ComputedDictValue)
        ret.value = {}
        try:
            dataset_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataset, id_only=True)
            if not dataset_ids:
                ret.status = ComputedValueAvailability.NOTAVAILABLE
                ret.reason = "No dataset associated with this data product"
            else:
                replay_granule = self.clients.data_retriever.retrieve_last_data_points(dataset_ids[0], number_of_points=1)
                #replay_granule = self.clients.data_retriever.retrieve_last_granule(dataset_ids[0])
                rdt = RecordDictionaryTool.load_from_granule(replay_granule)
                retval = {}
                for k,v in rdt.iteritems():
                    element = np.atleast_1d(rdt[k]).flatten()[0]
                    if element == rdt._pdict.get_context(k).fill_value:
                        retval[k] = '%s: Empty' % k
                    elif 'seconds' in rdt._pdict.get_context(k).uom:
                        units = rdt._pdict.get_context(k).uom
                        element = np.atleast_1d(rdt[k]).flatten()[0]
                        unix_ts = TimeUtils.units_to_ts(units, element)
                        dtg = datetime.utcfromtimestamp(unix_ts)
                        try:
                            retval[k] = '%s: %s' %(k,dtg.strftime('%Y-%m-%dT%H:%M:%SZ'))
                        except:
                            retval[k] = '%s: %s' %(k, element)

                    else:
                        retval[k] = '%s: %s' %(k, element)
                ret.value = retval
#                ret.value =  {k : str(rdt[k].tolist()[0]) for k,v in rdt.iteritems()}
                ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "FIXME: this message should say why the calculation couldn't be done"
        except Exception as e:
            raise e

        return ret


    def get_recent_granules(self, data_product_id=''):
        # Provides information for users who have in the past acquired this data product, but for which that acquisition was terminated
        ret = IonObject(OT.ComputedDictValue)
        ret.value = {}
        ret.status = ComputedValueAvailability.NOTAVAILABLE
#        try:
#            dataset_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataset, id_only=True)
#            if not dataset_ids:
#                ret.status = ComputedValueAvailability.NOTAVAILABLE
#                ret.reason = "No dataset associated with this data product"
#            else:
#                replay_granule = self.clients.data_retriever.retrieve_last_data_points(dataset_ids[0])
#                rdt = RecordDictionaryTool.load_from_granule(replay_granule)
#                ret.value =  {k : rdt[k].tolist() for k,v in rdt.iteritems()}
#                ret.status = ComputedValueAvailability.PROVIDED
#        except NotFound:
#            ret.status = ComputedValueAvailability.NOTAVAILABLE
#            ret.reason = "FIXME: this message should say why the calculation couldn't be done"
#        except Exception as e:
#            raise e

        return ret


    def get_is_persisted(self, data_product_id=''):
        # Returns True if data product is currently being persisted
        ret = IonObject(OT.ComputedIntValue)
        ret.value = self.is_persisted(data_product_id)
        ret.status = ComputedValueAvailability.PROVIDED

        return ret




    def _find_producers(self, data_product_id='', provenance_results=''):
        source_ids = []
        # get the link to the DataProducer resource
        log.debug("DataProductMgmt:_find_producers start %s", data_product_id)
        producer_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataProducer, id_only=True)
        for producer_id in producer_ids:
            # get the link to that resources parent DataProducer
            parent_ids, _ = self.clients.resource_registry.find_objects(subject=producer_id, predicate=PRED.hasParent, id_only=True)
            for parent_id in parent_ids:
                # get the producer that this DataProducer represents
                nxt_producer_ids, _ = self.clients.resource_registry.find_subjects( predicate=PRED.hasDataProducer, object=parent_id, id_only=True)
                producer_list = []
                inputs = {}

                for nxt_producer_id in nxt_producer_ids:
                    nxt_producer_obj = self.clients.resource_registry.read(nxt_producer_id)
                    log.debug("DataProductMgmt:_find_producers nxt_producer %s", nxt_producer_obj.name)
                    #todo: check the type of resource; instrument, data process or extDataset'
                    #todo: check if this is a SiteDataProduct name=SiteDataProduct and desc=site_id
                    inputs_to_nxt_producer = self._find_producer_in_products(nxt_producer_id)
                    log.debug("DataProductMgmt:_find_producers inputs_to_nxt_producer %s", str(inputs_to_nxt_producer))
                    producer_list.append(nxt_producer_id)
                    inputs[nxt_producer_id] = inputs_to_nxt_producer
                    #provenance_results[data_product_id] = { 'producerctx':self._extract_producer_context(nxt_producer_id) , 'producer': nxt_producer_id, 'inputs': inputs_to_nxt_producer }
                    log.debug("DataProductMgmt:_find_producers self.provenance_results %s", str(provenance_results))
                    for input in inputs_to_nxt_producer:
                        self._find_producers(input, provenance_results)

                    provenance_results[data_product_id] = { 'producer': producer_list, 'inputs': inputs }

        log.debug("DataProductMgmt:_find_producers: %s", str(source_ids))
        return

    def _find_producer_in_products(self, producer_id=''):
        # get the link to the inout DataProduct resource
        product_ids, _ = self.clients.resource_registry.find_objects(   subject=producer_id,
                                                                        predicate=PRED.hasInputProduct,
                                                                        id_only=True)
        for product_id in product_ids:
            product_obj = self.clients.resource_registry.read(product_id)
            log.debug("DataProductMgmt:_find_producer_in_products: %s", product_obj.name)

        return product_ids


    def _write_product_provenance_report(self, data_product_id='', provenance_results=''):

        results = ''

        if not data_product_id:
            raise BadRequest('Data Product Id %s must be provided' % str(data_product_id))
        if not provenance_results:
            raise BadRequest('Data Product provenance data %s must be provided' % str(provenance_results))

        #set up xml doc
        self.page = etree.Element('lineage')
        self.doc = etree.ElementTree(self.page)

        in_data_products = []
        next_input_set = []
        self._write_product_info(data_product_id, provenance_results)

        #get the set of inputs to the producer which created this data product
        for key, value in provenance_results[data_product_id]['inputs'].items():
            in_data_products.extend(value)
        log.debug("DataProductMgmt:_write_product_provenance_report in_data_products: %s",
                  str(in_data_products))

        while in_data_products:
            for in_data_product in in_data_products:
                # write the provenance for each of those products
                self._write_product_info(in_data_product, provenance_results)
                log.debug("DataProductMgmt:_write_product_provenance_report next_input_set: %s",
                          str(provenance_results[in_data_product]['inputs']))
                # provenance_results[in_data_product]['inputs'] contains a dict that is produce_id:[input_product_list]
                for key, value in provenance_results[in_data_product]['inputs'].items():
                    next_input_set.extend(value)
                #switch to the input for these producers
            in_data_products =  next_input_set
            next_input_set = []
            log.debug("DataProductMgmt:_write_product_provenance_report in_data_products (end loop): %s",
                      str(in_data_products))


        result = etree.tostring(self.page, pretty_print=True, encoding=None)

        log.debug("DataProductMgmt:_write_product_provenance_report result: %s", str(result))

        return results


    def _write_object_info(self, data_obj=None, etree_node=None):

        fields, schema = data_obj.__dict__, data_obj._schema

        for att_name, attr_type in schema.iteritems():
        #            log.debug("DataProductMgmt:_write_product_info att_name %s",  str(att_name))
        #            log.debug("DataProductMgmt:_write_product_info attr_type %s",  str(attr_type))
        #            log.debug("DataProductMgmt:_write_product_info attr_type [type] %s",  str(attr_type['type']))
            attr_value = getattr(data_obj, att_name)
            log.debug("DataProductMgmt:_write_product_info att_value %s",  str(attr_value))
            if isinstance(attr_value, IonObjectBase):
                log.debug("DataProductMgmt:_write_product_info IonObjectBase  att_value %s", str(attr_value))
            if isinstance(fields[att_name], IonObjectBase):
                sub_elem = etree.SubElement(etree_node, att_name)
                log.debug("DataProductMgmt:_write_product_info IonObjectBase  fields[att_name] %s", str(fields[att_name]))
                self._write_object_info(data_obj=attr_value, etree_node=sub_elem)
            elif attr_type['type'] == 'list' and attr_value:
                sub_elem = etree.SubElement(etree_node, att_name)
                for list_element in attr_value:
                    log.debug("DataProductMgmt:_list_element %s",  str(list_element))
                    if isinstance(list_element, IonObjectBase):
                        self._write_object_info(data_obj=list_element, etree_node=sub_elem)

            elif attr_type['type'] == 'dict' and attr_value:
                sub_elem = etree.SubElement(etree_node, att_name)
                for key, val in attr_value.iteritems():
                    log.debug("DataProductMgmt:dict key %s    val%s",  str(key), str(val) )
                    if isinstance(val, IonObjectBase):
                        self._write_object_info(data_obj=val, etree_node=sub_elem)
                    else:
                        log.debug("DataProductMgmt:dict new simple elem key %s ",  str(key) )
                        dict_sub_elem = etree.SubElement(sub_elem, key)
                        dict_sub_elem.text = str(val)


            #special processing for timestamp elements:
            elif  attr_type['type'] == 'str' and  '_time' in att_name  :
                log.debug("DataProductMgmt:_format_ion_time  att_name %s   attr_value %s ", str(att_name), str(attr_value))
                if len(attr_value) == 16 :
                    attr_value = self._format_ion_time(attr_value)
                sub_elem = etree.SubElement(etree_node, att_name)
                sub_elem.text = str(attr_value)
            else:
                sub_elem = etree.SubElement(etree_node, att_name)
                sub_elem.text = str(attr_value)

    def _extract_producer_context(self, producer_id=''):

        producer_obj = self.clients.resource_registry.read(producer_id)
        producertype = type(producer_obj).__name__

        context = {}
        if RT.DataProcess == producertype :
            context['DataProcess'] = str(producer_obj)
            data_proc_def_objs, _ = self.clients.resource_registry.find_objects( subject=producer_id, predicate=PRED.hasProcessDefinition, object_type=RT.DataProcessDefinition)
            for data_proc_def_obj in data_proc_def_objs:
                proc_def_type = type(data_proc_def_obj).__name__
                if RT.DataProcessDefinition == proc_def_type :
                    context['DataProcessDefinition'] = str(data_proc_def_obj)
                if RT.ProcessDefinition == proc_def_type :
                    context['ProcessDefinition'] = str(data_proc_def_obj)
            transform_objs, _ = self.clients.resource_registry.find_objects( subject=producer_id, predicate=PRED.hasTransform, object_type=RT.Transform)
            if transform_objs:
                context['Transform'] = str(transform_objs[0])
        if RT.InstrumentDevice == producertype :
            context['InstrumentDevice'] = str(producer_obj)
            inst_model_objs, _ = self.clients.resource_registry.find_objects( subject=producer_id, predicate=PRED.hasModel, object_type=RT.InstrumentModel)
            if inst_model_objs:
                context['InstrumentModel'] = str(inst_model_objs[0])
        return context





    def _write_product_info(self, data_product_id='', provenance_results=''):
        #--------------------------------------------------------------------------------
        # Data Product metadata
        #--------------------------------------------------------------------------------
        log.debug("DataProductMgmt:provenance_report data_product_id %s",  str(data_product_id))
        processing_step = etree.SubElement(self.page, 'processing_step')
        product_obj = self.clients.resource_registry.read(data_product_id)
        data_product_tag = etree.SubElement(processing_step, 'data_product')

        self._write_object_info(data_obj=product_obj, etree_node=data_product_tag)


        #--------------------------------------------------------------------------------
        # Data Producer metadata
        #--------------------------------------------------------------------------------
        producer_dict = provenance_results[data_product_id]
        log.debug("DataProductMgmt:provenance_report  producer_dict %s ", str(producer_dict))
        producer_list = provenance_results[data_product_id]['producer']
        data_producer_list_tag = etree.SubElement(processing_step, 'data_producer_list')
        for producer_id in producer_list:
            log.debug("DataProductMgmt:reading producer  %s ", str(producer_id))
            producer_obj = self.clients.resource_registry.read(producer_id)
            data_producer_tag = etree.SubElement(data_producer_list_tag, 'data_producer')
            self._write_object_info(data_obj=producer_obj, etree_node=data_producer_tag)


            #retrieve the assoc data producer resource
            data_producer_objs, producer_assns = self.clients.resource_registry.find_objects(subject=producer_id, predicate=PRED.hasDataProducer, id_only=False)
            if not data_producer_objs:
                raise BadRequest('No Data Producer resource associated with the Producer %s' % str(producer_id))
            data_producer_obj = data_producer_objs[0]
            sub_elem = etree.SubElement(data_producer_tag, 'data_producer_config')
            log.debug("DataProductMgmt:data_producer_obj  %s ", str(data_producer_obj))
            self._write_object_info(data_obj=data_producer_obj, etree_node=sub_elem)

            # add the input product names for these producers
            in_product_list = provenance_results[data_product_id]['inputs'][producer_id]
            if in_product_list:
                input_products_tag = etree.SubElement(data_producer_tag, "input_products")
                for in_product in in_product_list:
                    input_product_tag = etree.SubElement(input_products_tag, "input_product")
                    #product_name_tag = etree.SubElement(input_product_tag, "name")
                    product_obj = self.clients.resource_registry.read(in_product)
                    self._write_object_info(data_obj=product_obj, etree_node=input_product_tag)
                    #product_name_tag.text = product_obj.name


            # check for attached deployment
            deployment_ids, _ = self.clients.resource_registry.find_objects( subject=producer_id, predicate=PRED.hasDeployment, object_type=RT.Deployment, id_only=True)
            #todo: match when this prouct was produced with the correct deployment object
            if deployment_ids:
                data_producer_deploys_tag = etree.SubElement(data_producer_tag, 'data_producer_deployments')
                for deployment_id in deployment_ids:
                    deployment_tag = etree.SubElement(data_producer_deploys_tag, 'deployment')
                    deployment_obj = self.clients.resource_registry.read(deployment_id)
                    #find the site
                    self._write_object_info(data_obj=deployment_obj, etree_node=deployment_tag)
                    deployment_site_ids, _ = self.clients.resource_registry.find_subjects( subject_type=RT.InstrumentSite, predicate=PRED.hasDeployment, object=deployment_id, id_only=True)
                    for deployment_site_id in deployment_site_ids:
                        deploy_site_tag = etree.SubElement(deployment_tag, 'deployment_site')
                        site_obj = self.clients.resource_registry.read(deployment_site_id)
                        self._write_object_info(data_obj=site_obj, etree_node=deploy_site_tag)

            # check for lookup table attachments
            att_ids = self.clients.resource_registry.find_attachments(producer_id, keyword="DataProcessInput", id_only=True)
            if att_ids:
                data_producer_lookups_tag = etree.SubElement(data_producer_tag, 'data_producer_attachments')
                for att_id in att_ids:
                    lookup_tag = etree.SubElement(data_producer_lookups_tag, 'attachment')
                    attach_obj = self.clients.resource_registry.read(att_id)
                    self._write_object_info(data_obj=attach_obj, etree_node=lookup_tag)



    def _format_ion_time(self, ion_time=''):
        ion_time_obj = IonTime.from_string(ion_time)
        #todo: fix this and return str( ion_time_obj)
        return str(ion_time_obj)
