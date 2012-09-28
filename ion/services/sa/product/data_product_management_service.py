#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.public import  log, IonObject
from interface.services.sa.idata_product_management_service import BaseDataProductManagementService
from ion.services.sa.product.data_product_impl import DataProductImpl
from interface.objects import DataProduct, DataProductVersion

from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT, PRED, LCS
from pyon.util.arg_check import validate_is_instance, validate_is_not_none, validate_false


class DataProductManagementService(BaseDataProductManagementService):
    """ @author     Bill Bollenbacher
        @file       ion/services/sa/product/data_product_management_service.py
        @brief      Implementation of the data product management service
    """

    def on_init(self):
        self.override_clients(self.clients)

    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """
        self.data_product   = DataProductImpl(self.clients)


    def create_data_product(self, data_product=None, stream_definition_id='', parameter_dictionary=None, exchange_point=''):
        """
        @param      data_product IonObject which defines the general data product resource
        @param      source_resource_id IonObject id which defines the source for the data
        @retval     data_product_id
        """

        res, _ = self.clients.resource_registry.find_resources(restype=RT.DataProduct, name=data_product.name, id_only=True)
        validate_false(len(res), 'A data product with the name %s already exists.' % data_product.name)
        log.info('Creating DataProduct: %s', data_product.name)
        log.debug('%s', data_product.__dict__)


        # Create will validate and register a new data product within the system
        validate_is_not_none(parameter_dictionary, 'A parameter dictionary must be passed to register a data product')
        validate_is_not_none(stream_definition_id, 'A stream definition id must be passed to register a data product')
        validate_is_not_none(data_product, 'A data product (ion object) must be passed to register a data product')
        exchange_point = exchange_point or 'science_data'

        #--------------------------------------------------------------------------------
        # Register - create and store a new DataProduct resource using provided metadata
        #--------------------------------------------------------------------------------
        data_product_id, rev = self.clients.resource_registry.create(data_product)


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
        self.data_product.link_stream(data_product_id, stream_id)


        # create a dataset...
        data_set_id = self.clients.dataset_management.create_dataset(   name= 'data_set_%s' % stream_id,
                                                                        stream_id=stream_id,
                                                                        parameter_dict=parameter_dictionary,
                                                                        temporal_domain=data_product.temporal_domain,
                                                                        spatial_domain=data_product.spatial_domain)

        # link dataset with data product. This creates the association in the resource registry
        self.data_product.link_data_set(data_product_id=data_product_id, data_set_id=data_set_id)

        # Return the id of the new data product
        return data_product_id

    def read_data_product(self, data_product_id=''):
        """
        method docstring
        """
        # Retrieve all metadata for a specific data product
        # Return data product resource

        data_product = self.data_product.read_one(data_product_id)
        validate_is_instance(data_product,DataProduct)

        return data_product


    def update_data_product(self, data_product=None):
        """
        @todo document this interface!!!

        @param data_product    DataProduct
        @throws NotFound    object with specified id does not exist
        """
        validate_is_instance(data_product, DataProduct)

        self.data_product.update_one(data_product)

        #TODO: any changes to producer? Call DataAcquisitionMgmtSvc?

    def delete_data_product(self, data_product_id=''):

        #Check if this data product is associated to a producer
        #todo: convert to impl call
        producer_ids = self.data_product.find_stemming_data_producer(data_product_id)

        for producer_id in producer_ids:
            self.clients.data_acquisition_management.unassign_data_product(producer_id, data_product_id)



        #--------------------------------------------------------------------------------
        # remove stream associations
        #--------------------------------------------------------------------------------
        self.remove_streams(data_product_id)


        #--------------------------------------------------------------------------------
        # remove dataset associations
        #--------------------------------------------------------------------------------
        dataset_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.DataSet, id_only=True)

        for dataset_id in dataset_ids:
            self.data_product.unlink_data_set(data_product_id=data_product_id, data_set_id=dataset_id)

        #--------------------------------------------------------------------------------
        # Delete the data product
        #--------------------------------------------------------------------------------
        data_product_obj = self.read_data_product(data_product_id)

        if data_product_obj.lcstate != LCS.RETIRED:
            self.data_product.delete_one(data_product_id)

    def hard_delete_data_product(self, data_product_id=''):
        pass

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

        return self.data_product.find_some(filters)



    def activate_data_product_persistence(self, data_product_id=''):
        """Persist data product data into a data set

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """
        #--------------------------------------------------------------------------------
        # retrieve the data_process object
        #--------------------------------------------------------------------------------
        data_product_obj = self.data_product.read_one(data_product_id)

        validate_is_not_none(data_product_obj, "The data product id should correspond to a valid registered data product.")

        #--------------------------------------------------------------------------------
        # get the Stream associated with this data product; if no stream then create one, if multiple streams then Throw
        #--------------------------------------------------------------------------------
        streams = self.data_product.find_stemming_stream(data_product_id)
        if not streams:
            raise BadRequest('Data Product %s must have one stream associated' % str(data_product_id))

        stream_id = streams[0]._id
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
        dataset_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.DataSet, id_only=True)

        log.debug("Found the following datasets: %s, for the data product: %s" % (dataset_ids, data_product_id))

        for dataset_id in dataset_ids:
            log.debug("Activating data product persistence for dataset_id: %s"  % str(dataset_id))
            dataset_id = self.clients.ingestion_management.persist_data_stream(stream_id=stream_id,
                                                    ingestion_configuration_id=ingestion_configuration_id,
                                                    dataset_id=dataset_id)

        #--------------------------------------------------------------------------------
        # todo: dataset_configuration_obj contains the ingest config for now...
        # Update the data product object
        #--------------------------------------------------------------------------------
        data_product_obj.dataset_configuration_id = ingestion_configuration_id
        self.update_data_product(data_product_obj)



    def suspend_data_product_persistence(self, data_product_id=''):
        """Suspend data product data persistence into a data set, multiple options

        @param data_product_id    str
        @param type    str
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
        stream_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        validate_is_not_none(stream_ids, 'Data Product %s must have one stream associated' % str(data_product_id))

        for stream_id in stream_ids:
            ret = self.clients.ingestion_management.unpersist_data_stream(stream_id=stream_id, ingestion_configuration_id=data_product_obj.dataset_configuration_id)

        #--------------------------------------------------------------------------------
        # detach the dataset from this data product
        #--------------------------------------------------------------------------------
        dataset_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataset, id_only=True)
        for dataset_id in dataset_ids:
            self.data_product.unlink_data_set(data_product_id, dataset_id)


    def get_data_product_provenance(self, data_product_id=''):

        # Retrieve information that characterizes how this data was produced
        # Return in a dictionary

        self.provenance_results = {}

        data_product = self.data_product.read_one(data_product_id)
        validate_is_not_none(data_product, "Should have got a non empty data product")

        # todo: get the start time of this data product
        self.data_product._find_producers(data_product_id, self.provenance_results)

        return self.provenance_results

    def get_data_product_provenance_report(self, data_product_id=''):

        # Retrieve information that characterizes how this data was produced
        # Return in a dictionary

        self.provenance_results = self.get_data_product_provenance(data_product_id)

        results = ''

        results = self.data_product._write_product_provenance_report(data_product_id, self.provenance_results)

        return results


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

        @param data_product    DataProductVersion
        @throws NotFound    object with specified id does not exist
        """

        validate_is_not_none(data_product_collection, "Should not pass in a None object")

        self.clients.resource_registry.update(data_product_collection)

        #TODO: any changes to producer? Call DataAcquisitionMgmtSvc?

        return

    def read_data_product_collection(self, data_product_collection_id=''):
        """Retrieve data product information

        @param data_product_version_id    str
        @retval data_product    DataProductVersion
        """
        result = self.clients.resource_registry.read(data_product_collection_id)

        validate_is_not_none(result, "Should not have returned an empty result")

        return result

    def delete_data_product_collection(self, data_product_collection_id=''):
        """Remove a version of an data product.

        @param data_product_version_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        pass

    def add_data_product_version_to_collection(self, data_product_id='', data_product_collection_id='', version_name='', version_description=''):


        dp_collection_obj =self.clients.resource_registry.read(data_product_collection_id)

        new_data_product_obj = self.clients.resource_registry.read(data_product_id)

        #base_data_product_obj = self.clients.resource_registry.read(dp_collection_obj.version_list[0])


        #todo: validate that the parameter dict, spatial/temporal domain match the base data product


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
        return self.data_product.advance_lcs(data_product_id, lifecycle_event)

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




    ############################
    #
    #  EXTENDED RESOURCES
    #
    ############################



    def get_data_product_extension(self, data_product_id='', ext_associations=None, ext_exclude=None):
        #Returns an DataProductExtension object containing additional related information

        pass

    def get_data_datetime(self, data_product_id=''):
        # Returns a temporal bounds object of the span of data product life span (may exist without getting a granule)

        pass

    def get_data_ingestion_datetime(self, data_product_id=''):
        # Returns a temporal bounds object of the earliest/most recent values ingested into in the data product

        pass

    def get_product_download_size_estimated(self, data_product_id=''):
        # Returns the size of the full data product if downloaded/presented in a given presentation form

        pass

    def get_stored_data_size(self, data_product_id=''):
        # Returns the storage size occupied by the data content of the resource, in bytes.

        pass

    def get_data_contents_updated(self, data_product_id=''):
        # the datetime when the contents of the data were last modified in any way.
        # This is distinct from modifications to the data product attributes
        pass

    def get_parameters(self, data_product_id=''):
        # The set of Parameter objects describing each variable in this data product
        pass

    def get_data_url(self, data_product_id=''):
        # The unique pointer to this set of data
        pass

    def get_provenance(self, data_product_id=''):
        # Provides an audit trail for modifications to the original data

        #todo - call get_data_product_provenance when it is completed
        pass

    def get_number_active_subscriptions(self, data_product_id=''):
        # The number of current subscriptions to the data

        pass

    def get_descriptors(self, data_product_id=''):
        # Returns a list of keyword/authority pairs with optional urls

        pass

    def get_active_user_subscriptions(self, data_product_id=''):
        # The UserSubscription objects for this data product

        pass

    def get_past_user_subscriptions(self, data_product_id=''):
        # Provides information for users who have in the past acquired this data product, but for which that acquisition was terminated

        pass