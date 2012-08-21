#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from interface.services.sa.idata_product_management_service import BaseDataProductManagementService
from ion.services.sa.product.data_product_impl import DataProductImpl
from interface.objects import DataProduct, DataProductVersion

from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT, PRED, LCS
from pyon.util.arg_check import validate_is_instance, validate_true, validate_is_not_none

from coverage_model.basic_types import AbstractIdentifiable, AbstractBase, AxisTypeEnum, MutabilityEnum
from coverage_model.coverage import CRS, GridDomain, GridShape

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


    def create_data_product(self, data_product=None, stream_definition_id='', parameter_dictionary = None):
        """
        @param      data_product IonObject which defines the general data product resource
        @param      source_resource_id IonObject id which defines the source for the data
        @retval     data_product_id
        """

        # Create will validate and register a new data product within the system
        validate_is_not_none(parameter_dictionary, 'A parameter dictionary must be passed to register a data product')
        validate_is_not_none(stream_definition_id, 'A stream definition id must be passed to register a data product')
        validate_is_not_none(data_product, 'A data product (ion object) must be passed to register a data product')

        #--------------------------------------------------------------------------------
        # Register - create and store a new DataProduct resource using provided metadata
        #--------------------------------------------------------------------------------
        data_product_id, rev = self.clients.resource_registry.create(data_product)

        log.debug("data product id: %s" % data_product_id)

        #--------------------------------------------------------------------------------
        # Register - create and store the default DataProductVersion resource using provided metadata
        #--------------------------------------------------------------------------------
        #create the initial/default data product version
        data_product_version = DataProductVersion()
        data_product_version.name = data_product.name
        data_product_version.description = data_product.description
        dpv_id, rev = self.clients.resource_registry.create(data_product_version)
        self.clients.resource_registry.create_association( subject=data_product_id, predicate=PRED.hasVersion, object=dpv_id)

        #-----------------------------------------------------------------------------------------------
        #Create the stream and a dataset if a stream definition is provided
        #-----------------------------------------------------------------------------------------------
        log.debug("DataProductManagementService:create_data_product: stream definition id = %s" % stream_definition_id)

        #if stream_definition_id:
        stream_id = self.clients.pubsub_management.create_stream(name=data_product.name,
                                                                description=data_product.description,
                                                                stream_definition_id=stream_definition_id)

        # Associate the Stream with the main Data Product and with the default data product version
        self.data_product.link_stream(data_product_id, stream_id)
        self.clients.resource_registry.create_association( subject=dpv_id, predicate=PRED.hasStream, object=stream_id)


        # create a dataset...
        data_set_id = self.clients.dataset_management.create_dataset(   name= 'data_set_%s' % stream_id,
                                                                        stream_id=stream_id,
                                                                        parameter_dict=parameter_dictionary,
                                                                        temporal_domain=data_product.temporal_domain,
                                                                        spatial_domain=data_product.spatial_domain)

        log.debug("DataProductManagementService:create_data_product: data_set_id = %s" % str(data_set_id))
        data_set_obj = self.clients.dataset_management.read_dataset(data_set_id)
        log.debug("DataProductManagementService:create_data_product: data_set_obj = %s" % str(data_set_obj))

        # link dataset with data product. This creates the association in the resource registry
        self.data_product.link_data_set(data_product_id=data_product_id, data_set_id=data_set_id)
        self.clients.resource_registry.create_association( subject=dpv_id, predicate=PRED.hasDataset, object=data_set_id)

        # Return the id of the new data product
        return data_product_id

    def read_data_product(self, data_product_id=''):
        """
        method docstring
        """
        # Retrieve all metadata for a specific data product
        # Return data product resource

        log.debug("DataProductManagementService:read_data_product: %s" % str(data_product_id))

        result = self.data_product.read_one(data_product_id)

        return result


    def update_data_product(self, data_product=None):
        """
        @todo document this interface!!!

        @param data_product    DataProduct
        @throws NotFound    object with specified id does not exist
        """
        validate_is_instance(data_product, DataProduct)

        log.debug("DataProductManagementService:update_data_product: %s" % str(data_product))

        self.data_product.update_one(data_product)

        #TODO: any changes to producer? Call DataAcquisitionMgmtSvc?

        return


    def delete_data_product(self, data_product_id=''):

        #Check if this data product is associated to a producer
        #todo: convert to impl call
        producer_ids = self.data_product.find_stemming_data_producer(data_product_id)

        for producer_id in producer_ids:
            log.debug("DataProductManagementService:delete_data_product unassigning data producers: %s")
            self.clients.data_acquisition_management.unassign_data_product(producer_id, data_product_id)



        #--------------------------------------------------------------------------------
        # remove stream associations
        #--------------------------------------------------------------------------------
        stream_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStream, RT.Stream, id_only=True)

        for stream_id in stream_ids:
            self.data_product.unlink_stream(data_product_id=data_product_id, stream_id=stream_id)


        #--------------------------------------------------------------------------------
        # remove dataset associations
        #--------------------------------------------------------------------------------
        dataset_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.DataSet, id_only=True)

        for dataset_id in dataset_ids:
            self.data_product.unlink_data_set(data_product_id=data_product_id, data_set_id=dataset_id)

        #        # delete the hasOutputDataProduct associations link
        #        dp_assocs = self.clients.resource_registry.find_associations(data_product_id, PRED.hasOutputProduct)
        #        for dp_assoc in dp_assocs:
        #            self.clients.resource_registry.delete_association(dp_assoc)
        #        # delete the hasInputDataProduct associations link
        #        dp_assocs = self.clients.resource_registry.find_associations(data_product_id, PRED.hasInputProduct)
        #        for dp_assoc in dp_assocs:
        #            self.clients.resource_registry.delete_association(dp_assoc)

        #--------------------------------------------------------------------------------
        # Delete the data product
        #--------------------------------------------------------------------------------
        data_product_obj = self.read_data_product(data_product_id)

        validate_is_instance(data_product_obj, DataProduct)

        if data_product_obj.lcstate != LCS.RETIRED:
            self.data_product.delete_one(data_product_id)

        #self.clients.resource_registry.delete(data_product_id)
        #self.clients.resource_registry.set_lifecycle_state(data_product_id, LCS.RETIRED)
        return

    def hard_delete_data_product(self, data_product_id=''):

        return


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
        log.debug("activate_data_product_persistence: stream = %s"  % str(stream_id))



        #-----------------------------------------------------------------------------------------
        # grab the ingestion configuration id from the data_product in order to use to persist it
        #-----------------------------------------------------------------------------------------
        if data_product_obj.dataset_configuration_id:
            ingestion_configuration_id = data_product_obj.dataset_configuration_id
        else:
            ingestion_configuration_id = self.clients.ingestion_management.list_ingestion_configurations(id_only=True)[0]

        log.debug("ingestion_configuration_id for data product: %s" % ingestion_configuration_id)

        #--------------------------------------------------------------------------------
        # persist the data stream using the ingestion config id and stream id
        #--------------------------------------------------------------------------------

        # find datasets for the data product
        dataset_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.DataSet, id_only=True)

        log.debug("Found the following datasets for the data product: %s" % dataset_ids)
        for dataset_id in dataset_ids:

            try:
                dataset_id = self.clients.ingestion_management.persist_data_stream(stream_id=stream_id,
                    ingestion_configuration_id=ingestion_configuration_id,
                    dataset_id=dataset_id)
            except BadRequest:
                log.warning("Activate data product may have resulted in a duplicate attempt to associate a stream to a dataset")
                log.warning("Please note that creating a data product calls the create_dataset() method which already makes an association")

            log.debug("activate_data_product_persistence: dataset_id = %s"  % str(dataset_id))

            # link data set to data product
            #self.data_product.link_data_set(data_product_id, dataset_id)

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

        log.debug("suspend_data_product_persistence: data_product_id = %s"  % str(data_product_id))

        #--------------------------------------------------------------------------------
        # retrieve the data_process object
        #--------------------------------------------------------------------------------
        data_product_obj = self.clients.resource_registry.read(data_product_id)

        validate_is_not_none(data_product_obj, 'Should not have been empty')
        validate_is_instance(data_product_obj, DataProduct)

        if data_product_obj.dataset_configuration_id is None:
            raise NotFound("Data Product %s dataset configuration does not exist" % data_product_id)

        log.debug("Data product: %s" % data_product_obj)

        #--------------------------------------------------------------------------------
        # get the Stream associated with this data product; if no stream then create one, if multiple streams then Throw
        #streams = self.data_product.find_stemming_stream(data_product_id)
        #--------------------------------------------------------------------------------
        stream_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        if not stream_ids:
            raise BadRequest('Data Product %s must have one stream associated' % str(data_product_id))

        for stream_id in stream_ids:
            log.debug("suspend_data_product_persistence: stream = %s"  % str(stream_id))
            log.debug("data_product_obj.dataset_configuration_id: %s" % data_product_obj.dataset_configuration_id)

            ret = self.clients.ingestion_management.unpersist_data_stream(stream_id=stream_id, ingestion_configuration_id=data_product_obj.dataset_configuration_id)
            log.debug("suspend_data_product_persistence: deactivate = %s"  % str(ret))

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

        log.debug("DataProductManagementService:get_data_product_provenance: %s" % str(data_product_id))

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


    def create_data_product_version(self, data_product_id='', data_product_version=None):
        """Define a new version of an existing set of information that represent an inprovement in the quality or
        understanding of the information. Only creates the second and higher versions of a DataProduct.
        The first version is implicit in the crate_data_product() operation.

        @param data_product_id    str
        @param data_product_version    DataProductVersion
        @retval data_product_version_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        validate_is_not_none(data_product_id, 'A data product identifier must be passed to create a data product version')
        validate_is_not_none(data_product_version, 'A data product version (ion object) must be passed to create a data product version')

        data_product_version_id, rev = self.clients.resource_registry.create(data_product_version)
        self.clients.resource_registry.create_association( subject=data_product_id, predicate=PRED.hasVersion, object=data_product_version_id)


        #-----------------------------------------------------------------------------------------------
        #Create the stream and a dataset for the new version
        #-----------------------------------------------------------------------------------------------
        stream_id = self.clients.pubsub_management.create_stream(name=data_product_version.name,
                                                                description=data_product_version.description)

        # Associate the Stream with the main Data Product and with the default data product version
        self.clients.resource_registry.create_association( subject=data_product_version_id, predicate=PRED.hasStream, object=stream_id)

        #get the parameter_dictionary assoc with the original dataset
        dataset_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataset, object_type=RT.DataSet, id_only=True)
        if not dataset_ids:
            raise BadRequest('No Dataset associated with the DataProduct %s' % str(data_product_id))

        log.debug("DataProductManagementService:create_data_product_version base_dataset_id: %s", str(dataset_ids[0]))
        base_dataset_obj = self.clients.dataset_management.read_dataset(str(dataset_ids[0]))
        log.debug("DataProductManagementService:create_data_product_version base_dataset_obj: %s" % str(base_dataset_obj))

        # create a dataset for this version. must have same parameter dictionary and spatial/temporal domain as original data product.
        data_set_id = self.clients.dataset_management.create_dataset(   name= 'data_set_%s' % stream_id,
                                                                        stream_id=stream_id,
                                                                        parameter_dict=base_dataset_obj.parameter_dictionary,
                                                                        temporal_domain=base_dataset_obj.temporal_domain,
                                                                        spatial_domain=base_dataset_obj.spatial_domain)
        self.clients.resource_registry.create_association(subject=data_product_version_id, predicate=PRED.hasDataset, object=data_set_id)

        return data_product_version_id

    def update_data_product_version(self, data_product_version=None):
        """@todo document this interface!!!

        @param data_product    DataProductVersion
        @throws NotFound    object with specified id does not exist
        """

        validate_is_not_none(data_product_version, "Should not pass in a None object")

        log.debug("DataProductManagementService:update_data_product_version: %s" % str(data_product_version))

        self.clients.resource_registry.update(data_product_version)

        #TODO: any changes to producer? Call DataAcquisitionMgmtSvc?

        return

    def read_data_product_version(self, data_product_version_id=''):
        """Retrieve data product information

        @param data_product_version_id    str
        @retval data_product    DataProductVersion
        """
        log.debug("DataProductManagementService:read_data_product_version: %s" % str(data_product_version_id))

        result = self.clients.resource_registry.read(data_product_version_id)

        validate_is_not_none(result, "Should not have returned an empty result")

        return result

    def delete_data_product_version(self, data_product_version_id=''):
        """Remove a version of an data product.

        @param data_product_version_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        pass


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

    def get_earliest_data_datetime(self, data_product_id=''):
        # Returns the datetime of the earliest values in the data product

        pass


    def get_product_download_size_estimated(self, data_product_id=''):
        # Returns the size of the full data product if downloaded/presented in a given presentation form

        pass

    def get_product_storage_size_estimated(self, data_product_id=''):
        # Returns the storage size occupied by the data content of the resource, in bytes.

        pass

