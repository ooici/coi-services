#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from interface.services.sa.idata_product_management_service import BaseDataProductManagementService
from ion.services.sa.resource_impl.data_product_impl import DataProductImpl

from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT, PRED

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

    

    def create_data_product(self, data_product=None, stream_definition_id=''):
        """
        @param      data_product IonObject which defines the general data product resource
        @param      source_resource_id IonObject id which defines the source for the data
        @retval     data_product_id
        """ 
        #   1. Verify that a data product with same name does not already exist 
        #   2. Validate that the data product IonObject does not contain an id_ element     
        #   3. Create a new data product
        #       - User must supply the name in the data product
        
        # Create will validate and register a new data product within the system

        # Validate - TBD by the work that Karen Stocks is driving with John Graybeal

        # Register - create and store a new DataProduct resource using provided metadata
        log.debug("DataProductManagementService:create_data_product: %s" % str(data_product))
        data_product_id = self.data_product.create_one(data_product)


        #Create the stream if a stream definition is provided
        log.debug("DataProductManagementService:create_data_product: stream definition id = %s" % stream_definition_id)

        if stream_definition_id:
            stream_id = self.clients.pubsub_management.create_stream(name=data_product.name,  description=data_product.description, stream_definition_id=stream_definition_id)
            log.debug("create_data_product: create stream stream_id %s" % stream_id)
            # Associate the Stream with the main Data Product
            self.clients.resource_registry.create_association(data_product_id,  PRED.hasStream, stream_id)

        # Return a resource ref to the new data product
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
 
        log.debug("DataProductManagementService:update_data_product: %s" % str(data_product))
               
        self.clients.resource_registry.update(data_product)

        #TODO: any changes to producer? Call DataAcquisitionMgmtSvc?

        return


    def delete_data_product(self, data_product_id=''):

        #Check if this data product is associated to a producer
        producer_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataProducer, RT.DataProducer, id_only=True)
        if producer_ids:
            log.debug("DataProductManagementService:delete_data_product: %s" % str(producer_ids))
            self.clients.data_acquisition_management.unassign_data_product(data_product_id)
        
        # Delete the data product
        self.clients.resource_registry.delete(data_product_id)
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

        #find the items in the store
        if filters is None:
            objects, _ = self.clients.resource_registry.find_resources(RT.DataProduct, None, None, False)
        else:  # TODO: code for all the filter types
            objects = []
        return objects


    def activate_data_product_persistence(self, data_product_id='', persist_data=True, persist_metadata=True):
        """Persist data product data into a data set

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """
        # retrieve the data_process object
        data_product_obj = self.clients.resource_registry.read(data_product_id)

        # get the Stream associated with this data set; if no stream then create one, if multiple streams then Throw
        streams, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStream, RT.Stream, True)
        if not streams:
            raise BadRequest('Data Product %s must have one stream associated' % str(data_product_id))

        stream = streams[0]
        log.debug("activate_data_product_persistence: stream = %s"  % str(stream))

        # Find THE ingestion configuration in the RR to create a ingestion configuration
        # todo: how are multiple ingest configs for a site managed?
        ingest_config_objs, _ = self.clients.resource_registry.find_resources(restype=RT.IngestionConfiguration, id_only=False)
        if len(ingest_config_objs) != 1:
            log.debug("activate_data_product_persistence: ERROR ingest_config_objs = %s"  % str(ingest_config_objs))
            raise BadRequest('Data Product must have one ingestion configuration %s' % str(data_product_id))

        ingestion_configuration_obj = ingest_config_objs[0]
        log.debug("activate_data_product_persistence: ingestion_configuration_obj = %s"  % str(ingestion_configuration_obj))

        # create the dataset for the data
        # TODO: what if the Dataset already exists (activate-suspend-activate case???)
        # !!!!!!!! (Currently) The Datastore name MUST MATCH the ingestion configuration name!!!
        data_product_obj.dataset_id = self.clients.dataset_management.create_dataset(stream_id=stream,
                datastore_name=ingestion_configuration_obj.couch_storage.datastore_name, description=data_product_obj.description)
        log.debug("activate_data_product_persistence: create_dataset = %s"  % str(data_product_obj.dataset_id))

        self.update_data_product(data_product_obj)
        # Need to read again, because the _rev has changed. Otherwise error on update later.
        data_product_obj = self.clients.resource_registry.read(data_product_id)

        # call ingestion management to create a dataset configuration
        log.debug('activate_data_product_persistence: Calling create_dataset_configuration', )
        dataset_configuration_id = self.clients.ingestion_management.create_dataset_configuration(
                    dataset_id=data_product_obj.dataset_id, archive_data=persist_data,
                    archive_metadata=persist_metadata, ingestion_configuration_id=ingestion_configuration_obj._id)
        log.debug("activate_data_product_persistence: create_dataset_configuration = %s"  % str(dataset_configuration_id))

        # save the dataset_configuration_id in the product resource? Can this be found via the stream id?

        data_product_obj.dataset_configuration_id = dataset_configuration_id
        self.update_data_product(data_product_obj)

        return

    def suspend_data_product_persistence(self, data_product_id=''):
        """Suspend data product data persistnce into a data set, multiple options

        @param data_product_id    str
        @param type    str
        @throws NotFound    object with specified id does not exist
        """

        # retrieve the data_process object
        data_product_obj = self.clients.resource_registry.read(data_product_id)
        if data_product_obj is None:
            raise NotFound("Data Product %s does not exist" % data_product_id)
        if data_product_obj.ingestion_configuration_id is None:
            raise NotFound("Data Product %s ingestion configuration does not exist" % data_product_id)
        if data_product_obj.dataset_configuration_id is None:
            raise NotFound("Data Product %s dataset configuration does not exist" % data_product_id)


        #retrieve the dataset configuation object so that attrs can be changed
        dataset_configuration_obj = self.clients.resource_registry.read(data_product_obj.dataset_configuration_id)
        if dataset_configuration_obj is None:
            raise NotFound("Dataset Configuration %s does not exist" % data_product_obj.dataset_configuration_id)

        #Set the dataset config archive data/metadata attrs to false
        dataset_configuration_obj.archive_data = False
        dataset_configuration_obj.archive_metadata = False

        ret = self.clients.ingestion_management.update_dataset_config(dataset_configuration_obj)

        log.debug("suspend_data_product_persistence: deactivate = %s"  % str(ret))

        return

    def set_data_product_lifecycle(self, data_product_id="", lifecycle_state=""):
       """
       declare a data_product to be in a given state
       @param data_product_id the resource id
       """
       return self.data_product.advance_lcs(data_product_id, lifecycle_state)

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
