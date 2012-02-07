#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from interface.services.sa.idata_product_management_service import BaseDataProductManagementService

from ion.services.sa.resource_impl.data_product_impl import DataProductImpl

from pyon.datastore.datastore import DataStore
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS

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


    def producer_for_product(self, producer_obj, product_obj):
        """
        sets an appropriate name for a data producer that's associated with a given data product
        """
        producer_obj.name = str(product_obj.name + " Producer")
        producer_obj.description = str("DataProducer for " + product_obj.name)
        return producer_obj
    

    def create_data_product(self, data_product=None, source_resource_id=''):
        """
        @param      data_product IonObject which defines the general data product resource
        @param      source_resource_id IonObject id which defines the source for the data
        @retval     data_product_id
        """ 
        #   1. Verify that a data product with same name does not already exist 
        #   2. Validate that the data product IonObject does not contain an id_ element     
        #   3. Create a new data product
        #       - User must supply the name in the data product
        #   4. Create a new data producer if supplied
        
        # Create will validate and register a new data product within the system

        # Validate - TBD by the work that Karen Stocks is driving with John Graybeal

        # Register - create and store a new DataProduct resource using provided metadata

        # Create necessary associations to owner, instrument, etc

        # Call Data Aquisition Mgmt Svc:create_data_producer to coordinate creation of topic and connection to source

        # Return a resource ref
        
        log.debug("DataProductManagementService:create_data_product: %s" % str(data_product))

        data_product_id = self.data_product.create_one(data_product)

        if source_resource_id:
            log.debug("DataProductManagementService:create_data_product: source resource id = %s" % source_resource_id)
            self.clients.data_acquisition_management.assign_data_product(source_resource_id, data_product_id)  # TODO: what errors can occur here?
            
        else:
            #create a data producer to go with this product, and associate it
            pducer_obj = self.producer_for_product(IonObject(RT.DataProducer), data_product)
            pducer_id = self.clients.data_acquisition_management.create_data_producer(pducer_obj)
            log.debug("I GOT A PRODUCER ID='%s'" % pducer_id)
            self.data_product.link_data_producer(data_product_id, pducer_id)
            

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
               
        self.data_product.update_one(data_product)

        #keep associated data producer name in sync with data product
        data_producer_ids, _ = self.data_product.find_stemming_data_producer(data_product._id)
        #TODO: error check / consistency check
        if 1 == len(data_producer_ids):
            data_producer_id = data_producer_ids[0]
            data_producer_obj = self.clients.data_acquisition_management.read_data_producer(data_producer_id)
            data_producer_obj = self.producer_for_product(data_producer_obj, data_product)
            self.clients.data_acquisition_management.update_data_producer(data_producer_obj)

        return


    def delete_data_product(self, data_product_id=''):
        """
        @todo document this interface!!!

        @param data_product_id    DataProduct identifier
        @throws NotFound    object with specified id does not exist
        """

        log.debug("DataProductManagementService:delete_data_product: %s" % str(data_product_id))
        
        # Attempt to change the life cycle state of data product
        self.data_product.delete_one(data_product_id)

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
