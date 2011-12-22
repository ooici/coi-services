#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from interface.services.sa.idata_product_management_service import BaseDataProductManagementService
from pyon.datastore.datastore import DataStore
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, AT, LCS

class DataProductManagementService(BaseDataProductManagementService):
    """ @author     Bill Bollenbacher
        @file       ion/services/sa/data_product_management/data_product_management_service.py
        @brief      Implementation of the data product management service
    """
    
    def create_data_product(self, data_product={}, data_producer={}):
        """
        @param      data_product IonObject which defines the general data product resource 
        @param      data_producer IonObject which defines the general data producer resource 
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
        
        result, _ = self.clients.resource_registry.find_resources(RT.DataProduct, None, data_product.name, True)
        if len(result) != 0:
            raise BadRequest("A data product named '%s' already exists" % data_product.name)  

        data_product_id, version = self.clients.resource_registry.create(data_product)
            
        if data_producer != {}:
            log.debug("DataProductManagementService:create_data_product: data producer = %s" % str(data_producer))
            data_producer_id = self.clients.data_acquisition_management.create_data_producer(data_producer)  # TODO: what errors can occur here?
            log.debug("DataProductManagementService.define_data_product create_data_producer result: %s " % data_producer_id)
            self.clients.resource_registry.create_association(data_product_id, 
                                                              AT.hasDataProducer, 
                                                              data_producer_id)
            
        return data_product_id


    def read_data_product(self, data_product_id=''):
        """
        method docstring
        """
        # Retrieve all metadata for a specific data product
        # Return data product resource

        log.debug("DataProductManagementService:read_data_product: %s" % str(data_product_id))
        
        result = self.clients.resource_registry.read(data_product_id)
        
        return result


    def update_data_product(self, data_product={}):
        """
        method docstring
        """
        # Update metadata for a specific data product
        # Return updated data product resource
 
        log.debug("DataProductManagementService:update_data_product: %s" % str(data_product))
        
        if ('_id' not in data_product):
            raise BadRequest("The _id field was not set in data product object")         
        
        data_product_id = self.clients.resource_registry.update(data_product)
            
        return True


    def delete_data_product(self, data_product_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #

        log.debug("DataProductManagementService:delete_data_product: %s" % str(data_product_id))
        
        # Attempt to change the life cycle state of data product
        self.clients.resource_registry.delete(data_product_id)

        return True

    def find_data_products(self, filters={}):
        """
        method docstring
        """
        # Validate the input filter and augment context as required

        # Define set of resource attributes to filter on, change parameter from "filter" to include attributes and filter values.
        #     potentially: title, keywords, date_created, creator_name, project, geospatial coords, time range

        # Call DM DiscoveryService to query the catalog for matches

        # Organize and return the list of matches with summary metadata (title, summary, keywords)

        #find the item in the store
        pass
