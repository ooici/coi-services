#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from interface.services.sa.idata_product_management_service import BaseDataProductManagementService
from pyon.datastore.datastore import DataStore
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound, Conflict

class DataProductManagementService(BaseDataProductManagementService):

    
    def create_data_product(self, data_product={}, data_producer={}):
        """
        method docstring
        """
        # Create will validate and register a new data product within the system

        # Validate - TBD by the work that Karen Stocks is driving with John Graybeal

        # Register - create and store a new DataProduct resource using provided metadata

        # Create necessary associations to owner, instrument, etc

        # Call Data Aquisition Mgmt Svc:define_data_producer to coordinate creation of topic and connection to source

        # Return a resource ref
        
        log.debug("DataProductManagementService:create_data_product: %s" % str(data_product))
        
        result, asso = self.clients.resource_registry.find_by_name(data_product["name"], "DataProduct", True)
        print "result = ", result
        if len(result) != 0:
            raise BadRequest("A data product named '%s' already exists" % data_product["name"])  

        #dp_obj = IonObject("DataProduct", name=data_product["name"], description=data_product["description"])
        dp_obj = IonObject("DataProduct", name=data_product["name"])
        dataProduct_id, revision = self.clients.resource_registry.create(dp_obj)
            
        if len(data_producer) != 0:
            result = self.clients.data_acquisition_management.create_data_producer(data_producer)  # TODO: what errors can occur here?
            log.info("DataProductManagementService.define_data_product create_data_producer result: %s " % str(result))
            DataProducerId = result
            DataStreamId = ''        # TODO: what data_acquisition_management operation gets this value?
            # TODO: make associations between data_producer and data_product
            
        return dataProduct_id


    def update_data_product(self, data_product={}):
        """
        method docstring
        """
        # Update metadata for a specific data product
        # Return updated data product resource
 
        log.debug("DataProductManagementService:update_data_product: %s" % str(data_product))
        
        result = self.clients.resource_registry.find_by_name(data_product["name"], "DataProduct", id_only=False)
        if len(result) == 0:
            raise BadRequest("The data product named '%s' does not exists" % data_product["name"])
        
        try:  
            dataProduct = self.clients.resource_registry.update(data_product)
        except BadRequest as ex:
            raise ex
        except Conflict as ex:
            raise ex
            
        return dataProduct


    def read_data_product(self, data_product_id=''):
        """
        method docstring
        """
        # Retrieve all metadata for a specific data product
        # Return data product resource

        log.debug("DataProductManagementService:read_data_product: %s" % str(data_product_id))
        
        try:
            result = self.clients.resource_registry.read(data_product_id)
        except NotFound:
            raise BadRequest("The data product with id '%s' does not exists" % str(data_product_id))  
        return result


    def delete_data_product(self, data_product_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #

        log.debug("DataProductManagementService:delete_data_product: %s" % str(data_product_id))
        
        try:
            result = self.clients.resource_registry.read(data_product_id)
        except NotFound:
            raise BadRequest("The data product with id '%s' does not exists" % str(data_product_id))  
        try:
            self.clients.resource_registry.delete(result)
        except NotFound:
            raise BadRequest("The data product with id '%s' does not exists" % str(data_product_id))  


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
