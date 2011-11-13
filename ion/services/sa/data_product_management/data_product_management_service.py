#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.sa.idata_product_management_service import BaseDataProductManagementService

class DataProductManagementService(BaseDataProductManagementService):

    def define_data_product(self, product={}):
        # DefineDataProduct will validate and register a new data product within the system

        # Validate - TBD by the work that Karen Stocks is driving with John Graybeal

        # Register - create and store a new DataProduct resource using provided metadata

        # Create necessary associations to owner, instrument, etc

        # Call Data Aquisition Mgmt Svc:define_data_producer to coordinate creation of topic and connection to source

        # Return a resource ref
        pass

    def find_data_products(self, filters={}):
        # Validate the input filter and augment context as required

        # Define set of resource attributes to filter on, change parameter from "filter" to include attributes and filter values.
        #     potentially: title, keywords, date_created, creator_name, project, geospatial coords, time range

        # Call DM DiscoveryService to query the catalog for matches

        # Organize and return the list of matches with summary metadata (title, summary, keywords)
        
        #find the item in the store
        pass

    def get_data_product_detail(self, dataProductId=''):
        # Retrieve all metadata for a specific data product
        # Return data product resource
        pass

    def set_data_product_detail(self, dataProductId='', updates={}):
        # Update metadata for a specific data product
        # Return updated data product resource
        pass
  