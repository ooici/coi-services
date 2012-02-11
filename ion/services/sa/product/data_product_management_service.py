#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from interface.services.sa.idata_product_management_service import BaseDataProductManagementService

from ion.services.sa.resource_impl.data_product_impl import DataProductImpl

from pyon.datastore.datastore import DataStore
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS, PRED

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
            self.clients.data_acquisition_management.assign_data_product(source_resource_id, data_product_id, True)  # TODO: what errors can occur here?
            
#        else:
#            #create a data producer to go with this product, and associate it
#            pducer_obj = self.producer_for_product(IonObject(RT.DataProducer), data_product)
#            pducer_id = self.clients.data_acquisition_management.create_data_producer(pducer_obj)
#            log.debug("I GOT A PRODUCER ID='%s'" % pducer_id)
#            self.data_product.link_data_producer(data_product_id, pducer_id)
            

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
        data_producer_ids = self.data_product.find_stemming_data_producer(data_product._id)
        #TODO: any changes to producer? Call DataAcquisitionMgmtSvc?

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

    def activate_data_product_persistence(self, data_product_id=''):
        """Persist data product data into a data set

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """

        #dataset_management, ingestion_management]

        # get the Stream associated with this data set; if no stream then create one, if multiple streams then Throw
        streams, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStream, RT.Stream, True)
        if len(streams) > 1:
            raise BadRequest('There are  multiple streams linked to this Data Product %s' % str(data_product_id))


        # delete the transform associations link as well
        self.clients.dataset_management.create_dataset(self, stream_id='', name='', description='', contact=None, user_metadata={})




        pass

    def suspend_data_product_persistence(self, data_product_id='', type=''):
        """Suspend data product data persistnce into a data set, multiple options

        @param data_product_id    str
        @param type    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def set_data_product_lifecycle(self, data_product_id="", lifecycle_state=""):
       """
       declare a data_product to be in a given state
       @param data_product_id the resource id
       """
       return self.data_product.advance_lcs(data_product_id, lifecycle_state)
