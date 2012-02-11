#!/usr/bin/env python

'''
@package ion.services.sa.acquisition.data_acquisition_management_service Implementation of IDataAcquisitionManagementService interface
@file ion/services/sa/acquisition/data_acquisition_management_management_service.py
@author M Manning
@brief Data Acquisition Management service to keep track of Data Producers, Data Sources
and the relationships between them
'''

from interface.services.sa.idata_acquisition_management_service import BaseDataAcquisitionManagementService
from pyon.core.exception import NotFound, BadRequest
from pyon.public import CFG, IonObject, log, RT, LCS, PRED



class DataAcquisitionManagementService(BaseDataAcquisitionManagementService):


    # -----------------
    # The following operations register different types of data producers
    # -----------------

    def _remove_producer(self, resource_id='', producers=None):
        log.debug("Removing DataProducer objects and links")
        for producer in producers:
            # List all association ids with given subject, predicate, object triples
            assoc_ids = self.clients.resource_registry.find_associations(resource_id, PRED.hasDataProducer, producer, True)
            self.clients.resource_registry.delete_association(assoc_ids[0])

            # DELETE THE STREAM associated with the data producer via call to PubSub
            res_ids, _ = self.clients.resource_registry.find_objects(producer, PRED.hasStream, None, True)
            if res_ids is None:
                raise NotFound("Stream for Data Producer  %d does not exist" % producer)
            for streamId in res_ids:
                self.clients.pubsub_management.delete_stream(streamId)

            self.clients.resource_registry.delete(producer)

        return


    def register_external_data_set(self, external_dataset_id=''):
        """Register an existing external data set as data producer

        @param external_dataset_id    str
        @retval data_producer_id    str
        """
        # retrieve the data_source object
        data_set_obj = self.clients.resource_registry.read(external_dataset_id)
        if data_set_obj is None:
            raise NotFound("External Data Set %s does not exist" % external_dataset_id)

        #create data producer resource and associate to this external_dataset_id
        data_producer_obj = IonObject(RT.DataProducer,name=data_set_obj.name, description="primary producer resource for this data set", is_primary=True)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        # Create association
        self.clients.resource_registry.create_association(external_dataset_id, PRED.hasDataProducer, data_producer_id)

        return data_producer_id

    def unregister_external_data_set(self, external_dataset_id=''):
        """

        @param external_dataset_id    str
        @throws NotFound    object with specified id does not exist
        """
        """
        Remove the associated DataProducer

        """
        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        res_ids, _ = self.clients.resource_registry.find_objects(external_dataset_id, PRED.hasDataProducer, None, True)
        if res_ids is None:
            raise NotFound("Data Producer for External Data Set %d does not exist" % external_dataset_id)

        return self._remove_producer(external_dataset_id, res_ids)
    

    def register_process(self, data_process_id=''):
        """
        Register an existing data process as data producer
        """

        # retrieve the data_process object
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        if data_process_obj is None:
            raise NotFound("Data Process %s does not exist" % data_process_id)

        #create data producer resource and associate to this data_process_id
        data_producer_obj = IonObject(RT.DataProducer,name=data_process_obj.name, description="primary producer resource for this process", is_primary=True)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        # Create association
        self.clients.resource_registry.create_association(data_process_id, PRED.hasDataProducer, data_producer_id)

        # TODO: Walk up the assocations to find parent producers:
        # proc->subscription->stream->prod

        return data_producer_id

    def unregister_process(self, data_process_id=''):
        """
        Remove the associated DataProcess

        """
        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        res_ids, _ = self.clients.resource_registry.find_objects(data_process_id, PRED.hasDataProducer, None, True)
        if res_ids is None:
            raise NotFound("Data Producer for Data Process %d does not exist" % data_process_id)

        return self._remove_producer(data_process_id, res_ids)

    def register_instrument(self, instrument_id=''):
        """
        Register an existing instrument as data producer
        """
        # retrieve the data_process object
        instrument_obj = self.clients.resource_registry.read(instrument_id)
        if instrument_obj is None:
            raise NotFound("Instrument %s does not exist" % instrument_id)

        #create data producer resource and associate to this instrument_id
        data_producer_obj = IonObject(RT.DataProducer,name=instrument_obj.name, description="primary producer resource for this instrument", is_primary=True)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)
        log.debug("register_instrument  data_producer_id %s" % data_producer_id)

        # Create association
        self.clients.resource_registry.create_association(instrument_id, PRED.hasDataProducer, data_producer_id)

        return data_producer_id

    def unregister_instrument(self, instrument_id=''):
        """
        Remove the associated DataProcess

        """
        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        res_ids, _ = self.clients.resource_registry.find_objects(instrument_id, PRED.hasDataProducer, None, True)
        if res_ids is None:
            raise NotFound("Data Producer for Instrument %d does not exist" % instrument_id)

        return self._remove_producer(instrument_id, res_ids)


    def assign_data_product(self, input_resource_id='', data_product_id='', create_stream=False):
        """Connect the producer for an existing input resource with a data product

        @param input_resource_id    str
        @param data_product_id    str
        @retval data_producer_id    str
        """
        # Verify that both ids are valid
        input_resource_obj = self.clients.resource_registry.read(input_resource_id)
        if not input_resource_obj:
            raise BadRequest("Source resource %s does not exist" % input_resource_id)
        data_product_obj = self.clients.resource_registry.read(data_product_id)
        if not data_product_obj:
            raise BadRequest("Data Product resource %s does not exist" % data_product_id)

        #find the data producer resource associated with the source resource that is creating the data product
        producer_ids, _ = self.clients.resource_registry.find_objects(input_resource_id, PRED.hasDataProducer, RT.DataProducer, id_only=True)
        if producer_ids is None:
            raise NotFound("No Data Producers associated with source resource ID " + str(input_resource_id))
        #find the 'head' producer
        self.primary_producer = None
        for producer_id in producer_ids:
            producer_obj = self.clients.resource_registry.read(producer_id)
            if not producer_obj:
                raise NotFound("Data Producer %s does not exist" % producer_id)
            if producer_obj.is_primary:
                self.primary_producer = producer_id

        if self.primary_producer is None:
            raise NotFound("No primary Data Producer associated with source resource ID " + str(input_resource_id))

        #create data producer resource and associate to this instrument_id
        data_producer_obj = IonObject(RT.DataProducer,name=data_product_obj.name, description=data_product_obj.description)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        # Associate the Product with the Producer
        self.clients.resource_registry.create_association(data_product_id,  PRED.hasDataProducer,  data_producer_id)
        # Associate the Producer with the main Producer
        self.clients.resource_registry.create_association(data_producer_id,  PRED.hasParent,  self.primary_producer)

        #Create the stream if requested
        if create_stream:
            stream_id = self.clients.pubsub_management.create_stream(name=data_product_obj.name,  description=data_product_obj.description)
            # Associate the Stream with the main Data Product
            self.clients.resource_registry.create_association(data_product_id,  PRED.hasStream, stream_id)

        return

    def unassign_data_product(self, input_resource_id='', data_product_id=''):
        """@todo document this interface!!!

        @param input_resource_id    str
        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass


    def create_data_producer(name='', description=''):
        """Create a data producer resource, create a stream reource via DM then associate the two resources. Currently, data producers and streams are one-to-one. If the data producer is a process, connect the data producer to any parent data producers.

        @param name    str
        @param description    str
        @retval data_producer_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

    def update_data_producer(self, data_producer=None):
        '''
        Update an existing data producer.

        @param data_producer The data_producer object with updated properties.
        @retval success Boolean to indicate successful update.
        @todo Add logic to validate optional attributes. Is this interface correct?
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Updating data_producer object: %s" % data_producer.name)
        return self.clients.resource_registry.update(data_producer)

    def read_data_producer(self, data_producer_id=''):
        '''
        Get an existing data_producer object.

        @param data_producer The id of the stream.
        @retval data_producer The data_producer object.
        @throws NotFound when data_producer doesn't exist.
        '''
        # Return Value
        # ------------
        # data_producer: {}
        #
        log.debug("Reading data_producer object id: %s" % data_producer_id)
        data_producer_obj = self.clients.resource_registry.read(data_producer_id)
        if data_producer_obj is None:
            raise NotFound("Data producer %s does not exist" % data_producer_id)
        return data_producer_obj

    def delete_data_producer(self, data_producer_id=''):
        '''
        Delete an existing data_producer.

        @param data_producer_id The id of the stream.
        @retval success Boolean to indicate successful deletion.
        @throws NotFound when data_producer doesn't exist.
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Deleting data_producer id: %s" % data_producer_id)
        data_producer_obj = self.read_data_producer(data_producer_id)
        if data_producer_obj is None:
            raise NotFound("Data producer %d does not exist" % data_producer_id)

        #Unregister the data producer with PubSub
        self.clients.pubsub_management.unregister_producer(data_producer_obj.name, data_producer_obj.stream_id)

        #TODO tell PubSub to delete the stream??

        return self.clients.resource_registry.delete(data_producer_obj)


    # -----------------
    # The following operations manage EOI resources
    # -----------------

    def create_external_data_provider(self, external_data_provider=None):
        # Persist ExternalDataProvider object and return object _id as OOI id
        external_data_provider_id, version = self.clients.resource_registry.create(external_data_provider)
        return external_data_provider_id

    def update_external_data_provider(self, external_data_provider=None):
        # Overwrite ExternalDataProvider object
        self.clients.resource_registry.update(external_data_provider)

    def read_external_data_provider(self, external_data_provider_id=''):
        # Read ExternalDataProvider object with _id matching passed user id
        external_data_provider = self.clients.resource_registry.read(external_data_provider_id)
        if not external_data_provider:
            raise NotFound("ExternalDataProvider %s does not exist" % external_data_provider_id)
        return external_data_provider

    def delete_external_data_provider(self, external_data_provider_id=''):
        # Read and delete specified ExternalDataProvider object
        external_data_provider = self.clients.resource_registry.read(external_data_provider_id)
        if not external_data_provider:
            raise NotFound("ExternalDataProvider %s does not exist" % external_data_provider_id)
        self.clients.resource_registry.delete(external_data_provider_id)



    def create_data_source(self, data_source=None):
        # Persist DataSource object and return object _id as OOI id
        data_source_id, version = self.clients.resource_registry.create(data_source)
        return data_source_id

    def update_data_source(self, data_source=None):
        # Overwrite DataSource object
        self.clients.resource_registry.update(data_source)

    def read_data_source(self, data_source_id=''):
        # Read DataSource object with _id matching passed user id
        log.debug("Reading DataSource object id: %s" % data_source_id)
        data_source_obj = self.clients.resource_registry.read(data_source_id)
        if not data_source_obj:
            raise NotFound("DataSource %s does not exist" % data_source_id)
        return data_source_obj

    def delete_data_source(self, data_source_id=''):
        # Read and delete specified DataSource object
        log.debug("Deleting DataSource id: %s" % data_source_id)
        data_source_obj = self.read_data_source(data_source_id)
        if data_source_obj is None:
            raise NotFound("DataSource %s does not exist" % data_source_id)

        return self.clients.resource_registry.delete(data_source_id)



    def create_external_dataset(self, external_dataset=None):
        # Persist ExternalDataSet object and return object _id as OOI id
        external_dataset_id, version = self.clients.resource_registry.create(external_dataset)
        return external_dataset_id

    def update_external_dataset(self, external_dataset=None):
        # Overwrite ExternalDataSet object
        self.clients.resource_registry.update(external_dataset)

    def read_external_dataset(self, external_dataset_id=''):
        # Read ExternalDataSet object with _id matching passed user id
        external_dataset = self.clients.resource_registry.read(external_dataset_id)
        if not external_dataset:
            raise NotFound("ExternalDataSet %s does not exist" % external_dataset_id)
        return external_dataset

    def delete_external_dataset(self, external_dataset_id=''):
        # Read and delete specified ExternalDataSet object
        external_dataset = self.clients.resource_registry.read(external_dataset_id)
        if not external_dataset:
            raise NotFound("ExternalDataSet %s does not exist" % external_dataset_id)
        self.clients.resource_registry.delete(external_dataset_id)



    def create_external_data_agent(self, external_data_agent=None):
        # Persist ExternalDataAgent object and return object _id as OOI id
        external_data_agent_id, version = self.clients.resource_registry.create(external_data_agent)
        return external_data_agent_id

    def update_external_data_agent(self, external_data_agent=None):
        # Overwrite ExternalDataAgent object
        self.clients.resource_registry.update(external_data_agent)

    def read_external_data_agent(self, external_data_agent_id=''):
        # Read ExternalDataAgent object with _id matching passed user id
        external_data_agent = self.clients.resource_registry.read(external_data_agent_id)
        if not external_data_agent:
            raise NotFound("ExternalDataAgent %s does not exist" % external_data_agent_id)
        return external_data_agent

    def delete_external_data_agent(self, external_data_agent_id=''):
        # Read and delete specified ExternalDataAgent object
        external_data_agent = self.clients.resource_registry.read(external_data_agent_id)
        if not external_data_agent:
            raise NotFound("ExternalDataAgent %s does not exist" % external_data_agent_id)
        self.clients.resource_registry.delete(external_data_agent_id)



    def create_external_data_agent_instance(self, external_data_agent_instance=None):
        # Persist ExternalDataAgentInstance object and return object _id as OOI id
        external_data_agent_instance_id, version = self.clients.resource_registry.create(external_data_agent_instance)
        return external_data_agent_instance_id

    def update_external_data_agent_instance(self, external_data_agent_instance=None):
        # Overwrite ExternalDataAgentInstance object
        self.clients.resource_registry.update(external_data_agent_instance)

    def read_external_data_agent_instance(self, external_data_agent_instance_id=''):
        # Read ExternalDataAgentInstance object with _id matching passed user id
        external_data_agent_instance = self.clients.resource_registry.read(external_data_agent_instance_id)
        if not external_data_agent_instance:
            raise NotFound("ExternalDataAgentInstance %s does not exist" % external_data_agent_instance_id)
        return external_data_agent_instance

    def delete_external_data_agent_instance(self, external_data_agent_instance_id=''):
        # Read and delete specified ExternalDataAgentInstance object
        external_data_agent_instance = self.clients.resource_registry.read(external_data_agent_instance_id)
        if not external_data_agent_instance:
            raise NotFound("ExternalDataAgentInstance %s does not exist" % external_data_agent_instance_id)
        self.clients.resource_registry.delete(external_data_agent_instance_id)



    def create_external_data_source_model(self, external_data_source_model=None):
        # Persist ExternalDataSourceModel object and return object _id as OOI id
        external_data_source_model_id, version = self.clients.resource_registry.create(external_data_source_model)
        return external_data_source_model_id

    def update_external_data_source_model(self, external_data_source_model=None):
        # Overwrite ExternalDataSourceModel object
        self.clients.resource_registry.update(external_data_source_model)

    def read_external_data_source_model(self, external_data_source_model_id=''):
        # Read ExternalDataSourceModel object with _id matching passed user id
        external_data_source_model = self.clients.resource_registry.read(external_data_source_model_id)
        if not external_data_source_model:
            raise NotFound("ExternalDataSourceModel %s does not exist" % external_data_source_model_id)
        return external_data_source_model

    def delete_external_source_model_instance(self, external_data_source_model_id=''):
        # Read and delete specified ExternalDataSourceModel object
        external_data_source_model = self.clients.resource_registry.read(external_data_source_model_id)
        if not external_data_source_model:
            raise NotFound("ExternalDataSourceModel %s does not exist" % external_data_source_model_id)
        self.clients.resource_registry.delete(external_data_source_model_id)



    def assign_data_source_to_external_data_provider(self, data_source_id='', external_data_provider_id=''):
        #Connect the data source with an external data provider
        data_source = self.clients.resource_registry.read(data_source_id)
        if not data_source:
            raise NotFound("ExternalDataSource resource %s does not exist" % data_source_id)

        agent_instance = self.clients.resource_registry.read(external_data_provider_id)
        if not agent_instance:
            raise NotFound("External Data Provider resource %s does not exist" % external_data_provider_id)

        self.clients.resource_registry.create_association(data_source_id,  PRED.hasProvider,  external_data_provider_id)

    def unassign_data_source_from_external_data_provider(self, data_source_id='', external_data_provider_id=''):
        #Disconnect the data source from the external data provider
        data_source = self.clients.resource_registry.read(data_source_id)
        if not data_source:
            raise NotFound("ExternalDataSource resource %s does not exist" % data_source_id)

        agent_instance = self.clients.resource_registry.read(external_data_provider_id)
        if not agent_instance:
            raise NotFound("External Data Provider resource %s does not exist" % external_data_provider_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(data_source_id, PRED.hasProvider, external_data_provider_id, True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def assign_data_source_to_data_model(self, data_source_id='', data_source_model_id=''):
        #Connect the data source with an external data model
        data_source = self.clients.resource_registry.read(data_source_id)
        if not data_source:
            raise NotFound("ExternalDataSource resource %s does not exist" % data_source_id)

        agent_instance = self.clients.resource_registry.read(data_source_model_id)
        if not agent_instance:
            raise NotFound("External Data Source Model resource %s does not exist" % data_source_model_id)

        self.clients.resource_registry.create_association(data_source_id,  PRED.hasModel,  data_source_model_id)

    def unassign_data_source_from_data_model(self, data_source_id='', data_source_model_id=''):
        #Disonnect the data source from the external data model
        data_source = self.clients.resource_registry.read(data_source_id)
        if not data_source:
            raise NotFound("ExternalDataSource resource %s does not exist" % data_source_id)

        agent_instance = self.clients.resource_registry.read(data_source_model_id)
        if not agent_instance:
            raise NotFound("External Data Source Model resource %s does not exist" % data_source_model_id)
        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(data_source_id,  PRED.hasModel,  data_source_model_id, True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)



    def assign_external_dataset_to_agent_instance(self, external_dataset_id='', agent_instance_id=''):
        #Connect the agent instance with an external data set
        data_source = self.clients.resource_registry.read(external_dataset_id)
        if not data_source:
            raise NotFound("ExternalDataSet resource %s does not exist" % external_dataset_id)

        agent_instance = self.clients.resource_registry.read(agent_instance_id)
        if not agent_instance:
            raise NotFound("External Data Agent Instance resource %s does not exist" % agent_instance_id)

        self.clients.resource_registry.create_association(external_dataset_id,  PRED.hasAgentInstance,  agent_instance_id)

    def unassign_external_dataset_from_agent_instance(self, external_dataset_id='', agent_instance_id=''):
        data_source = self.clients.resource_registry.read(external_dataset_id)
        if not data_source:
            raise NotFound("ExternalDataSet resource %s does not exist" % external_dataset_id)

        agent_instance = self.clients.resource_registry.read(agent_instance_id)
        if not agent_instance:
            raise NotFound("External Data Agent Instance resource %s does not exist" % agent_instance_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasAgentInstance,  agent_instance_id, True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)



    def assign_external_data_agent_to_agent_instance(self, external_data_agent_id='', agent_instance_id=''):
        #Connect the agent with an agent instance
        data_source = self.clients.resource_registry.read(external_data_agent_id)
        if not data_source:
            raise NotFound("ExternalDataSet resource %s does not exist" % external_data_agent_id)

        agent_instance = self.clients.resource_registry.read(agent_instance_id)
        if not agent_instance:
            raise NotFound("External Data Agent Instance resource %s does not exist" % agent_instance_id)

        self.clients.resource_registry.create_association(external_data_agent_id,  PRED.hasInstance,  agent_instance_id)

    def unassign_external_data_agent_from_agent_instance(self, external_data_agent_id='', agent_instance_id=''):
        data_source = self.clients.resource_registry.read(external_data_agent_id)
        if not data_source:
            raise NotFound("ExternalDataSet resource %s does not exist" % external_data_agent_id)

        agent_instance = self.clients.resource_registry.read(agent_instance_id)
        if not agent_instance:
            raise NotFound("External Data Agent Instance resource %s does not exist" % agent_instance_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(external_data_agent_id,  PRED.hasInstance,  agent_instance_id, True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def assign_external_data_agent_to_data_model(self, external_data_agent_id='', data_source_model_id=''):
        #Connect the external data agent with an external data model
        external_data_agent = self.clients.resource_registry.read(external_data_agent_id)
        if not external_data_agent:
            raise NotFound("ExternalDataAgent resource %s does not exist" % external_data_agent_id)

        agent_instance = self.clients.resource_registry.read(data_source_model_id)
        if not agent_instance:
            raise NotFound("External Data Source Model resource %s does not exist" % data_source_model_id)

        self.clients.resource_registry.create_association(external_data_agent_id,  PRED.hasModel,  data_source_model_id)

    def unassign_external_data_agent_from_data_model(self, external_data_agent_id='', data_source_model_id=''):
        #Disonnect the external data agent from the external data model
        external_data_agent = self.clients.resource_registry.read(external_data_agent_id)
        if not external_data_agent:
            raise NotFound("ExternalDataAgent resource %s does not exist" % external_data_agent_id)

        agent_instance = self.clients.resource_registry.read(data_source_model_id)
        if not agent_instance:
            raise NotFound("External Data Source Model resource %s does not exist" % data_source_model_id)
        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(external_data_agent_id,  PRED.hasModel,  data_source_model_id, True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def assign_external_dataset_to_data_source(self, external_dataset_id='', data_source_id=''):
        #Connect the external data set to a data source
        data_source = self.clients.resource_registry.read(external_dataset_id)
        if not data_source:
            raise NotFound("ExternalDataSet resource %s does not exist" % external_dataset_id)

        agent_instance = self.clients.resource_registry.read(data_source_id)
        if not agent_instance:
            raise NotFound("External Data Source Instance resource %s does not exist" % data_source_id)

        self.clients.resource_registry.create_association(external_dataset_id,  PRED.hasSource,  data_source_id)


    def unassign_external_dataset_from_data_source(self, external_dataset_id='', data_source_id=''):
        #Disonnect the external data set from the data source
        data_source = self.clients.resource_registry.read(external_dataset_id)
        if not data_source:
            raise NotFound("ExternalDataSet resource %s does not exist" % external_dataset_id)

        agent_instance = self.clients.resource_registry.read(data_source_id)
        if not agent_instance:
            raise NotFound("External Data Source Instance resource %s does not exist" % data_source_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasSource,  data_source_id, True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)



    def assign_eoi_resources(self, external_data_provider_id='', data_source_id='', data_source_model_id='', external_dataset_id='', external_data_agent_id='', agent_instance_id=''):
        #Connects multiple eoi resources in batch with assocations

        self.assign_data_source_to_external_data_provider(data_source_id, external_data_provider_id )

        self.assign_data_source_to_data_model(data_source_id, data_source_model_id)

        self.assign_external_dataset_to_data_source(external_dataset_id, data_source_id)

        self.assign_external_dataset_to_agent_instance(external_dataset_id, agent_instance_id)

        self.assign_external_data_agent_to_data_model(external_data_agent_id, data_source_model_id)

        self.assign_external_data_agent_to_agent_instance(external_data_agent_id, agent_instance_id)
        



