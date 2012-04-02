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

        #todo: check that there are not attached data products?

        #todo: delete the data producer object and assoc to ext_data_set

        return
    

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
        Remove the associated DataProcess and disc

        """
        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        res_ids, _ = self.clients.resource_registry.find_objects(data_process_id, PRED.hasDataProducer, None, True)
        if res_ids is None:
            raise NotFound("Data Producer for Data Process %d does not exist" % data_process_id)

        # TODO: remove associations

        #todo: check that there are not attached data products?

        #todo: delete the data producer object and assoc to ext_data_set

        return

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

        # Verify that both ids are valid
        input_resource_obj = self.clients.resource_registry.read(instrument_id)
        if not input_resource_obj:
            raise BadRequest("Source resource %s does not exist" % instrument_id)

        #find the data producer resource associated with the source resource that is creating the data product
        producer_ids, _ = self.clients.resource_registry.find_objects(instrument_id, PRED.hasDataProducer, RT.DataProducer, id_only=True)
        if producer_ids is None:
            raise NotFound("No Data Producers associated with source resource ID " + str(instrument_id) )
        if len(producer_ids) > 1:
            raise BadRequest("All child Data Producers associated with instrument must be unassigned before instrument is unregistered " + str(instrument_id))

        #find the primary producer
#        self.primary_producer = None
#        for producer_id in producer_ids:
#            producer_obj = self.clients.resource_registry.read(producer_id)
#            if not producer_obj:
#                raise NotFound("Data Producer %s does not exist" % producer_id)
#            if producer_obj.is_primary:
#                self.primary_producer = producer_id

#        if self.primary_producer is None:
#            raise NotFound("No primary Data Producer associated with source resource ID " + str(instrument_id))


        data_producer_obj = self.clients.resource_registry.read(producer_ids[0])
        if not data_producer_obj:
            raise NotFound("Data Producer %s does not exist" % producer_ids[0])
        if not data_producer_obj.is_primary:
            raise NotFound("Data Producer is not primary %s" % producer_ids[0])

        # Remove the link between the child Data Producer resource and the primary Data Producer resource
        associations = self.clients.resource_registry.find_associations(instrument_id, PRED.hasDataProducer, self.primary_producer, id_only=True)
        for association in associations:
            log.debug("unregister_instrument: link to primary DataProducer %s" % association)
            self.clients.resource_registry.delete_association(association)

        self.clients.resource_registry.delete(producer_ids[0])
        return


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

        self.clients.resource_registry.create_association(input_resource_id,  PRED.hasOutputProduct,  data_product_id)

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

        log.debug("DAMS:assign_data_product: primary_producer %s" % str(self.primary_producer))

        if self.primary_producer is None:
            raise NotFound("No primary Data Producer associated with source resource ID " + str(input_resource_id))

        #create data producer resource for this data product
        data_producer_obj = IonObject(RT.DataProducer,name=data_product_obj.name, description=data_product_obj.description)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)
        log.debug("DAMS:assign_data_product: data_producer_id %s" % str(data_producer_id))

        # Associate the Product with the Producer
        self.clients.resource_registry.create_association(data_product_id,  PRED.hasDataProducer,  data_producer_id)
        # Associate the Producer with the main Producer
        self.clients.resource_registry.create_association(data_producer_id,  PRED.hasParent,  self.primary_producer)
        # Associate the input resource with the child data Producer
        self.clients.resource_registry.create_association(input_resource_id,  PRED.hasDataProducer, data_producer_id)

        #Create the stream if requested
        log.debug("assign_data_product: create_stream %s" % create_stream)
        if create_stream:
            stream_id = self.clients.pubsub_management.create_stream(name=data_product_obj.name,  description=data_product_obj.description)
            log.debug("assign_data_product: create stream stream_id %s" % stream_id)
            # Associate the Stream with the main Data Product
            self.clients.resource_registry.create_association(data_product_id,  PRED.hasStream, stream_id)

        return

    def unassign_data_product(self, input_resource_id='', data_product_id='', delete_stream=False):
        """
        Disconnect the Data Product from the Data Producer

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """
        # Verify that both ids are valid
        input_resource_obj = self.clients.resource_registry.read(input_resource_id)
        if not input_resource_obj:
            raise BadRequest("Source resource %s does not exist" % input_resource_id)
        data_product_obj = self.clients.resource_registry.read(data_product_id)
        if not data_product_obj:
            raise BadRequest("Data Product resource %s does not exist" % data_product_id)

        #find the data producer resource associated with the source resource that is creating the data product
        producer_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataProducer, RT.DataProducer, id_only=True)
        if producer_ids is None or len(producer_ids) > 1:
            raise NotFound("Invalid Data Producers associated with data product ID " + str(data_product_id))

        data_producer_obj = self.read_data_producer(producer_ids[0])
        if data_producer_obj is None:
            raise NotFound("Data producer %d does not exist" % producer_ids[0])

        # Remove the link between the child Data Producer resource and the primary Data Producer resource
        associations = self.clients.resource_registry.find_associations(producer_ids[0], PRED.hasParent, RT.DataProducer, id_only=True)
        for association in associations:
            log.debug("unassign_data_product: link to primary DataProducer %s" % association)
            self.clients.resource_registry.delete_association(association)

        # Remove the link between the input resource (instrument/process/ext_data_set) resource and the child Data Producer resource
        associations = self.clients.resource_registry.find_associations(input_resource_id, PRED.hasDataProducer, producer_ids[0], id_only=True)
        for association in associations:
            log.debug("unassign_data_product: link from input resource to child DataProducer %s" % association)
            self.clients.resource_registry.delete_association(association)

        self.clients.resource_registry.delete(producer_ids[0])

        return


        #Delete  the stream if requested
        log.debug("assign_data_product: delete_stream %s" % delete_stream)
        if delete_stream:
            #find the data producer resource associated with the source resource that is creating the data product
            stream_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStream, RT.Stream, id_only=True)
            if stream_ids is None or len(stream_ids) > 1:
                raise NotFound("Invalid Streams associated with data product ID " + str(data_product_id))
            # List all association ids with given subject, predicate, object triples
            associations = self.clients.resource_registry.find_associations(data_product_id, PRED.hasStream, stream_ids[0], id_only=True)
            for association in associations:
                self.clients.resource_registry.delete_association(association)
            self.clients.pubsub_management.delete_stream(stream_ids[0])


        return


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

    ##########################################################################
    #
    # External Data Provider
    #
    ##########################################################################

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

    ##########################################################################
    #
    # Data Source
    #
    ##########################################################################

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

    def create_data_source_model(self, data_source_model=None):
        # Persist DataSourceModel object and return object _id as OOI id
        data_source_model_id, version = self.clients.resource_registry.create(data_source_model)
        return data_source_model_id

    def update_data_source_model(self, data_source_model=None):
        # Overwrite DataSourceModel object
        self.clients.resource_registry.update(data_source_model)

    def read_data_source_model(self, data_source_model_id=''):
        # Read DataSourceModel object with _id matching passed user id
        data_source_model = self.clients.resource_registry.read(data_source_model_id)
        if not data_source_model:
            raise NotFound("DataSourceModel %s does not exist" % data_source_model_id)
        return data_source_model

    def delete_data_source_model(self, data_source_model_id=''):
        # Read and delete specified ExternalDatasetModel object
        data_source_model = self.clients.resource_registry.read(data_source_model_id)
        if not data_source_model:
            raise NotFound("DataSourceModel %s does not exist" % data_source_model_id)
        self.clients.resource_registry.delete(data_source_model_id)


    def create_data_source_agent(self, data_source_agent=None, data_source_model_id='' ):
        # Persist ExternalDataSourcAgent object and return object _id as OOI id
        data_source_agent_id, version = self.clients.resource_registry.create(data_source_agent)

        if data_source_model_id:
            self.clients.resource_registry.create_association(data_source_agent_id,  PRED.hasModel, data_source_model_id)
        return data_source_agent_id

    def update_data_source_agent(self, data_source_agent=None):
        # Overwrite DataSourceAgent object
        self.clients.resource_registry.update(data_source_agent)

    def read_data_source_agent(self, data_source_agent_id=''):
        # Read DataSourceAgent object with _id matching passed user id
        data_source_agent = self.clients.resource_registry.read(data_source_agent_id)
        if not data_source_agent:
            raise NotFound("DataSourceAgent %s does not exist" % data_source_agent_id)
        return data_source_agent

    def delete_data_source_agent(self, data_source_agent_id=''):
        # Read and delete specified DataSourceAgent object
        data_source_agent = self.clients.resource_registry.read(data_source_agent_id)
        if not data_source_agent:
            raise NotFound("DataSourceAgent %s does not exist" % data_source_agent_id)

        # Find and break association with DataSource Model
        assocs = self.clients.resource_registry.find_associations(data_source_agent_id, PRED.hasModel)
        if assocs is None:
            raise NotFound("DataSource Agent to External Data Source Model association for data source agent id %s does not exist" % data_source_agent_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with DataSource Models

        self.clients.resource_registry.delete(data_source_agent_id)

    def create_data_source_agent_instance(self, data_source_agent_instance=None, data_source_agent_id='', data_source_id=''):
        # Persist DataSourceAgentInstance object and return object _id as OOI id
        data_source_agent_instance_id, version = self.clients.resource_registry.create(data_source_agent_instance)

        if data_source_id:
            self.clients.resource_registry.create_association(data_source_id,  PRED.hasAgentInstance, data_source_agent_instance_id)

        if data_source_agent_id:
            self.clients.resource_registry.create_association(data_source_agent_id,  PRED.hasInstance, data_source_agent_instance_id)

        return data_source_agent_instance_id

    def update_data_source_agent_instance(self, data_source_agent_instance=None):
        # Overwrite DataSourceAgentInstance object
        self.clients.resource_registry.update(data_source_agent_instance)

    def read_data_source_agent_instance(self, data_source_agent_instance_id=''):
        # Read DataSourceAgentInstance object with _id matching passed user id
        data_source_agent_instance = self.clients.resource_registry.read(data_source_agent_instance_id)
        if not data_source_agent_instance:
            raise NotFound("DataSourceAgentInstance %s does not exist" % data_source_agent_instance_id)
        return data_source_agent_instance

    def delete_data_source_agent_instance(self, data_source_agent_instance_id=''):
        # Read and delete specified DataSourceAgentInstance object
        data_source_agent_instance = self.clients.resource_registry.read(data_source_agent_instance_id)
        if not data_source_agent_instance:
            raise NotFound("DataSourceAgentInstance %s does not exist" % data_source_agent_instance_id)

        # Find and break association with ExternalDataSource
        assocs = self.clients.resource_registry.find_associations(RT.ExternalDataSource, PRED.hasAgentInstance, data_source_agent_instance_id)
        if assocs is None:
            raise NotFound("ExternalDataSource  to External DataSource Agent Instance association for datasource agent instance id %s does not exist" % data_source_agent_instance_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with ExternalDataSource

        # Find and break association with ExternalDataSourceAgent
        assocs = self.clients.resource_registry.find_associations(RT.ExternalDataSourceAgent, PRED.hasInstance, data_source_agent_instance_id)
        if assocs is None:
            raise NotFound("ExternalDataSourceAgent  to External DataSource Agent Instance association for datasource agent instance id %s does not exist" % data_source_agent_instance_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with ExternalDataSourceAgent

        self.clients.resource_registry.delete(data_source_agent_instance_id)


    ##########################################################################
    #
    # External Data Set
    #
    ##########################################################################
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


    def create_external_dataset_model(self, external_dataset_model=None):
        # Persist ExternalDatasetModel object and return object _id as OOI id
        external_dataset_model_id, version = self.clients.resource_registry.create(external_dataset_model)
        return external_dataset_model_id

    def update_external_dataset_model(self, external_dataset_model=None):
        # Overwrite ExternalDatasetModel object
        self.clients.resource_registry.update(external_dataset_model)

    def read_external_dataset_model(self, external_dataset_model_id=''):
        # Read ExternalDatasetModel object with _id matching passed user id
        external_dataset_model = self.clients.resource_registry.read(external_dataset_model_id)
        if not external_dataset_model:
            raise NotFound("ExternalDatasetModel %s does not exist" % external_dataset_model_id)
        return external_dataset_model

    def delete_external_dataset_model(self, external_dataset_model_id=''):
        # Read and delete specified ExternalDatasetModel object
        external_dataset_model = self.clients.resource_registry.read(external_dataset_model_id)
        if not external_dataset_model:
            raise NotFound("ExternalDatasetModel %s does not exist" % external_dataset_model_id)
        self.clients.resource_registry.delete(external_dataset_model_id)


    def create_external_dataset_agent(self, external_dataset_agent=None, external_dataset_model_id=''):
        # Persist ExternalDatasetAgent object and return object _id as OOI id
        external_dataset_agent_id, version = self.clients.resource_registry.create(external_dataset_agent)
        if external_dataset_model_id:
            self.clients.resource_registry.create_association(external_dataset_agent_id,  PRED.hasModel, external_dataset_model_id)
        return external_dataset_agent_id

    def update_external_dataset_agent(self, external_dataset_agent=None):
        # Overwrite ExternalDataAgent object
        self.clients.resource_registry.update(external_dataset_agent)

    def read_external_dataset_agent(self, external_dataset_agent_id=''):
        # Read ExternalDatasetAgent object with _id matching passed user id
        external_dataset_agent = self.clients.resource_registry.read(external_dataset_agent_id)
        if not external_dataset_agent:
            raise NotFound("ExternalDatasetAgent %s does not exist" % external_dataset_agent_id)
        return external_dataset_agent

    def delete_external_dataset_agent(self, external_dataset_agent_id=''):
        # Read and delete specified ExternalDataAgent object
        external_dataset_agent = self.clients.resource_registry.read(external_dataset_agent_id)
        if not external_dataset_agent:
            raise NotFound("ExternalDataAgent %s does not exist" % external_dataset_agent_id)

        # Find and break association with Dataset Model
        assocs = self.clients.resource_registry.find_associations(external_dataset_agent_id, PRED.hasModel)
        if assocs is None:
            raise NotFound("Dataset Agent to External Dataset Model association for dataset agent id %s does not exist" % external_dataset_agent_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with Dataset Models

        self.clients.resource_registry.delete(external_dataset_agent_id)


    def create_external_dataset_agent_instance(self, external_dataset_agent_instance=None, external_dataset_agent_id='', external_dataset_id=''):
        # Persist ExternalDatasetAgentInstance object and return object _id as OOI id
        external_dataset_agent_instance_id, version = self.clients.resource_registry.create(external_dataset_agent_instance)

        if external_dataset_id:
            self.clients.resource_registry.create_association(external_dataset_id,  PRED.hasAgentInstance, external_dataset_agent_instance_id)
        self.assign_external_data_agent_to_agent_instance(external_dataset_agent_id, external_dataset_agent_instance)
        return external_dataset_agent_instance_id

    def update_external_dataset_agent_instance(self, external_dataset_agent_instance=None):
        # Overwrite ExternalDataAgent object
        self.clients.resource_registry.update(external_dataset_agent_instance)

    def read_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        # Read ExternalDatasetAgent object with _id matching passed user id
        external_dataset_agent_instance = self.clients.resource_registry.read(external_dataset_agent_instance_id)
        if not external_dataset_agent_instance:
            raise NotFound("ExternalDatasetAgent %s does not exist" % external_dataset_agent_instance_id)
        return external_dataset_agent_instance

    def delete_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        # Read and delete specified ExternalDataAgent object
        external_dataset_agent_instance = self.clients.resource_registry.read(external_dataset_agent_instance_id)
        if not external_dataset_agent_instance:
            raise NotFound("ExternalDataAgent %s does not exist" % external_dataset_agent_instance_id)

        # Find and break association with ExternalDataset
        assocs = self.clients.resource_registry.find_associations(RT.ExternalDataset, PRED.hasAgentInstance, external_dataset_agent_instance_id)
        if assocs is None:
            raise NotFound("ExteranlDataset  to External Dataset Agent Instance association for dataset agent instance id %s does not exist" % external_dataset_agent_instance_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with ExternalDataset

        # Find and break association with ExternalDatasetAgent
        assocs = self.clients.resource_registry.find_associations(RT.ExternalDataAgent, PRED.hasInstance, external_dataset_agent_instance_id)
        if assocs is None:
            raise NotFound("ExteranlDatasetAgent  to External Dataset Agent Instance association for dataset agent instance id %s does not exist" % external_dataset_agent_instance_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with ExternalDatasetAgent

        self.clients.resource_registry.delete(external_dataset_agent_instance_id)

    ##########################################################################
    #
    # Resource Assign Functions
    #
    ##########################################################################

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
        associations = self.clients.resource_registry.find_associations(data_source_id, PRED.hasProvider, external_data_provider_id, id_only=True)
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
        associations = self.clients.resource_registry.find_associations(data_source_id,  PRED.hasModel,  data_source_model_id, id_only=True)
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
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasAgentInstance,  agent_instance_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)



    def assign_external_data_agent_to_agent_instance(self, external_data_agent_id='', agent_instance_id=''):
        #Connect the agent with an agent instance
        data_source = self.clients.resource_registry.read(external_data_agent_id)
        if not data_source:
            raise NotFound("ExternalDataSetAgent resource %s does not exist" % external_data_agent_id)

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
        associations = self.clients.resource_registry.find_associations(external_data_agent_id,  PRED.hasInstance,  agent_instance_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def assign_dataset_agent_to_external_dataset_model(self, dataset_agent_id='', external_dataset_model_id=''):
        #Connect the external data agent with an external data model
        external_data_agent = self.clients.resource_registry.read(dataset_agent_id)
        if not dataset_agent:
            raise NotFound("DatasetAgent resource %s does not exist" % dataset_agent_id)

        external_dataset_model = self.clients.resource_registry.read(external_dataset_model_id)
        if not external_dataset_model:
            raise NotFound("External Data Source Model resource %s does not exist" % external_dataset_model_id)

        self.clients.resource_registry.create_association(dataset_agent_id,  PRED.hasModel,  external_dataset_model_id)

    def unassign_dataset_agent_from_external_dataset_model(self, dataset_agent_id='', external_dataset_model_id=''):
        #Disonnect the external data agent from the external data model
        dataset_agent = self.clients.resource_registry.read(dataset_agent_id)
        if not dataset_agent:
            raise NotFound("ExternalDataAgent resource %s does not exist" % dataset_agent_id)

        external_dataset_model = self.clients.resource_registry.read(external_dataset_model_id)
        if not external_dataset_model:
            raise NotFound("External Data Source Model resource %s does not exist" % external_dataset_model_id)
        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(dataset_agent_id,  PRED.hasModel,  external_dataset_model_id, id_only=True)
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
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasSource,  data_source_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)






