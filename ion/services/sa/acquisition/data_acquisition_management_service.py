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
from pyon.public import CFG, IonObject, log, RT, LCS, PRED, OT

from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget


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

        #create a ExtDatasetProducerContext to hold the state of the this producer
        producer_context_obj = IonObject(OT.ExtDatasetProducerContext)

        #create data producer resource and associate to this external_dataset_id
        data_producer_obj = IonObject(RT.DataProducer,name=data_set_obj.name,
            description="Primary DataProducer for ExternalDataSet %s" % data_set_obj.name,
            producer_context=producer_context_obj, is_primary=True)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        # Create association
        self.clients.resource_registry.create_association(external_dataset_id, PRED.hasDataProducer, data_producer_id)

        return data_producer_id

    def unregister_external_data_set(self, external_dataset_id=''):
        """

        @param external_dataset_id    str
        @throws NotFound    object with specified id does not exist
        """
        # Verify that  id is valid
        external_data_set_obj = self.clients.resource_registry.read(external_dataset_id)

        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        producers, producer_assns = self.clients.resource_registry.find_objects(
            subject=external_dataset_id, predicate=PRED.hasDataProducer, id_only=True)
        for producer, producer_assn in zip(producers, producer_assns):
            log.debug("DataAcquisitionManagementService:unregister_external_data_set  delete association %s", str(producer_assn))
            self.clients.resource_registry.delete_association(producer_assn)
            log.debug("DataAcquisitionManagementService:unregister_external_data_set  delete producer %s", str(producer))
            self.clients.resource_registry.delete(producer)

        return


    def register_process(self, data_process_id=''):
        """
        Register an existing data process as data producer
        """

        # retrieve the data_process object
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        if data_process_obj is None:
            raise NotFound("Data Process %s does not exist" % data_process_id)

        #find the data process definition
        data_process_def_objs, _ = self.clients.resource_registry.find_objects(
            subject=data_process_id,  predicate=PRED.hasProcessDefinition, object_type=RT.DataProcessDefinition, id_only=False)
        if not data_process_def_objs:
            raise NotFound("Data Process Definition for Data Process %s does not exist" % data_process_id)

        #create a DataProcessProducerContext to hold the state of the this producer
        producer_context_obj = IonObject(OT.DataProcessProducerContext,  configuration=data_process_obj.configuration, parameters=data_process_def_objs[0].parameters)

        #create data producer resource and associate to this data_process_id
        data_producer_obj = IonObject(RT.DataProducer,name=data_process_obj.name,
            description="Primary DataProducer for DataProcess %s" % data_process_obj.name,
            producer_context=producer_context_obj, is_primary=True)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        # Create association
        self.clients.resource_registry.create_association(data_process_id, PRED.hasDataProducer, data_producer_id)

        return data_producer_id

    def register_event_process(self, process_id=''):
        """
        Register an existing data process as data producer
        """

        # retrieve the data_process object
        process_obj = self.clients.resource_registry.read(process_id)
        if process_obj is None:
            raise NotFound("Process %s does not exist" % process_id)

        #find the data process definition
        process_def_objs, _ = self.clients.resource_registry.find_objects(subject=process_id,  predicate=PRED.hasProcessDefinition, object_type=RT.ProcessDefinition, id_only=False)
        if not process_def_objs:
            raise NotFound("Process Definition for Process %s does not exist" % process_id)

        #create a DataProcessProducerContext to hold the state of the this producer
        producer_context_obj = IonObject(OT.DataProcessProducerContext,  configuration=process_obj.process_configuration)

        #create data producer resource and associate to this process_id
        data_producer_obj = IonObject(RT.DataProducer,name=process_obj.name, description="primary producer resource for this process",
            producer_context=producer_context_obj, is_primary=True)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        # Create association
        self.clients.resource_registry.create_association(process_id, PRED.hasDataProducer, data_producer_id)

        return data_producer_id

    def unregister_process(self, data_process_id=''):
        """
        Remove the associated DataProcess and disc

        """
        # Verify that  id is valid
        input_process_obj = self.clients.resource_registry.read(data_process_id)

        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        producers, producer_assns = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasDataProducer, id_only=True)
        for producer, producer_assn in zip(producers, producer_assns):
            log.debug("DataAcquisitionManagementService:unregister_process  delete association %s", str(producer_assn))
            self.clients.resource_registry.delete_association(producer_assn)
            log.debug("DataAcquisitionManagementService:unregister_process  delete producer %s", str(producer))

            log.debug("DAMS:unregister_process delete producer: %s ", str(producer) )
            self.clients.resource_registry.delete(producer)

        return

    def unregister_event_process(self, process_id=''):
        """
        Remove the associated Process and disc

        """
        # Verify that  id is valid
        input_process_obj = self.clients.resource_registry.read(process_id)

        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        producers, producer_assns = self.clients.resource_registry.find_objects(subject=process_id, predicate=PRED.hasDataProducer, id_only=True)
        for producer, producer_assn in zip(producers, producer_assns):
            log.debug("DataAcquisitionManagementService:unregister_process  delete association %s", str(producer_assn))
            self.clients.resource_registry.delete_association(producer_assn)
            log.debug("DataAcquisitionManagementService:unregister_process  delete producer %s", str(producer))

            log.debug("DAMS:unregister_process delete producer: %s ", str(producer) )
            self.clients.resource_registry.delete(producer)

    def register_instrument(self, instrument_id=''):
        """
        Register an existing instrument as data producer
        """
        # retrieve the data_process object
        instrument_obj = self.clients.resource_registry.read(instrument_id)

        #create a InstrumentProducerContext to hold the state of the this producer
        producer_context_obj = IonObject(OT.InstrumentProducerContext)

        #create data producer resource and associate to this instrument_id
        data_producer_obj = IonObject(RT.DataProducer, name=instrument_obj.name,
            description="Primary DataProducer for InstrumentDevice %s" % instrument_obj.name,
            producer_context=producer_context_obj, is_primary=True)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)
        log.debug("register_instrument  data_producer_id %s" % data_producer_id)

        # Create association
        self.clients.resource_registry.create_association(instrument_id, PRED.hasDataProducer, data_producer_id)

        return data_producer_id

    def unregister_instrument(self, instrument_id=''):

        # Verify that  id is valid
        input_resource_obj = self.clients.resource_registry.read(instrument_id)

        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        producers, producer_assns = self.clients.resource_registry.find_objects(subject=instrument_id, predicate=PRED.hasDataProducer, id_only=True)
        for producer, producer_assn in zip(producers, producer_assns):
            log.debug("DataAcquisitionManagementService:unregister_instrument  delete association %s", str(producer_assn))
            self.clients.resource_registry.delete_association(producer_assn)
            log.debug("DataAcquisitionManagementService:unregister_instrument  delete producer %s", str(producer))
            self.clients.resource_registry.delete(producer)
        return


    def assign_data_product(self, input_resource_id='', data_product_id=''):
        #Connect the producer for an existing input resource with a data product

        # Verify that both ids are valid
        input_resource_obj = self.clients.resource_registry.read(input_resource_id)
        data_product_obj = self.clients.resource_registry.read(data_product_id)

        #find the data producer resource associated with the source resource that is creating the data product
        primary_producer_ids, _ = self.clients.resource_registry.find_objects(input_resource_id, PRED.hasDataProducer, RT.DataProducer, id_only=True)

        if not primary_producer_ids:
            raise NotFound("Data Producer for input resource %s does not exist" % input_resource_id)

        data_producer_id = ''

        #connect the producer to the product directly
        self.clients.resource_registry.create_association(input_resource_id,  PRED.hasOutputProduct,  data_product_id)

        #create data producer resource for this data product
        data_producer_obj = IonObject(RT.DataProducer,name=data_product_obj.name, description=data_product_obj.description)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)
        log.debug("DAMS:assign_data_product: data_producer_id %s" % str(data_producer_id))

        # Associate the Product with the Producer
        self.clients.resource_registry.create_association(data_product_id,  PRED.hasDataProducer,  data_producer_id)

        # Associate the Producer with the main Producer
        self.clients.resource_registry.create_association(data_producer_id,  PRED.hasParent,  primary_producer_ids[0])
        # Associate the input resource with the child data Producer
        self.clients.resource_registry.create_association(input_resource_id,  PRED.hasDataProducer, data_producer_id)

        return

    def unassign_data_product(self, input_resource_id='', data_product_id=''):
        """
        Disconnect the Data Product from the Data Producer

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """
        # Verify that both ids are valid
        input_resource_obj = self.clients.resource_registry.read(input_resource_id)
        data_product_obj = self.clients.resource_registry.read(data_product_id)

        #find the data producer resource associated with the source resource that is creating the data product
        primary_producer_ids, _ = self.clients.resource_registry.find_objects(input_resource_id, PRED.hasDataProducer, RT.DataProducer, id_only=True)
        if not primary_producer_ids:
            raise NotFound("Data Producer for input resource %s does not exist" % input_resource_id)
        else:
            log.debug("unassign_data_product: primary producer ids %s" % str(primary_producer_ids))


        #find the hasDataProduct association between the data product and the input resource
        associations = self.clients.resource_registry.find_associations(subject=input_resource_id, predicate=PRED.hasOutputProduct, object=data_product_id, id_only=True)
        for association in associations:
            log.debug("unassign_data_product: unlink input resource with data product %s" % association)
            self.clients.resource_registry.delete_association(association)

        #find the data producer resource associated with the source resource that is creating the data product
        producers, producer_assns = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataProducer, RT.DataProducer, True)
        for producer, producer_assn in zip(producers, producer_assns):
            #remove the link to the data product
            self.clients.resource_registry.delete_association(producer_assn)

            #remove the link to the parent data producer
            associations = self.clients.resource_registry.find_associations(subject=producer, predicate=PRED.hasParent, id_only=True)
            for association in associations:
                self.clients.resource_registry.delete_association(association)

            #remove the link to the input resource
            associations = self.clients.resource_registry.find_associations(input_resource_id, PRED.hasDataProducer, producer, id_only=True)
            for association in associations:
                self.clients.resource_registry.delete_association(association)

            log.debug("DAMS:unassign_data_product delete producer: %s ", str(producer) )
            self.clients.resource_registry.delete(producer)

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

        return self.clients.resource_registry.retire(data_producer_id)


    def force_delete_data_producer(self, data_producer_id=''):
        self._remove_associations(data_producer_id)
        self.clients.resource_registry.delete(data_producer_id)

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
        return

    def read_external_data_provider(self, external_data_provider_id=''):
        # Read ExternalDataProvider object with _id matching passed user id
        external_data_provider = self.clients.resource_registry.read(external_data_provider_id)
        return external_data_provider

    def delete_external_data_provider(self, external_data_provider_id=''):
        self.clients.resource_registry.retire(external_data_provider_id)
        return

    def force_delete_external_data_provider(self, external_data_provider_id=''):
        self._remove_associations(external_data_provider_id)
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
        return data_source_obj

    def delete_data_source(self, data_source_id=''):
        # Read and delete specified DataSource object
        log.debug("Deleting DataSource id: %s" % data_source_id)
        self.clients.resource_registry.retire(data_source_id)
        return

    def force_delete_data_source(self, data_source_id=''):
        self._remove_associations(data_source_id)
        self.clients.resource_registry.delete(data_source_id)

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
        return data_source_model

    def delete_data_source_model(self, data_source_model_id=''):
        # Read and delete specified ExternalDatasetModel object
        self.clients.resource_registry.retire(data_source_model_id)
        return

    def force_delete_data_source_model(self, data_source_model_id=''):
        self._remove_associations(data_source_model_id)
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
        return data_source_agent

    def delete_data_source_agent(self, data_source_agent_id=''):
        # Read and delete specified DataSourceAgent object
        data_source_agent = self.clients.resource_registry.read(data_source_agent_id)


        # Find and break association with DataSource Model
        assocs = self.clients.resource_registry.find_associations(data_source_agent_id, PRED.hasModel)
        if assocs is None:
            raise NotFound("DataSource Agent to External Data Source Model association for data source agent id %s does not exist" % data_source_agent_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with DataSource Models

        self.clients.resource_registry.retire(data_source_agent_id)

    def force_delete_data_source_agent(self, data_source_agent_id=''):
        self._remove_associations(data_source_agent_id)
        self.clients.resource_registry.delete(data_source_agent_id)


    def create_data_source_agent_instance(self, data_source_agent_instance=None, data_source_agent_id='', data_source_id=''):
        # Persist DataSourceAgentInstance object and return object _id as OOI id
        data_source_agent_instance_id, version = self.clients.resource_registry.create(data_source_agent_instance)

        if data_source_id:
            self.clients.resource_registry.create_association(data_source_id,  PRED.hasAgentInstance, data_source_agent_instance_id)

        if data_source_agent_id:
            self.clients.resource_registry.create_association(data_source_agent_instance_id,  PRED.hasAgentDef, data_source_agent_id)

        return data_source_agent_instance_id

    def update_data_source_agent_instance(self, data_source_agent_instance=None):
        # Overwrite DataSourceAgentInstance object
        self.clients.resource_registry.update(data_source_agent_instance)

    def read_data_source_agent_instance(self, data_source_agent_instance_id=''):
        # Read DataSourceAgentInstance object with _id matching passed user id
        data_source_agent_instance = self.clients.resource_registry.read(data_source_agent_instance_id)
        return data_source_agent_instance

    def delete_data_source_agent_instance(self, data_source_agent_instance_id=''):
        # Read and delete specified DataSourceAgentInstance object
        data_source_agent_instance = self.clients.resource_registry.read(data_source_agent_instance_id)


        # Find and break association with ExternalDataSource
        assocs = self.clients.resource_registry.find_associations(RT.ExternalDataSource, PRED.hasAgentInstance, data_source_agent_instance_id)
        if assocs is None:
            raise NotFound("ExternalDataSource  to External DataSource Agent Instance association for datasource agent instance id %s does not exist" % data_source_agent_instance_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with ExternalDataSource

        # Find and break association with ExternalDataSourceAgent
        assocs = self.clients.resource_registry.find_associations(data_source_agent_instance_id, PRED.hasAgentDefinition,  RT.ExternalDataSourceAgent,)
        if assocs is None:
            raise NotFound("ExternalDataSourceAgent  to External DataSource Agent Instance association for datasource agent instance id %s does not exist" % data_source_agent_instance_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with ExternalDataSourceAgent

        self.clients.resource_registry.retire(data_source_agent_instance_id)

    def force_delete_data_source_agent_instance(self, data_source_agent_instance_id=''):
        self._remove_associations(data_source_agent_instance_id)
        self.clients.resource_registry.delete(data_source_agent_instance_id)

    def start_data_source_agent_instance(self, data_source_agent_instance_id=''):
        """Launch an data source agent instance process and return its process id. Agent instance resource
        must exist and be associated with an external data source

        @param data_source_agent_instance_id    str
        @retval process_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def stop_data_source_agent_instance(self, data_source_agent_instance_id=''):
        """Deactivate the  agent instance process

        @param data_source_agent_instance_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass


    ##########################################################################
    #
    # External Data Set
    #
    ##########################################################################
    def create_external_dataset(self, external_dataset=None, external_dataset_model_id=''):
        # Persist ExternalDataSet object and return object _id as OOI id
        external_dataset_id, version = self.clients.resource_registry.create(external_dataset)
        if external_dataset_model_id:
            self.clients.resource_registry.create_association(external_dataset_id,  PRED.hasModel, external_dataset_model_id)
        return external_dataset_id

    def update_external_dataset(self, external_dataset=None):
        # Overwrite ExternalDataSet object
        self.clients.resource_registry.update(external_dataset)

    def read_external_dataset(self, external_dataset_id=''):
        # Read ExternalDataSet object with _id matching passed user id
        external_dataset = self.clients.resource_registry.read(external_dataset_id)

        return external_dataset

    def delete_external_dataset(self, external_dataset_id=''):
        # Read and delete specified ExternalDataSet object
        external_dataset = self.clients.resource_registry.read(external_dataset_id)

        self.clients.resource_registry.retire(external_dataset_id)

    def force_delete_external_dataset(self, external_dataset_id=''):
        self._remove_associations(external_dataset_id)
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

        return external_dataset_model

    def delete_external_dataset_model(self, external_dataset_model_id=''):
        # Read and delete specified ExternalDatasetModel object
        external_dataset_model = self.clients.resource_registry.read(external_dataset_model_id)

        self.clients.resource_registry.retire(external_dataset_model_id)

    def force_delete_external_dataset_model(self, external_dataset_model_id=''):
        self._remove_associations(external_dataset_model_id)
        self.clients.resource_registry.delete(external_dataset_model_id)

    def create_external_dataset_agent(self, external_dataset_agent=None, external_dataset_model_id=''):
        # Persist ExternalDatasetAgent object and return object _id as OOI id
        external_dataset_agent_id, version = self.clients.resource_registry.create(external_dataset_agent)
        if external_dataset_model_id:
            self.clients.resource_registry.create_association(external_dataset_agent_id,  PRED.hasModel, external_dataset_model_id)

        # Create the process definition to launch the agent
        process_definition = ProcessDefinition()
        process_definition.executable['module']= external_dataset_agent.handler_module
        process_definition.executable['class'] = external_dataset_agent.handler_class
#        process_definition.executable['module']='ion.agents.instrument.instrument_agent'
#        process_definition.executable['class'] = 'InstrumentAgent'
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)
        log.debug("create_external_dataset_agent: create_process_definition id %s"  +  str(process_definition_id))

        #associate the agent and the process def
        self.clients.resource_registry.create_association(external_dataset_agent_id,  PRED.hasProcessDefinition, process_definition_id)

        return external_dataset_agent_id

    def update_external_dataset_agent(self, external_dataset_agent=None):
        # Overwrite ExternalDataAgent object
        self.clients.resource_registry.update(external_dataset_agent)

    def read_external_dataset_agent(self, external_dataset_agent_id=''):
        # Read ExternalDatasetAgent object with _id matching passed user id
        external_dataset_agent = self.clients.resource_registry.read(external_dataset_agent_id)

        return external_dataset_agent

    def delete_external_dataset_agent(self, external_dataset_agent_id=''):
        # Read and delete specified ExternalDataAgent object
        external_dataset_agent = self.clients.resource_registry.read(external_dataset_agent_id)

        # Find and break association with Dataset Model
        assocs = self.clients.resource_registry.find_associations(external_dataset_agent_id, PRED.hasModel)
        if assocs is None:
            raise NotFound("Dataset Agent to External Dataset Model association for dataset agent id %s does not exist" % external_dataset_agent_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with Dataset Models

        self.clients.resource_registry.retire(external_dataset_agent_id)

    def force_delete_external_dataset_agent(self, external_dataset_agent_id=''):

    # if not yet deleted, the first execute delete logic
        dp_obj = self.read_external_dataset_agent(external_dataset_agent_id)
        if dp_obj.lcstate != LCS.RETIRED:
            self.delete_external_dataset_agent(external_dataset_agent_id)
        self._remove_associations(external_dataset_agent_id)
        self.clients.resource_registry.delete(external_dataset_agent_id)

    def create_external_dataset_agent_instance(self, external_dataset_agent_instance=None, external_dataset_agent_id='', external_dataset_id=''):
        # Persist ExternalDatasetAgentInstance object and return object _id as OOI id
        external_dataset_agent_instance_id, version = self.clients.resource_registry.create(external_dataset_agent_instance)

        if external_dataset_id:
            self.clients.resource_registry.create_association(external_dataset_id,  PRED.hasAgentInstance, external_dataset_agent_instance_id)
        self.assign_external_data_agent_to_agent_instance(external_dataset_agent_id, external_dataset_agent_instance_id)
        return external_dataset_agent_instance_id

    def update_external_dataset_agent_instance(self, external_dataset_agent_instance=None):
        # Overwrite ExternalDataAgent object
        self.clients.resource_registry.update(external_dataset_agent_instance)

    def read_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        # Read ExternalDatasetAgent object with _id matching passed user id
        external_dataset_agent_instance = self.clients.resource_registry.read(external_dataset_agent_instance_id)

        return external_dataset_agent_instance

    def delete_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        # Read and delete specified ExternalDataAgent object
        external_dataset_agent_instance = self.clients.resource_registry.read(external_dataset_agent_instance_id)

        # Find and break association with ExternalDataset
        assocs = self.clients.resource_registry.find_associations(RT.ExternalDataset, PRED.hasAgentInstance, external_dataset_agent_instance_id)
        if assocs is None:
            raise NotFound("ExteranlDataset  to External Dataset Agent Instance association for dataset agent instance id %s does not exist" % external_dataset_agent_instance_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with ExternalDataset

        # Find and break association with ExternalDatasetAgent
        assocs = self.clients.resource_registry.find_associations(external_dataset_agent_instance_id, PRED.hasAgentDefinition,  RT.ExternalDataAgent)
        if assocs is None:
            raise NotFound("ExteranlDatasetAgent  to External Dataset Agent Instance association for dataset agent instance id %s does not exist" % external_dataset_agent_instance_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with ExternalDatasetAgent

        self.clients.resource_registry.retire(external_dataset_agent_instance_id)

    def force_delete_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
    # if not yet deleted, the first execute delete logic
        dp_obj = self.read_external_dataset_agent_instance(external_dataset_agent_instance_id)
        if dp_obj.lcstate != LCS.RETIRED:
            self.delete_external_dataset_agent_instance(external_dataset_agent_instance_id)

        self._remove_associations(external_dataset_agent_instance_id)
        self.clients.resource_registry.delete(external_dataset_agent_instance_id)

    def start_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        """Launch an dataset agent instance process and return its process id. Agent instance resource
        must exist and be associated with an external dataset

        @param external_dataset_agent_instance_id    str
        @retval process_id    str
        @throws NotFound    object with specified id does not exist
        """
        #todo: may want to call retrieve_external_dataset_agent_instance here
        #todo:  if instance running, then return or throw
        #todo: if instance exists and dataset_agent_instance_obj.dataset_agent_config is completd then just schedule_process


        dataset_agent_instance_obj = self.clients.resource_registry.read(external_dataset_agent_instance_id)

        #retrieve the associated external dataset device
        ext_dataset_ids, _ = self.clients.resource_registry.find_subjects(RT.ExternalDataset, PRED.hasAgentInstance, external_dataset_agent_instance_id, True)
        if not ext_dataset_ids:
            raise NotFound("No External Dataset attached to this Dataset Agent Instance " + str(external_dataset_agent_instance_id))
        if len(ext_dataset_ids) > 1:
            raise BadRequest("Dataset Agent Instance should only have ONE External Dataset" + str(external_dataset_agent_instance_id))
        ext_dataset_id = ext_dataset_ids[0]
        log.debug("start_external_dataset_agent_instance: external dataset is %s connected to dataset agent instance %s ", str(ext_dataset_id),  str(external_dataset_agent_instance_id))


        #retrieve the external dataset model
        model_ids, _ = self.clients.resource_registry.find_objects(ext_dataset_id, PRED.hasModel, RT.ExternalDatasetModel, True)
        if not model_ids:
            raise NotFound("No External Dataset Model  attached to this External Dataset " + str(ext_dataset_id))

        ext_dataset_model_id = model_ids[0]
        log.debug("start_external_dataset_agent_instance:External Dataset Model %s"  +  str(ext_dataset_model_id))


        #retrieve the associated instrument agent
        agent_ids, _ = self.clients.resource_registry.find_subjects(RT.ExternalDatasetAgent, PRED.hasModel, ext_dataset_model_id, True)
        if not agent_ids:
            raise NotFound("No External Dataset Agent  attached to this External Dataset Model " + str(ext_dataset_model_id))

        ext_dataset_agent_id = agent_ids[0]
        log.debug("start_external_dataset_agent_instance: external dataset agent '%s'" % ext_dataset_agent_id)

        #retrieve the associated process definition
        process_def_ids, _ = self.clients.resource_registry.find_objects(ext_dataset_agent_id, PRED.hasProcessDefinition, RT.ProcessDefinition, True)
        if not process_def_ids:
            raise NotFound("No Process Definition  attached to this ExtDataset Agent " + str(ext_dataset_agent_id))
        if len(process_def_ids) > 1:
            raise BadRequest("ExtDataset Agent should only have ONE Process Definition" + str(ext_dataset_agent_id))

        process_definition_id = process_def_ids[0]
        log.debug("activate_instrument: agent process definition %s"  +  str(process_definition_id))

        # retrieve the process definition information
        process_def_obj = self.clients.resource_registry.read(process_definition_id)

        out_streams = {}
        #retrieve the output products
        data_product_ids, _ = self.clients.resource_registry.find_objects(ext_dataset_id, PRED.hasOutputProduct, RT.DataProduct, True)
        if not data_product_ids:
            raise NotFound("No output Data Products attached to this External Dataset " + str(ext_dataset_id))

        for product_id in data_product_ids:
            stream_ids, _ = self.clients.resource_registry.find_objects(product_id, PRED.hasStream, RT.Stream, True)

            log.debug("start_external_dataset_agent_instance:output stream ids: %s"  +  str(stream_ids))
            #One stream per product ...for now.
            if not stream_ids:
                raise NotFound("No Stream  attached to this Data Product " + str(product_id))
            if len(stream_ids) > 1:
                raise BadRequest("Data Product should only have ONE Stream" + str(product_id))

            # retrieve the stream
            stream_obj = self.clients.resource_registry.read(stream_ids[0])

            out_streams['parsed'] = stream_ids[0]


        # Create agent config.
        dataset_agent_instance_obj.dataset_agent_config = {
            'driver_config' : dataset_agent_instance_obj.dataset_driver_config,
            'stream_config' : out_streams,
            'agent'         : {'resource_id': ext_dataset_id},
            'test_mode' : True
        }

        log.debug("start_external_dataset_agent_instance: agent_config %s ", str(dataset_agent_instance_obj.dataset_agent_config))

        pid = self.clients.process_dispatcher.schedule_process(process_definition_id=process_definition_id,
                                                               schedule=None,
                                                               configuration=dataset_agent_instance_obj.dataset_agent_config)
        log.debug("start_external_dataset_agent_instance: schedule_process %s", pid)


        # add the process id and update the resource
        dataset_agent_instance_obj.agent_process_id = pid
        self.update_external_dataset_agent_instance(dataset_agent_instance_obj)

        return pid

    def stop_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        """
        Deactivate the agent instance process
        """
        external_dataset_agent_instance_obj = self.clients.resource_registry.read(external_dataset_agent_instance_id)

        # Cancels the execution of the given process id.
        self.clients.process_dispatcher.cancel_process(external_dataset_agent_instance_obj.agent_process_id)

        external_dataset_agent_instance_obj.agent_process_id = ''

        self.clients.resource_registry.update(external_dataset_agent_instance_obj)



    def retrieve_external_dataset_agent_instance(self, external_dataset_id=''):
        """
        Retrieve the agent instance for an external dataset and check if it is running
        """
        #Connect the data source with an external data provider
        data_set = self.clients.resource_registry.read(external_dataset_id)

        # check if the association already exists
        ai_ids, _  = self.clients.resource_registry.find_objects(external_dataset_id,  PRED.hasAgentInstance, id_only=True)
        if len(ai_ids) > 1:
            raise NotFound("ExternalDataset resource %s is associated with multiple agent instances" % external_dataset_id)

        if ai_ids is None:
            return None, None
        else:
            dataset_agent_instance_obj = self.clients.resource_registry.read(ai_ids[0])

            if not dataset_agent_instance_obj.agent_process_id:
                active = False
            else:
                active = True
            return ai_ids[0], active


    ##########################################################################
    #
    # Resource Assign Functions
    #
    ##########################################################################

    def assign_data_source_to_external_data_provider(self, data_source_id='', external_data_provider_id=''):
        #Connect the data source with an external data provider
        data_source = self.clients.resource_registry.read(data_source_id)
        agent_instance = self.clients.resource_registry.read(external_data_provider_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(data_source_id,  PRED.hasProvider,  external_data_provider_id, id_only=True)
        if associations is None:
            self.clients.resource_registry.create_association(data_source_id,  PRED.hasProvider,  external_data_provider_id)

    def unassign_data_source_from_external_data_provider(self, data_source_id='', external_data_provider_id=''):
        #Disconnect the data source from the external data provider
        data_source = self.clients.resource_registry.read(data_source_id)
        agent_instance = self.clients.resource_registry.read(external_data_provider_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(data_source_id, PRED.hasProvider, external_data_provider_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def assign_data_source_to_data_model(self, data_source_id='', data_source_model_id=''):
        #Connect the data source with an external data model
        data_source = self.clients.resource_registry.read(data_source_id)
        agent_instance = self.clients.resource_registry.read(data_source_model_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(data_source_id,  PRED.hasModel,  data_source_model_id, id_only=True)
        if associations is None:
            self.clients.resource_registry.create_association(data_source_id,  PRED.hasModel,  data_source_model_id)

    def unassign_data_source_from_data_model(self, data_source_id='', data_source_model_id=''):
        #Disonnect the data source from the external data model
        data_source = self.clients.resource_registry.read(data_source_id)
        agent_instance = self.clients.resource_registry.read(data_source_model_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(data_source_id,  PRED.hasModel,  data_source_model_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)



    def assign_external_dataset_to_agent_instance(self, external_dataset_id='', agent_instance_id=''):
        #Connect the agent instance with an external data set
        data_source = self.clients.resource_registry.read(external_dataset_id)
        agent_instance = self.clients.resource_registry.read(agent_instance_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasAgentInstance,  agent_instance_id, id_only=True)
        if associations is None:
            self.clients.resource_registry.create_association(external_dataset_id,  PRED.hasAgentInstance,  agent_instance_id)

    def unassign_external_dataset_from_agent_instance(self, external_dataset_id='', agent_instance_id=''):
        data_source = self.clients.resource_registry.read(external_dataset_id)
        agent_instance = self.clients.resource_registry.read(agent_instance_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasAgentInstance,  agent_instance_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)



    def assign_external_data_agent_to_agent_instance(self, external_data_agent_id='', agent_instance_id=''):
        #Connect the agent with an agent instance
        data_source = self.clients.resource_registry.read(external_data_agent_id)
        agent_instance = self.clients.resource_registry.read(agent_instance_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(agent_instance_id,  PRED.hasAgentDefinition,   external_data_agent_id, id_only=True)
        if associations is None:
            self.clients.resource_registry.create_association(agent_instance_id,  PRED.hasAgentDefinition,   external_data_agent_id)

    def unassign_external_data_agent_from_agent_instance(self, external_data_agent_id='', agent_instance_id=''):
        data_source = self.clients.resource_registry.read(external_data_agent_id)
        agent_instance = self.clients.resource_registry.read(agent_instance_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(agent_instance_id,  PRED.hasAgentDefinition,  external_data_agent_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def assign_dataset_agent_to_external_dataset_model(self, dataset_agent_id='', external_dataset_model_id=''):
        #Connect the external data agent with an external data model
        external_data_agent = self.clients.resource_registry.read(dataset_agent_id)
        external_dataset_model = self.clients.resource_registry.read(external_dataset_model_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(dataset_agent_id,  PRED.hasModel,  external_dataset_model_id, id_only=True)
        if associations is None:
            self.clients.resource_registry.create_association(dataset_agent_id,  PRED.hasModel,  external_dataset_model_id)

    def unassign_dataset_agent_from_external_dataset_model(self, dataset_agent_id='', external_dataset_model_id=''):
        #Disonnect the external data agent from the external data model
        dataset_agent = self.clients.resource_registry.read(dataset_agent_id)
        external_dataset_model = self.clients.resource_registry.read(external_dataset_model_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(dataset_agent_id,  PRED.hasModel,  external_dataset_model_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def assign_external_dataset_to_data_source(self, external_dataset_id='', data_source_id=''):
        #Connect the external data set to a data source
        data_source = self.clients.resource_registry.read(external_dataset_id)
        agent_instance = self.clients.resource_registry.read(data_source_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasSource,  data_source_id, id_only=True)
        if associations is None:
            self.clients.resource_registry.create_association(external_dataset_id,  PRED.hasSource,  data_source_id)


    def unassign_external_dataset_from_data_source(self, external_dataset_id='', data_source_id=''):
        #Disonnect the external data set from the data source
        data_source = self.clients.resource_registry.read(external_dataset_id)
        agent_instance = self.clients.resource_registry.read(data_source_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasSource,  data_source_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)




    def _remove_associations(self, resource_id=''):
        """
        delete all associations to/from a resource
        """

        # find all associations where this is the subject
        _, obj_assns = self.clients.resource_registry.find_objects(subject=resource_id, id_only=True)

        # find all associations where this is the object
        _, sbj_assns = self.clients.resource_registry.find_subjects(object=resource_id, id_only=True)

        log.debug("_remove_associations will remove %s subject associations and %s object associations",
                 len(sbj_assns), len(obj_assns))

        for assn in obj_assns:
            log.debug("_remove_associations deleting object association %s", assn)
            self.clients.resource_registry.delete_association(assn)

        for assn in sbj_assns:
            log.debug("_remove_associations deleting subject association %s", assn)
            self.clients.resource_registry.delete_association(assn)

        # find all associations where this is the subject
        _, obj_assns = self.clients.resource_registry.find_objects(subject=resource_id, id_only=True)

        # find all associations where this is the object
        _, sbj_assns = self.clients.resource_registry.find_subjects(object=resource_id, id_only=True)

        log.debug("post-deletions, _remove_associations found %s subject associations and %s object associations",
                 len(sbj_assns), len(obj_assns))

