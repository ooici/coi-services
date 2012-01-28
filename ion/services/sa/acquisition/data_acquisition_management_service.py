#!/usr/bin/env python

'''
@package ion.services.sa.acquisition.data_acquisition_management_service Implementation of IDataAcquisitionManagementService interface
@file ion/services/sa/acquisition/data_acquisition_management_management_service.py
@author M Manning
@brief Data Acquisition Management service to keep track of Data Producers, Data Sources
and the relationships between them
'''

from interface.services.sa.idata_acquisition_management_service import BaseDataAcquisitionManagementService
from pyon.core.exception import NotFound
from pyon.public import CFG, IonObject, log, RT, AT, LCS



class DataAcquisitionManagementService(BaseDataAcquisitionManagementService):


    # -----------------
    # The following operations register different types of data producers
    # -----------------

    def _remove_producer(self, resource_id='', producers=None):
        log.debug("Removing DataProducer objects and links")
        for producer in producers:
            # List all association ids with given subject, predicate, object triples
            assoc_ids = self.clients.resource_registry.find_associations(resource_id, AT.hasDataProducer, producer, True)
            self.clients.resource_registry.delete_association(assoc_ids[0])

            # DELETE THE STREAM associated with the data producer via call to PubSub
            res_ids, _ = self.clients.resource_registry.find_objects(producer, AT.hasStream, None, True)
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
        data_producer_id = self.create_data_producer(name=data_set_obj.name, description=data_set_obj.description)

        # Create association
        self.clients.resource_registry.create_association(external_dataset_id, AT.hasDataProducer, data_producer_id)

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
        res_ids, _ = self.clients.resource_registry.find_objects(external_dataset_id, AT.hasDataProducer, None, True)
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
        data_producer_id = self.create_data_producer(name=data_process_obj.name, description=data_process_obj.description)

        # Create association
        self.clients.resource_registry.create_association(data_process_id, AT.hasDataProducer, data_producer_id)

        # TODO: Walk up the assocations to find parent producers:
        # proc->subscription->stream->prod

        return data_producer_id

    def unregister_process(self, data_process_id=''):
        """
        Remove the associated DataProcess

        """
        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        res_ids, _ = self.clients.resource_registry.find_objects(data_process_id, AT.hasDataProducer, None, True)
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
        data_producer_id = self.create_data_producer(name=instrument_obj.name, description=instrument_obj.description)
        log.debug("register_instrument  data_producer_id %s" % data_producer_id)

        # Create association
        self.clients.resource_registry.create_association(instrument_id, AT.hasDataProducer, data_producer_id)

        return data_producer_id

    def unregister_instrument(self, instrument_id=''):
        """
        Remove the associated DataProcess

        """
        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        res_ids, _ = self.clients.resource_registry.find_objects(instrument_id, AT.hasDataProducer, None, True)
        if res_ids is None:
            raise NotFound("Data Producer for Instrument %d does not exist" % instrument_id)

        return self._remove_producer(instrument_id, res_ids)


    def assign_data_product(self, input_resource_id='', data_product_id=''):
        """Connect the producer for an existing input resource with a data product

        @param input_resource_id    str
        @param data_product_id    str
        @retval data_producer_id    str
        """
        source_obj = self.clients.resource_registry.read(input_resource_id)
        if not source_obj:
            raise NotFound("Source resource %s does not exist" % input_resource_id)

        #find the data producer resource associated with the source resource that is creating the data product
        producer_ids, _ = self.clients.resource_registry.find_objects(input_resource_id, AT.hasDataProducer, RT.DataProducer, id_only=True)
        if producer_ids is None:
            raise NotFound("No Data Producers associated with source resource ID " + str(input_resource_id))

        self.clients.resource_registry.create_association(data_product_id,  AT.hasDataProducer,  producer_ids[0])
        return

    def unassign_data_product(self, input_resource_id='', data_product_id=''):
        """@todo document this interface!!!

        @param input_resource_id    str
        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def create_data_producer(self, name='', description=''):
        """
        Create a new data_producer.

        @param name    str
        @param description    str
        @retval data_producer_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """

        log.debug("Creating DataProducer object")
        data_producer_obj = IonObject(RT.DataProducer,name=name, description=description)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)
        log.debug("create_data_producer  data producer id %s" % data_producer_id)

        # create the stream for this data producer
        stream = IonObject(RT.Stream, name=name)
        streamId = self.clients.pubsub_management.create_stream(stream)

        log.debug("create_data_producer  Stream id %s" % streamId)

        # register the data producer with the PubSub service
#        self.StreamRoute = self.clients.pubsub_management.register_producer(data_producer.name, self.streamID)
#
#        log.debug("create_data_producer  Stream routing_key %s" % self.StreamRoute.routing_key)
#        data_producer.stream_id = self.streamID
#        data_producer.routing_key = self.StreamRoute.routing_key
#        data_producer.exchange_name = self.StreamRoute.exchange_name
#        data_producer.credentials = self.StreamRoute.credentials


        # Create association
        self.clients.resource_registry.create_association(data_producer_id, AT.hasStream, streamId)

        return data_producer_id

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
        """@todo document this interface!!!

        @param external_data_provider    ExternalDataProvider
        @retval external_data_provider_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

    def update_external_data_provider(self, external_data_provider=None):
        """@todo document this interface!!!

        @param external_data_provider    ExternalDataProvider
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        pass

    def read_external_data_provider(self, external_data_provider_id=''):
        """@todo document this interface!!!

        @param external_data_provider_id    str
        @retval external_data_provider    ExternalDataProvider
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_external_data_provider(self, external_data_provider_id=''):
        """@todo document this interface!!!

        @param external_data_provider_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def create_data_source(self, data_source=None):
        """@todo document this interface!!!

        @param data_source    DataSource
        @retval data_source_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        log.debug("Creating data_source object")

        data_source_id, rev = self.clients.resource_registry.create(data_source)

        return data_source_id

    def update_data_source(self, data_source=None):
        """@todo document this interface!!!

        @param data_source    DataSource
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        return self.clients.resource_registry.update(data_source)

    def read_data_source(self, data_source_id=''):
        """@todo document this interface!!!

        @param data_source_id    str
        @retval data_source    DataSource
        @throws NotFound    object with specified id does not exist
        """
        log.debug("Reading DataSource object id: %s" % data_source_id)
        data_source_obj = self.clients.resource_registry.read(data_source_id)
        if not data_source_obj:
            raise NotFound("DataSource %s does not exist" % data_source_id)
        return data_source_obj

    def delete_data_source(self, data_source_id=''):
        """@todo document this interface!!!

        @param data_source_id    str
        @throws NotFound    object with specified id does not exist
        """
        log.debug("Deleting DataSource id: %s" % data_source_id)
        data_source_obj = self.read_data_source(data_source_id)
        if data_source_obj is None:
            raise NotFound("DataSource %s does not exist" % data_source_id)

        return self.clients.resource_registry.delete(data_source_id)


    def create_external_dataset(self, external_dataset=None):
        """@todo document this interface!!!

        @param external_dataset    ExternalDataset
        @retval external_dataset_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

    def update_external_dataset(self, external_dataset=None):
        """@todo document this interface!!!

        @param external_dataset    ExternalDataset
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        pass

    def read_external_dataset(self, external_dataset_id=''):
        """@todo document this interface!!!

        @param external_dataset_id    str
        @retval external_dataset    ExternalDataset
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_external_dataset(self, external_dataset_id=''):
        """@todo document this interface!!!

        @param external_dataset_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def assign_data_agent(self, external_dataset_id='', agent_instance=None):
        """Connect the agent instance description with an external data set

        @param external_dataset_id    str
        @param agent_instance    AgentInstance
        @throws NotFound    object with specified id does not exist
        """
        pass

    def unassign_data_agent(self, data_agent_id='', external_dataset_id=''):
        """@todo document this interface!!!

        @param data_agent_id    str
        @param external_dataset_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass