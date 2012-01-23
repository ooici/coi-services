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

    def create_data_source(self, data_source=None):
        '''
        Create a new data_source.

        @param data_source New data source properties.
        @retval id New data_source id.
        '''
        log.debug("Creating data_source object")

        data_source_id, rev = self.clients.resource_registry.create(data_source)

        return data_source_id

    def update_data_source(self, data_source=None):
        '''
        Update an existing data_source.

        @param data_source The data_source object with updated properties.
        @retval success Boolean to indicate successful update.
        @todo Add logic to validate optional attributes. Is this interface correct?
        '''

        return self.clients.resource_registry.update(data_source)

    def read_data_source(self, data_source_id=''):
        '''
        Get an existing data_source object.

        @param data_source_id The id of the stream.
        @retval data_source_obj The data_source object.
        @throws NotFound when data_source doesn't exist.
        '''

        log.debug("Reading DataSource object id: %s" % data_source_id)
        data_source_obj = self.clients.resource_registry.read(data_source_id)
        if not data_source_obj:
            raise NotFound("DataSource %s does not exist" % data_source_id)
        return data_source_obj

    def delete_data_source(self, data_source_id=''):
        '''
        Delete an existing data_source.

        @param data_source_id The id of the subscription.
        @retval success Boolean to indicate successful deletion.
        @throws NotFound when data_source doesn't exist.
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Deleting DataSource id: %s" % data_source_id)
        data_source_obj = self.read_data_source(data_source_id)
        if data_source_obj is None:
            raise NotFound("DataSource %s does not exist" % data_source_id)

        return self.clients.resource_registry.delete(data_source_id)

    def assign_data_agent(self, data_source_id='', agent_instance=None):
        """Connect the agent instance description with a data source
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_data_agent(self, data_agent_id='', data_source_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass


    # -----------------
    # The following operations register different types of data producers
    # -----------------

        # try to get a resource
    def _create_producer_resource(self, data_producer=None):
        log.debug("Creating DataProducer object")
        #data_producer_obj = IonObject("DataProducer", attrs)

        data_producer_id, rev = self.clients.resource_registry.create(data_producer)
        return data_producer_id

    def _remove_producer(self, resource_id='', producers=None):
        log.debug("Removing DataProducer objects and links")
        for x in producers:
            # List all association ids with given subject, predicate, object triples
            assoc_ids, _ = self.clients.resource_registry.find_associations(resource_id, AT.hasDataProducer, x, True)
            for y in assoc_ids:
                self.clients.resource_registry.delete_association(y)

            self.clients.resource_registry.delete(x)
        return True

    def register_data_source(self, data_source_id=''):
        """Register an existing data source as data producer
        """
        # retrieve the data_source object
        data_source_obj = self.clients.resource_registry.read(data_source_id)
        if data_source_obj is None:
            raise NotFound("Data Source %s does not exist" % data_source_id)

        #create data producer resource and associate to this data_source_id
        dp = IonObject(RT.DataSource, {'name':data_source_obj.name, 'description':data_source_obj.description})
        data_producer_id = self._create_producer_resource(dp)

        # Create association
        self.clients.resource_registry.create_association(data_source_id, AT.hasDataProducer, data_producer_id)

        return data_producer_id

    def unregister_data_source(self, data_source_id=''):
        """
        Remove the associated DataProducer

        """
        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        res_ids, _ = self.clients.resource_registry.find_objects(data_source_id, AT.hasDataProducer, None, True)
        if res_ids is None:
            raise NotFound("Data Producer for Data Source %d does not exist" % data_source_id)

        return self._remove_producer(data_source_id, res_ids)



    def register_process(self, data_process_id=''):
        """
        Register an existing data process as data producer
        """

        # retrieve the data_process object
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        if data_process_obj is None:
            raise NotFound("Data Process %s does not exist" % data_process_id)

        #create data producer resource and associate to this data_process_id
        dp = IonObject(RT.DataProducer, {'name':data_process_obj.name, 'description':data_process_obj.description})
        data_producer_id = self._create_producer_resource(dp)

        # Create association
        self.clients.resource_registry.create_association(data_process_id, AT.hasDataProducer, data_producer_id)

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

        #create data producer resource and associate to this data_process_id
        inst = IonObject(RT.DataProducer, {'name':instrument_obj.name, 'description':instrument_obj.description})
        data_producer_id = self._create_producer_resource(inst)

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

    def create_data_producer(self, data_producer=None):
        '''
        Create a new data_producer.

        @param data_producer New data producer properties.
        @retval id New data_producer id.
        '''
        log.debug("Creating DataProducer object")
        #data_producer_obj = IonObject("DataProducer", data_producer)

        # create the stream for this data producer
        stream = IonObject(RT.Stream, name=data_producer.name)
        self.streamID = self.clients.pubsub_management.create_stream(stream)

        log.debug("create_data_producer  Stream id %s" % self.streamID)

        # register the data producer with the PubSub service
        self.StreamRoute = self.clients.pubsub_management.register_producer(data_producer.name, self.streamID)

        log.debug("create_data_producer  Stream routing_key %s" % self.StreamRoute.routing_key)
        data_producer.stream_id = self.streamID
        data_producer.routing_key = self.StreamRoute.routing_key
        data_producer.exchange_name = self.StreamRoute.exchange_name
        data_producer.credentials = self.StreamRoute.credentials

        data_producer_id, rev = self.clients.resource_registry.create(data_producer)
        log.debug("create_data_producer  data producer id %s" % data_producer_id)

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

    def create_external_data_provider(self, external_data_provider=None):
        """@todo document this interface!!!

        @param external_data_provider    ExternalDataProvider
        @retval external_data_provider_id    str
        """
        pass

    def update_external_data_provider(self, external_data_provider=None):
        """@todo document this interface!!!

        @param external_data_provider    ExternalDataProvider
        @retval success    bool
        """
        pass

    def read_external_data_provider(self, external_data_provider_id=''):
        """@todo document this interface!!!

        @param external_data_provider_id    str
        @retval external_data_provider    ExternalDataProvider
        """
        pass

    def delete_external_data_provider(self, external_data_provider_id=''):
        """@todo document this interface!!!

        @param external_data_provider_id    str
        @retval success    bool
        """
        pass

    def create_external_dataset(self, external_dataset=None):
        """@todo document this interface!!!

        @param external_dataset    ExternalDataset
        @retval external_dataset_id    str
        """
        pass

    def update_external_dataset(self, external_dataset=None):
        """@todo document this interface!!!

        @param external_dataset    ExternalDataset
        @retval success    bool
        """
        pass

    def read_external_dataset(self, external_dataset_id=''):
        """@todo document this interface!!!

        @param external_dataset_id    str
        @retval external_dataset    ExternalDataset
        """
        pass

    def delete_external_dataset(self, external_dataset_id=''):
        """@todo document this interface!!!

        @param external_dataset_id    str
        @retval success    bool
        """
        pass