#!/usr/bin/env python

'''
@package ion.services.sa.data_acquisition_management.data_acquisition_management_service Implementation of IDataAcquisitionManagementService interface
@file ion/services/sa/data_acquisition_management/data_acquisition_management_management_service.py
@author M Manning
@brief Data Acquisition Management service to keep track of Data Producers, Data Sources
and the relationships between them
'''

from interface.services.sa.idata_acquisition_management_service import BaseDataAcquisitionManagementService
from pyon.core.exception import NotFound
from pyon.core.bootstrap import IonObject

from pyon.public import log


class DataAcquisitionManagementService(BaseDataAcquisitionManagementService):

    def create_data_source(self, data_source={}):
        '''
        Create a new data_source.

        @param data_source New data source properties.
        @retval id New data_source id.
        '''
        log.debug("Creating data_source object")
        data_source_obj = IonObject("DataSource", data_source)
        data_source_id, rev = self.clients.resource_registry.create(data_source_obj)

        return data_source_id

    def update_data_source(self, data_source={}):
        '''
        Update an existing data_source.

        @param data_source The data_source object with updated properties.
        @retval success Boolean to indicate successful update.
        @todo Add logic to validate optional attributes. Is this interface correct?
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Updating DataSource object: %s" % data_source.name)
        return self.clients.resource_registry.update(data_source)

    def read_data_source(self, data_source_id=''):
        '''
        Get an existing data_source object.

        @param data_source_id The id of the stream.
        @retval data_source_obj The data_source object.
        @throws NotFound when data_source doesn't exist.
        '''
        # Return Value
        # ------------
        # stream: {}
        #
        log.debug("Reading DataSource object id: %s" % data_source_id)
        data_source_obj = self.clients.resource_registry.read(data_source_id)
        if data_source_obj is None:
            raise NotFound("DataSource %d does not exist" % data_source_id)
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

        return self.clients.resource_registry.delete(data_source_obj)

    def assign_data_agent(self, data_source_id='', agent_instance={}):
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

    def register_data_source(self, data_source_id=''):
        """Register an existing data source as data producer
        """
        # Return Value
        # ------------
        # {data_source_id: ''}
        #
        pass

    def unregister_data_source(self, data_source_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def register_process(self, data_process_id=''):
        """Register an existing data process as data producer
        """
        # Return Value
        # ------------
        # {data_producer_id: ''}
        #
        pass

    def unregister_process(self, data_process_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def register_instrument(self, instrument_id=''):
        """Register an existing instrument as data producer
        """
        # Return Value
        # ------------
        # {data_producer_id: ''}
        #
        pass

    def unregister_instrument(self, instrument_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_data_producer(self, data_producer={}):
        '''
        Create a new data_producer.

        @param data_producer New data producer properties.
        @retval id New data_producer id.
        '''
        log.debug("Creating DataProducer object")
        data_producer_obj = IonObject("DataProducer", data_producer)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        return data_producer_id

    def update_data_producer(self, data_producer={}):
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
            raise NotFound("Stream %d does not exist" % data_producer_id)
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
        log.debug("Deleting stream id: %s" % data_producer_id)
        data_producer_obj = self.read_data_producer(data_producer_id)
        if data_producer_obj is None:
            raise NotFound("Stream %d does not exist" % data_producer_id)

        return self.clients.resource_registry.delete(data_producer_obj)
