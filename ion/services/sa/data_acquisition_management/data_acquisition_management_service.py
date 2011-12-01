#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.sa.idata_acquisition_management_service import BaseDataAcquisitionManagementService

class DataAcquisitionManagementService(BaseDataAcquisitionManagementService):

    def create_data_source(self, data_source={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {data_source_id: ''}
        #
        pass

    def update_data_source(self, data_source={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_data_source(self, data_source_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # data_source: {}
        #
        pass

    def delete_data_source(self, data_source_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

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
        """method docstring
        """
        log.debug("define_data_producer" + data_producer.name)
        assert not hasattr(data_producer, "_id"), "ID already set"

        # coordinate creation of the stream channel; call PubsubMgmtSvc with characterization of data stream to define the topic and the producer
        stream = {}
        stream["name"] = data_producer.name
        stream["roles"] = ""
        strm_id = self.clients.pubsub_management.create_stream(stream)

        data_producer.streamid = strm_id

        # create and store the resource and associations (eg instrument or process that is producing)
        dp_id,rev = self.clients.resource_registry.create(data_producer)
        #aid = self.clients.resource_registry.create_association(org_id, AT.HAS_A, xs_id)

        #register the data producer
        credentials = self.clients.pubsub_management.register_producer('sci_data', strm_id)
        data_producer.credentials = credentials

        # Return the XP information to the data agent
        return dp_id

        # Return Value
        # ------------
        # {data_producer_id: ''}
        #
        pass

    def update_data_producer(self, data_producer={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_data_producer(self, data_producer_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # data_producer: {}
        #
        pass

    def delete_data_producer(self, data_producer_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass
