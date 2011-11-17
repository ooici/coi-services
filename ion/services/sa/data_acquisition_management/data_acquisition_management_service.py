#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.sa.idata_acquisition_management_service import BaseDataAcquisitionManagementService

class DataAcquisitionManagementService(BaseDataAcquisitionManagementService):

    def define_data_agent(self, basics={}):
        # Register the data agent; create resource with metadata and associations
        raise NotImplementedError()

    def define_data_producer(self, producer=None):
        log.debug("define_data_producer" + producer.name)
        assert not hasattr(producer, "_id"), "ID already set"


        # coordinate creation of the stream channel; call PubsubMgmtSvc with characterization of data stream to define the topic and the producer
        stream = {}
        stream["name"] = producer.name
        stream["roles"] = ""
        strm_id = self.clients.pubsub_management.create_stream(stream)

        producer.streamid = strm_id

        # create and store the resource and associations (eg instrument or process that is producing)
        dp_id,rev = self.clients.resource_registry.create(producer)
        #aid = self.clients.resource_registry.create_association(org_id, AT.HAS_A, xs_id)

        #register the data producer
        credentials = self.clients.pubsub_management.register_producer('sci_data', strm_id)
        producer.credentials = credentials

        # Return the XP information to the data agent
        return dp_id

