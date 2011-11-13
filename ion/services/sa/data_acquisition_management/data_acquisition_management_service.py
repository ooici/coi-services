#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.sa.idata_acquisition_management_service import BaseDataAcquisitionManagementService

class DataAcquisitionManagementService(BaseDataAcquisitionManagementService):

    def define_data_agent(self, basics={}):
        # Register the data agent; create resource with metadata and associations
        pass

    def define_data_producer(self, basics={}):
        # Register the data producer; create and store the resource and associations (eg instrument or process that is producing)


        # Coordinate creation of the stream channel; call PubsubMgmtSvc with characterization of data stream to define the topic and the producer


        # Return the XP information to the data agent
        
        pass
