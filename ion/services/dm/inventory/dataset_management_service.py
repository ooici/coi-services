#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.idataset_management_service import DatasetManagementService

class DatasetManagementService(BaseDatasetManagementService):

    def create_dataset_resource(self, dataset={}):
        # This method creates an empty dataset resource and returns its ID.
        # It assumes that the caller provides an Instrument Info Object in a Resource Configuration Request message which should be made into a resource.
        pass

    def find_dataset_resource(self, filter={}):
        # This method returns a list of Dataset Resource References that match the request.
        # ------------
        # status: []
        #
        pass

    def get_metadata(self, datasetId=''):
        # This method returns a Dataset Resource for the input id
        # ------------
        # dataset: {}
        #
        #
        
        pass
  