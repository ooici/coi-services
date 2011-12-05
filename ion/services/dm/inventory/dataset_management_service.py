#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.idataset_management_service import BaseDatasetManagementService

class DatasetManagementService(BaseDatasetManagementService):

    """
    class docstring
    """

    def create_dataset(self, dataset={}):
        """
        method docstring
        """
        # This method creates an empty dataset resource and returns its ID.
        # It assumes that the caller provides an Instrument Info Object in a Resource Configuration Request message which should be made into a resource.
        # Return Value
        # ------------
        # {dataset_id: ''}
        #
        pass

    def update_dataset(self, dataset={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        # 
        pass

    def read_dataset(self, dataset_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # dataset: {}
        #
        pass

    def delete_dataset(self, dataset_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_datasets(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # dataset_list: []
        #
        pass
  