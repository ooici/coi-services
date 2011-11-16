#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.sa.idata_process_management_service  import BaseDataProcessManagementService

class DataProcessManagementService(BaseDataProcessManagementService):

    def create_data_process(self, data_process={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {data_process_id: ''}
        #
        pass

    def update_data_process(self, data_process={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_data_process(self, data_process_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # data_process: {}
        #
        pass

    def delete_data_process(self, data_process_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_data_process(self, filters={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # data_process_list: []
        #
        pass

    def attach_process(self, process=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass
