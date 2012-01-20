#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'


from interface.services.cei.iepu_management_service import BaseEpuManagementService

class EpuManagementService(BaseEpuManagementService):

    def create_EPU_definition(self, EPU_definition=None):
        """ Should receive an EPUDefinition object
        """
        # Return Value
        # ------------
        # {EPU_definition_id: ''}
        #
        pass

    def update_EPU_definition(self, EPU_definition=None):
        """ Should receive an EPUDefinition object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_EPU_definition(self, EPU_definition_id=''):
        """ Should return an EPUDefinition object
        """
        # Return Value
        # ------------
        # EPU_definition: {}
        #
        pass

    def delete_EPU_definition(self, EPU_definition_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_EPU(self, EPU=None):
        """ Should receive an EPU object
        """
        # Return Value
        # ------------
        # {EPU_id: ''}
        #
        pass

    def update_EPU(self, EPU=None):
        """ Should receive an EPU object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_EPU(self, EPU_id=''):
        """ Should return an EPU object
        """
        # Return Value
        # ------------
        # EPU: {}
        #
        pass

    def delete_EPU(self, EPU_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_deployable_type(self, deployable_type=None):
        """ Should receive a DeployableType object
        """
        # Return Value
        # ------------
        # {deployable_type_id: ''}
        #
        pass

    def update_deployable_type(self, deployable_type=None):
        """ Should receive a DeployableType object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_deployable_type(self, deployable_type_id=''):
        """ Should return a DeployableType object
        """
        # Return Value
        # ------------
        # deployable_type: {}
        #
        pass

    def delete_deployable_type(self, deployable_type_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass


