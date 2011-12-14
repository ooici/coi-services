#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'


from interface.services.cei.iexecution_engine_management_service import BaseExecutionEngineManagementService

class ExecutionEngineManagementService(BaseExecutionEngineManagementService):

    def create_execution_engine_definition(self, execution_engine_definition={}):
        """ Should receive an ExecutionEngineDefinition object
        """
        # Return Value
        # ------------
        # {execution_engine_definition_id: ''}
        #
        pass

    def update_execution_engine_definition(self, execution_engine_definition={}):
        """ Should receive an ExecutionEngineDefinition object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_execution_engine_definition(self, execution_engine_definition_id=''):
        """ Should return an ExecutionEngineDefinition object
        """
        # Return Value
        # ------------
        # execution_engine_definition: {}
        #
        pass

    def delete_execution_engine_definition(self, execution_engine_definition_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_execution_engine(self, execution_engine={}):
        """ Should receive an ExecutionEngine object
        """
        # Return Value
        # ------------
        # {execution_engine_id: ''}
        #
        pass

    def update_execution_engine(self, execution_engine={}):
        """ Should receive an ExecutionEngine object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_execution_engine(self, execution_engine_id=''):
        """ Should return an ExecutionEngine object
        """
        # Return Value
        # ------------
        # execution_engine: {}
        #
        pass

    def delete_execution_engine(self, execution_engine_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass


