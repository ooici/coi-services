#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'


from interface.services.cei.iprocess_dispatcher_service import BaseProcessDispatcherService

class ProcessDispatcherService(BaseProcessDispatcherService):

    def create_process_definition(self, process_definition=None):
        """ Should receive a ProcessDefinition object
        """
        # Return Value
        # ------------
        # {process_definition_id: ''}
        #
        pass

    def update_process_definition(self, process_definition=None):
        """ Should receive a ProcessDefinition object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_process_definition(self, process_definition_id=''):
        """ Should return a ProcessDefinition object
        """
        # Return Value
        # ------------
        # process_definition: {}
        #
        pass

    def delete_process_definition(self, process_definition_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_process_definitions(self, filters=None):
        """ Should receive a Query object
        """
        # Return Value
        # ------------
        # process_definition_list: []
        #
        pass

    def schedule_process(self, process_definition_id='', schedule=None):
        """ Should receive a ProcessSchedule object
        """
        # Return Value
        # ------------
        # {process_id: ''}
        #
        pass

    def cancel_process(self, process_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

