#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.istate_repository_service import BaseStateRepositoryService

class StateRepositoryService(BaseStateRepositoryService):
    """The Service State Repository is based on the ION Common Object Model. Its purpose is to provide persistent storage for transient process state at the end of the processing of messages.
    """

    def read(self, process_state_id=''):
        """Reads a process state object from the repository when initiating a process
          Should return a ProcessState object
        """
        # Return Value
        # ------------
        # process_state: {}
        #
        pass

    def write(self, process_state={}):
        """Writes a process state object from the repository when ending a process.
           Should receive a ProcessState object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass