#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.iconversation_management_service import BaseConversationManagementService

class ConversationManagementService(BaseConversationManagementService):


    def create_conversation(self, conversation={}):
        """ Should receive a Conversation object
        """
        # Return Value
        # ------------
        # {conversation_id: ''}
        #
        pass

    def update_conversation(self, conversation={}):
        """ Should receive a Conversation object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_conversation(self, conversation_id=''):
        """ Should return a Conversation object
        """
        # Return Value
        # ------------
        # conversation: {}
        #
        pass

    def delete_conversation(self, conversation_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_conversations(self, filters={}):
        """ Should receive a ResourceFilter object
        """
        # Return Value
        # ------------
        # conversation_list: []
        #
        pass

    def create_conversation_type(self, conversation_type={}):
        """ Should receive a ConversationType object
        """
        # Return Value
        # ------------
        # {conversation_type_id: ''}
        #
        pass

    def update_conversation_type(self, conversation_type={}):
        """ Should receive a ConversationType object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_conversation_type(self, conversation_type_id=''):
        """ Should return a ConversationType object
        """
        # Return Value
        # ------------
        # conversation_type: {}
        #
        pass

    def delete_conversation_type(self, conversation_type_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def log_message(self, message={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

