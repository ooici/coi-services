#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'


from pyon.core.exception import NotFound, BadRequest, Inconsistent
from pyon.public import PRED, RT, Container, CFG, OT, IonObject
from pyon.util.containers import is_basic_identifier
from interface.services.coi.iconversation_management_service import BaseConversationManagementService

class ConversationManagementService(BaseConversationManagementService):

    """
    The Conversation Management Service is the service that manages the Conversations and their types which are allowed in the system.
    """

    def create_conversation(self, conversation=None):
        """Creates a Conversation resource from the parameter Conversation object.

        @param conversation    Conversation
        @retval conversation_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

    def update_conversation(self, conversation=None):
        """Updates an existing Conversation resource.

        @param conversation    Conversation
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        pass

    def read_conversation(self, conversation_id=''):
        """Returns an existing Conversation resource.

        @param conversation_id    str
        @retval conversation    Conversation
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_conversation(self, conversation_id=''):
        """Deletes an existing Conversation resource.

        @param conversation_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass



###########


    def create_conversation_type(self, conversation_type=None):
        """Creates a Conversation Type resource from the parameter ConversationType object.

        @param conversation_type    ConversationType
        @retval conversation_type_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        if not is_basic_identifier(conversation_type.name):
            raise BadRequest("The conversation type name '%s' can only contain alphanumeric and underscore characters" % conversation_type.name)

        conversation_type_id, version = self.clients.resource_registry.create(conversation_type)
        return conversation_type_id

    def update_conversation_type(self, conversation_type=None):
        """Updates an existing Conversation Type resource.

        @param conversation_type    ConversationType
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        if not is_basic_identifier(conversation_type.name):
            raise BadRequest("The conversation type name '%s' can only contain alphanumeric and underscore characters" % conversation_type.name)

        self.clients.resource_registry.update(conversation_type)

    def read_conversation_type(self, conversation_type_id=''):
        """Returns an existing Conversation Type resource.

        @param conversation_type_id    str
        @retval conversation_type    ConversationType
        @throws NotFound    object with specified id does not exist
        """
        if not conversation_type_id:
            raise BadRequest("The conversation_type_id parameter is missing")

        conversation_type = self.clients.resource_registry.read(conversation_type_id)
        if not conversation_type:
            raise NotFound("Conversation Type '%s' does not exist" % conversation_type_id)

        return conversation_type

    def delete_conversation_type(self, conversation_type_id=''):
        """Deletes an existing Conversation Type resource.

        @param conversation_type_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not conversation_type_id:
            raise BadRequest("The conversation_type_id parameter is missing")

        conversation_type = self.clients.resource_registry.read(conversation_type_id)
        if not conversation_type:
            raise NotFound("Conversation Type '%s' does not exist" % conversation_type_id)

       #check for and associated ConversationRole objects
        alist,_ = self.clients.resource_registry.find_objects(conversation_type, PRED.hasRole, RT.ConversationRole)
        if len(alist) > 0:
            raise BadRequest("Conversation Type '%s' cannot be removed as there are %s Conversation Roles associated to it" % (conversation_type.name, str(len(alist))))

        self.clients.resource_registry.delete(conversation_type_id)

    def bind_conversation_type_to_role(self, conversation_type_id='', conversation_role_id=''):
        """Binds an existing Conversation Type resource to a Conversation Role

        @param conversation_type_id    str
        @param conversation_role_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not conversation_type_id:
            raise BadRequest("The conversation_type_id parameter is missing")

        conversation_type = self.clients.resource_registry.read(conversation_type_id)
        if not conversation_type:
            raise NotFound("Conversation Type '%s' does not exist" % conversation_type_id)

        if not conversation_role_id:
            raise BadRequest("The conversation_role_id parameter is missing")

        conversation_role = self.clients.resource_registry.read(conversation_role_id)
        if not conversation_role:
            raise NotFound("Conversation Role '%s' does not exist" % conversation_role_id)

        aid = self.clients.resource_registry.create_association(conversation_type, PRED.hasRole, conversation_role)


    def unbind_conversation_type_to_role(self, conversation_type_id='', conversation_role_id=''):
        """Removes the binding between an existing Conversation Type resource and a Conversation Role

        @param conversation_type_id    str
        @param conversation_role_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not conversation_type_id:
            raise BadRequest("The conversation_type_id parameter is missing")

        conversation_type = self.clients.resource_registry.read(conversation_type_id)
        if not conversation_type:
            raise NotFound("Conversation Type '%s' does not exist" % conversation_type_id)

        if not conversation_role_id:
            raise BadRequest("The conversation_role_id parameter is missing")

        conversation_role = self.clients.resource_registry.read(conversation_role_id)
        if not conversation_role:
            raise NotFound("Conversation Role '%s' does not exist" % conversation_role_id)

        aid = self.clients.resource_registry.get_association(conversation_type, PRED.hasRole, conversation_role)
        if not aid:
            raise NotFound("The shared association between the specified Conversation Type and Conversation Role is not found")

        self.clients.resource_registry.delete_association(aid)



        #########


    def create_conversation_role(self, conversation_role=None):
        """Creates a Conversation Role resource from the parameter ConversationRole object.

        @param conversation_role    ConversationRole
        @retval conversation_role_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        if not is_basic_identifier(conversation_role.name):
            raise BadRequest("The conversation role name '%s' can only contain alphanumeric and underscore characters" % conversation_role.name)

        conversation_role_id, version = self.clients.resource_registry.create(conversation_role)
        return conversation_role_id

    def update_conversation_role(self, conversation_role=None):
        """Updates an existing Conversation Role resource.

        @param conversation_role    ConversationRole
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        if not is_basic_identifier(conversation_role.name):
            raise BadRequest("The conversation type name '%s' can only contain alphanumeric and underscore characters" % conversation_role.name)

        self.clients.resource_registry.update(conversation_role)


    def read_conversation_role(self, conversation_role_id=''):
        """Returns an existing Conversation Role resource.

        @param conversation_role_id    str
        @retval conversation_role    ConversationRole
        @throws NotFound    object with specified id does not exist
        """
        if not conversation_role_id:
            raise BadRequest("The conversation_role_id parameter is missing")

        conversation_role = self.clients.resource_registry.read(conversation_role_id)
        if not conversation_role:
            raise NotFound("Conversation Role '%s' does not exist" % conversation_role_id)

        return conversation_role

    def delete_conversation_role(self, conversation_role_id=''):
        """Deletes an existing Conversation Role resource.

        @param conversation_role_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not conversation_role_id:
            raise BadRequest("The conversation_role_id parameter is missing")

        conversation_role = self.clients.resource_registry.read(conversation_role_id)
        if not conversation_role:
            raise NotFound("Conversation Role '%s' does not exist" % conversation_role_id)

        #check for and associated ConversationType objects
        alist,_ = self.clients.resource_registry.find_subjects(RT.ConversationType, PRED.hasRole, conversation_role)
        if len(alist) > 0:
            raise BadRequest("Conversation Role '%s' cannot be removed as there are %s Conversation Type associated to it" % (conversation_role.name, str(len(alist))))

        self.clients.resource_registry.delete(conversation_role_id)


