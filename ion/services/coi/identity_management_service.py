#!/usr/bin/env python

__author__ = 'Thomas R. Lennan'
__license__ = 'Apache 2.0'

from pyon.core.exception import Conflict, Inconsistent, NotFound
from pyon.public import AT, RT
from pyon.util.log import log

from interface.services.coi.iidentity_management_service import BaseIdentityManagementService

class IdentityManagementService(BaseIdentityManagementService):

    """
    A resource registry that stores identities of users and resources, including bindings of internal identities to external identities. Also stores metadata such as a user profile.a	A resource registry that stores identities of users and resources, including bindings of internal identities to external identities. Also stores metadata such as a user profile.a
    """
    
    def create_user_identity(self, user_identity={}):
        # Persist UserIdentity object and return object _id as OOI id
        user_id, version = self.clients.resource_registry.create(user_identity)
        return user_id

    def update_user_identity(self, user_identity={}):
        # Overwrite UserIdentity object
        self.clients.resource_registry.update(user_identity)

    def read_user_identity(self, user_id=''):
        # Read UserIdentity object with _id matching passed user id
        user_identity = self.clients.resource_registry.read(user_id)
        if not user_identity:
            raise NotFound("UserIdentity %s does not exist" % user_id)
        return user_identity

    def delete_user_identity(self, user_id=''):
        # Read and delete specified UserIdentity object
        user_identity = self.clients.resource_registry.read(user_id)
        if not user_identity:
            raise NotFound("UserIdentity %s does not exist" % user_id)
        self.clients.resource_registry.delete(user_identity)

    def register_user_credentials(self, user_id='', credentials={}):
        # Create UserCredentials object
        credentials_obj_id, version = self.clients.resource_registry.create(credentials)
        # Create association with user identity object
        self.clients.resource_registry.create_association(user_id, AT.hasCredentials, credentials_obj_id)

    def unregister_user_credentials(self, user_id='', credentials_name=''):
        # Read UserCredentials
        credentials_obj = self.clients.resource_registry.find_resources(RT.UserCredentials, None, credentials_name, False)
        if not credentials_obj:
            raise NotFound("UserCredentials %s does not exist" % credentials_name)
        # Find and break association with UserIdentity
        objects, assocs = self.clients.resource_registry.find_objects(user_id, AT.hasCredentials, RT.UserCredentials)
        if not objects:
            raise NotFound("UserIdentity to UserCredentials association for user id %s to credential %s does not exist" % (user_id,credentials_name))
        association_id = [assoc._id for obj,assoc in zip(objects, assocs) if obj._id == credentials_obj._id][0]
        self.clients.resource_registry.delete_association(association_id)
        # Delete the UserCredentials
        self.clients.resource_registry.delete(credentials_obj)

    def create_user_info(self, user_id="", user_info={}):
        # Ensure UserInfo association does not already exist
        objects, assocs = self.clients.resource_registry.find_objects(user_id, AT.hasInfo, RT.UserInfo)
        if objects:
            raise Conflict("UserInfo already exists for user id %s" % (user_id))
        # Create UserInfo object
        user_info_id, version = self.clients.resource_registry.create(user_info)
        # Create association with user identity object
        self.clients.resource_registry.create_association(user_id, AT.hasInfo, user_info_id)
        return user_info_id

    def update_user_info(self, user_info={}):
        # Overwrite UserInfo object
        self.clients.resource_registry.update(user_info)

    def read_user_info(self, user_info_id=''):
        # Read UserInfo object with _id matching passed user id
        user_info = self.clients.resource_registry.read(user_info_id)
        if not user_info:
            raise NotFound("UserInfo %s does not exist" % user_info_id)
        return user_info

    def delete_user_info(self, user_info_id=''):
        # Read UserInfo
        user_info = self.clients.resource_registry.read(user_info_id)
        if not user_info:
            raise NotFound("UserInfo %s does not exist" % user_info_id)
        # Find and break association with UserIdentity
        subjects, assocs = self.clients.resource_registry.find_subjects(user_info_id, AT.hasInfo, RT.UserIdentity)
        if not assocs:
            raise NotFound("UserIdentity to UserInfo association for user info id %s does not exist" % user_info_id)
        association_id = assocs[0]._id
        self.clients.resource_registry.delete_association(association_id)
        # Delete the UserInfo
        self.clients.resource_registry.delete(user_info)

    def find_user_info_by_id(self, user_id=''):
        # Look up UserInfo via association with UserIdentity
        objects, assocs = self.clients.resource_registry.find_objects(user_id, AT.hasInfo, RT.UserInfo)
        if not objects:
            raise NotFound("UserInfo for user id %s does not exist" % user_id)
        user_info = objects[0]
        return user_info

    def find_user_info_by_name(self, name=''):
        objects, assocs = self.clients.resource_registry.find_resources(RT.UserInfo, None, name, False)
        if not objects:
            raise NotFound("UserInfo with name %s does not exist" % name)
        if len(objects) > 1:
            raise Inconsistent("Multiple UserInfos with name %s exist" % name)
        return objects[0]

    def find_user_info_by_subject(self, subject=''):
        # Find UserCredentials
        objects, assocs = self.clients.resource_registry.find_resources(RT.UserCredentials, None, subject, False)      
        if not objects:
            raise NotFound("UserCredentials with subject %s does not exist" % subject)
        if len(objects) > 1:
            raise Inconsistent("Multiple UserCredentials with subject %s exist" % subject)
        if not assocs:
            raise NotFound("UserIdentity to UserCredentials association for subject %s does not exist" % subject)
        if len(assocs) > 1:
            raise Inconsistent("Multiple UserIdentity to UserCredentials associations for subject %s exist" % subject)
        user_identity_id = assocs[0].s
        # Look up UserInfo via association with UserIdentity
        objects, assocs = self.clients.resource_registry.find_objects(user_identity_id, AT.hasInfo, RT.UserInfo)
        if not objects:
            raise NotFound("UserInfo for subject %s does not exist" % subject)
        if len(objects) > 1:
            raise Inconsistent("Multiple UserInfos for subject %s exist" % subject)
        user_info = objects[0]
        return user_info

    def create_resource_identity(self, resource_identity={}):
        # Persist ResourceIdentity object and return object _id as OOI id
        resource_identity_id, version = self.clients.resource_registry.create(resource_identity)
        return resource_identity_id

    def update_resource_identity(self, resource_identity={}):
        # Overwrite ResourceIdentity object
        self.clients.resource_registry.update(resource_identity)

    def read_resource_identity(self, resource_identity_id=''):
        # Read ResourceIdentity object with _id matching passed user id
        return self.clients.resource_registry.read(resource_identity_id)

    def delete_resource_identity(self, resource_identity_id=''):
        # Read and delete specified ResourceIdentity object
        resource_identity = self.clients.resource_registry.read(resource_identity_id)
        self.clients.resource_registry.delete(resource_identity)
