#!/usr/bin/env python

__author__ = 'Thomas R. Lennan'
__license__ = 'Apache 2.0'

from pyon.core.exception import Conflict, Inconsistent, NotFound
from pyon.core.security.authentication import Authentication
from pyon.public import PRED, RT, IonObject
from pyon.util.log import log

import time

from interface.services.coi.iidentity_management_service import BaseIdentityManagementService

class IdentityManagementService(BaseIdentityManagementService):

    """
    A resource registry that stores identities of users and resources, including bindings of internal identities to external identities. Also stores metadata such as a user profile.a	A resource registry that stores identities of users and resources, including bindings of internal identities to external identities. Also stores metadata such as a user profile.a
    """

    def on_init(self):
        self.authentication = Authentication()
    
    def create_actor_identity(self, actor_identity=None):
        # Persist ActorIdentity object and return object _id as OOI id
        user_id, version = self.clients.resource_registry.create(actor_identity)
        return user_id

    def update_actor_identity(self, actor_identity=None):
        # Overwrite ActorIdentity object
        self.clients.resource_registry.update(actor_identity)

    def read_actor_identity(self, user_id=''):
        # Read ActorIdentity object with _id matching passed user id
        actor_identity = self.clients.resource_registry.read(user_id)
        if not actor_identity:
            raise NotFound("ActorIdentity %s does not exist" % user_id)
        return actor_identity

    def delete_actor_identity(self, user_id=''):
        # Read and delete specified ActorIdentity object
        actor_identity = self.clients.resource_registry.read(user_id)
        if not actor_identity:
            raise NotFound("ActorIdentity %s does not exist" % user_id)
        self.clients.resource_registry.delete(user_id)

    def find_actor_identity_by_name(self, name=''):
        """Return the ActorIdentity object whose name attribute matches the passed value.

        @param name    str
        @retval user_info    ActorIdentity
        @throws NotFound    failed to find ActorIdentity
        @throws Inconsistent    Multiple ActorIdentity objects matched name
        """
        objects, matches = self.clients.resource_registry.find_resources(RT.ActorIdentity, None, name, False)
        if not objects:
            raise NotFound("ActorIdentity with name %s does not exist" % name)
        if len(objects) > 1:
            raise Inconsistent("Multiple ActorIdentity objects with name %s exist" % name)
        return objects[0]


    def register_user_credentials(self, user_id='', credentials=None):
        # Create UserCredentials object
        credentials_obj_id, version = self.clients.resource_registry.create(credentials)
        # Create association with user identity object
        res = self.clients.resource_registry.create_association(user_id, PRED.hasCredentials, credentials_obj_id)

    def unregister_user_credentials(self, user_id='', credentials_name=''):
        # Read UserCredentials
        objects, matches = self.clients.resource_registry.find_resources(RT.UserCredentials, None, credentials_name, False)
        if not objects or len(objects) == 0:
            raise NotFound("UserCredentials %s does not exist" % credentials_name)
        if len(objects) > 1:
            raise Conflict("Multiple UserCredentials objects found for subject %s" % credentials_name)
        user_credentials_id = objects[0]._id
        # Find and break association with ActorIdentity
        assocs = self.clients.resource_registry.find_associations(user_id, PRED.hasCredentials, user_credentials_id)
        if not assocs or len(assocs) == 0:
            raise NotFound("ActorIdentity to UserCredentials association for user id %s to credential %s does not exist" % (user_id,credentials_name))
        association_id = assocs[0]._id
        self.clients.resource_registry.delete_association(association_id)
        # Delete the UserCredentials
        self.clients.resource_registry.delete(user_credentials_id)

    def create_user_info(self, user_id="", user_info=None):
        # Ensure UserInfo association does not already exist
        objects, assocs = self.clients.resource_registry.find_objects(user_id, PRED.hasInfo, RT.UserInfo)
        if objects:
            raise Conflict("UserInfo already exists for user id %s" % (user_id))
        # Create UserInfo object
        user_info_id, version = self.clients.resource_registry.create(user_info)
        # Create association with user identity object
        self.clients.resource_registry.create_association(user_id, PRED.hasInfo, user_info_id)
        return user_info_id

    def update_user_info(self, user_info=None):
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
        # Find and break association with ActorIdentity
        subjects, assocs = self.clients.resource_registry.find_subjects(RT.ActorIdentity, PRED.hasInfo, user_info_id)
        if not assocs:
            raise NotFound("ActorIdentity to UserInfo association for user info id %s does not exist" % user_info_id)
        actor_identity_id = subjects[0]._id

        assocs = self.clients.resource_registry.find_associations(actor_identity_id, PRED.hasInfo, user_info_id)
        if not assocs:
            raise NotFound("ActorIdentity to UserInfo association for user info id %s does not exist" % user_info_id)
        association_id = assocs[0]._id
        
        self.clients.resource_registry.delete_association(association_id)
        # Delete the UserInfo
        self.clients.resource_registry.delete(user_info_id)

    def find_user_info_by_id(self, user_id=''):
        # Look up UserInfo via association with ActorIdentity
        objects, assocs = self.clients.resource_registry.find_objects(user_id, PRED.hasInfo, RT.UserInfo)
        if not objects:
            raise NotFound("UserInfo for user id %s does not exist" % user_id)
        user_info = objects[0]
        return user_info

    def find_user_info_by_name(self, name=''):
        objects, matches = self.clients.resource_registry.find_resources(RT.UserInfo, None, name, False)
        if not objects:
            raise NotFound("UserInfo with name %s does not exist" % name)
        if len(objects) > 1:
            raise Inconsistent("Multiple UserInfo objects with name %s exist" % name)
        return objects[0]

    def find_user_info_by_subject(self, subject=''):
        # Find UserCredentials
        objects, matches = self.clients.resource_registry.find_resources(RT.UserCredentials, None, subject, False)      
        if not objects:
            raise NotFound("UserCredentials with subject %s does not exist" % subject)
        if len(objects) > 1:
            raise Inconsistent("Multiple UserCredentials with subject %s exist" % subject)
        user_credentials_id = objects[0]._id
        subjects, assocs = self.clients.resource_registry.find_subjects(RT.ActorIdentity, PRED.hasCredentials, user_credentials_id)
        if not subjects or len(subjects) == 0:
            raise NotFound("ActorIdentity to UserCredentials association for subject %s does not exist" % subject)
        if len(subjects) > 1:
            raise Inconsistent("Multiple ActorIdentity to UserCredentials associations for subject %s exist" % subject)
        actor_identity_id = subjects[0]._id
        # Look up UserInfo via association with ActorIdentity
        objects, assocs = self.clients.resource_registry.find_objects(actor_identity_id, PRED.hasInfo, RT.UserInfo)
        if not objects:
            raise NotFound("UserInfo for subject %s does not exist" % subject)
        if len(objects) > 1:
            raise Inconsistent("Multiple UserInfos for subject %s exist" % subject)
        user_info = objects[0]
        return user_info

    def signon(self, certificate='', ignore_date_range=False):
        log.debug("Signon with certificate:\n%s" % certificate)
        # Check the certificate is currently valid
        if not ignore_date_range:
            if not self.authentication.is_certificate_within_date_range(certificate):
                raise BadRequest("Certificate expired or not yet valid")

        # Extract subject line
        attributes = self.authentication.decode_certificate(certificate)
        subject = attributes["subject"]
        valid_until_str = attributes["not_valid_after"]
        log.debug("Signon request for subject %s with string valid_until %s" % (subject, valid_until_str))
        valid_until_tuple = time.strptime(valid_until_str, "%b %d %H:%M:%S %Y %Z")
        valid_until = str(int(time.mktime(valid_until_tuple)) * 1000)

        # Look for matching UserCredentials object
        objects, assocs = self.clients.resource_registry.find_resources(RT.UserCredentials, None, subject, True)
        if len(objects) > 1:
            raise Conflict("More than one UserCredentials object was found for subject %s" % subject)
        if len(assocs) > 1:
            raise Conflict("More than one ActorIdentity object is associated with subject %s" % subject)
        if len(objects) == 1:
            log.debug("Signon known subject %s" % (subject))
            # Known user, get ActorIdentity object
            user_credentials_id = objects[0]
            subjects, assocs = self.clients.resource_registry.find_subjects(RT.ActorIdentity, PRED.hasCredentials, user_credentials_id)

            if len(subjects) == 0:
                raise Conflict("ActorIdentity object with subject %s was previously created but is not associated with a ActorIdentity object" % subject)
            user_id = subjects[0]._id
            # Find associated UserInfo
            registered = True
            try:
                self.find_user_info_by_id(user_id)
            except NotFound:
                registered = False
            log.debug("Signon returning user_id, valid_until, registered: %s, %s, %s" % (user_id, valid_until, str(registered)))
            return user_id, valid_until, registered
        else:
            log.debug("Signon new subject %s" % (subject))
            # New user.  Create ActorIdentity and UserCredentials
            actor_identity = IonObject("ActorIdentity", {"name": subject})
            user_id = self.create_actor_identity(actor_identity)

            user_credentials = IonObject("UserCredentials", {"name": subject})
            self.register_user_credentials(user_id, user_credentials)
            log.debug("Signon returning user_id, valid_until, registered: %s, %s, False" % (user_id, valid_until))
            return user_id, valid_until, False
        

    def create_resource_identity(self, resource_identity=None):
        # Persist ResourceIdentity object and return object _id as OOI id
        resource_identity_id, version = self.clients.resource_registry.create(resource_identity)
        return resource_identity_id

    def update_resource_identity(self, resource_identity=None):
        # Overwrite ResourceIdentity object
        self.clients.resource_registry.update(resource_identity)

    def read_resource_identity(self, resource_identity_id=''):
        # Read ResourceIdentity object with _id matching passed user id
        return self.clients.resource_registry.read(resource_identity_id)

    def delete_resource_identity(self, resource_identity_id=''):
        # Read and delete specified ResourceIdentity object
        resource_identity = self.clients.resource_registry.read(resource_identity_id)
        self.clients.resource_registry.delete(resource_identity_id)
