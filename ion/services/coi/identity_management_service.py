#!/usr/bin/env python

__author__ = 'Thomas R. Lennan'
__license__ = 'Apache 2.0'

from pyon.core.exception import Conflict, Inconsistent, NotFound, BadRequest
from pyon.core.security.authentication import Authentication
from pyon.public import PRED, RT, IonObject
from pyon.util.log import log

import time

from interface.services.coi.iidentity_management_service import BaseIdentityManagementService

class IdentityManagementService(BaseIdentityManagementService):

    """
    A resource registry that stores identities of actors (users
    are an instance of actor) and resources, including bindings
    of internal identities to external identities. Also stores
    metadata such as user contact info.
    """

    def on_init(self):
        self.authentication = Authentication()
    
    def create_actor_identity(self, actor_identity=None):
        # Persist ActorIdentity object and return object _id as OOI id
        actor_id, version = self.clients.resource_registry.create(actor_identity)
        return actor_id

    def update_actor_identity(self, actor_identity=None):
        # Overwrite ActorIdentity object
        self.clients.resource_registry.update(actor_identity)

    def read_actor_identity(self, actor_id=''):
        # Read ActorIdentity object with _id matching passed actor id
        actor_identity = self.clients.resource_registry.read(actor_id)
        if not actor_identity:
            raise NotFound("ActorIdentity %s does not exist" % actor_id)
        return actor_identity

    def delete_actor_identity(self, actor_id=''):
        # Read and delete specified ActorIdentity object
        actor_identity = self.clients.resource_registry.read(actor_id)
        if not actor_identity:
            raise NotFound("ActorIdentity %s does not exist" % actor_id)
        self.clients.resource_registry.delete(actor_id)

    def find_actor_identity_by_name(self, name=''):
        """Return the ActorIdentity object whose name attribute matches the passed value.

        @param name    str
        @retval actor_info    ActorIdentity
        @throws NotFound    failed to find ActorIdentity
        @throws Inconsistent    Multiple ActorIdentity objects matched name
        """
        objects, matches = self.clients.resource_registry.find_resources(RT.ActorIdentity, None, name, False)
        if not objects:
            raise NotFound("ActorIdentity with name %s does not exist" % name)
        if len(objects) > 1:
            raise Inconsistent("Multiple ActorIdentity objects with name %s exist" % name)
        return objects[0]


    def register_actor_credentials(self, actor_id='', credentials=None):
        # Create ActorCredentials object
        credentials_obj_id, version = self.clients.resource_registry.create(credentials)
        # Create association with actor identity object
        res = self.clients.resource_registry.create_association(actor_id, PRED.hasCredentials, credentials_obj_id)

    def unregister_actor_credentials(self, actor_id='', credentials_name=''):
        # Read ActorCredentials
        objects, matches = self.clients.resource_registry.find_resources(RT.ActorCredentials, None, credentials_name, False)
        if not objects or len(objects) == 0:
            raise NotFound("ActorCredentials %s does not exist" % credentials_name)
        if len(objects) > 1:
            raise Conflict("Multiple ActorCredentials objects found for subject %s" % credentials_name)
        actor_credentials_id = objects[0]._id
        # Find and break association with ActorIdentity
        assocs = self.clients.resource_registry.find_associations(actor_id, PRED.hasCredentials, actor_credentials_id)
        if not assocs or len(assocs) == 0:
            raise NotFound("ActorIdentity to ActorCredentials association for actor id %s to credential %s does not exist" % (actor_id,credentials_name))
        association_id = assocs[0]._id
        self.clients.resource_registry.delete_association(association_id)
        # Delete the ActorCredentials
        self.clients.resource_registry.delete(actor_credentials_id)

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

        # Look for matching ActorCredentials object
        objects, assocs = self.clients.resource_registry.find_resources(RT.ActorCredentials, None, subject, True)
        if len(objects) > 1:
            raise Conflict("More than one ActorCredentials object was found for subject %s" % subject)
        if len(assocs) > 1:
            raise Conflict("More than one ActorIdentity object is associated with subject %s" % subject)
        if len(objects) == 1:
            log.debug("Signon known subject %s" % (subject))
            # Known actor, get ActorIdentity object
            actor_credentials_id = objects[0]
            subjects, assocs = self.clients.resource_registry.find_subjects(RT.ActorIdentity, PRED.hasCredentials, actor_credentials_id)

            if len(subjects) == 0:
                raise Conflict("ActorIdentity object with subject %s was previously created but is not associated with a ActorIdentity object" % subject)
            actor_identity = subjects[0]
            actor_id = actor_identity._id

            def is_registered(identity):
                if identity.contact.name:
                    if len(identity.contact.name) > 0:
                        if identity.contact.email:
                            if len(identity.contact.email) > 0:
                                return True
                return False

            # Determine if contact info has been specified previously
            # Minimally, actor needs to provide name and email address
            registered = is_registered(actor_identity)
            log.debug("Signon returning actor_id, valid_until, registered: %s, %s, %s" % (actor_id, valid_until, str(registered)))
            return actor_id, valid_until, registered
        else:
            log.debug("Signon new subject %s" % (subject))
            # New actor.  Create ActorIdentity and ActorCredentials
            actor_identity = IonObject("ActorIdentity", {"name": subject})
            actor_id = self.create_actor_identity(actor_identity)

            actor_credentials = IonObject("ActorCredentials", {"name": subject})
            self.register_actor_credentials(actor_id, actor_credentials)
            log.debug("Signon returning actor_id, valid_until, registered: %s, %s, False" % (actor_id, valid_until))
            return actor_id, valid_until, False

    def update_actor_contact_info(self, actor_id="", contact_info=None):
        # Read ActorIdentity object
        actor_identity = self.read_actor_identity(actor_id)
        # Overwrite contact attribute
        actor_identity.contact = contact_info
        # Write back
        self.update_actor_identity(actor_identity)

    def create_resource_identity(self, resource_identity=None):
        # Persist ResourceIdentity object and return object _id as OOI id
        resource_identity_id, version = self.clients.resource_registry.create(resource_identity)
        return resource_identity_id

    def update_resource_identity(self, resource_identity=None):
        # Overwrite ResourceIdentity object
        self.clients.resource_registry.update(resource_identity)

    def read_resource_identity(self, resource_identity_id=''):
        # Read ResourceIdentity object with _id matching passed actor id
        return self.clients.resource_registry.read(resource_identity_id)

    def delete_resource_identity(self, resource_identity_id=''):
        # Read and delete specified ResourceIdentity object
        resource_identity = self.clients.resource_registry.read(resource_identity_id)
        self.clients.resource_registry.delete(resource_identity_id)
