#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Stephen Henrie'
__license__ = 'Apache 2.0'

from pyon.core.exception import Conflict, Inconsistent, NotFound, BadRequest
from pyon.core.security.authentication import Authentication
from pyon.ion.resource import ExtendedResourceContainer
from pyon.public import PRED, RT, IonObject, OT
from pyon.util.log import log
from uuid import uuid4
from datetime import datetime, timedelta
import time
import copy

from interface.services.coi.iidentity_management_service import BaseIdentityManagementService
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient


class IdentityManagementService(BaseIdentityManagementService):
    """
    A resource registry that stores identities of users and resources, including bindings of internal identities to external identities. Also stores metadata such as a user profile.a	A resource registry that stores identities of users and resources, including bindings of internal identities to external identities. Also stores metadata such as a user profile.a
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
        # Read ActorIdentity object with _id matching passed user id
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

    def register_user_credentials(self, actor_id='', credentials=None):
        # Create UserCredentials object
        credentials_obj_id, version = self.clients.resource_registry.create(credentials)
        # Create association with user identity object
        res = self.clients.resource_registry.create_association(actor_id, PRED.hasCredentials, credentials_obj_id)

    def unregister_user_credentials(self, actor_id='', credentials_name=''):
        # Read UserCredentials
        objects, matches = self.clients.resource_registry.find_resources(RT.UserCredentials, None, credentials_name, False)
        if not objects or len(objects) == 0:
            raise NotFound("UserCredentials %s does not exist" % credentials_name)
        if len(objects) > 1:
            raise Conflict("Multiple UserCredentials objects found for subject %s" % credentials_name)
        user_credentials_id = objects[0]._id
        # Find and break association with ActorIdentity
        assocs = self.clients.resource_registry.find_associations(actor_id, PRED.hasCredentials, user_credentials_id)
        if not assocs or len(assocs) == 0:
            raise NotFound("ActorIdentity to UserCredentials association for user id %s to credential %s does not exist" % (actor_id, credentials_name))
        association_id = assocs[0]._id
        self.clients.resource_registry.delete_association(association_id)
        # Delete the UserCredentials
        self.clients.resource_registry.delete(user_credentials_id)

    def create_user_info(self, actor_id="", user_info=None):
        # Ensure UserInfo association does not already exist
        objects, assocs = self.clients.resource_registry.find_objects(actor_id, PRED.hasInfo, RT.UserInfo)
        if objects:
            raise Conflict("UserInfo already exists for user id %s" % (actor_id))
        # Create UserInfo object
        user_info_id, version = self.clients.resource_registry.create(user_info)
        # Create association with user identity object
        self.clients.resource_registry.create_association(actor_id, PRED.hasInfo, user_info_id)
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

    def delete_user_info(self, user_info_id='', actor_identity_id=''):
        # Read UserInfo
        user_info = self.clients.resource_registry.read(user_info_id)
        if not user_info:
            raise NotFound("UserInfo %s does not exist" % user_info_id)
        # Find and break association with ActorIdentity
        if not actor_identity_id:
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

    def find_user_info_by_email(self, user_email=''):
        #return self.clients.resource_registry.find_resources_ext(restype=RT.UserInfo, attr_name="contact.email", attr_value=user_email, id_only=False)
        user_infos, _ = self.clients.resource_registry.find_resources(RT.UserInfo)
        for user_info in user_infos:
            if hasattr(user_info.contact, 'email') and user_info.contact.email == user_email:
                return user_info
        return None

    def find_user_info_by_id(self, actor_id=''):
        # Look up UserInfo via association with ActorIdentity
        objects, assocs = self.clients.resource_registry.find_objects(actor_id, PRED.hasInfo, RT.UserInfo)
        if not objects:
            raise NotFound("UserInfo for user id %s does not exist" % actor_id)
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
        attributes = self.authentication.decode_certificate_string(certificate)
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
            actor_id = subjects[0]._id
            # Find associated UserInfo
            registered = True
            try:
                self.find_user_info_by_id(actor_id)
            except NotFound:
                registered = False
            log.debug("Signon returning actor_id, valid_until, registered: %s, %s, %s" % (actor_id, valid_until, str(registered)))
            return actor_id, valid_until, registered
        else:
            log.debug("Signon new subject %s" % (subject))
            # New user.  Create ActorIdentity and UserCredentials
            actor_identity = IonObject("ActorIdentity", {"name": subject})
            actor_id = self.create_actor_identity(actor_identity)

            user_credentials = IonObject("UserCredentials", {"name": subject})
            self.register_user_credentials(actor_id, user_credentials)
            log.debug("Signon returning actor_id, valid_until, registered: %s, %s, False" % (actor_id, valid_until))
            return actor_id, valid_until, False

    def get_user_info_extension(self, user_info_id=''):
        """Returns an UserInfoExtension object containing additional related information

        @param user_info_id    str
        @retval user_info    UserInfoExtension
        @throws BadRequest    A parameter is missing
        @throws NotFound    An object with the specified actor_id does not exist
        """
        if not user_info_id:
            raise BadRequest("The user_info_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)
        extended_user = extended_resource_handler.create_extended_resource_container(OT.UserInfoExtension, user_info_id)

        #If the org_id is not provided then skip looking for Org related roles.
        if extended_user:
            #Did not setup a dependency to org_management service to avoid a potential circular bootstrap issue
            # since this method should never be called until the system is fully running
            try:
                org_client = OrgManagementServiceProcessClient(process=self)
                roles = org_client.find_all_roles_by_user(extended_user.actor_identity._id)
                extended_user.roles = list()
                for org_name in roles:
                    for role in roles[org_name]:
                        flattened_role = copy.copy(role.__dict__)
                        del flattened_role['type_']  #Have to do this to appease the message validators for ION objects
                        flattened_role['org_name'] = org_name  #Nothing like forcing a value into the dict to appease the UI code
                        extended_user.roles.append(flattened_role)

            except Exception, e:
                raise NotFound('Could not retrieve UserRoles for User Info id: %s - %s' % (user_info_id, e.message))

        # replace list of lists with single list
        replacement_owned_resources = []
        for inner_list in extended_user.owned_resources:
            if inner_list:
                for actual_data_product in inner_list:
                    if actual_data_product:
                        replacement_owned_resources.append(actual_data_product)
        extended_user.owned_resources = replacement_owned_resources

        return extended_user

    def delete_user_credential_association(self, user_credential_id, actor_identity_id):
        association_id = self.clients.resource_registry.find_associations(actor_identity_id, PRED.hasCredentials, user_credential_id, id_only=True)
        if not association_id:
            raise NotFound("complete_account_merge: Association between UserCredentials and ActorIdentity not found. ActorIdentity ID %s, UserCredentials ID: %s " % actor_identity_id % user_credential_id)
        self.clients.resource_registry.delete_association(association_id[0])

    def __generate_token(self):
        return str(uuid4()) + "_" + str(uuid4())

    def __get_current_user_id(self):
        ctx = self.get_context()
        return ctx.get('ion-actor-id', None) if ctx else None

    def __update_user_info_token(self, token=""):
        if not token:
            raise BadRequest("__update_user_info_token: token must be set")
        ion_actor_id = self.__get_current_user_id()
        if ion_actor_id:
            current_user_info = self.find_user_info_by_id(ion_actor_id)
            current_user_info.tokens.append(token)
            self.update_user_info(current_user_info)
        else:
            raise BadRequest("__update_user_info_token: Current UserInfo not found")

    def __validate_token_string(self, token_string, user_info):
        # Find the token from the  UserInfo
        token_obj = [token for token in user_info.tokens if token.token_string == token_string]
        if not token_obj or not token_obj[0].merge_email or not token_obj[0].expires:
            raise NotFound("__validate_token: Token data not found")
        token_obj = token_obj[0]
        # Validate the expiration time and token status
        current_time = time.mktime((datetime.utcnow()).timetuple())
        if current_time > token_obj.expires or "OPEN" != token_obj.status:
            raise BadRequest("__validate_token: access token expired or token status is invalid")
        return token_obj

    def initiate_account_merge(self, merge_user_email=''):
        '''
        Initiate merging two accounts

        @throws BadRequest: A parameter is missing
        @throws NotFound: Merge user email address is not found
        @retval token_string: Token string that will be email to the user for verification
        '''
        if not merge_user_email:
            raise BadRequest("initiate_account_merge: merge_user_email must be set")

        # Find UserInfo of the account that needs to be merged into
        merge_user_info = self.find_user_info_by_email(merge_user_email)
        if not merge_user_info:
            raise NotFound("initiate_account_merge: email address not found")
        # Validate current user and the merger user are two different accounts
        if self.find_user_info_by_id(self.__get_current_user_id())._id == merge_user_info._id:
            raise BadRequest("initiate_account_merge: current and merge accounts are the same")
        token_str = self.__generate_token()
        expire_time = time.mktime((datetime.utcnow() + timedelta(days=2)).timetuple())  # Set token expire time
        token = IonObject("TokenInformation", {"token_string": token_str, "expires": expire_time, "status": "OPEN", "merge_email": merge_user_email})
        self.__update_user_info_token(token)

        return token_str

    def complete_account_merge(self, token_string=""):
        '''
        Completes merging the two accounts after verifying the token string
        @throws BadRequest  A parameter is missing
        @throws NotFound  Merge data not found
        '''
        log.debug("complete_account_merge with token string: %s" % token_string)
        if not token_string:
            raise BadRequest("complete_account_merge: token_str must be set")

        # Get current UserInfo
        current_user_id = self.__get_current_user_id()
        current_user_info = self.find_user_info_by_id(current_user_id)

        # Find all the necessary data of the merge account
        token_obj = self.__validate_token_string(token_string, current_user_info)
        merge_user_info_obj = self.find_user_info_by_email(token_obj.merge_email)  # Find UserInfo of the merge account
        merge_user_info_id = merge_user_info_obj._id
        subjects, associations = self.clients.resource_registry.find_subjects(RT.ActorIdentity, PRED.hasInfo, merge_user_info_id)  # Find ActorIdentity of the merge account
        if not associations:
            raise NotFound("complete_account_merge: ActorIdentity and UserInfo association does not exist. UserInfo ID: %s" % merge_user_info_id)
        merge_actor_identity_id = subjects[0]._id

        # Find  UserCredentials of the merge account
        merge_user_credential_id, matches = self.clients.resource_registry.find_objects(merge_actor_identity_id, PRED.hasCredentials, RT.UserCredentials, id_only=True)
        if not merge_user_credential_id:
            raise NotFound("complete_account_merge: UserCredentials and ActorIdentity association does not exist.ActorIdentity ID: %s" % merge_actor_identity_id)
        merge_user_credential_id = merge_user_credential_id[0]

        # Remove association bewteeen ActorIdentity and UserCreentials
        log.debug("complete_account_merge: merge account data: merge_user_info_id: %s merge_user_credential_id:%s merge_actor_identity_id:%s" % (merge_user_info_id, merge_user_credential_id, merge_actor_identity_id))
        self.delete_user_credential_association(merge_user_credential_id, merge_actor_identity_id)

        # Remove the merge account ActorIdentity and merge UserInfo.
        self.delete_user_info(merge_user_info_id, merge_actor_identity_id)
        self.delete_actor_identity(merge_actor_identity_id)

        # Create association between the current user ActorIdentity and the merge user UserCredentials
        self.clients.resource_registry.create_association(current_user_id, PRED.hasCredentials, merge_user_credential_id)

        # Update the token status
        token_obj.status = "VERIFIED"
        self.update_user_info(current_user_info)
        log.debug("complete_account_merge: account merge completed from %s to %s" %(merge_actor_identity_id, current_user_id))
        return True
