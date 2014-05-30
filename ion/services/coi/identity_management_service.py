#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Stephen Henrie, Michael Meisinger'

import calendar
import copy
from datetime import datetime, timedelta
import time
from uuid import uuid4

from pyon.core.security.authentication import Authentication
from pyon.core.exception import Unauthorized
from pyon.ion.resource import ExtendedResourceContainer
from pyon.public import log, PRED, RT, IonObject, OT, Conflict, Inconsistent, NotFound, BadRequest, get_ion_ts_millis

from interface.objects import ProposalOriginatorEnum, NegotiationStatusEnum, NegotiationTypeEnum, \
    SecurityToken, TokenTypeEnum

from interface.services.coi.iidentity_management_service import BaseIdentityManagementService
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient

MAX_TOKEN_VALIDITY = 365*24*60*60


class IdentityManagementService(BaseIdentityManagementService):
    """
    Stores identities of users and resources, including bindings of internal
    identities to external identities. Also stores metadata such as a user profile.
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
        return actor_identity

    def delete_actor_identity(self, actor_id=''):
        # Delete specified ActorIdentity object
        self.clients.resource_registry.delete(actor_id)

    def find_actor_identity_by_name(self, name=''):
        """Return the ActorIdentity object whose name attribute matches the passed value.

        @param name    str
        @retval user_info    ActorIdentity
        @throws NotFound    failed to find ActorIdentity
        @throws Inconsistent    Multiple ActorIdentity objects matched name
        """
        objects, matches = self.clients.resource_registry.find_resources(RT.ActorIdentity, None, name, id_only=False)
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
        objects, matches = self.clients.resource_registry.find_resources(RT.UserCredentials, None, credentials_name, id_only=False)
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
        user_infos, _ = self.clients.resource_registry.find_resources_ext(RT.UserInfo, attr_name="contact.email", attr_value=user_email)
        if len(user_infos) > 1:
            log.warn("More than one UserInfo found for email '%s': %s" % (user_email, [ui._id for ui in user_infos]))
        if user_infos:
            return user_infos[0]
        return None

    def find_user_info_by_id(self, actor_id=''):
        # Look up UserInfo via association with ActorIdentity
        objects, assocs = self.clients.resource_registry.find_objects(actor_id, PRED.hasInfo, RT.UserInfo)
        if not objects:
            raise NotFound("UserInfo for user id %s does not exist" % actor_id)
        user_info = objects[0]
        return user_info

    def find_user_info_by_name(self, name=''):
        objects, matches = self.clients.resource_registry.find_resources(RT.UserInfo, None, name, id_only=False)
        if not objects:
            raise NotFound("UserInfo with name %s does not exist" % name)
        if len(objects) > 1:
            raise Inconsistent("Multiple UserInfo objects with name %s exist" % name)
        return objects[0]

    def find_user_info_by_subject(self, subject=''):
        # Find UserCredentials
        objects, matches = self.clients.resource_registry.find_resources(RT.UserCredentials, None, subject, id_only=False)
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
            actor_name = "Identity for %s" % subject
            actor_identity = IonObject("ActorIdentity", name=actor_name)
            actor_id = self.create_actor_identity(actor_identity)

            user_credentials = IonObject("UserCredentials", name=subject, description="Default credentials for %s" % subject)
            self.register_user_credentials(actor_id, user_credentials)
            log.debug("Signon returning actor_id, valid_until, registered: %s, %s, False" % (actor_id, valid_until))
            return actor_id, valid_until, False

    def get_user_info_extension(self, user_info_id='', org_id=''):
        """Returns an UserInfoExtension object containing additional related information

        @param user_info_id    str
        @param org_id    str  - An optional org id that the user is interested in filtering against.
        @retval user_info    UserInfoExtension
        @throws BadRequest    A parameter is missing
        @throws NotFound    An object with the specified actor_id does not exist
        """
        if not user_info_id:
            raise BadRequest("The user_info_id parameter is empty")


        #THis is a hack to get the UI going. It would be preferable to get the actor id from the extended resource
        #container below, but their would need to be a guarantee of order of field processing in order
        #to ensure that the actor identity has been found BEFORE the negotiation methods are called - and probably
        #some elegant way to indicate the field and sub field; ie actor_identity._id
        actors, _ = self.clients.resource_registry.find_subjects(subject_type=RT.ActorIdentity, predicate=PRED.hasInfo, object=user_info_id, id_only=True)
        actor_id = actors[0] if len(actors) > 0 else ''


        extended_resource_handler = ExtendedResourceContainer(self)
        extended_user = extended_resource_handler.create_extended_resource_container(
            extended_resource_type=OT.UserInfoExtension,
            resource_id=user_info_id,
            computed_resource_type=OT.ComputedAttributes,
            user_id=user_info_id,
            org_id=org_id,
            actor_id=actor_id)

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

            #filter notification requests that are retired
            extended_user.subscriptions = [nr for nr in extended_user.subscriptions if nr.temporal_bounds.end_datetime == '']

            #filter owned resources that are retired
            nr_removed = []
            for rsrc in extended_user.owned_resources:
                #remove all the Notifications
                if rsrc.type_ != OT.NotificationRequest:
                    nr_removed.append(rsrc)
            extended_user.owned_resources = [rsrc for rsrc in nr_removed if rsrc.lcstate != 'DELETED']
            #now append the active NotificationRequests
            extended_user.owned_resources.extend(extended_user.subscriptions)

        from ion.util.extresource import strip_resource_extension
        strip_resource_extension(extended_user)

        return extended_user

    def prepare_user_info_support(self, user_info_id=''):
        """
        Returns the object containing the data to create/update a user info resource
        """

        # TODO: Precondition or check: only the actor identity associated with this user or an ORG_MANAGER/ION_MANAGER
        # can edit this user info resource

        extended_resource_handler = ExtendedResourceContainer(self)

        resource_data = extended_resource_handler.create_prepare_resource_support(user_info_id, OT.UserInfoPrepareSupport)

        return resource_data

    def find_user_open_requests(self, user_info_id='', actor_id='', org_id=''):
        """
        Local function to be called by extended resource framework from get_user_info_extension operation. The first
        parameter MUST be the same user_info_id from that operation even though it is not used.

        @param user_info_id:
        @param actor_id:
        @param org_id:
        @return:
        """
        org_client = OrgManagementServiceProcessClient(process=self)

        neg_list = org_client.find_user_negotiations(actor_id=actor_id, org_id=org_id, negotiation_status=NegotiationStatusEnum.OPEN)

        return self._convert_negotiations_to_requests(neg_list, user_info_id, org_id)

    def find_user_closed_requests(self, user_info_id='', actor_id='', org_id=''):
        """
        Local function to be called by extended resource framework from get_user_info_extension operation. The first
        parameter MUST be the same user_info_id from that operation even though it is not used.
        @param user_info_id:
        @param actor_id:
        @param org_id:
        @return:
        """
        org_client = OrgManagementServiceProcessClient(process=self)

        neg_list = org_client.find_user_negotiations(actor_id=actor_id, org_id=org_id)

        #Filter out non Open negotiations
        neg_list = [neg for neg in neg_list if neg.negotiation_status != NegotiationStatusEnum.OPEN]

        return self._convert_negotiations_to_requests(neg_list, user_info_id, org_id)

    def _convert_negotiations_to_requests(self, negotiations=None, user_info_id='', org_id=''):
        assert isinstance(negotiations, list)

        orgs,_ = self.clients.resource_registry.find_resources(restype=RT.Org)

        ret_list = []
        for neg in negotiations:

            request = IonObject(OT.OrgUserNegotiationRequest, ts_updated=neg.ts_updated, negotiation_id=neg._id,
                negotiation_type=NegotiationTypeEnum._str_map[neg.negotiation_type],
                negotiation_status=NegotiationStatusEnum._str_map[neg.negotiation_status],
                originator=ProposalOriginatorEnum._str_map[neg.proposals[-1].originator],
                request_type=neg.proposals[-1].type_,
                description=neg.description, reason=neg.reason,
                user_id=user_info_id)

            # since this is a proxy for the Negotiation object, simulate its id to help the UI deal with it
            request._id = neg._id

            org_request = [ o for o in orgs if o._id == neg.proposals[-1].provider ]
            if org_request:
                request.org_id = org_request[0]._id
                request.name = org_request[0].name

            ret_list.append(request)

        return ret_list


    def delete_user_credential_association(self, user_credential_id, actor_identity_id):
        association_id = self.clients.resource_registry.find_associations(actor_identity_id, PRED.hasCredentials, user_credential_id, id_only=True)
        if not association_id:
            raise NotFound("complete_account_merge: Association between UserCredentials and ActorIdentity not found. ActorIdentity ID %s, UserCredentials ID: %s " % actor_identity_id % user_credential_id)
        self.clients.resource_registry.delete_association(association_id[0])

    # -------------------------------------------------------------------------
    # Merge account support

    def _generate_token(self):
        return str(uuid4()) + "_" + str(uuid4())

    def _get_current_user_id(self):
        ctx = self.get_context()
        return ctx.get('ion-actor-id', None) if ctx else None

    def _update_user_info_token(self, token=""):
        if not token:
            raise BadRequest("_update_user_info_token: token must be set")
        ion_actor_id = self._get_current_user_id()
        if ion_actor_id:
            current_user_info = self.find_user_info_by_id(ion_actor_id)
            current_user_info.tokens.append(token)
            self.update_user_info(current_user_info)
        else:
            raise BadRequest("_update_user_info_token: Current UserInfo not found")

    def _validate_token_string(self, token_string, user_info):
        # Find the token from the  UserInfo
        token_obj = [token for token in user_info.tokens if token.token_string == token_string]
        if not token_obj or not token_obj[0].merge_email or not token_obj[0].expires:
            raise NotFound("_validate_token: Token data not found")
        token_obj = token_obj[0]
        # Validate the expiration time and token status
        current_time = calendar.timegm((datetime.utcnow()).timetuple())
        if current_time > token_obj.expires or "OPEN" != token_obj.status:
            raise BadRequest("_validate_token: access token expired or token status is invalid")
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
        if self.find_user_info_by_id(self._get_current_user_id())._id == merge_user_info._id:
            raise BadRequest("initiate_account_merge: current and merge accounts are the same")
        token_str = self._generate_token()
        expire_time = calendar.timegm((datetime.utcnow() + timedelta(days=2)).timetuple())  # Set token expire time
        token = SecurityToken(token_string=token_str, expires=expire_time, status="OPEN", merge_email=merge_user_email)
        self._update_user_info_token(token)

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
        current_user_id = self._get_current_user_id()
        current_user_info = self.find_user_info_by_id(current_user_id)

        # Find all the necessary data of the merge account
        token_obj = self._validate_token_string(token_string, current_user_info)
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


    # -------------------------------------------------------------------------
    # Manage authentication tokens (R2 M185)

    def _generate_auth_token(self, actor_id=None, expires=""):
        token_string = uuid4().hex
        token = SecurityToken(token_type=TokenTypeEnum.ACTOR_AUTH, token_string=token_string,
                              actor_id=actor_id, expires=expires, status="OPEN")
        return token

    def create_authentication_token(self, actor_id='', start_time='', validity=0):
        """Create an authentification token for provided actor id with a given start time and validity.
        start_time defaults to current time if empty and uses a system timestamp.
        validity is in seconds and must be set.

        @param actor_id    str
        @param start_time    str
        @param validity    int
        @retval token_string    str
        @throws BadRequest    Illegal parameter type of value
        @throws NotFound    Object not found
        """
        if not actor_id:
            raise BadRequest("Must provide argument: actor_id")
        actor_obj = self.clients.resource_registry.read(actor_id)
        if actor_obj.type_ != RT.ActorIdentity:
            raise BadRequest("Illegal type for argument actor_id")
        if type(validity) not in (int, long):
            raise BadRequest("Illegal type for argument validity")
        if validity <= 0 or validity > MAX_TOKEN_VALIDITY:
            raise BadRequest("Illegal value for argument validity")
        cur_time = get_ion_ts_millis()
        if not start_time:
            start_time = cur_time
        start_time = int(start_time)
        if start_time > cur_time:
            raise BadRequest("Illegal value for start_time: Future values not allowed")
        if (start_time + 1000*validity) < cur_time:
            raise BadRequest("Illegal value for start_time: Already expired")
        expires = str(start_time + 1000*validity)

        token = self._generate_auth_token(actor_id, expires=expires)
        token_id = "token_%s" % token.token_string

        self.container.object_store.create(token, token_id)

        return token.token_string

    def read_authentication_token(self, token_string=''):
        """Returns the token object for given actor authentication token string.

        @param token_string    str
        @retval token    SecurityToken
        @throws BadRequest    Illegal parameter type of value
        @throws NotFound    Token string not found
        """
        token_id = "token_%s" % token_string
        token = self.container.object_store.read(token_id)
        if not isinstance(token, SecurityToken):
            raise Inconsistent("Token illegal type")
        return token

    def update_authentication_token(self, token=None):
        """Updates the given token.

        @param token    SecurityToken
        @throws BadRequest    Illegal parameter type of value
        @throws NotFound    Token not found
        """
        if not isinstance(token, SecurityToken):
            raise BadRequest("Illegal argument type: token")
        if token.token_type != TokenTypeEnum.ACTOR_AUTH:
            raise BadRequest("Argument token: Illegal type")
        cur_time = get_ion_ts_millis()
        token_exp = int(token.expires)
        if token_exp > cur_time + 1000*MAX_TOKEN_VALIDITY:
            raise BadRequest("Argument token: Maximum expiry extended")

        self.container.object_store.update(token)

    def invalidate_authentication_token(self, token_string=''):
        """Invalidates an authentication token, but leaves it in place for auditing purposes.

        @param token_string    str
        @throws BadRequest    Illegal parameter type of value
        @throws NotFound    Token string not found
        """
        token_id = "token_%s" % token_string
        token = self.container.object_store.read(token_id)
        if not isinstance(token, SecurityToken):
            raise Inconsistent("Token illegal type")
        if token.token_type != TokenTypeEnum.ACTOR_AUTH:
            raise BadRequest("Illegal token type")
        token.status = "INVALID"
        self.container.object_store.update(token)
        log.info("Invalidated security auth token: %s", token.token_string)

    def check_authentication_token(self, token_string=''):
        """Checks given token and returns a dict with actor id if valid.

        @param token_string    str
        @retval token_info    dict
        @throws BadRequest    Illegal parameter type of value
        @throws NotFound    Token string not found
        @throws Unauthorized    Token not valid anymore or otherwise
        """
        token_id = "token_%s" % token_string
        token = self.container.object_store.read(token_id)
        if not isinstance(token, SecurityToken):
            raise Inconsistent("Token illegal type")
        if token.token_type != TokenTypeEnum.ACTOR_AUTH:
            raise BadRequest("Illegal token type")
        if token.token_string != token_string:
            raise Inconsistent("Found token's token_string does not match")
        cur_time = get_ion_ts_millis()
        if token.status != "OPEN":
            raise Unauthorized("Token status invalid")
        if cur_time >= int(token.expires):
            raise Unauthorized("Token expired")

        token_info = dict(actor_id=token.actor_id,
                    expiry=token.expires,
                    token=token,
                    token_id=token_id)

        log.info("Authentication token %s resolved to actor %s, expiry %s", token_string, token.actor_id, token.expires)

        return token_info

    def _get_actor_authentication_tokens(self, actor_id):
        actor_tokens = []
        raise NotImplemented("TODO")
        return actor_tokens
