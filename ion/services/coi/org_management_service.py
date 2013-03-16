#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'

from pyon.public import CFG, IonObject, RT, PRED, OT, LCS
from pyon.core.exception import  Inconsistent, NotFound, BadRequest
from pyon.ion.directory import Directory
from pyon.ion.resource import ExtendedResourceContainer
from pyon.core.registry import issubtype
from pyon.util.log import log
from pyon.event.event import EventPublisher
from pyon.util.containers import is_basic_identifier, get_ion_ts, create_basic_identifier
from pyon.core.governance.negotiation import Negotiation
from interface.objects import ProposalStatusEnum, ProposalOriginatorEnum, NegotiationStatusEnum, ComputedValueAvailability, ComputedIntValue, StatusType
from interface.services.coi.iorg_management_service import BaseOrgManagementService
from pyon.core.governance import ORG_MANAGER_ROLE, ORG_MEMBER_ROLE
from ion.services.sa.observatory.observatory_management_service import INSTRUMENT_OPERATOR_ROLE

#Supported Negotiations - perhaps move these to data at some point if there are more negotiation types and/or remove
#references to local functions to make this more dynamic
negotiation_rules = {
    OT.EnrollmentProposal: {
        'pre_conditions': ['is_registered(sap.consumer)', 'not is_enrolled(sap.provider,sap.consumer)',
                           'not is_enroll_negotiation_open(sap.provider,sap.consumer)'],
        'accept_action': 'enroll_member(sap.provider,sap.consumer)',
        'auto_accept': True
    },

    OT.RequestRoleProposal: {
        'pre_conditions': ['is_enrolled(sap.provider,sap.consumer)'],
        'accept_action': 'grant_role(sap.provider,sap.consumer,sap.role_name)',
        'auto_accept': True
    },

    OT.AcquireResourceProposal: {
        'pre_conditions': ['is_enrolled(sap.provider,sap.consumer)',
                           'has_role(sap.provider,sap.consumer,"' + INSTRUMENT_OPERATOR_ROLE + '")',
                           'is_resource_shared(sap.provider,sap.resource_id)'],
        'accept_action': 'acquire_resource(sap)',
        'auto_accept': True
    },

    OT.AcquireResourceExclusiveProposal: {
        'pre_conditions': ['is_resource_acquired(sap.consumer, sap.resource_id)',
                           'not is_resource_acquired_exclusively(sap.consumer, sap.resource_id)'],
        'accept_action': 'acquire_resource(sap)',
        'auto_accept': True
    }
}


class OrgManagementService(BaseOrgManagementService):

    """
    Services to define and administer a facility (synonymous Org, community), to enroll/remove members and to provide
    access to the resources of an Org to enrolled or affiliated entities (identities). Contains contract
    and commitment repository
    """

    def on_init(self):

        self.event_pub = EventPublisher()
        self.negotiation_handler = Negotiation(self, negotiation_rules, self.event_pub)


    def _get_root_org_name(self):

        if self.container is None or self.container.governance_controller is None:
            return CFG.get_safe('system.root_org' , "ION")

        return self.container.governance_controller.system_root_org_name

    def _validate_parameters(self, **kwargs):

        parameter_objects = dict()

        org_id = None

        if kwargs.has_key('org_id'):

            org_id = kwargs['org_id']

            if not org_id:
                raise BadRequest("The org_id parameter is missing")

            org = self.clients.resource_registry.read(org_id)
            if not org:
                raise NotFound("Org %s does not exist" % org_id)

            parameter_objects['org'] = org

        if kwargs.has_key('actor_id'):

            actor_id = kwargs['actor_id']

            if not actor_id:
                raise BadRequest("The actor_id parameter is missing")

            actor = self.clients.resource_registry.read(actor_id)
            if not actor:
                raise NotFound("Actor %s does not exist" % actor)

            parameter_objects['actor'] = actor


        if kwargs.has_key('role_name'):

            role_name = kwargs['role_name']

            if not role_name:
                raise BadRequest("The role_name parameter is missing")

            if org_id is None:
                raise BadRequest("The org_id parameter is missing")

            user_role = self._find_role(org_id, role_name)
            if user_role is None:
                raise BadRequest("The User Role '%s' does not exist for this Org" % role_name)

            parameter_objects['user_role'] = user_role


        if kwargs.has_key('resource_id'):

            resource_id = kwargs['resource_id']

            if not resource_id:
                raise BadRequest("The resource_id parameter is missing")

            resource = self.clients.resource_registry.read(resource_id)
            if not resource:
                raise NotFound("Resource %s does not exist" % resource_id)

            parameter_objects['resource'] = resource


        if kwargs.has_key('negotiation_id'):

            negotiation_id = kwargs['negotiation_id']

            if not negotiation_id:
                raise BadRequest("The negotiation_id parameter is missing")

            negotiation = self.clients.resource_registry.read(negotiation_id)
            if not negotiation:
                raise NotFound("Negotiation %s does not exist" % negotiation_id)

            parameter_objects['negotiation'] = negotiation

        if kwargs.has_key('affiliate_org_id'):

            affiliate_org_id = kwargs['affiliate_org_id']

            if not affiliate_org_id:
                raise BadRequest("The affiliate_org_id parameter is missing")

            affiliate_org = self.clients.resource_registry.read(affiliate_org_id)
            if not affiliate_org:
                raise NotFound("Org %s does not exist" % affiliate_org_id)

            parameter_objects['affiliate_org'] = affiliate_org

        return parameter_objects

    def create_org(self, org=None):
        """Creates an Org based on the provided object. The id string returned
        is the internal id by which Org will be identified in the data store.

        @param org    Org
        @retval org_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """

        if not org:
            raise BadRequest("The org parameter is missing")

        #Only allow one root ION Org in the system
        if org.name == self._get_root_org_name():
            res_list,_  = self.clients.resource_registry.find_resources(restype=RT.Org, name=self._get_root_org_name())
            if len(res_list) > 0:
                raise BadRequest('There can only be one Org named %s' % self._get_root_org_name())

        #If this governance identifier is not set, then set to a safe version of the org name.
        if not org.org_governance_name:
            org.org_governance_name = create_basic_identifier(org.name)

        if not is_basic_identifier(org.org_governance_name):
            raise BadRequest("The Org org_governance_name '%s' can only contain alphanumeric and underscore characters" % org.org_governance_name)


        org_id, org_rev = self.clients.resource_registry.create(org)
        org._id = org_id
        org._rev = org_rev

        #Instantiate a Directory for this Org
        directory = Directory(orgname=org.name)

        #Instantiate initial set of User Roles for this Org
        manager_role = IonObject(RT.UserRole, name='Observatory Administrator', governance_name=ORG_MANAGER_ROLE, description='Change Observatory Information, assign Roles, post Observatory events')
        self.add_user_role(org_id, manager_role)

        member_role = IonObject(RT.UserRole, name='Observatory Member', governance_name=ORG_MEMBER_ROLE, description='Subscribe to events, set personal preferences')
        self.add_user_role(org_id, member_role)

        return org_id

    def update_org(self, org=None):
        """Updates the Org based on provided object.

        @param org    Org
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        if not org:
            raise BadRequest("The org parameter is missing")

        #If this governance identifier is not set, then set to a safe version of the org name.
        if not org.org_governance_name:
            org.org_governance_name = create_basic_identifier(org.name)

        if not is_basic_identifier(org.org_governance_name):
            raise BadRequest("The Org org_governance_name '%s' can only contain alphanumeric and underscore characters" % org.org_governance_name)

        self.clients.resource_registry.update(org)

    def read_org(self, org_id=''):
        """Returns the Org object for the specified id.
        Throws exception if id does not match any persisted Org
        objects.

        @param org_id    str
        @retval org    Org
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id)

        return param_objects['org']

    def delete_org(self, org_id=''):
        """Permanently deletes Org object with the specified
        id. Throws exception if id does not match any persisted Org object.

        @param org_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not org_id:
            raise BadRequest("The org_id parameter is missing")

        self.clients.resource_registry.delete(org_id)

    def find_org(self, name=''):
        """Finds an Org object with the specified name. Defaults to the
        root ION object. Throws a NotFound exception if the object
        does not exist.

        @param name    str
        @retval org    Org
        @throws NotFound    Org with name does not exist
        """

        #Default to the root ION Org if not specified
        if not name:
            name = self._get_root_org_name()

        res_list,_  = self.clients.resource_registry.find_resources(restype=RT.Org, name=name)
        if not res_list:
            raise NotFound('The Org with name %s does not exist' % name )
        return res_list[0]


    def add_user_role(self, org_id='', user_role=None):
        """Adds a UserRole to an Org. Will call Policy Management Service to actually
        create the role object that is passed in, if the role by the specified
        name does not exist. Throws exception if either id does not exist.

        @param org_id    str
        @param user_role    UserRole
        @retval user_role_id    str
        @throws NotFound    object with specified name does not exist
        """

        param_objects = self._validate_parameters(org_id=org_id)
        org = param_objects['org']

        if not user_role:
            raise BadRequest("The user_role parameter is missing")

        if self._find_role(org_id, user_role.governance_name) is not None:
            raise BadRequest("The user role '%s' is already associated with this Org" % user_role.governance_name)

        user_role.org_governance_name = org.org_governance_name
        user_role_id = self.clients.policy_management.create_role(user_role)

        aid = self.clients.resource_registry.create_association(org, PRED.hasRole, user_role_id)

        return user_role_id

    def remove_user_role(self, org_id='', role_name='', force_removal=False):
        """Removes a UserRole from an Org. The UserRole will not be removed if there are
        users associated with the UserRole unless the force_removal parameter is set to True
        Throws exception if either id does not exist.

        @param org_id    str
        @param name    str
        @param force_removal    bool
        @retval success    bool
        @throws NotFound    object with specified name does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, role_name=role_name)
        org = param_objects['org']
        user_role = param_objects['user_role']

        if not force_removal:
            alist,_ = self.clients.resource_registry.find_subjects(RT.ActorIdentity, PRED.hasRole, user_role)
            if len(alist) > 0:
                raise BadRequest('The User Role %s cannot be removed as there are %s users associated to it' %
                                 (user_role.name, str(len(alist))))


        #Finally remove the association to the Org
        aid = self.clients.resource_registry.get_association(org, PRED.hasMembership, user_role)
        if not aid:
            raise NotFound("The role association between the specified Org (%s) and UserRole (%s) is not found" %
                           (org_id, user_role.name))

        self.clients.resource_registry.delete_association(aid)

        return True

    def find_org_role_by_name(self, org_id='', role_name=''):
        """Returns the User Role object for the specified name in the Org.
        Throws exception if name does not match any persisted User Role or the Org does not exist.
        objects.

        @param org_id    str
        @param name    str
        @retval user_role    UserRole
        @throws NotFound    object with specified name or if does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, role_name=role_name)
        user_role = param_objects['user_role']

        return user_role


    def _find_role(self, org_id='', role_name=''):

        if not org_id:
            raise BadRequest("The org_id parameter is missing")

        if not role_name:
            raise BadRequest("The governance_name parameter is missing")

        org_roles = self.find_org_roles(org_id)
        for role in org_roles:
            if role.governance_name == role_name:
                return role

        return None


    def find_org_roles(self, org_id=''):
        """Returns a list of roles available in an Org. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param org_id    str
        @retval user_role_list    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id)
        org = param_objects['org']

        role_list,_ = self.clients.resource_registry.find_objects(org, PRED.hasRole, RT.UserRole)

        return role_list

    def negotiate(self, sap=None):
        """A generic operation for negotiating actions with an Org, such as for enrollment, role request or to acquire a
        resource managed by the Org. The Service Agreement Proposal is used to specify conditions of the proposal as well
        as counter proposals and the Org will create Negotiation Resource to track the history and status of the negotiation.

        @param sap    ServiceAgreementProposal
        @retval sap    ServiceAgreementProposal
        @throws BadRequest    If an SAP is not provided or incomplete
        @throws Inconsistent    If an SAP has inconsistent information
        @throws NotFound    If any of the ids in the SAP do not exist
        """

        if sap is None or ( sap.type_ != OT.ServiceAgreementProposal and not issubtype(sap.type_, OT.ServiceAgreementProposal)):
            raise BadRequest('The sap parameter must be a valid Service Agreement Proposal object')

        if sap.proposal_status == ProposalStatusEnum.INITIAL:
            neg_id = self.negotiation_handler.create_negotiation(sap)

            #Synchronize the internal reference for later use
            sap.negotiation_id = neg_id

        #Get the most recent version of the Negotiation resource
        negotiation = self.negotiation_handler.read_negotiation(sap)

        #Update the Negotiation object with the latest SAP
        neg_id = self.negotiation_handler.update_negotiation(sap)

        #Get the most recent version of the Negotiation resource
        negotiation = self.clients.resource_registry.read(neg_id)

        #hardcodng some rules at the moment - could be replaced by a Rules Engine
        if sap.type_ == OT.AcquireResourceExclusiveProposal:

            if self.is_resource_acquired_exclusively(None, sap.resource_id):
                #Automatically accept the proposal for exclusive access if it is not already acquired exclusively
                provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.REJECTED, ProposalOriginatorEnum.PROVIDER)

                rejection_reason = "The resource has already been acquired exclusively"

                #Update the Negotiation object with the latest SAP
                neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap, rejection_reason)

                #Get the most recent version of the Negotiation resource
                negotiation = self.clients.resource_registry.read(neg_id)

            else:

                #Automatically reject the proposal if the exipration request is greater than 12 hours from now or 0
                cur_time = int(get_ion_ts())
                expiration = cur_time +  ( 12 * 60 * 60 * 1000 ) # 12 hours from now
                if sap.expiration == 0 or sap.expiration > expiration:
                    #Automatically accept the proposal for exclusive access if it is not already acquired exclusively
                    provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.REJECTED, ProposalOriginatorEnum.PROVIDER)

                    rejection_reason = "A proposal to acquire a resource exclusively must be more than 0 and be less than 12 hours."

                    #Update the Negotiation object with the latest SAP
                    neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap, rejection_reason)

                    #Get the most recent version of the Negotiation resource
                    negotiation = self.clients.resource_registry.read(neg_id)

                else:

                    #Automatically accept the proposal for exclusive access if it is not already acquired exclusively
                    provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)

                    #Update the Negotiation object with the latest SAP
                    neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap)

                    #Get the most recent version of the Negotiation resource
                    negotiation = self.clients.resource_registry.read(neg_id)

        #Check to see if the rules allow for auto acceptance of the negotiations - where the second party is assumed to accept if the
        #first party accepts.
        if negotiation_rules[sap.type_]['auto_accept']:

            #Automatically accept for the consumer if the Org Manager as provider accepts the proposal
            latest_sap = negotiation.proposals[-1]

            if latest_sap.proposal_status == ProposalStatusEnum.ACCEPTED and latest_sap.originator == ProposalOriginatorEnum.PROVIDER:
                consumer_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED)

                #Update the Negotiation object with the latest SAP
                neg_id = self.negotiation_handler.update_negotiation(consumer_accept_sap)

                #Get the most recent version of the Negotiation resource
                negotiation = self.clients.resource_registry.read(neg_id)

            elif latest_sap.proposal_status == ProposalStatusEnum.ACCEPTED and latest_sap.originator == ProposalOriginatorEnum.CONSUMER:
                provider_accept_sap = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)

                #Update the Negotiation object with the latest SAP
                neg_id = self.negotiation_handler.update_negotiation(provider_accept_sap)

                #Get the most recent version of the Negotiation resource
                negotiation = self.clients.resource_registry.read(neg_id)


        #Return the latest proposal
        return negotiation.proposals[-1]


    def find_org_negotiations(self, org_id='', proposal_type='', negotiation_status=''):
        """Returns a list of negotiations for an Org. An optional proposal_type can be supplied
        or else all proposals will be returned. An optional negotiation_status can be supplied
        or else all proposals will be returned. Will throw a not NotFound exception
        if any of the specified ids do not exist.

        @param org_id    str
        @param proposal_type    str
        @param negotiation_status    str
        @retval negotiation    list
        @throws NotFound    object with specified id does not exist
        """

        param_objects = self._validate_parameters(org_id=org_id)

        neg_list,_ = self.clients.resource_registry.find_objects(org_id, PRED.hasNegotiation)

        if proposal_type != '':
            neg_list = [neg for neg in neg_list if neg.proposals[0].type_ == proposal_type]

        if negotiation_status != '':
            neg_list = [neg for neg in neg_list if neg.negotiation_status == negotiation_status]

        return neg_list

    def find_org_closed_negotiations(self, org_id='', proposal_type=''):
        """Returns a list of closed negotiations for an Org - those which are Accepted or Rejected.
        Will throw a not NotFound exception if any of the specified ids do not exist.

        @param org_id    str
        @param proposal_type    str
        @retval negotiation    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id)

        neg_list,_ = self.clients.resource_registry.find_objects(org_id, PRED.hasNegotiation)

        if proposal_type != '':
            neg_list = [neg for neg in neg_list if neg.proposals[0].type_ == proposal_type]


        neg_list = [neg for neg in neg_list if neg.negotiation_status != NegotiationStatusEnum.OPEN]

        return neg_list

    def find_user_negotiations(self, actor_id='', org_id='', proposal_type='', negotiation_status=''):
        """Returns a list of negotiations for a specified Actor. All negotiations for all Orgs will be returned
        unless an org_id is specified. An optional proposal_type can be supplied
        or else all proposals will be returned. An optional negotiation_status can be provided
        or else all proposals will be returned. Will throw a not NotFound exception
        if any of the specified ids do not exist.

        @param actor_id    str
        @param org_id    str
        @param proposal_type    str
        @param negotiation_status    str
        @retval negotiation    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(actor_id=actor_id)
        actor = param_objects['actor']


        neg_list,_ = self.clients.resource_registry.find_objects(actor, PRED.hasNegotiation)

        if org_id:
            param_objects = self._validate_parameters(org_id=org_id)
            org = param_objects['org']

            neg_list = [neg for neg in neg_list if neg.proposals[0].provider == org_id]

        if proposal_type != '':
            neg_list = [neg for neg in neg_list if neg.proposals[0].type_ == proposal_type]

        if negotiation_status != '':
            neg_list = [neg for neg in neg_list if neg.negotiation_status == negotiation_status]

        return neg_list


    def enroll_member(self, org_id='', actor_id=''):
        """Enrolls a specified actor into the specified Org so that they may find and negotiate to use resources
        of the Org. Membership in the ION Org is implied by registration with the system, so a membership
        association to the ION Org is not maintained. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param actor_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id)
        org = param_objects['org']
        actor = param_objects['actor']

        if org.name == self._get_root_org_name():
            raise BadRequest("A request to enroll in the root ION Org is not allowed")

        aid = self.clients.resource_registry.create_association(org, PRED.hasMembership, actor)


        if not aid:
            return False

        member_role = self.find_org_role_by_name(org._id,ORG_MEMBER_ROLE )
        self._add_role_association(org, actor, member_role)

        self.event_pub.publish_event(event_type=OT.OrgMembershipGrantedEvent, origin=org._id, origin_type='Org',
            description='The member has enrolled in the Org', actor_id=actor._id, org_name=org.name )

        return True

    def cancel_member_enrollment(self, org_id='', actor_id=''):
        """Cancels the membership of a specific actor actor within the specified Org. Once canceled, the actor will no longer
        have access to the resource of that Org. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param actor_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id)
        org = param_objects['org']
        actor = param_objects['actor']

        if org.name == self._get_root_org_name():
            raise BadRequest("A request to cancel enrollment in the root ION Org is not allowed")

        #First remove all associations to any roles
        role_list = self.find_org_roles_by_user(org_id, actor_id)
        for user_role in role_list:
            self._delete_role_association(org, actor, user_role)

        #Finally remove the association to the Org
        aid = self.clients.resource_registry.get_association(org, PRED.hasMembership, actor)
        if not aid:
            raise NotFound("The membership association between the specified actor and Org is not found")

        self.clients.resource_registry.delete_association(aid)

        self.event_pub.publish_event(event_type=OT.OrgMembershipCancelledEvent, origin=org._id, origin_type='Org',
            description='The member has cancelled enrollment in the Org', actor_id=actor._id, org_name=org.name )

        return True

    def is_registered(self, actor_id=''):
        """Returns True if the specified actor_id is registered with the ION system; otherwise False.

        @param actor_id    str
        @retval is_registered    bool
        @throws BadRequest    if the actor_id is not specified.
        """
        if not actor_id:
            raise BadRequest("The actor_id parameter is missing")

        try:
            user = self.clients.resource_registry.read(actor_id)
            return True
        except Exception, e:
            log.error('is_registered: %s for actor_id:%s' %  (e.message, actor_id))

        return False

    def is_enrolled(self, org_id='', actor_id=''):
        """Returns True if the specified actor_id is enrolled in the Org and False if not.
        Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param actor_id    str
        @retval is_enrolled    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id)
        org = param_objects['org']
        actor = param_objects['actor']

        #Membership into the Root ION Org is implied as part of registration
        if org.name == self._get_root_org_name():
            return True

        try:
            aid = self.clients.resource_registry.get_association(org, PRED.hasMembership, actor)
        except NotFound, e:
            return False

        return True


    def find_enrolled_users(self, org_id=''):
        """Returns a list of users enrolled in an Org. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param org_id    str
        @retval user_list    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id)
        org = param_objects['org']

        #Membership into the Root ION Org is implied as part of registration
        if org.name == self._get_root_org_name():
            user_list,_ = self.clients.resource_registry.find_resources(RT.ActorIdentity)
        else:
            user_list,_ = self.clients.resource_registry.find_objects(org, PRED.hasMembership, RT.ActorIdentity)

        return user_list

    def find_enrolled_orgs(self, actor_id=''):
        """Returns a list of Orgs that the actor is enrolled in. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param actor_id    str
        @retval org_list    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(actor_id=actor_id)
        actor = param_objects['actor']

        org_list,_ = self.clients.resource_registry.find_subjects(RT.Org,PRED.hasMembership, actor )

        #Membership into the Root ION Org is implied as part of registration
        ion_org = self.find_org()
        org_list.append(ion_org)

        return org_list


    def grant_role(self, org_id='', actor_id='', role_name='', scope=None):
        """Grants a defined role within an organization to a specific actor. A role of Member is
        automatically implied with successful enrollment. Will throw a not NotFound exception
        if none of the specified ids or role_name does not exist.

        @param org_id    str
        @param actor_id    str
        @param role_name    str
        @param scope    RoleScope
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id, role_name=role_name)
        org = param_objects['org']
        actor = param_objects['actor']
        user_role = param_objects['user_role']

        if not self.is_enrolled(org_id,actor_id):
            raise BadRequest("The actor is not a member of the specified Org (%s)" % org.name)

        ret = self._add_role_association(org, actor, user_role)

        return ret


    def _add_role_association(self, org, actor, user_role):

        aid = self.clients.resource_registry.create_association(actor, PRED.hasRole, user_role)
        if not aid:
            return False

        self.event_pub.publish_event(event_type=OT.UserRoleGrantedEvent, origin=org._id, origin_type='Org', sub_type=user_role.governance_name,
            description='Granted the %s role' % user_role.name,
            actor_id=actor._id, role_name=user_role.governance_name, org_name=org.name )

        return True

    def _delete_role_association(self, org, actor, user_role):
        aid = self.clients.resource_registry.get_association(actor, PRED.hasRole, user_role)
        if not aid:
            raise NotFound("The association between the specified ActorIdentity %s and User Role %s was not found" % (actor._id, user_role._id))

        self.clients.resource_registry.delete_association(aid)

        self.event_pub.publish_event(event_type=OT.UserRoleRevokedEvent, origin=org._id, origin_type='Org', sub_type=user_role.governance_name,
            description='Revoked the %s role' % user_role.name,
            actor_id=actor._id, role_name=user_role.governance_name, org_name=org.name )

        return True

    def revoke_role(self, org_id='', actor_id='', role_name=''):
        """Revokes a defined Role within an organization to a specific actor. Will throw a not NotFound exception
        if none of the specified ids or role_name does not exist.

        @param org_id    str
        @param actor_id    str
        @param role_name    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """

        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id, role_name=role_name)
        org = param_objects['org']
        actor = param_objects['actor']
        user_role = param_objects['user_role']

        ret = self._delete_role_association(org, actor, user_role)

        return ret

    def has_role(self, org_id='', actor_id='', role_name=''):
        """Returns True if the specified actor_id has the specified role_name in the Org and False if not.
        Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param actor_id    str
        @param role_name    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id, role_name=role_name)
        org = param_objects['org']
        actor = param_objects['actor']

        role_list = self._find_org_roles_by_user(org, actor)

        for role in role_list:
            if role.governance_name == role_name:
                return True

        return False

    def _find_org_roles_by_user(self, org=None, actor=None):

        if org is None:
            raise BadRequest("The org parameter is missing")

        if actor is None:
            raise BadRequest("The actor parameter is missing")

        role_list,_ = self.clients.resource_registry.find_objects(actor, PRED.hasRole, RT.UserRole)

        #Iterate the list of roles associated with user and filter by the org_id. TODO - replace this when
        #better indexing/views are available in couch
        ret_list = []
        for role in role_list:
            if role.org_governance_name == org.org_governance_name:
                ret_list.append(role)

        if org.org_governance_name == self.container.governance_controller.system_root_org_name:

            #Because a user is automatically enrolled with the ION Org then the membership role is implied - so add it to the list
            member_role = self._find_role(org._id, ORG_MEMBER_ROLE)
            if member_role is None:
                raise Inconsistent('The %s User Role is not found.' % ORG_MEMBER_ROLE)

            ret_list.append(member_role)

        return ret_list


    def find_org_roles_by_user(self, org_id='', actor_id=''):
        """Returns a list of User Roles for a specific actor in an Org.
        Will throw a not NotFound exception if either of the IDs do not exist.

        @param org_id    str
        @param actor_id    str
        @retval user_role_list    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id)
        org = param_objects['org']
        actor = param_objects['actor']

        role_list = self._find_org_roles_by_user(org, actor)

        return role_list


    def find_all_roles_by_user(self, actor_id=''):
        """Returns a list of all User Roles roles by Org associated with the specified actor.
        Will throw a not NotFound exception if either of the IDs do not exist.

        @param actor_id    str
        @retval user_roles_by_org    dict
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(actor_id=actor_id)
        actor = param_objects['actor']

        ret_val = dict()

        org_list = self.find_enrolled_orgs(actor_id)

        #Membership with the ION Root Org is implied
        for org in org_list:
            role_list = self._find_org_roles_by_user(org, actor)
            ret_val[org.org_governance_name] = role_list

        return ret_val




    def share_resource(self, org_id='', resource_id=''):
        """Share a specified resource with the specified Org. Once shared, the resource will be added to a directory
        of available resources within the Org. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param resource_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, resource_id=resource_id)
        org = param_objects['org']
        resource = param_objects['resource']

        aid = self.clients.resource_registry.create_association(org, PRED.hasResource, resource)
        if not aid:
            return False

        self.event_pub.publish_event(event_type=OT.ResourceSharedEvent, origin=org._id, origin_type='Org', sub_type=resource.type_,
            description='The resource has been shared in the Org', resource_id=resource_id, org_name=org.name )

        return True


    def unshare_resource(self, org_id='', resource_id=''):
        """Unshare a specified resource with the specified Org. Once unshared, the resource will be removed from a directory
        of available resources within the Org. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param resource_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, resource_id=resource_id)
        org = param_objects['org']
        resource = param_objects['resource']

        aid = self.clients.resource_registry.get_association(org, PRED.hasResource, resource)
        if not aid:
            raise NotFound("The shared association between the specified resource and Org is not found")

        self.clients.resource_registry.delete_association(aid)

        self.event_pub.publish_event(event_type=OT.ResourceUnsharedEvent, origin=org._id, origin_type='Org', sub_type=resource.type_,
            description='The resource has been unshared in the Org', resource_id=resource_id, org_name=org.name )

        return True

    def is_resource_shared(self, org_id='', resource_id=''):

        if not org_id:
            raise BadRequest("The org_id parameter is missing")

        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        try:
            res_list,_ = self.clients.resource_registry.find_objects(org_id, PRED.hasResource)

            if res_list:
                for res in res_list:
                    if res._id == resource_id:
                        return True

        except Exception, e:
            log.error('is_resource_shared: %s for org_id:%s and resource_id:%s' %  (e.message, org_id, resource_id))

        return False


    def acquire_resource(self, sap=None):
        """Creates a Commitment Resource for the specified resource for a specified user withing the specified Org as defined in the
        proposal. Once shared, the resource is committed to the user. Throws a NotFound exception if none of the ids are found.

        @param proposal    AcquireResourceProposal
        @retval commitment_id    str
        @throws NotFound    object with specified id does not exist
        """

        if not sap:
            raise BadRequest("The sap parameter is missing")

        if sap.type_ == OT.AcquireResourceExclusiveProposal:
            exclusive = True
        else:
            exclusive = False

        commitment_id = self.create_resource_commitment(sap.provider, sap.consumer, sap.resource_id, exclusive, sap.expiration)

        #Create association between the Commitment and the Negotiation objects
        self.clients.resource_registry.create_association(sap.negotiation_id, PRED.hasContract, commitment_id)

        return commitment_id

    def create_resource_commitment(self, org_id='', actor_id='', resource_id='', exclusive=False, expiration=0):
        """Creates a Commitment Resource for the specified resource for a specified actor withing the specified Org. Once shared,
        the resource is committed to the actor. Throws a NotFound exception if none of the ids are found.

        @param org_id    str
        @param actor_id    str
        @param resource_id    str
        @param exclusive    bool
        @param expiration    int
        @retval commitment_id    str
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, actor_id=actor_id, resource_id=resource_id)
        org = param_objects['org']
        actor = param_objects['actor']
        resource = param_objects['resource']

        res_commitment = IonObject(OT.ResourceCommitment, resource_id=resource_id, exclusive=exclusive)

        commitment = IonObject(RT.Commitment, name='', provider=org_id, consumer=actor_id, commitment=res_commitment,
             description='Resource Commitment', expiration=expiration)

        commitment_id, commitment_rev = self.clients.resource_registry.create(commitment)
        commitment._id = commitment_id
        commitment._rev = commitment_rev

        #Creating associations to all related objects
        self.clients.resource_registry.create_association(org_id, PRED.hasCommitment, commitment_id)
        self.clients.resource_registry.create_association(actor_id, PRED.hasCommitment, commitment_id)
        self.clients.resource_registry.create_association(resource_id, PRED.hasCommitment, commitment_id)

        self.event_pub.publish_event(event_type=OT.ResourceCommitmentCreatedEvent, origin=org_id, origin_type='Org', sub_type=resource.type_,
            description='The resource has been committed by the Org', resource_id=resource_id, org_name=org.name,
            commitment_id=commitment._id, commitment_type=commitment.commitment.type_)

        return commitment_id

    def release_commitment(self, commitment_id=''):
        """Release the commitment that was created for resources. Throws a NotFound exception if none of the ids are found.

        @param commitment_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        if not commitment_id:
            raise BadRequest("The commitment_id parameter is missing")

        self.clients.resource_registry.retire(commitment_id)

        commitment = self.clients.resource_registry.read(commitment_id)

        self.event_pub.publish_event(event_type=OT.ResourceCommitmentReleasedEvent, origin=commitment.provider, origin_type='Org', sub_type='',
            description='The resource has been uncommitted by the Org', resource_id=commitment.commitment.resource_id,
            commitment_id=commitment._id, commitment_type=commitment.commitment.type_ )

        return True


    def is_resource_acquired(self, actor_id='', resource_id=''):
        """Returns True if the specified resource_id has been acquired. The actor_id is optional, as the operation can
        return True if the resource is acquired by any actor or specifically by the specified actor_id, otherwise
        False is returned.

        @param actor_id    str
        @param resource_id    str
        @retval success    bool
        @throws BadRequest    if resource_id is not specified
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        try:
            cur_time = int(get_ion_ts())
            commitments,_ = self.clients.resource_registry.find_objects(resource_id,PRED.hasCommitment, RT.Commitment)
            if commitments:
                for com in commitments:
                    if com.lcstate == LCS.RETIRED: #TODO remove when RR find_objects does not include retired objects
                        continue

                    #If the expiration is not 0 make sure it has not expired
                    if ( actor_id is None or com.consumer == actor_id) and (( com.expiration == 0 ) or (com.expiration > 0 and cur_time < com.expiration)):
                        return True

        except Exception, e:
            log.error('is_resource_acquired: %s for actor_id:%s and resource_id:%s' %  (e.message, actor_id, resource_id))

        return False

    def is_resource_acquired_exclusively(self, actor_id='', resource_id=''):
        """Returns True if the specified resource_id has been acquired exclusively. The actor_id is optional, as the operation can
        return True if the resource is acquired exclusively by any actor or specifically by the specified actor_id,
        otherwise False is returned.

        @param actor_id    str
        @param resource_id    str
        @retval success    bool
        @throws BadRequest    if resource_id is not specified
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        try:
            cur_time = int(get_ion_ts())
            commitments,_ = self.clients.resource_registry.find_objects(resource_id,PRED.hasCommitment, RT.Commitment)
            if commitments:
                for com in commitments:
                    if com.lcstate == LCS.RETIRED: #TODO remove when RR find_objects does not include retired objects
                        continue

                    #If the expiration is not 0 make sure it has not expired
                    if ( actor_id is None or actor_id == com.consumer )  and com.commitment.exclusive and\
                       com.expiration > 0 and cur_time < com.expiration:
                        return True

        except Exception, e:
            log.error('is_resource_acquired_exclusively: %s for actor_id:%s and resource_id:%s' %  (e.message, actor_id, resource_id))

        return False

    def is_in_org(self, container):

        container_list,_ = self.clients.resource_registry.find_subjects(RT.Org, PRED.hasResource, container)
        if container_list:
            return True

        return False

    def find_org_containers(self, org_id=''):
        """Returns a list of containers associated with an Org. Will throw a not NotFound exception
        if the specified id does not exist.

        @param org_id    str
        @retval container_list    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id)
        org = param_objects['org']

        #Containers in the Root ION Org are implied
        if org.org_governance_name == self._get_root_org_name():
            container_list,_ = self.clients.resource_registry.find_resources(RT.CapabilityContainer)
            container_list[:] = [container for container in container_list if not self.is_in_org(container)]
        else:
            container_list,_ = self.clients.resource_registry.find_objects(org, PRED.hasResource, RT.CapabilityContainer)

        return container_list

    def affiliate_org(self, org_id='', affiliate_org_id=''):
        """Creates an association between multiple Orgs as an affiliation
        so that they may coordinate activities between them.
        Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param affiliate_org_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, affiliate_org_id=affiliate_org_id)
        org = param_objects['org']
        affiliate_org = param_objects['affiliate_org']

        aid = self.clients.resource_registry.create_association(org, PRED.affiliatedWith, affiliate_org)
        if not aid:
            return False

        return True


    def unaffiliate_org(self, org_id='', affiliate_org_id=''):
        """Removes an association between multiple Orgs as an affiliation.
        Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param affiliate_org_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, affiliate_org_id=affiliate_org_id)
        org = param_objects['org']
        affiliate_org = param_objects['affiliate_org']

        aid = self.clients.resource_registry.get_association(org, PRED.affiliatedWith, affiliate_org)
        if not aid:
            raise NotFound("The affiliation association between the specified Orgs is not found")

        self.clients.resource_registry.delete_association(aid)
        return True

    #Local helper functions are below - do not remove them

    def is_enroll_negotiation_open(self, org_id, actor_id):

        try:
            neg_list = self.find_user_negotiations(actor_id, org_id, proposal_type=OT.EnrollmentProposal, negotiation_status=NegotiationStatusEnum.OPEN )

            if neg_list:
                return True

        except Exception, e:
            log.error('is_enroll_negotiation_open: %s for org_id:%s and actor_id:%s' %  (e.message, org_id, actor_id))

        return False




    #-----------------------------------------------
    #  COMPUTED RESOURCES
    #-----------------------------------------------
    def get_marine_facility_extension(self, org_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Returns an MarineFacilityOrgExtension object containing additional related information

        @param org_id    str
        @param ext_associations    dict
        @param ext_exclude    list
        @retval observatory    ObservatoryExtension
        @throws BadRequest    A parameter is missing
        @throws NotFound    An object with the specified observatory_id does not exist
        """

        if not org_id:
            raise BadRequest("The org_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_org = extended_resource_handler.create_extended_resource_container(
            extended_resource_type=OT.MarineFacilityOrgExtension,
            resource_id=org_id,
            computed_resource_type=OT.MarineFacilityOrgComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id)


        #Fill out service request information for requesting data products
        extended_org.data_products_request.service_name = 'resource_registry'
        extended_org.data_products_request.service_operation = 'find_objects'
        extended_org.data_products_request.request_parameters = {
            'subject': org_id,
            'predicate': 'hasResource',
            'object_type': 'DataProduct',
            'id_only': False,
            'limit': 10,
            'skip': 0
            }

        #Fill out service request information for requesting open negotiations
        extended_org.open_negotiations_request.service_name = 'org_management'
        extended_org.open_negotiations_request.service_operation = 'find_org_negotiations'
        extended_org.open_negotiations_request.request_parameters = {
            'org_id': org_id,
            'negotiation_status': 0
        }

        #Fill out service request information for requesting open negotiations
        extended_org.closed_negotiations_request.service_name = 'org_management'
        extended_org.closed_negotiations_request.service_operation = 'find_org_closed_negotiations'
        extended_org.closed_negotiations_request.request_parameters = {
            'org_id': org_id,
        }

        # set org members from the ION org
        ion_org = self.find_org()
        if org_id == ion_org._id:

            # clients.resource_registry may return us the container's resource_registry instance
            self._rr = self.clients.resource_registry
            log.debug("get_marine_facility_extension: self._rr:  %s ", str(self._rr))

            actors_list = self.find_enrolled_users(org_id)
            log.debug("get_marine_facility_extension: actors_list:  %s ", str(actors_list))
            for actor in actors_list:
                log.debug("get_marine_facility_extension: actor:  %s ", str(actor))
                user_info_objs, _ = self._rr.find_objects(subject=actor._id, predicate=PRED.hasInfo, object_type=RT.UserInfo, id_only=False)
                if user_info_objs:
                    log.debug("get_marine_facility_extension: user_info_obj  %s ", str(user_info_objs[0]))
                    extended_org.members.append( user_info_objs[0] )
        else:
            #For all other Orgs, flatten out the list of lists returned from the extended resource framework.
            if extended_org.members:
                user_info_list = list()
                for m in extended_org.members:
                    for u in m:
                        user_info_list.append(u)

                extended_org.members = user_info_list


        instruments_not_deployed = []
        #compute the non deployed devices
        if hasattr(extended_org, 'instruments') and hasattr(extended_org, 'instruments_deployed') :
            # a compound assoc returns a list of lists but only one hasDevice assoc is permitted between
            #     a site and a device so get the only element from inside this list
            extended_org.instruments_deployed = [d[0] for d in extended_org.instruments_deployed
                                                 if len(d) and hasattr(d, "type_") and d.type_ == RT.InstrumentDevice]

#            instruments_not_deployed = [x for x in extended_org.instruments
#                                        if x not in extended_org.instruments_deployed]

        platforms_not_deployed = []
        if hasattr(extended_org, 'platforms') and hasattr(extended_org, 'platforms_deployed'):
            # a compound assoc returns a list of lists but only one hasDevice assoc is permitted between
            #     a site and a device so get the only element from inside this list
            extended_org.platforms_deployed = [d[0] for d in extended_org.platforms_deployed
                                                 if len(d) and hasattr(d, "type_") and d.type_ == RT.PlatformDevice]

#            platforms_not_deployed = [x for x in extended_org.platforms
#                                      if x not in extended_org.platforms_deployed]

        open_negotiations = []
        #filer out the accepted/rejected negotiations and place in closed_negotiations
        if hasattr(extended_org, 'open_negotiations'):
            for negotiation in extended_org.open_negotiations:
                if negotiation.negotiation_status == NegotiationStatusEnum.OPEN:
                    open_negotiations.append(negotiation)
                elif negotiation.negotiation_status == NegotiationStatusEnum.ACCEPTED or \
                     negotiation.negotiation_status == NegotiationStatusEnum.REJECTED:
                    extended_org.closed_negotiations.append(negotiation)
            extended_org.open_negotiations = open_negotiations

        # Status computation
        from ion.services.sa.observatory.observatory_util import ObservatoryUtil

        # lookup all hasModel predicates
        # lookup is a 2d associative array of [subject type][subject id] -> object id (model)
        lookup = dict([(rt, {}) for rt in [RT.InstrumentDevice, RT.PlatformDevice]])
        for a in self.clients.resource_registry.find_associations(predicate=PRED.hasModel, id_only=False):
            if a.st in lookup:
                lookup[a.st][a.s] = a.o

        def retrieve_model_objs(rsrc_list, object_type):
            # rsrc_list is devices that need models looked up.  object_type is the resource type (a device)
            # not all devices have models (represented as None), which kills read_mult.  so, extract the models ids,
            #  look up all the model ids, then create the proper output
            model_list = [lookup[object_type].get(r._id) for r in rsrc_list]
            model_uniq = list(set([m for m in model_list if m is not None]))
            model_objs = self.clients.resource_registry.read_mult(model_uniq)
            model_dict = dict(zip(model_uniq, model_objs))
            return [model_dict.get(m) for m in model_list]

        extended_org.instrument_models = retrieve_model_objs(extended_org.instruments, RT.InstrumentDevice)
        extended_org.platform_models = retrieve_model_objs(extended_org.platforms, RT.PlatformDevice)


        s_unknown = StatusType.STATUS_UNKNOWN

        # initialize computed attributes
        extended_org.computed.instrument_status = [s_unknown] * len(extended_org.instruments)
        extended_org.computed.platform_status   = [s_unknown] * len(extended_org.platforms)
        extended_org.computed.site_status       = [s_unknown] * len(extended_org.sites)

        # shortcut constructor for default value
        def status_unknown():
            return ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=StatusType.STATUS_UNKNOWN)

        extended_org.computed.communications_status_roll_up = status_unknown()
        extended_org.computed.power_status_roll_up          = status_unknown()
        extended_org.computed.data_status_roll_up           = status_unknown()
        extended_org.computed.location_status_roll_up       = status_unknown()
        extended_org.computed.aggregated_status             = status_unknown()


        # calculate computed attributes
        try:
            outil = ObservatoryUtil(self)
            status_rollups = outil.get_status_roll_ups(org_id, extended_org.resource._get_type())
            extended_org.computed.instrument_status = [status_rollups.get(idev._id, {}).get("agg", s_unknown)
                                                       for idev in extended_org.instruments]
            extended_org.computed.platform_status   = [status_rollups.get(pdev._id, {}).get("agg", s_unknown)
                                                       for pdev in extended_org.platforms]
            extended_org.computed.site_status       = [status_rollups.get(site._id, {}).get("agg", s_unknown)
                                                       for site in extended_org.sites]
        except Exception as ex:
            log.exception("Computed attribute failed for org %s" % org_id)


        # shortcut constructor for computed int value
        def short_status_rollup(key):
            return ComputedIntValue(status=ComputedValueAvailability.PROVIDED,
                                    value=status_rollups.get(org_id, {}).get(key, s_unknown))

        extended_org.computed.communications_status_roll_up = short_status_rollup("comms")
        extended_org.computed.power_status_roll_up          = short_status_rollup("power")
        extended_org.computed.data_status_roll_up           = short_status_rollup("data")
        extended_org.computed.location_status_roll_up       = short_status_rollup("loc")
        extended_org.computed.aggregated_status             = short_status_rollup("agg")


        #set counter attributes
        extended_org.number_of_platforms            = len(extended_org.platforms)
        extended_org.number_of_platforms_deployed   = len(extended_org.platforms_deployed)
        extended_org.number_of_instruments          = len(extended_org.instruments)
        extended_org.number_of_instruments_deployed = len(extended_org.instruments_deployed)
        extended_org.number_of_data_products        = len(extended_org.data_products)

        return extended_org


