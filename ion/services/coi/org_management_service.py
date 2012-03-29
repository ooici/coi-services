#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'

from pyon.public import CFG, IonObject, RT, PRED

from interface.services.coi.iorg_management_service import BaseOrgManagementService
from ion.services.coi.policy_management_service import MEMBER_ROLE, MANAGER_ROLE
from pyon.core.exception import  Inconsistent, NotFound, BadRequest
from pyon.ion.directory import Directory
from pyon.event.event import EventPublisher
from pyon.util.containers import is_basic_identifier
from pyon.util.log import log
from pyon.core.governance.negotiate_request import NegotiateRequest, NegotiateRequestFactory

ROOT_ION_ORG_NAME = CFG.system.root_org

class OrgManagementService(BaseOrgManagementService):

    """
    Services to define and administer a facility (synonymous Org, community), to enroll/remove members and to provide
    access to the resources of an Org to enrolled or affiliated entities (identities). Contains contract
    and commitment repository
    """

    def on_init(self):
        self.request_handler = NegotiateRequest(self)
        self.event_pub = EventPublisher()


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

        if kwargs.has_key('user_id'):

            user_id = kwargs['user_id']

            if not user_id:
                raise BadRequest("The user_id parameter is missing")

            user = self.clients.resource_registry.read(user_id)
            if not user:
                raise NotFound("User %s does not exist" % user_id)

            parameter_objects['user'] = user


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


        if kwargs.has_key('request_id'):

            request_id = kwargs['request_id']

            if not request_id:
                raise BadRequest("The request_id parameter is missing")

            request = self.clients.resource_registry.read(request_id)
            if not request:
                raise NotFound("User Request %s does not exist" % request_id)

            parameter_objects['request'] = request

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
        """Persists the provided Org object. The id string returned
        is the internal id by which Org will be identified in the data store.

        @param org    Org
        @retval org_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """

        if not org:
            raise BadRequest("The org parameter is missing")

        #Only allow one root ION Org in the system
        if org.name == ROOT_ION_ORG_NAME:
            res_list,_  = self.clients.resource_registry.find_resources(restype=RT.Org, name=ROOT_ION_ORG_NAME)
            if len(res_list) > 0:
                raise BadRequest('There can only be one Org named %s' % ROOT_ION_ORG_NAME)

        if not is_basic_identifier(org.name):
            raise BadRequest("The Org name '%s' can only contain alphanumeric and underscore characters" % org.name)


        org_id, org_rev = self.clients.resource_registry.create(org)
        org._id = org_id
        org._rev = org_rev

        #Instantiate a Directory for this Org
        directory = Directory(orgname=org.name)

        #Instantiate initial set of User Roles for this Org
        manager_role = IonObject(RT.UserRole, name=MANAGER_ROLE,label='Org Manager', description='Org Manager')
        self.add_user_role(org_id, manager_role)

        member_role = IonObject(RT.UserRole, name=MEMBER_ROLE,label='Org Member', description='Org Member')
        self.add_user_role(org_id, member_role)

        return org_id

    def update_org(self, org=None):
        """Updates the provided Org object.  Throws NotFound exception if
        an existing version of Org is not found.  Throws Conflict if
        the provided Policy object is not based on the latest persisted
        version of the object.

        @param org    Org
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        if not org:
            raise BadRequest("The org parameter is missing")

        self.clients.resource_registry.update(org)

    def read_org(self, org_id=''):
        """Returns the Org object for the specified policy id.
        Throws exception if id does not match any persisted Policy
        objects.

        @param org_id    str
        @retval org    Org
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id)

        return param_objects['org']

    def delete_org(self, org_id=''):
        """Permanently deletes Org object with the specified
        id. Throws exception if id does not match any persisted Policy.

        @param org_id    str
        @retval success    bool
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
            name = ROOT_ION_ORG_NAME

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

        if self._find_role(org_id, user_role.name) is not None:
            raise BadRequest("The user role '%s' is already associated with this Org" % user_role.name)

        user_role.org_id = org_id
        user_role_id = self.clients.policy_management.create_role(user_role)

        aid = self.clients.resource_registry.create_association(org, PRED.hasRole, user_role_id)

        return user_role_id

    def remove_user_role(self, org_id='', role_name='', force_removal=False):
        """Removes a UserRole from an Org. The UserRole will not be removed if there are
        users associated with the UserRole unless the force_removal paramater is set to True
        Throws exception if either id does not exist.

        @param org_id    str
        @param name    str
        @param force_removal    bool
        @retval success    bool
        @throws NotFound    object with specified name does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, role_name=role_name)
        user_role = param_objects['user_role']

        if not force_removal:
            alist,_ = self.clients.resource_registry.find_subjects(RT.UserIdentity, PRED.hasRole, user_role)
            if len(alist) > 0:
                raise BadRequest('The User Role %s cannot be removed as there are %s users associated to it' %
                                 (user_role.name, str(len(alist))))

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


    def _find_role(self, org_id='', name=''):

        if not org_id:
            raise BadRequest("The org_id parameter is missing")

        if not name:
            raise BadRequest("The name parameter is missing")

        org_roles = self.find_org_roles(org_id)
        for role in org_roles:
            if role.name == name:
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


    def request_enroll(self, org_id='', user_id=''):
        """Requests for an enrollment with an Org which must be accepted or denied as a separate process.
        Membership in the ION Org is implied by registration with the system, so a membership
        association to the ION Org is not maintained. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param user_id    str
        @retval request_id    str
        @throws NotFound    object with specified id does not exist
        """

        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id)
        org = param_objects['org']

        if org.name == ROOT_ION_ORG_NAME:
            raise BadRequest("A request to enroll in the root ION Org is not allowed")

        #Initiate request
        req_obj = NegotiateRequestFactory.create_enrollment_request(org_id, user_id)

        req_id = self.request_handler.open_request(req_obj)

        return req_id


    def request_role(self, org_id='', user_id='', role_name=''):
        """Requests for an role within an Org which must be accepted or denied as a separate process.
        A role of Member is automatically implied with successfull enrollment.  Throws a
        NotFound exception if neither id is found.

        @param org_id    str
        @param user_id    str
        @param role_name    str
        @retval request_id    str
        @throws NotFound    object with specified id does not exist
        """

        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id, role_name=role_name)

        if role_name == MEMBER_ROLE:
            raise BadRequest("The Member User Role is already assigned with an enrollment to an Org")

        #Initiate request
        req_obj = NegotiateRequestFactory.create_role_request(org_id, user_id)

        req_id = self.request_handler.open_request(req_obj)

        return req_id

    def request_acquire_resource(self, org_id='', user_id='', resource_id=''):
        """Requests to acquire a resource within an Org which must be accepted or denied as a separate process.
        Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param user_id    str
        @param resource_id    str
        @retval request_id    str
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id, resource_id=resource_id)

        #Initiate request
        req_obj = NegotiateRequestFactory.create_acquire_resource(org_id, user_id, resource_id)

        req_id = self.request_handler.open_request(req_obj)

        return req_id

    def find_requests(self, org_id='', request_type='', request_status=''):
        """Returns a list of open requests for an Org. An optional request_type can be supplied
        or else all types will be returned. An optional request_status can be supplied
        or else requests will be returned. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param org_id    str
        @param request_type    str
        @param request_status    str
        @retval requests    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id)

        if request_type != '':
            request_list,_ = self.clients.resource_registry.find_objects(org_id, PRED.hasRequest, request_type)
        else:
             request_list,_ = self.clients.resource_registry.find_objects(org_id, PRED.hasRequest)

        if not request_status:
            return request_list

        return_list = []
        for req in request_list:
            if req.status == request_status:
                return_list.append(req)

        return return_list

    def approve_request(self, org_id='', request_id=''):
        """Approves a request made to an Org. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param org_id    str
        @param request_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """

        param_objects = self._validate_parameters(org_id=org_id, request_id=request_id)
        request = param_objects['request']
        self.request_handler.approve_request(request)


    def deny_request(self, org_id='', request_id='', reason=''):
        """Denys a request made to an Org. An optional reason can be recorded with the denial.
        Will throw a not NotFound exception if none of the specified ids do not exist.

        @param org_id    str
        @param request_id    str
        @param reason    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, request_id=request_id)
        request = param_objects['request']
        self.request_handler.deny_request(request, reason)


    def find_user_requests(self, user_id='', org_id='', request_type='', request_status=''):
        """Returns a list of requests for a specified User. All requests for all Orgs will be returned
        unless an org_id is specified. An optional request_type can be supplied
        or else all types will be returned. An optional request_status can be supplied
        or else requests will be returned. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param user_id    str
        @param org_id    str
        @param request_type    str
        @param request_status    str
        @retval requests    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(user_id=user_id)
        user = param_objects['user']

        ret_list = []

        if org_id:
            param_objects = self._validate_parameters(org_id=org_id)
            org = param_objects['org']

            if request_type != '':
                request_list,_ = self.clients.resource_registry.find_objects(org, PRED.hasRequest, request_type)
            else:
                request_list,_ = self.clients.resource_registry.find_objects(org, PRED.hasRequest)

            for req in request_list:
                if req.user_id == user_id:
                    ret_list.append(req)

            if not request_status:
                return ret_list

            return_req_list = []
            for req in ret_list:
                if req.status == request_status:
                    return_req_list.append(req)

            return return_req_list


        if request_type != '':
            request_list,_ = self.clients.resource_registry.find_objects(user, PRED.hasRequest, request_type)
        else:
            request_list,_ = self.clients.resource_registry.find_objects(user, PRED.hasRequest)

        if not request_status:
            return request_list

        return_list = []
        for req in request_list:
            if req.status == request_status:
                return_list.append(req)

        return return_list


    def accept_request(self, org_id='', request_id=''):
        """Accepts and invitation for a request made to an Org.  Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param org_id    str
        @param request_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, request_id=request_id)
        request = param_objects['request']
        self.request_handler.accept_request(request)

        #Since the request was accepted by the user, proceed with the defined action of the negotiation
        request = self.clients.resource_registry.read(request_id)  # Pass in the most recent version
        ret = self.request_handler.execute_accept_action(request)

        return ret

    def reject_request(self, org_id='', request_id='', reason=''):
        """Rejects an invitation made to an Org. An optional reason can be recorded with the rejection.
        Will throw a not NotFound exception if none of the specified ids do not exist.

        @param org_id    str
        @param request_id    str
        @param reason    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, request_id=request_id)
        request = param_objects['request']
        self.request_handler.reject_request(request,reason)

        return True

    def enroll_member(self, org_id='', user_id=''):
        """Enrolls a specified user into the specified Org so that they may find and negotiate to use resources
        of the Org. Membership in the ION Org is implied by registration with the system, so a membership
        association to the ION Org is not maintained. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param user_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id)
        org = param_objects['org']
        user = param_objects['user']

        if org.name == ROOT_ION_ORG_NAME:
            raise BadRequest("A request to enroll in the root ION Org is not allowed")

        aid = self.clients.resource_registry.create_association(org, PRED.hasMembership, user)

        if not aid:
            return False

        return True

    def cancel_member_enrollment(self, org_id='', user_id=''):
        """Cancels the membership of a specified user within the specified Org. Once canceled, the user will no longer
        have access to the resource of that Org. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param user_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id)
        org = param_objects['org']
        user = param_objects['user']

        if org.name == ROOT_ION_ORG_NAME:
            raise BadRequest("A request to cancel enrollment in the root ION Org is not allowed")

        #First remove all associations to any roles
        role_list = self.find_roles_by_user(org_id, user_id)
        for user_role in role_list:
            self._delete_role_association(user, user_role)

        #Finally remove the association to the Org
        aid = self.clients.resource_registry.get_association(org, PRED.hasMembership, user)
        if not aid:
            raise NotFound("The membership association between the specified user and Org is not found")

        self.clients.resource_registry.delete_association(aid)
        return True

    def is_enrolled(self, org_id='', user_id=''):
        """Returns True if the specified user_id is enrolled in the Org and False if not.
        Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param user_id    str
        @retval is_enrolled    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id)
        org = param_objects['org']
        user = param_objects['user']

        #Membership into the Root ION Org is implied as part of registration
        if org.name == ROOT_ION_ORG_NAME:
            return True

        try:
            aid = self.clients.resource_registry.get_association(org, PRED.hasMembership, user)
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
        if org.name == ROOT_ION_ORG_NAME:
            user_list,_ = self.clients.resource_registry.find_resources(RT.UserIdentity)
        else:
            user_list,_ = self.clients.resource_registry.find_objects(org, PRED.hasMembership, RT.UserIdentity)

        return user_list

    def find_enrolled_orgs(self, user_id=''):
        """Returns a list of Orgs that the user is enrolled in. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param user_id    str
        @retval org_list    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(user_id=user_id)
        user = param_objects['user']

        org_list,_ = self.clients.resource_registry.find_subjects(RT.Org,PRED.hasMembership, user )

        #Membership into the Root ION Org is implied as part of registration
        ion_org = self.find_org()
        org_list.append(ion_org)

        return org_list


    def grant_role(self, org_id='', user_id='', role_name='', scope=None):
        """Grants a defined role within an organization to a specific user. A role of Member is
        automatically implied with successfull enrollment. Will throw a not NotFound exception
        if none of the specified ids or role_name does not exist.

        @param org_id    str
        @param user_id    str
        @param role_name    str
        @param scope    RoleScope
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id, role_name=role_name)
        user = param_objects['user']
        user_role = param_objects['user_role']

        return self._add_role_association(user, user_role)

    def _add_role_association(self, user, user_role):

        aid = self.clients.resource_registry.create_association(user, PRED.hasRole, user_role)
        if not aid:
            return False

        return True

    def _delete_role_association(self, user, user_role):
        aid = self.clients.resource_registry.get_association(user, PRED.hasRole, user_role)
        if not aid:
            raise NotFound("The association between the specified User %s and User Role %s was not found" % (user._id, user_role._id))

        self.clients.resource_registry.delete_association(aid)
        return True


    def revoke_role(self, org_id='', user_id='', role_name=''):
        """Revokes a defined Role within an organization to a specific user. Will throw a not NotFound exception
        if none of the specified ids or role_name does not exist.

        @param org_id    str
        @param user_id    str
        @param role_name    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """

        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id, role_name=role_name)
        user = param_objects['user']
        user_role = param_objects['user_role']

        return self._delete_role_association(user, user_role)

    def _find_org_roles_by_user(self, org=None, user=None):

        if org is None:
            raise BadRequest("The org parameter is missing")

        if user is None:
            raise BadRequest("The user parameter is missing")

        role_list,_ = self.clients.resource_registry.find_objects(user, PRED.hasRole, RT.UserRole)

        #Iterate the list of roles associated with user and filter by the org_id. TODO - replace this when
        #better indexing/views are available in couch
        ret_list = []
        for role in role_list:
            if role.org_id == org._id:
                ret_list.append(role)

        #Because a user is enrolled with an Org then the membership role is implied - so add it to the list
        member_role = self._find_role(org._id, MEMBER_ROLE)
        if member_role is None:
            raise Inconsistent('The %s User Role is not found.' % MEMBER_ROLE)

        ret_list.append(member_role)

        return ret_list


    def find_roles_by_user(self, org_id='', user_id=''):
        """Returns a list of User Roles for a specific user in an Org.
        Will throw a not NotFound exception if either of the IDs do not exist.

        @param org_id    str
        @param user_id    str
        @retval user_role_list    list
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id)
        org = param_objects['org']
        user = param_objects['user']

        role_list = self._find_org_roles_by_user(org, user)

        return role_list


    def find_all_roles_by_user(self, user_id=''):
        """Returns a list of all User Roles roles by Org associated with the specified user.
        Will throw a not NotFound exception if either of the IDs do not exist.

        @param user_id    str
        @retval user_roles_by_org    dict
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(user_id=user_id)
        user = param_objects['user']

        ret_val = dict()

        org_list = self.find_enrolled_orgs(user_id)

        #Membership with the ION Root Org is implied thrgoun
        for org in org_list:
            role_list = self._find_org_roles_by_user(org, user)
            ret_val[org.name] = role_list

        return ret_val


    def has_permission(self, org_id='', user_id='', action_id='', resource_id=''):
        """Returns a boolean of the specified user has permission for the specified action on a specified resource. Will
        throw a NotFound exception if none of the specified ids do not exist.

        @param org_id    str
        @param user_id    str
        @param action_id    str
        @param resource_id    str
        @retval has_permission    bool
        @throws NotFound    object with specified id does not exist
        """
        raise NotImplementedError()


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
        return True

    def acquire_resource(self, org_id='', user_id='', resource_id=''):
        """Acquire the specified resource for a specified user withing the specified Org. Once shared, the resource is
        committed to the user. Throws a NotFound exception if none of the ids are found.

        @param org_id    str
        @param user_id    str
        @param resource_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id, resource_id=resource_id)

        commitment = IonObject(RT.ResourceCommitment, name='', org_id=org_id, user_id=user_id, resource_id=resource_id,
            description='Resource Commitment')

        commitment_id, commitment_rev = self.clients.resource_registry.create(commitment)
        commitment._id = commitment_id
        commitment._rev = commitment_rev
        self.clients.resource_registry.create_association(user_id, PRED.hasCommitment, commitment)
        self.clients.resource_registry.create_association(resource_id, PRED.hasCommitment, commitment)

        return True

    def release_resource(self, org_id='', user_id='', resource_id=''):
        """Release the specified resource from an existing commitment. Throws a NotFound exception if none of the ids are found.

        @param org_id    str
        @param user_id    str
        @param resource_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        param_objects = self._validate_parameters(org_id=org_id, user_id=user_id, resource_id=resource_id)

        commitment_list,_ = self.clients.resource_registry.find_objects(resource_id, PRED.hasCommitment, RT.ResourceCommitment)
        for com in commitment_list:
            if com.org_id == org_id and com.user_id == user_id:
                aid = self.clients.resource_registry.find_associations(resource_id, PRED.hasCommitment, com)
                self.clients.resource_registry.delete_association(aid[0])

                aid = self.clients.resource_registry.find_associations(user_id, PRED.hasCommitment, com)
                self.clients.resource_registry.delete_association(aid[0])
                self.clients.resource_registry.delete(com._id)

        return True

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

