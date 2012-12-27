#!/usr/bin/env python



__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.coi.ipolicy_management_service import BasePolicyManagementService
from pyon.core.exception import NotFound, BadRequest, Inconsistent
from pyon.public import PRED, RT, Container, CFG, OT, IonObject
from pyon.util.containers import is_basic_identifier
from pyon.util.log import log
from pyon.event.event import EventPublisher
from pyon.ion.endpoint import ProcessEventSubscriber


class PolicyManagementService(BasePolicyManagementService):

    def __init__(self, *args, **kwargs):
        BasePolicyManagementService.__init__(self,*args,**kwargs)

        self.event_pub = None  # For unit tests


    def on_start(self):
        self.event_pub = EventPublisher()

        self.policy_event_subscriber = ProcessEventSubscriber(event_type="ResourceModifiedEvent", origin_type="Policy", callback=self._policy_event_callback, process=self)
        self._process.add_endpoint(self.policy_event_subscriber)

    """Provides the interface to define and manage policy and a repository to store and retrieve policy
    and templates for policy definitions, aka attribute authority.

    @see https://confluence.oceanobservatories.org/display/syseng/CIAD+COI+OV+Policy+Management+Service
    """

    def create_resource_access_policy(self, resource_id='', policy_name='', description='', policy_rule=''):
        """Helper operation for creating an access policy for a specific resource. The id string returned
        is the internal id by which Policy will be identified in the data store.

        @param resource_id    str
        @param policy_name    str
        @param description    str
        @param policy_rule    str
        @retval policy_id    str
        @throws BadRequest    If any of the paramaters are not set.
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        if not policy_name:
            raise BadRequest("The policy_name parameter is missing")

        if not description:
            raise BadRequest("The description parameter is missing")

        if not policy_rule:
            raise BadRequest("The policy_rule parameter is missing")


        service_policy_obj = IonObject(OT.ResourceAccessPolicy, policy_rule=policy_rule)

        policy_obj = IonObject(RT.Policy, name=policy_name, description=description, policy_type=service_policy_obj)

        policy_id = self.create_policy(policy_obj)

        self.add_resource_policy(resource_id, policy_id)

        return policy_id

    def create_service_access_policy(self, service_name='', policy_name='', description='', policy_rule=''):
        """Helper operation for creating an access policy for a specific service. The id string returned
        is the internal id by which Policy will be identified in the data store.

        @param service_name    str
        @param policy_name    str
        @param description    str
        @param policy_rule    str
        @retval policy_id    str
        @throws BadRequest    If any of the paramaters are not set.
        """
        if not service_name:
            raise BadRequest("The service_name parameter is missing")

        if not policy_name:
            raise BadRequest("The policy_name parameter is missing")

        if not description:
            raise BadRequest("The description parameter is missing")

        if not policy_rule:
            raise BadRequest("The policy_rule parameter is missing")


        service_policy_obj = IonObject(OT.ServiceAccessPolicy, policy_rule=policy_rule, service_name=service_name)

        policy_obj = IonObject(RT.Policy, name=policy_name, description=description, policy_type=service_policy_obj)

        return self.create_policy(policy_obj)

    def create_common_service_access_policy(self, policy_name='', description='', policy_rule=''):
        """Helper operation for creating a service access policy common to all services. The id string returned
        is the internal id by which Policy will be identified in the data store.

        @param policy_name    str
        @param description    str
        @param policy_rule    str
        @retval policy_id    str
        @throws BadRequest    If any of the paramaters are not set.
        """

        if not policy_name:
            raise BadRequest("The policy_name parameter is missing")

        if not description:
            raise BadRequest("The description parameter is missing")

        if not policy_rule:
            raise BadRequest("The policy_rule parameter is missing")


        service_policy_obj = IonObject(OT.CommonServiceAccessPolicy, policy_rule=policy_rule)

        policy_obj = IonObject(RT.Policy, name=policy_name, description=description, policy_type=service_policy_obj)

        return self.create_policy(policy_obj)


    def add_process_operation_precondition_policy(self, process_name='', op='', policy_content=''):
        """Helper operation for adding a precondition policy for a specific process operation; could be a service or agent.
        The id string returned is the internal id by which Policy will be identified in the data store. The precondition
        method must return a tuple (boolean, string).

        @param process_name    str
        @param op    str
        @param policy_content    str
        @retval policy_id    str
        @throws BadRequest    If any of the parameters are not set.
        """
        if not process_name:
            raise BadRequest("The process_name parameter is missing")

        if not op:
            raise BadRequest("The op parameter is missing")

        if not policy_content:
            raise BadRequest("The policy_content parameter is missing")

        policy_name = process_name + "_" + op + "_Precondition_Policies"

        policies,_ = self.clients.resource_registry.find_resources(restype=RT.Policy, name=policy_name)
        if policies:
            #Update existing policy by adding to list
            if len(policies) > 1:
                raise Inconsistent('There should only be one Policy object per process_name operation')

            if policies[0].policy_type.op != op or  policies[0].policy_type.type_ != OT.ProcessOperationPreconditionPolicy:
                raise Inconsistent('There Policy object %s does not match the requested process operation %s: %s' % ( policies[0].name, process_name, op ))

            policies[0].policy_type.preconditions.append(policy_content)

            self.update_policy(policies[0])

            return policies[0]._id

        else:
            #Create a new policy object

            op_policy_obj = IonObject(OT.ProcessOperationPreconditionPolicy,  process_name=process_name, op=op)
            op_policy_obj.preconditions.append(policy_content)

            policy_obj = IonObject(RT.Policy, name=policy_name, policy_type=op_policy_obj, description='List of operation precondition policies for ' + process_name)

            return self.create_policy(policy_obj)



    def create_policy(self, policy=None):
        """Persists the provided Policy object The id string returned
        is the internal id by which Policy will be identified in the data store.

        @param policy    Policy
        @retval policy_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        if not is_basic_identifier(policy.name):
            raise BadRequest("The policy name '%s' can only contain alphanumeric and underscore characters" % policy.name)

        #If there is a policy_rule field then try to add the policy name and decription to the rule text
        if hasattr(policy.policy_type, 'policy_rule'):
            policy.policy_type.policy_rule = policy.policy_type.policy_rule % (policy.name, policy.description)

        policy_id, version = self.clients.resource_registry.create(policy)

        log.debug('Policy created: ' + policy.name)

        return policy_id

    def update_policy(self, policy=None):
        """Updates the provided Policy object.  Throws NotFound exception if
        an existing version of Policy is not found.  Throws Conflict if
        the provided Policy object is not based on the latest persisted
        version of the object.

        @param policy    Policy
        @throws NotFound    object with specified id does not exist
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws Conflict    object not based on latest persisted object version
        """
        if not is_basic_identifier(policy.name):
            raise BadRequest("The policy name '%s' can only contain alphanumeric and underscore characters" % policy.name)

        self.clients.resource_registry.update(policy)

    def read_policy(self, policy_id=''):
        """Returns the Policy object for the specified policy id.
        Throws exception if id does not match any persisted Policy
        objects.

        @param policy_id    str
        @retval policy    Policy
        @throws NotFound    object with specified id does not exist
        """
        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)
        return policy

    def delete_policy(self, policy_id=''):
        """For now, permanently deletes Policy object with the specified
        id. Throws exception if id does not match any persisted Policy.

        @param policy_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)

        res_list = self._find_resources_for_policy(policy_id)
        for res in res_list:
            self._remove_resource_policy(res, policy)

        self.clients.resource_registry.delete(policy_id)

        #Force a publish since the policy object will have been deleted
        self._publish_policy_event(policy)

    def enable_policy(self, policy_id=''):
        """Sets a flag to enable the use of the policy

        @param policy_id    str
        @throws NotFound    object with specified id does not exist
        """
        policy = self.read_policy(policy_id)
        policy.enabled = True
        self.update_policy(policy)


    def disable_policy(self, policy_id=''):
        """Resets a flag to disable the use of the policy

        @param policy_id    str
        @throws NotFound    object with specified id does not exist
        """
        policy = self.read_policy(policy_id)
        policy.enabled = False
        self.update_policy(policy)



    def add_resource_policy(self, resource_id='', policy_id=''):
        """Associates a policy to a specific resource

        @param resource_id    str
        @param policy_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """

        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)

        self._add_resource_policy(resource, policy)

        return True


    def remove_resource_policy(self, resource_id='', policy_id=''):
        """Removes an association for a policy to a specific resource

        @param resource_id    str
        @param policy_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        if not policy_id:
            raise BadRequest("The policy_id parameter is missing")

        policy = self.clients.resource_registry.read(policy_id)
        if not policy:
            raise NotFound("Policy %s does not exist" % policy_id)

        self._remove_resource_policy(resource, policy)

        return True

    #Internal helper function for removing a policy resource association and publish event for containers to update
    def _add_resource_policy(self, resource, policy):
        aid = self.clients.resource_registry.create_association(resource, PRED.hasPolicy, policy)
        if not aid:
            return False

        #Publish an event that the resource policy has changed
        self._publish_resource_policy_event(policy, resource)

        return True

    def _remove_resource_policy(self, resource, policy):
        aid = self.clients.resource_registry.get_association(resource, PRED.hasPolicy, policy)
        if not aid:
            raise NotFound("The association between the specified Resource %s and Policy %s was not found" % (resource._id, policy._id))

        self.clients.resource_registry.delete_association(aid)

        #Publish an event that the resource policy has changed
        self._publish_resource_policy_event(policy, resource)



    def _policy_event_callback(self, *args, **kwargs):
        """
        This method is a callback function for receiving Policy Events.
        """
        policy_event = args[0]
        policy_id = policy_event.origin
        log.debug("Policy modified: %s" % policy_id)

        try:
            policy = self.clients.resource_registry.read(policy_id)
            if policy:
                self._publish_policy_event(policy)

        except Exception, e:
            #If this is a delete operation, then don't bother with not finding the object.
            if policy_event.sub_type != 'DELETE':
                log.error(e)

    def _publish_policy_event(self, policy):

        if policy.policy_type.type_ == OT.CommonServiceAccessPolicy:
            self._publish_service_policy_event(policy)
        elif policy.policy_type.type_ == OT.ServiceAccessPolicy or policy.policy_type.type_ == OT.ProcessOperationPreconditionPolicy:
            self._publish_service_policy_event(policy)
        else:
            #Need to publish an event that a policy has changed for any associated resource
            res_list = self._find_resources_for_policy(policy._id)
            for res in res_list:
                self._publish_resource_policy_event(policy, res)


    def _publish_resource_policy_event(self, policy, resource):
        #Sent ResourcePolicyEvent event

        if self.event_pub:
            event_data = dict()
            event_data['origin_type'] = 'Resource_Policy'
            event_data['description'] = 'Updated Resource Policy'
            event_data['resource_id'] = resource._id
            event_data['resource_type'] = resource.type_
            event_data['resource_name'] = resource.name

            self.event_pub.publish_event(event_type='ResourcePolicyEvent', origin=policy._id, **event_data)

    def _publish_service_policy_event(self, policy):
        #Sent ServicePolicyEvent event

        if self.event_pub:
            event_data = dict()
            event_data['origin_type'] = 'Service_Policy'
            event_data['description'] = 'Updated Service Policy'

            if policy.policy_type.type_ == OT.ProcessOperationPreconditionPolicy:
                event_data['op'] =  policy.policy_type.op

            if hasattr(policy.policy_type, 'service_name'):
                event_data['service_name'] = policy.policy_type.service_name
            elif hasattr(policy.policy_type, 'process_name'):
                event_data['service_name'] = policy.policy_type.process_name
            else:
                event_data['service_name'] = ''

            self.event_pub.publish_event(event_type='ServicePolicyEvent', origin=policy._id, **event_data)


    def find_resource_policies(self, resource_id=''):
        """Finds all policies associated with a specific resource

        @param resource_id    str
        @retval policy_list    list
        @throws NotFound    object with specified id does not exist
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        return self._find_resource_policies(resource)

    def _find_resource_policies(self, resource, policy=None):
        policy_list,_ = self.clients.resource_registry.find_objects(resource, PRED.hasPolicy, policy)
        return policy_list

    def _find_resources_for_policy(self, policy_id=''):
        """Finds all resources associated with a specific policy

        @param policy_id    str
        @retval resource_list    list
        @throws NotFound    object with specified id does not exist
        """
        resource_list,_ = self.clients.resource_registry.find_subjects(None, PRED.hasPolicy, policy_id)

        return resource_list


    def get_active_resource_access_policy_rules(self, resource_id='', org_name=''):
        """Generates the set of all enabled access policies for the specified resource within the specified Org. If the org_name
        is not provided, then the root ION Org will be assumed.

        @param resource_id    str
        @param org_name    str
        @retval policy_rules    str
        @throws NotFound    object with specified id does not exist
        """
        if not resource_id:
            raise BadRequest("The resource_id parameter is missing")


        #TODO - extend to handle Org specific service policies at some point.

        rules = ""
        policy_set = self.find_resource_policies(resource_id)

        for p in policy_set:
            if p.enabled and p.policy_type.type_ == OT.ResourceAccessPolicy :
                rules += p.policy_type.policy_rule


        return rules



    def get_active_service_access_policy_rules(self, service_name='', org_name=''):
        """Generates the set of all enabled access policies for the specified service within the specified Org. If the org_name
        is not provided, then the root ION Org will be assumed.

        @param service_name    str
        @param org_name    str
        @retval policy_rules    str
        @throws NotFound    object with specified id does not exist
        """
        #TODO - extend to handle Org specific service policies at some point.

        rules = ""
        if not service_name:
            policy_set,_ = self.clients.resource_registry.find_resources_ext(restype=RT.Policy, nested_type=OT.CommonServiceAccessPolicy)
            for p in policy_set:
                if p.enabled:
                    rules += p.policy_type.policy_rule

        else:
            policy_set,_ = self.clients.resource_registry.find_resources_ext(restype=RT.Policy, nested_type=OT.ServiceAccessPolicy)
            for p in policy_set:
                if p.enabled and p.policy_type.service_name == service_name:
                    rules += p.policy_type.policy_rule

        return rules

    def get_active_process_operation_preconditions(self, process_name='', op='', org_name=''):
        """Generates the set of all enabled precondition policies for the specified process operation within the specified
        Org; could be a service or resource agent. If the org_name is not provided, then the root ION Org will be assumed.

        @param process_name    str
        @param op    str
        @param org_name    str
        @retval preconditions    list
        @throws NotFound    object with specified id does not exist
        """
        if not process_name:
            raise BadRequest("The process_name parameter is missing")


        #TODO - extend to handle Org specific service policies at some point.

        preconditions = list()
        policy_set,_ = self.clients.resource_registry.find_resources_ext(restype=RT.Policy, nested_type=OT.ProcessOperationPreconditionPolicy)
        for p in policy_set:

            if op:
                if p.enabled and p.policy_type.process_name == process_name and p.policy_type.op == op:
                    preconditions.append(p.policy_type)
            else:
                if p.enabled and p.policy_type.process_name == process_name:
                    preconditions.append(p.policy_type)

        return preconditions

    #Local helper functions for testing policies - do not remove

    def func1_pass(self, msg, header):
        return True, ''

    def func2_deny(self,  msg, header):
        return False, 'Denied for no reason'



    #
    #  ROLE CRUD Operations
    #


    def create_role(self, user_role=None):
        """Persists the provided UserRole object. The name of a role can only contain
        alphanumeric and underscore characters while the description can me human
        readable. The id string returned is the internal id by which a UserRole will
        be indentified in the data store.

        @param user_role    UserRole
        @retval user_role_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """

        if not is_basic_identifier(user_role.name):
            raise BadRequest("The role name '%s' can only contain alphanumeric and underscore characters" % user_role.name)

        user_role_id, version = self.clients.resource_registry.create(user_role)
        return user_role_id

    def update_role(self, user_role=None):
        """Updates the provided UserRole object.  The name of a role can only contain
        alphanumeric and underscore characters while the description can me human
        readable.Throws NotFound exception if an existing version of UserRole is
        not found.  Throws Conflict if the provided UserRole object is not based on
        the latest persisted version of the object.

        @param user_role    UserRole
        @retval success    bool
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """

        if not is_basic_identifier(user_role.name):
            raise BadRequest("The role name '%s' can only contain alphanumeric and underscore characters" % user_role.name)

        self.clients.resource_registry.update(user_role)

    def read_role(self, user_role_id=''):
        """Returns the UserRole object for the specified role id.
        Throws exception if id does not match any persisted UserRole
        objects.

        @param user_role_id    str
        @retval user_role    UserRole
        @throws NotFound    object with specified id does not exist
        """
        if not user_role_id:
            raise BadRequest("The user_role_id parameter is missing")

        user_role = self.clients.resource_registry.read(user_role_id)
        if not user_role:
            raise NotFound("Role %s does not exist" % user_role_id)
        return user_role

    def delete_role(self, user_role_id=''):
        """For now, permanently deletes UserRole object with the specified
        id. Throws exception if id does not match any persisted UserRole.

        @throws NotFound    object with specified id does not exist
        """
        if not user_role_id:
            raise BadRequest("The user_role_id parameter is missing")

        user_role = self.clients.resource_registry.read(user_role_id)
        if not user_role:
            raise NotFound("Role %s does not exist" % user_role_id)

        alist,_ = self.clients.resource_registry.find_subjects(RT.ActorIdentity, PRED.hasRole, user_role)
        if len(alist) > 0:
            raise BadRequest('The User Role %s cannot be removed as there are %s users associated to it' % (user_role.name, str(len(alist))))

        self.clients.resource_registry.delete(user_role_id)
