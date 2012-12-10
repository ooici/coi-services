#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'

import re

from pyon.agent.agent import ResourceAgentClient
from pyon.core.bootstrap import get_service_registry
from pyon.public import log, IonObject, iex, NotFound, BadRequest, PRED
from pyon.util.config import Config
from pyon.util.containers import get_safe, named_any, get_ion_ts, is_basic_identifier

from interface.objects import AgentCapability, AgentCommandResult, CapabilityType, Resource
from interface.services.coi.iresource_management_service import BaseResourceManagementService


class ResourceManagementService(BaseResourceManagementService):
    """
    The Resource Management Service is the service that manages the Resource Types and Lifecycles
    associated with all Resources
    """

    def on_init(self):
        self.resource_interface = (Config(["res/config/resource_management.yml"])).data['ResourceInterface']

        self._augment_resource_interface_from_interfaces()

        # Keep a cache of known resource ids
        self.restype_cache = {}


    def create_resource_type(self, resource_type=None, object_id=""):
        """ Should receive a ResourceType object
        """
        # Return Value
        # ------------
        # {resource_type_id: ''}
        #
        if not is_basic_identifier(resource_type.name):
            raise BadRequest("Invalid resource name: %s " % resource_type.name)
        if not object_id:
            raise BadRequest("Object_id is missing")

        object_type= self.clients.resource_registry.read(object_id)
        if resource_type.name != object_type.name:
            raise BadRequest("Resource and object name don't match: %s - %s" (resource_type.name,object_type.name))
        resource_id, version = self.clients.resource_registry.create(resource_type)
        self.clients.resource_registry.create_association(resource_id, PRED.hasObjectType, object_id)
        return resource_id

    def update_resource_type(self, resource_type=None):
        """ Should receive a ResourceType object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError("Currently, updating ResourceType is not supported")

    def read_resource_type(self, resource_type_id=''):
        """ Should return a ResourceType object
        """
        # Return Value
        # ------------
        # resource_type: {}
        #
        if not resource_type_id:
            raise BadRequest("The resource_type_id parameter is missing")
        return self.clients.resource_registry.read(resource_type_id)

    def delete_resource_type(self, resource_type_id='', object_type_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        if not resource_type_id:
            raise BadRequest("The resource_type_id parameter is missing")
        if not object_type_id:
            raise BadRequest("The object_type_id parameter is missing")
        association_id = self.clients.resource_registry.get_association(resource_type_id, PRED.hasObjectType, object_type_id)
        self.clients.resource_registry.delete_association(association_id)
        return self.clients.resource_registry.delete(resource_type_id)

    def create_resource_lifecycle(self, resource_lifecycle=None):
        """ Should receive a ResourceLifeCycle object
        """
        # Return Value
        # ------------
        # {resource_lifecycle_id: ''}
        #
        raise NotImplementedError("Currently not supported")

    def update_resource_lifecycle(self, resource_lifecycle=None):
        """ Should receive a ResourceLifeCycle object
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError("Currently not supported")

    def read_resource_lifecycle(self, resource_lifecycle_id=''):
        """ Should return a ResourceLifeCycle object
        """
        # Return Value
        # ------------
        # resource_lifecycle: {}
        #
        raise NotImplementedError("Currently not supported")

    def delete_resource_lifecycle(self, resource_lifecycle_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError("Currently not supported")

    # -------------------------------------------------------------------------
    # Generic resource interface

    def create_resource(self, resource=None):
        """Creates an arbitrary resource object via its defined create function, so that it
        can successively can be accessed via the agent interface.

        @param resource    Resource
        @retval resource_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        if not isinstance(resource, Resource):
            raise BadRequest("Can only create resources, not type %s" % type(resource))

        res_type = resource._get_type()
        res_interface = self._get_type_interface(res_type)

        if not 'create' in res_interface:
            raise BadRequest("Resource type %s does not support: CREATE" % res_type)

        res = self._call_crud(res_interface['create'], resource, None, res_type)
        if type(res) in (list,tuple):
            res = res[0]
        return res

    def update_resource(self, resource=None):
        """Updates an existing resource via the configured service operation.

        @param resource    Resource
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        if not isinstance(resource, Resource):
            raise BadRequest("Can only update resources, not type %s" % type(resource))

        res_type = resource._get_type()
        res_interface = self._get_type_interface(res_type)

        if not 'update' in res_interface:
            raise BadRequest("Resource type %s does not support: UPDATE" % res_type)

        self._call_crud(res_interface['update'], resource, None, res_type)

    def read_resource(self, resource_id=''):
        """Returns an existing resource via the configured service operation.

        @param resource_id    str
        @retval resource    Resource
        @throws NotFound    object with specified id does not exist
        """
        res_type = self._get_resource_type(resource_id)
        res_interface = self._get_type_interface(res_type)

        if not 'read' in res_interface:
            raise BadRequest("Resource type %s does not support: READ" % res_type)

        res_obj = self._call_crud(res_interface['read'], None, resource_id, res_type)
        return res_obj

    def delete_resource(self, resource_id=''):
        """Deletes an existing resource via the configured service operation.

        @param resource_id    str
        @throws NotFound    object with specified id does not exist
        """
        res_type = self._get_resource_type(resource_id)
        res_interface = self._get_type_interface(res_type)

        if not 'delete' in res_interface:
            raise BadRequest("Resource type %s does not support: DELETE" % res_type)

        self._call_crud(res_interface['delete'], None, resource_id, res_type)

    def execute_lifecycle_transition(self, resource_id='', transition_event=''):
        """Alter object lifecycle according to given transition event. Throws exception
        if resource object does not exist or given transition_event is unknown/illegal.
        The new life cycle state after applying the transition is returned.

        @param resource_id    str
        @param transition_event    str
        @retval lcstate    str
        @throws NotFound    resource object does not exist
        @throws BadRequest    transition event unknown or illegal in current state; resource type has no lifecycle
        @throws Conflict    race condition while trying to update
        """
        res_type = self._get_resource_type(resource_id)
        res_interface = self._get_type_interface(res_type)

        if not 'execute_lifecycle_transition' in res_interface:
            raise BadRequest("Resource type %s does not support: execute_lifecycle_transition" % res_type)

        res = self._call_crud(res_interface['execute_lifecycle_transition'], transition_event, resource_id, res_type)
        return res

    def get_lifecycle_events(self, resource_id=''):
        """For a given resource, return a list of possible lifecycle transition events.

        @param resource_id    str
        @retval transition_events    list
        @throws NotFound    resource object does not exist
        @throws BadRequest    transition event unknown or illegal in current state; resource type has no lifecycle
        """
        pass

    # -------------------------------------------------------------------------
    # Agent interface

    def negotiate(self, resource_id='', sap_in=None):
        """Initiate a negotiation with this agent. The subject of this negotiation is the given
        ServiceAgreementProposal. The response is either a new ServiceAgreementProposal as counter-offer,
        or the same ServiceAgreementProposal indicating the offer has been accepted.
        NEEDS REFINEMENT.

        @param resource_id    str
        @param sap_in    ServiceAgreementProposal
        @retval sap_out    ServiceAgreementProposal
        """
        pass

    def get_capabilities(self, resource_id='', current_state=True):
        """Introspect for agent capabilities.
        @param resource_id The id of the resource agent.
        @param current_state Flag indicating to return capabilities for current
        state only (default True).
        @retval List of AgentCapabilities objects.

        @param resource_id    str
        @param current_state    bool
        @retval capability_list    list
        """

        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.get_capabilities(resource_id=resource_id, current_state=current_state)

        res_interface = self._get_type_interface(res_type)

        cap_list = []
        for param in res_interface['params'].keys():
            cap = AgentCapability(name=param, cap_type=CapabilityType.RES_PAR)
            cap_list.append(cap)

        for cmd in res_interface['commands'].keys():
            cap = AgentCapability(name=cmd, cap_type=CapabilityType.RES_CMD)
            cap_list.append(cap)

        return cap_list

    def execute_resource(self, resource_id='', command=None):
        """Execute command on the resource represented by agent.
        @param resource_id The id of the resource agennt.
        @param command An AgentCommand containing the command.
        @retval result An AgentCommandResult containing the result.
        @throws BadRequest if the command was malformed.
        @throws NotFound if the command is not available in current state.
        @throws ResourceError if the resource produced an error during execution.

        @param resource_id    str
        @param command    AgentCommand
        @retval result    AgentCommandResult
        @throws BadRequest    if the command was malformed.
        @throws NotFound    if the command is not implemented in the agent.
        @throws ResourceError    if the resource produced an error.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.execute_resource(resource_id=resource_id, command=command)

        cmd_res = None
        res_interface = self._get_type_interface(res_type)

        target = get_safe(res_interface, "commands.%s.execute" % command.command, None)
        if target:
            res = self._call_execute(target, resource_id, res_type, command.args, command.kwargs)
            cmd_res = AgentCommandResult(command_id=command.command_id,
                command=command.command,
                ts_execute=get_ion_ts(),
                status=0)
        else:
            log.warn("execute_resource(): command %s not defined", command.command)

        return cmd_res

    def get_resource(self, resource_id='', params=None):
        """Return the value of the given resource parameter.
        @param resource_id The id of the resource agennt.
        @param params A list of parameters names to query.
        @retval A dict of parameter name-value pairs.
        @throws BadRequest if the command was malformed.
        @throws NotFound if the resource does not support the parameter.

        @param resource_id    str
        @param params    list
        @retval result    AgentCommandResult
        @throws NotFound    if the parameter does not exist.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.get_resource(resource_id=resource_id, params=params)

        res_interface = self._get_type_interface(res_type)

        get_result = {}
        for param in params:
            getter = get_safe(res_interface, "params.%s.get" % param, None)
            if getter:
                get_res = self._call_getter(getter, resource_id, res_type)
                get_result['param'] = get_res
            else:
                get_result['param'] = None

        return get_result

    def set_resource(self, resource_id='', params=None):
        """Set the value of the given resource parameters.
        @param resource_id The id of the resource agent.
        @param params A dict of resource parameter name-value pairs.
        @throws BadRequest if the command was malformed.
        @throws NotFound if a parameter is not supported by the resource.
        @throws ResourceError if the resource encountered an error while setting
        the parameters.

        @param resource_id    str
        @param params    dict
        @throws BadRequest    if the command was malformed.
        @throws NotFound    if the parameter does not exist.
        @throws ResourceError    if the resource failed while trying to set the parameter.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.set_resource(resource_id=resource_id, params=params)

        res_interface = self._get_type_interface(res_type)

        for param in params:
            setter = get_safe(res_interface, "params.%s.set" % param, None)
            if setter:
                self._call_setter(setter, params[param], resource_id, res_type)
            else:
                log.warn("set_resource(): param %s not defined", param)

    def get_resource_state(self, resource_id=''):
        """Return the current resource specific state, if available.
        @param resource_id The id of the resource agennt.
        @retval A str containing the current resource specific state.

        @param resource_id    str
        @retval result    str
        @throws NotFound    if the resource does not utilize a specific state machine.
        @throws ResourceError    if the resource failed while trying to get the state.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.get_resource_state(resource_id=resource_id)

        raise BadRequest("Not implemented for resource type %s", res_type)

    def ping_resource(self, resource_id=''):
        """Ping the resource.
        @param resource_id The id of the resource agennt.
        @retval A str containing a string representation of the resource and
        timestamp.

        @param resource_id    str
        @retval result    str
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.ping_resource(resource_id=resource_id)

        raise BadRequest("Not implemented for resource type %s" % res_type)


    def execute_agent(self, resource_id='', command=None):
        """Execute command on the agent.
        @param resource_id The id of the resource agennt.
        @param command An AgentCommand containing the command.
        @retval result An AgentCommandResult containing the result.
        @throws BadRequest if the command was malformed.
        @throws NotFound if the command is not available in current state.

        @param resource_id    str
        @param command    AgentCommand
        @retval result    AgentCommandResult
        @throws BadRequest    if the command was malformed.
        @throws NotFound    if the command is not implemented in the agent.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.execute_agent(resource_id=resource_id, command=command)

        raise BadRequest("Not implemented for resource type %s" % res_type)

    def get_agent(self, resource_id='', params=None):
        """Return the value of the given agent parameters.
        @param resource_id The id of the resource agennt.
        @param params A list of parameters names to query.
        @retval A dict of parameter name-value pairs.
        @throws BadRequest if the command was malformed.
        @throws NotFound if the agent does not support the parameter.

        @param resource_id    str
        @param params    list
        @retval result    AgentCommandResult
        @throws BadRequest    if the command was malformed.
        @throws NotFound    if the parameter does not exist.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.get_agent(resource_id=resource_id, params=params)

        raise BadRequest("Not implemented for resource type %s" % res_type)

    def set_agent(self, resource_id='', params=None):
        """Set the value of the given agent parameters.
        @param resource_id The id of the resource agennt.
        @param params A dict of resource parameter name-value pairs.
        @throws BadRequest if the command was malformed.
        @throws NotFound if a parameter is not supported by the resource.

        @param resource_id    str
        @param params    dict
        @throws BadRequest    if the command was malformed.
        @throws NotFound    if the parameter does not exist.
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.set_agent(resource_id=resource_id, params=params)

        raise BadRequest("Not implemented for resource type %s" % res_type)

    def get_agent_state(self, resource_id=''):
        """Return the current resource agent common state.
        @param resource_id The id of the resource agennt.
        @retval A str containing the current agent state.

        @param resource_id    str
        @retval result    str
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.get_agent_state(resource_id=resource_id)

        raise BadRequest("Not implemented for resource type %s" % res_type)

    def ping_agent(self, resource_id=''):
        """Ping the agent.
        @param resource_id The id of the resource agennt.
        @retval A str containing a string representation of the agent
        and a timestamp.

        @param resource_id    str
        @retval result    str
        """
        res_type = self._get_resource_type(resource_id)
        if self._has_agent(res_type):
            rac = ResourceAgentClient(resource_id=resource_id)
            return rac.ping_agent(resource_id=resource_id)

        raise BadRequest("Not implemented for resource type %s" % res_type)

    # -----------------------------------------------------------------

    def _augment_resource_interface_from_interfaces(self):
        """
        Add resource type specific entries for CRUD, params and commands based on decorator
        annotations in service interfaces. This enables systematic definition and extension.
        @TODO Implement this so that static definitions are not needed anymore
        """
        pass

    def _get_resource_type(self, resource_id):
        if resource_id in self.restype_cache:
            return self.restype_cache[resource_id]
        res = self.container.resource_registry.read(resource_id)
        res_type = res._get_type()
        self.restype_cache[resource_id] = res_type
        if len(self.restype_cache) > 10000:
            log.warn("Resource type cache exceeds size: %s", len(self.restype_cache))
        return res_type

    def _has_agent(self, res_type):
        type_interface = self.resource_interface.get(res_type, None)
        return type_interface and type_interface.get('agent', False)

    def _get_type_interface(self, res_type):
        """
        Creates a merge of params and commands up the type inheritance chain.
        Note: Entire param and command entries if subtypes replace their super types definition.
        """
        res_interface = dict(params={}, commands={})

        base_types = IonObject(res_type)._get_extends()
        base_types.insert(0, res_type)

        for rt in reversed(base_types):
            type_interface = self.resource_interface.get(rt, None)
            if not type_interface:
                continue
            for tpar, tval in type_interface.iteritems():
                if tpar in res_interface:
                    rval = res_interface[tpar]
                    if isinstance(rval, dict):
                        rval.update(tval)
                    else:
                        res_interface[tpar] = tval
                else:
                    res_interface[tpar] = dict(tval) if isinstance(tval, dict) else tval

        return res_interface

    def _call_getter(self, func_sig, resource_id, res_type):
        return self._call_target(func_sig, resource_id=resource_id, res_type=res_type)

    def _call_setter(self, func_sig, value, resource_id, res_type):
        return self._call_target(func_sig, value=value, resource_id=resource_id, res_type=res_type)

    def _call_execute(self, func_sig, resource_id, res_type, cmd_args, cmd_kwargs):
        return self._call_target(func_sig, resource_id=resource_id, res_type=res_type, cmd_kwargs=cmd_kwargs)

    def _call_crud(self, func_sig, value, resource_id, res_type):
        return self._call_target(func_sig, value=value, resource_id=resource_id, res_type=res_type)

    def _call_target(self, target, value=None, resource_id=None, res_type=None, cmd_args=None, cmd_kwargs=None):
        """
        Makes a call to a specified function. Function specification can be of varying type.
        """
        try:
            if not target:
                return None
            match = re.match("(func|serviceop):([\w.]+)\s*\(\s*([\w,$\s]*)\s*\)\s*(?:->\s*([\w\.]+))?\s*$", target)
            if match:
                func_type, func_name, func_args, res_path = match.groups()
                func = None
                if func_type == "func":
                    if func_name.startswith("self."):
                        func = getattr(self, func_name[5:])
                    else:
                        func = named_any(func_name)
                elif func_type == "serviceop":
                    svc_name, svc_op = func_name.split('.', 1)
                    try:
                        svc_client_cls = get_service_registry().get_service_by_name(svc_name).client
                    except Exception as ex:
                        log.error("No service client found for service: %s", svc_name)
                    else:
                        svc_client = svc_client_cls(process=self)
                        func = getattr(svc_client, svc_op)

                if not func:
                    return None

                args = self._get_call_args(func_args, resource_id, res_type, value, cmd_args)
                kwargs = {} if not cmd_kwargs else cmd_kwargs

                func_res = func(*args, **kwargs)
                log.info("Function %s result: %s", func, func_res)

                if res_path and isinstance(func_res, dict):
                    func_res = get_safe(func_res, res_path, None)

                return func_res

            else:
                log.error("Unknown call target expression: %s", target)

        except Exception as ex:
            log.exception("_call_target exception")
            return None


    def _get_call_args(self, func_arg_str, resource_id, res_type, value=None, cmd_args=None):
        args = []
        func_args = func_arg_str.split(',')
        if func_args:
            for arg in func_args:
                arg = arg.strip()
                if arg == "$RESOURCE_ID":
                    args.append(resource_id)
                elif arg == "$RESOURCE_TYPE":
                    args.append(res_type)
                elif arg == "$VALUE" or arg == "$RESOURCE":
                    args.append(value)
                elif arg == "$ARGS":
                    if cmd_args is not None:
                        args.extend(cmd_args)
                elif not arg:
                    args.append(None)
                else:
                    args.append(arg)
        return args

    # Callable functions

    def get_resource_size(self, resource_id):
        res_obj = self.container.resource_registry.rr_store.read_doc(resource_id)
        import json
        obj_str = json.dumps(res_obj)
        res_len = len(obj_str)

        log.info("Resource %s length: %s", resource_id, res_len)
        return res_len

    def set_resource_description(self, resource_id, value):
        res_obj = self.container.resource_registry.read(resource_id)
        res_obj.description = value
        self.container.resource_registry.update(res_obj)

        log.info("Resource %s description updated: %s", resource_id, value)

# Helpers

def get_resource_size(resource_id):
    return 10
