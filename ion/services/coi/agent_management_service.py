#!/usr/bin/env python

__author__ = 'Michael Meisinger'

import re

from pyon.agent.agent import ResourceAgentClient
from pyon.public import log, IonObject, iex
from pyon.util.config import Config
from pyon.util.containers import get_safe, named_any

from interface.objects import AgentCapability, CapabilityType
from interface.services.coi.iagent_management_service import BaseAgentManagementService

class AgentManagementService(BaseAgentManagementService):

    """
    The Agent Management Service is the service that manages the Agent Definitions and the running Agents in the system.
    """
    def on_init(self):
        self.agent_interface = (Config(["res/config/agent_interface.yml"])).data['AgentInterface']

        # Keep a cache of known resource ids
        self.restype_cache = {}


    def create_agent_definition(self, agent_definition=None):
        """Creates an Agent Definition resource from the parameter AgentDefinition object.

        @param agent_definition    AgentDefinition
        @retval agent_definition_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

    def update_agent_definition(self, agent_definition=None):
        """Updates an existing Agent Definition resource.

        @param agent_definition    AgentDefinition
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        pass

    def read_agent_definition(self, agent_definition_id=''):
        """Returns an existing Agent Definition resource.

        @param agent_definition_id    str
        @retval agent_definition    AgentDefinition
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_agent_definition(self, agent_definition_id=''):
        """Deletes an existing Agent Definition resource.

        @param agent_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def create_agent_instance(self, agent_instance=None):
        """Creates an Agent Instance resource from the parameter AgentInstance object.

        @param agent_instance    AgentInstance
        @retval agent_instance_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

    def update_agent_instance(self, agent_instance=None):
        """Updates an existing Agent Instance resource.

        @param agent_instance    AgentInstance
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        pass

    def read_agent_instance(self, agent_instance_id=''):
        """Returns an existing Agent Instance resource.

        @param agent_instance_id    str
        @retval agent_instance    AgentInstance
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_agent_instance(self, agent_instance_id=''):
        """Deletes an existing Agent Instance resource.

        @param agent_instance_id    str
        @throws NotFound    object with specified id does not exist
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

        get_result = {}
        res_interface = self._get_type_interface(res_type)
        for param in params:
            getter = get_safe(res_interface, "params.%s.get" % param, None)
            if getter:
                self._call_getter(getter, resource_id, res_type)

                pass
            else:
                get_result['param'] = None

        return get_result

    def set_resource(self, resource_id='', params=None):
        """Set the value of the given resource parameters.
        @param resource_id The id of the resource agennt.
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

    # -----------------------------------------------------------------

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
        type_interface = self.agent_interface.get(res_type, None)
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
            type_interface = self.agent_interface.get(rt, None)
            if not type_interface:
                continue
            params = type_interface.get('params', None)
            if params and isinstance(params, dict):
                res_interface['params'].update(params)
            cmds = type_interface.get('commands', None)
            if cmds and isinstance(cmds, dict):
                res_interface['commands'].update(cmds)

        return res_interface

    def _call_getter(self, getter, resource_id, res_type):
        try:
            if not getter:
                return None
            elif getter.startswith("func:"):
                func_name = getter[5:].split('(', 1)
                if func_name:
                    func = named_any(func_name[0])
                    args = self._get_call_args(func_name[1], resource_id, res_type) if len(func_name) > 1 else []
                    fres = func(*args)
                    log.info("Function %s result: %s", func, fres)
                    return fres
            elif getter.startswith("self:"):
                func_name = getter[5:].split('(', 1)
                if func_name:
                    func = getattr(self, func_name[0])
                    args = self._get_call_args(func_name[1], resource_id, res_type) if len(func_name) > 1 else []
                    fres = func(*args)
                    log.info("Function %s result: %s", func, fres)
                    return fres
        except Exception as ex:
            log.warn("_call_getter exception: %r", repr(ex))
            return None

    def _get_call_args(self, func_sig, resource_id, res_type):
        args = []
        func_sig = re.split('[,)]', func_sig)
        if func_sig:
            for arg in func_sig[:-1]:
                if arg == "$RESOURCE_ID":
                    args.append(resource_id)
                elif arg == "$RESOURCE_TYPE":
                    args.append(res_type)
                elif not arg:
                    args.append(None)
                else:
                    args.append(arg)
        return args

    def get_resource_size(self, resource_id):
        res_obj = self.container.resource_registry.rr_store.read_doc(resource_id)
        import json
        obj_str = json.dumps(res_obj)
        res_len = len(obj_str)

        log.warn("Resource %s length: %s", resource_id, res_len)
        return res_len

# Helpers

def get_resource_size(resource_id):
    return 10
