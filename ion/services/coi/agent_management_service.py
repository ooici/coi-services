#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import BadRequest, log

from interface.objects import AgentInstance, InstrumentAgentInstance, PlatformAgentInstance, ExternalDatasetAgentInstance, \
    Resource, AgentDefinition, InstrumentAgent, PlatformAgent, ExternalDatasetAgent

from interface.services.coi.iagent_management_service import BaseAgentManagementService
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceProcessClient

AT_INSTUMENT_AGENT = "InstrumentAgentInstance"
AT_PLATFORM_AGENT = "PlatformAgentInstance"
AT_EXTDATASET_AGENT = "ExternalDatasetAgentInstance"


class AgentManagementService(BaseAgentManagementService):
    """The Agent Management Service is the service that manages Agent Definitions, Agent Instance
    configurations and running Agents in the system.

    @see https://confluence.oceanobservatories.org/display/syseng/CIAD+COI+OV+Agent+Management+Service
    """
    def on_init(self):
        self.ims_client = InstrumentManagementServiceProcessClient(process=self)
        self.dams_client = DataAcquisitionManagementServiceProcessClient(process=self)

    def create_agent_definition(self, agent_definition=None):
        """Creates an Agent Definition resource from the parameter AgentDefinition object.

        @param agent_definition    AgentDefinition
        @retval agent_definition_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        atype, ainst, svc_client = self._get_agent_type(agent_definition)
        if atype == AT_INSTUMENT_AGENT:
            res = self.ims_client.create_instrument_agent(agent_definition)
        elif atype == AT_PLATFORM_AGENT:
            res = self.ims_client.create_platform_agent(agent_definition)
        elif atype == AT_EXTDATASET_AGENT:
            res = self.dams_client.create_external_dataset_agent(agent_definition)
        else:
            raise BadRequest("Unknown agent type: %s" % atype)
        return res

    def update_agent_definition(self, agent_definition=None):
        """Updates an existing Agent Definition resource.

        @param agent_definition    AgentDefinition
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        atype, ainst, svc_client = self._get_agent_type(agent_definition)
        if atype == AT_INSTUMENT_AGENT:
            res = self.ims_client.update_instrument_agent(agent_definition)
        elif atype == AT_PLATFORM_AGENT:
            res = self.ims_client.update_platform_agent(agent_definition)
        elif atype == AT_EXTDATASET_AGENT:
            res = self.dams_client.update_external_dataset_agent(agent_definition)
        else:
            raise BadRequest("Unknown agent type: %s" % atype)
        return res


    def read_agent_definition(self, agent_definition_id=''):
        """Returns an existing Agent Definition resource.

        @param agent_definition_id    str
        @retval agent_definition    AgentDefinition
        @throws NotFound    object with specified id does not exist
        """
        atype, ainst, svc_client = self._get_agent_type(agent_definition_id)
        if atype == AT_INSTUMENT_AGENT:
            res = self.ims_client.read_instrument_agent(agent_definition_id)
        elif atype == AT_PLATFORM_AGENT:
            res = self.ims_client.read_platform_agent(agent_definition_id)
        elif atype == AT_EXTDATASET_AGENT:
            res = self.dams_client.read_external_dataset_agent(agent_definition_id)
        else:
            raise BadRequest("Unknown agent type: %s" % atype)
        return res

    def delete_agent_definition(self, agent_definition_id=''):
        """Deletes an existing Agent Definition resource.

        @param agent_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        atype, ainst, svc_client = self._get_agent_type(agent_definition_id)
        if atype == AT_INSTUMENT_AGENT:
            res = self.ims_client.delete_instrument_agent(agent_definition_id)
        elif atype == AT_PLATFORM_AGENT:
            res = self.ims_client.delete_platform_agent(agent_definition_id)
        elif atype == AT_EXTDATASET_AGENT:
            res = self.dams_client.delete_external_dataset_agent(agent_definition_id)
        else:
            raise BadRequest("Unknown agent type: %s" % atype)
        return res


    def create_agent_instance(self, agent_instance=None):
        """Creates an Agent Instance resource from the parameter AgentInstance object.

        @param agent_instance    AgentInstance
        @retval agent_instance_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        atype, ainst, svc_client = self._get_agent_type(agent_instance)
        if atype == AT_INSTUMENT_AGENT:
            res = self.ims_client.create_instrument_agent_instance_(agent_instance)
        elif atype == AT_PLATFORM_AGENT:
            res = self.ims_client.create_platform_agent_instance(agent_instance)
        elif atype == AT_EXTDATASET_AGENT:
            res = self.dams_client.create_external_dataset_agent_instance(agent_instance)
        else:
            raise BadRequest("Unknown agent type: %s" % atype)
        return res

    def update_agent_instance(self, agent_instance=None):
        """Updates an existing Agent Instance resource.

        @param agent_instance    AgentInstance
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        atype, ainst, svc_client = self._get_agent_type(agent_instance)
        if atype == AT_INSTUMENT_AGENT:
            res = self.ims_client.update_instrument_agent_instance(agent_instance)
        elif atype == AT_PLATFORM_AGENT:
            res = self.ims_client.update_platform_agent_instance(agent_instance)
        elif atype == AT_EXTDATASET_AGENT:
            res = self.dams_client.update_data_source_agent_instance(agent_instance)
        else:
            raise BadRequest("Unknown agent type: %s" % atype)
        return res

    def read_agent_instance(self, agent_instance_id=''):
        """Returns an existing Agent Instance resource.

        @param agent_instance_id    str
        @retval agent_instance    AgentInstance
        @throws NotFound    object with specified id does not exist
        """
        atype, ainst, svc_client = self._get_agent_type(agent_instance_id)
        if atype == AT_INSTUMENT_AGENT:
            res = self.ims_client.read_instrument_agent_instance(agent_instance_id)
        elif atype == AT_PLATFORM_AGENT:
            res = self.ims_client.read_platform_agent_instance(agent_instance_id)
        elif atype == AT_EXTDATASET_AGENT:
            res = self.dams_client.read_external_dataset_agent_instance(agent_instance_id)
        else:
            raise BadRequest("Unknown agent type: %s" % atype)
        return res

    def delete_agent_instance(self, agent_instance_id=''):
        """Deletes an existing Agent Instance resource.

        @param agent_instance_id    str
        @throws NotFound    object with specified id does not exist
        """
        atype, ainst, svc_client = self._get_agent_type(agent_instance_id)
        if atype == AT_INSTUMENT_AGENT:
            res = self.ims_client.delete_instrument_agent_instance(agent_instance_id)
        elif atype == AT_PLATFORM_AGENT:
            res = self.ims_client.delete_platform_agent_instance(agent_instance_id)
        elif atype == AT_EXTDATASET_AGENT:
            res = self.dams_client.delete_external_dataset_agent_instance(agent_instance_id)
        else:
            raise BadRequest("Unknown agent type: %s" % atype)
        return res

    def start_agent_instance(self, agent_instance_id=''):
        """Start the given Agent Instance. Delegates to the type specific service.

        @param agent_instance_id    str
        @retval process_id    str
        @throws NotFound    object with specified id does not exist
        """
        atype, ainst, svc_client = self._get_agent_type(agent_instance_id)
        if atype == AT_INSTUMENT_AGENT:
            res = self.ims_client.start_instrument_agent_instance(agent_instance_id)
        elif atype == AT_PLATFORM_AGENT:
            res = self.ims_client.start_platform_agent_instance(agent_instance_id)
        elif atype == AT_EXTDATASET_AGENT:
            res = self.dams_client.start_external_dataset_agent_instance(agent_instance_id)
        else:
            raise BadRequest("Unknown agent type: %s" % atype)
        return res

    def stop_agent_instance(self, agent_instance_id=''):
        """Stop the given Agent Instance. Delegates to the type specific service.

        @param agent_instance_id    str
        @throws NotFound    object with specified id does not exist
        """
        atype, ainst, svc_client = self._get_agent_type(agent_instance_id)
        if atype == AT_INSTUMENT_AGENT:
            res = self.ims_client.stop_instrument_agent_instance(agent_instance_id)
        elif atype == AT_PLATFORM_AGENT:
            res = self.ims_client.stop_platform_agent_instance(agent_instance_id)
        elif atype == AT_EXTDATASET_AGENT:
            res = self.dams_client.stop_external_dataset_agent_instance(agent_instance_id)
        else:
            raise BadRequest("Unknown agent type: %s" % atype)
        return res

    def _get_agent_type(self, agent=''):
        if isinstance(agent, str):
            res_obj = self.clients.resource_registry.read(agent)
        elif isinstance(agent, AgentInstance) or isinstance(agent, AgentDefinition):
            res_obj = agent
        else:
            raise BadRequest("Resource must be AgentInstance or AgentDefinition, is: %s" % agent.type_)

        atype = None
        svc_client = None
        is_instance = res_obj.type_.endswith("Instance")
        if isinstance(res_obj, InstrumentAgentInstance) or isinstance(res_obj, InstrumentAgent):
            atype = AT_INSTUMENT_AGENT
            svc_client = self.ims_client
        elif isinstance(res_obj, PlatformAgentInstance) or isinstance(res_obj, PlatformAgent):
            atype = AT_PLATFORM_AGENT
            svc_client = self.ims_client
        elif isinstance(res_obj, ExternalDatasetAgentInstance) or isinstance(res_obj, ExternalDatasetAgent):
            atype = AT_EXTDATASET_AGENT
            svc_client = self.dams_client
        else:
            raise BadRequest("Resource must is unknown AgentInstance: %s" % res_obj.type_)
        return atype, res_obj, svc_client

