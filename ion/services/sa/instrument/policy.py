#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.policy
@author   Ian Katz
"""

from pyon.public import PRED, RT, LCS
from pyon.ion.resource import get_maturity_visibility
from ion.services.sa.instrument.flag import KeywordFlag

class Policy(object):

    def __init__(self, clients):
        self.clients = clients

        if hasattr(clients, "resource_registry"):
            self.RR = self.clients.resource_registry

        self.on_policy_init()

    def on_policy_init(self):
        pass


    def _get_resource_type(self, resource):
        """
        get the type of a resource... simple wrapper
        @param resource a resource
        """
        restype = type(resource).__name__

        return restype


    def _get_resource_type_by_id(self, resource_id):
        """
        get the type of a resource by id
        @param resource_id a resource id
        """
        assert(type("") == type(resource_id))
        return self._get_resource_type(self.RR.read(resource_id))

    def _find_having(self, subject_type, association_predicate, some_object):
        """
        find resources having ____:
          find resource IDs of the predefined type that
          have the given association attached
        @param association_predicate one of the association types
        @param some_object the object "owned" by the association type
        """
        #log.debug("_find_having, from %s" % self._toplevel_call())
        ret, _ = self.RR.find_subjects(subject_type,
                                       association_predicate,
                                       some_object,
                                       False)
        return ret

    def _find_stemming(self, primary_object_id, association_predicate, some_object_type):
        """
        find resources stemming from _____:
          find resource IDs of the given object type that
          are associated with the primary object
        @param primary_object_id the id of the primary object
        @param association_prediate the association type
        @param some_object_type the type of associated object
        """
        #log.debug("_find_stemming, from %s" % self._toplevel_call())
        ret, _ = self.RR.find_objects(primary_object_id,
                                      association_predicate,
                                      some_object_type,
                                      False)
        return ret

    def _has_keyworded_attachment(self, resource_id, desired_keyword):
        for a in self._find_stemming(resource_id, PRED.hasAttachment, RT.Attachment):
            for k in a.keywords:
                if desired_keyword == k:
                    return True
        return False

    def _resource_lcstate_in(self, resource_obj, permissible_states=[]):
                
        parts = get_maturity_visibility(resource_obj.lcstate)

        return parts[0] in permissible_states
                      

class AgentPolicy(Policy):

    def lce_precondition_plan(self, agent_id):
        # always OK
        return True

    def lce_precondition_deploy(self, agent_id):
        if not self.lce_precondition_integrate(agent_id): return False

        #if no checking platform agents yet, uncomment this
        #if RT.PlatformAgent == self._get_resource_type_by_id(agent_id): return True

        return self._has_keyworded_attachment(agent_id, KeywordFlag.CERTIFICATION)

    def lce_precondition_integrate(self, agent_id):
        if not self.lce_precondition_develop(agent_id): return False

        #if not checking platform agents yet, uncomment this
        #if RT.PlatformAgent == self._get_resource_type_by_id(agent_id): return True

        return self._has_keyworded_attachment(agent_id, KeywordFlag.EGG_URL)

    def lce_precondition_develop(self, agent_id):

        agent_type = self._get_resource_type_by_id(agent_id)

        if RT.InstrumentAgent == agent_type:
            return 0 < len(self._find_stemming(agent_id, PRED.hasModel, RT.InstrumentModel))

        if RT.PlatformAgent == agent_type:
            return 0 < len(self._find_stemming(agent_id, PRED.hasModel, RT.PlatformModel))

        return False

    def lce_precondition_retire(self, agent_id):
        return 0 == self._find_having(RT.InstrumentAgentInstance, PRED.hasAgentDefinition, agent_id)
    

class ModelPolicy(Policy):
    def lce_precondition_plan(self, model_id):
        # always OK
        return True

    def lce_precondition_develop(self, model_id):
        # todo
        return True

    def lce_precondition_integrate(self, model_id):
        # todo
        return True

    def lce_precondition_deploy(self, model_id):
        # todo
        return True

    def lce_precondition_retire(self, model_id):
        # todo: more than checking agents, devices, and sites?

        model_type = self._get_resource_type_by_id(model_id)

        if RT.SensorModel == model_type:
            return 0 == len(self._find_having(RT.SensorDevice, PRED.hasModel, model_id))

        if RT.InstrumentModel == model_type:
            if 0 < len(self._find_having(RT.InstrumentDevice, PRED.hasModel, model_id)): return False
            if 0 < len(self._find_having(RT.InstrumentAgent, PRED.hasModel, model_id)): return False
            if 0 < len(self._find_having(RT.InstrumentSite, PRED.hasModel, model_id)): return False
            return True

        if RT.PlatformModel == model_type:
            if 0 < len(self._find_having(RT.PlatformDevice, PRED.hasModel, model_id)): return False
            if 0 < len(self._find_having(RT.PlatformAgent, PRED.hasModel, model_id)): return False
            if 0 < len(self._find_having(RT.PlatformSite, PRED.hasModel, model_id)): return False
            return True

        return False
        
class DevicePolicy(Policy):

    def lce_precondition_plan(self, device_id):
        # always OK
        return True

    def lce_precondition_develop(self, device_id):
        if not self.lce_precondition_plan(device_id): return False

        #have an agent/deployed, model/deployed

        device_type = self._get_resource_type_by_id(device_id)

        if RT.InstrumentDevice == device_type:
            models = self._find_stemming(device_id, PRED.hasModel, RT.InstrumentModel)
            if 0 == len(models): return False
            if not self._resource_lcstate_in(models[0], [LCS.DEPLOYED]): return False
        
            agents = self._find_stemming(device_id, PRED.hasModel, RT.InstrumentAgent)
            if 0 == len(agents): return False
            if not self._resource_lcstate_in(agents[0], [LCS.DEPLOYED]): return False

            return True
            
        if RT.PlatformDevice == device_type:
            models = self._find_stemming(device_id, PRED.hasModel, RT.PlatformModel)
            if 0 == len(models): return False
            if not self._resource_lcstate_in(models[0], [LCS.DEPLOYED]): return False
        
            agents = self._find_stemming(device_id, PRED.hasModel, RT.PlatformAgent)
            if 0 == len(agents): return False
            if not self._resource_lcstate_in(agents[0], [LCS.DEPLOYED]): return False

            return True

        return False


    def lce_precondition_integrate(self, device_id):
        if not self.lce_precondition_develop(device_id): return False

        #Have an instrument site/deployed, site has agent, site agent == device agent

        device_type = self._get_resource_type_by_id(device_id)

        if RT.InstrumentDevice == device_type:
            sites = self._find_having(RT.InstrumentSite, PRED.hasDevice, device_id)
            if 0 == len(sites): return False
            if not self._resource_lcstate_in(sites[0], [LCS.DEPLOYED]): return False

            siteagents = self._find_stemming(sites[0]._id, PRED.hasAgent, RT.InstrumentAgent)
            if 0 == len(siteagents): return False
        
            agents = self._find_stemming(device_id, PRED.hasModel, RT.InstrumentAgent)
            # we check the develop precondition here, which checks that there's an agent. so assume it.
            if siteagents[0]._id != agents[0]._id: return False
            
            return True
            
        if RT.PlatformDevice == device_type:
            sites = self._find_having(RT.PlatformSite, PRED.hasDevice, device_id)
            if 0 == len(sites): return False
            if not self._resource_lcstate_in(sites[0], [LCS.DEPLOYED]): return False

            siteagents = self._find_stemming(sites[0]._id, PRED.hasAgent, RT.PlatformAgent)
            if 0 == len(siteagents): return False
        
            agents = self._find_stemming(device_id, PRED.hasModel, RT.PlatformAgent)
            # we check the develop precondition here, which checks that there's an agent. so assume it.
            if siteagents[0]._id != agents[0]._id: return False
            return True

        return False


    def lce_precondition_deploy(self, device_id):
        if not self.lce_precondition_integrate(device_id): return False

        # Have associated agent instance, has a parent subsite which is deployed, platform device has platform site, all deployed.  

        device_type = self._get_resource_type_by_id(device_id)

        if RT.SensorDevice == device_type:
            return True

        if RT.InstrumentDevice == device_type:
            sites = self._find_having(RT.InstrumentSite, PRED.hasDevice, device_id)
            if 0 == len(sites): return False
            if not self._resource_lcstate_in(sites[0], [LCS.DEPLOYED]): return False

            siteagents = self._find_stemming(sites[0]._id, PRED.hasAgent, RT.InstrumentAgent)
            if 0 == len(siteagents): return False
        
            agents = self._find_stemming(device_id, PRED.hasModel, RT.InstrumentAgent)
            # we check the develop precondition here, which checks that there's an agent. so assume it.
            if siteagents[0]._id != agents[0]._id: return False

            # all sensor devices must be deployed
            for dev in self._find_stemming(device_id, PRED.hasDevice, RT.SensorDevice):
                if not self._resource_lcstate_in(dev, [LCS.DEPLOYED]): return False

            return True
            
        if RT.PlatformDevice == device_type:
            sites = self._find_having(RT.PlatformSite, PRED.hasDevice, device_id)
            if 0 == len(sites):
                return False
            if not self._resource_lcstate_in(sites[0], [LCS.DEPLOYED]):
                return False

            siteagents = self._find_stemming(sites[0]._id, PRED.hasAgent, RT.PlatformAgent)
            if 0 == len(siteagents):
                return False
        
            agents = self._find_stemming(device_id, PRED.hasModel, RT.PlatformAgent)
            # we check the develop precondition here, which checks that there's an agent. so assume it.
            if siteagents[0]._id != agents[0]._id:
                return False

            # all instrument devices must be deployed
            for dev in self._find_stemming(device_id, PRED.hasDevice, RT.InstrumentDevice):
                if not self._resource_lcstate_in(dev, [LCS.DEPLOYED]): return False

            return True

        return False

    def lce_precondition_retire(self, device_id):

        device_type = self._get_resource_type_by_id(device_id)

        if RT.SensorDevice == device_type:
            return True

        if RT.InstrumentDevice == device_type:
            if 0 < len(self._find_having(RT.InstrumentSite, PRED.hasDevice, device_id)): return False
            if 0 < len(self._find_stemming(device_id, PRED.hasDeployment, RT.Deployment)): return False

        if RT.PlatformDevice == device_type:
            if 0 < len(self._find_having(RT.PlatformSite, PRED.hasDevice, device_id)): return False
            if 0 < len(self._find_stemming(device_id, PRED.hasDeployment, RT.Deployment)): return False

        return False

class SitePolicy(Policy):
    def lce_precondition_plan(self, model_id):
        # always OK
        return True

    def lce_precondition_develop(self, model_id):
        # todo
        return True

    def lce_precondition_integrate(self, model_id):
        # todo
        return True

    def lce_precondition_deploy(self, model_id):
        # todo
        return True

    def lce_precondition_retire(self, model_id):
        # todo:
        return True
