#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.policy
@author   Ian Katz
"""

from pyon.public import PRED, RT
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

    def _find_having(self, association_predicate, some_object):
        """
        find resources having ____:
          find resource IDs of the predefined type that
          have the given association attached
        @param association_predicate one of the association types
        @param some_object the object "owned" by the association type
        """
        #log.debug("_find_having, from %s" % self._toplevel_call())
        ret, _ = self.RR.find_subjects(self.iontype,
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

    

class DevicePolicy(Policy):

    def lce_precondition_plan(self, device_id):
        # always OK
        return True

    def lce_precondition_plan(self, device_id):

        device_type = self._get_resource_type_by_id(agent_id)

        if RT.InstrumentDevice == device_type:
            return 0 < len(self._find_stemming(device_id, PRED.hasModel, RT.InstrumentModel))
        
        if RT.PlatformDevice == device_type:
            return 0 < len(self._find_stemming(device_id, PRED.hasModel, RT.PlatformModel))

        return False


    def lce_precondition_develop(self, device_id):
        if not self.lce_precondition_plan(device_id): return False
        
        device_type = self._get_resource_type_by_id(agent_id)

        if RT.InstrumentDevice == device_type:
            return 0 < len(self._find_stemming(device_id, PRED.hasAgentInstance, RT.InstrumentAgentInstance))
        
        if RT.PlatformDevice == device_type:
            return 0 < len(self._find_stemming(device_id, PRED.hasAgentInstance, RT.PlatformAgentInstance))

        return False


    def lce_precondition_integrate(self, device_id):
        if not self.lce_precondition_develop(device_id): return False
            
        return self._has_keyworded_attachment(agent_id, KeywordFlag.CERTIFICATION)


