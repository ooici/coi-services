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



class AgentPolicy(Policy):

    def lce_precondition_deploy(self, agent_id):
        if not self.lce_precondition_integrate(agent_id): return False

        #no checking platform agents yet
        if RT.PlatformAgent == self._get_resource_type_by_id(agent_id): return True

        found = False
        for a in self._find_stemming(agent_id, PRED.hasAttachment, RT.Attachment):
            for k in a.keywords:
                if KeywordFlag.CERTIFICATION == k:
                    found = True
                    break

        return found

    def lce_precondition_integrate(self, agent_id):
        if not self.lce_precondition_develop(agent_id): return False

        #no checking platform agents yet
        if RT.PlatformAgent == self._get_resource_type_by_id(agent_id): return True

        found = False
        for a in self._find_stemming(agent_id, PRED.hasAttachment, RT.Attachment):
            for k in a.keywords:
                if KeywordFlag.EGG_URL == k:
                    found = True
                    break

        return found

    def lce_precondition_develop(self, agent_id):

        agent_type = self._get_resource_type_by_id(agent_id)

        if RT.InstrumentAgent == agent_type:
            return 0 < len(self._find_stemming(agent_id, PRED.hasModel, RT.InstrumentModel))
        else:
            return False

    
