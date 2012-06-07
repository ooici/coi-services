#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.platform_agent_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT, LCE
from ion.services.sa.instrument.flag import KeywordFlag
from ion.services.sa.instrument.policy import AgentPolicy

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class PlatformAgentImpl(ResourceSimpleImpl):
    """
    @brief resource management for PlatformAgent resources
    """

    def _primary_object_name(self):
        return RT.PlatformAgent

    def _primary_object_label(self):
        return "platform_agent"

    def on_impl_init(self):
        self.policy = AgentPolicy(self.clients)
        
        self.add_lce_precondition(LCE.PLAN, self.use_policy(self.policy.lce_precondition_plan))
        self.add_lce_precondition(LCE.INTEGRATE, self.use_policy(self.policy.lce_precondition_integrate))
        self.add_lce_precondition(LCE.DEVELOP, self.use_policy(self.policy.lce_precondition_develop))
        self.add_lce_precondition(LCE.DEPLOY, self.use_policy(self.policy.lce_precondition_deploy))
        
        # self.add_lce_precondition(LCE.PLAN, (lambda r: "")) # no precondition to plan
        # self.add_lce_precondition(LCE.INTEGRATE, self.lce_precondition_integrate)
        # self.add_lce_precondition(LCE.DEVELOP, self.lce_precondition_develop)
        # self.add_lce_precondition(LCE.DEPLOY, self.lce_precondition_deploy)  
        
    
    def lce_precondition_deploy(self, platform_agent_id):
        pre = self.lce_precondition_integrate(platform_agent_id)
        if pre: 
            return pre
        
        found = False
        for a in self.find_stemming_attachment(platform_agent_id):
            for k in a.keywords:
                if KeywordFlag.CERTIFICATION == k:
                    found = True
                    break
        if found:
            return ""
        else:
            return "PlatformAgent LCS requires certification"

    def lce_precondition_integrate(self, platform_agent_id):
        pre = self.lce_precondition_develop(platform_agent_id)
        if pre: 
            return pre
        
        found = False
        for a in self.find_stemming_attachment(platform_agent_id):
            for k in a.keywords:
                if KeywordFlag.EGG_URL == k:
                    found = True
                    break

        if found:
            return ""
        else:
            return "PlatformAgent LCS requires a registered driver egg"

    def lce_precondition_develop(self, platform_agent_id):
        if 0 < len(self.find_stemming_model(platform_agent_id)):
            return ""
        else:
            return "PlatformAgent LCS requires an associated platform model"

    def link_model(self, platform_agent_id='', platform_model_id=''):
        return self._link_resources(platform_agent_id, PRED.hasModel, platform_model_id)

    def unlink_model(self, platform_agent_id='', platform_model_id=''):
        return self._unlink_resources(platform_agent_id, PRED.hasModel, platform_model_id)

    def find_having_model(self, platform_model_id):
        return self._find_having(PRED.hasModel, platform_model_id)

    def find_stemming_model(self, platform_agent_id):
        return self._find_stemming(platform_agent_id, PRED.hasModel, RT.PlatformModel)
