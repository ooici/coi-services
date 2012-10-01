#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.instrument_agent_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT, LCE
from ion.services.sa.instrument.flag import KeywordFlag
from ion.services.sa.instrument.resource_lcs_policy import AgentPolicy

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class InstrumentAgentImpl(ResourceSimpleImpl):
    """
    @brief Resource management for InstrumentAgent resources
    """

    def _primary_object_name(self):
        return RT.InstrumentAgent

    def _primary_object_label(self):
        return "instrument_agent"

    def on_impl_init(self):
        self.policy = AgentPolicy(self.clients)
        
        self.add_lce_precondition(LCE.PLAN, self.policy.lce_precondition_plan)
        self.add_lce_precondition(LCE.INTEGRATE, self.policy.lce_precondition_integrate)
        self.add_lce_precondition(LCE.DEVELOP, self.policy.lce_precondition_develop)
        self.add_lce_precondition(LCE.DEPLOY, self.policy.lce_precondition_deploy)
        self.add_lce_precondition(LCE.RETIRE, self.policy.lce_precondition_retire)
        
        # self.add_lce_precondition(LCE.PLAN, (lambda r: "")) # no precondition to plan
        # self.add_lce_precondition(LCE.INTEGRATE, self.lce_precondition_integrate)
        # self.add_lce_precondition(LCE.DEVELOP, self.lce_precondition_develop)
        # self.add_lce_precondition(LCE.DEPLOY, self.lce_precondition_deploy)
        
    
    def lce_precondition_deploy(self, instrument_agent_id):
        pre = self.lce_precondition_integrate(instrument_agent_id)
        if pre: 
            return pre
        
        found = False
        for a in self.find_stemming_attachment(instrument_agent_id):
            for k in a.keywords:
                if KeywordFlag.CERTIFICATION == k:
                    found = True
                    break
        if found:
            return ""
        else:
            return "InstrumentAgent LCS requires certfication"

    def lce_precondition_integrate(self, instrument_agent_id):
        pre = self.lce_precondition_develop(instrument_agent_id)
        if pre: 
            return pre
        
        found = False
        for a in self.find_stemming_attachment(instrument_agent_id):
            for k in a.keywords:
                if KeywordFlag.EGG_URL == k:
                    found = True
                    break

        if found:
            return ""
        else:
            return "InstrumentAgent LCS requires a registered driver egg"

    def lce_precondition_develop(self, instrument_agent_id):
        if 0 < len(self.find_stemming_model(instrument_agent_id)):
            return ""
        else:
            return "InstrumentAgent LCS requires an associated instrument model"

    def link_model(self, instrument_agent_id='', instrument_model_id=''):
        return self._link_resources_single_subject(instrument_agent_id, PRED.hasModel, instrument_model_id)

    def unlink_model(self, instrument_agent_id='', instrument_model_id=''):
        return self._unlink_resources(instrument_agent_id, PRED.hasModel, instrument_model_id)

    def find_having_model(self, instrument_model_id):
        return self._find_having_single(PRED.hasModel, instrument_model_id)

    def find_stemming_model(self, instrument_agent_id):
        return self._find_stemming(instrument_agent_id, PRED.hasModel, RT.InstrumentModel)

    def link_process_definition(self, instrument_agent_id='', process_definition_id=''):
        return self._link_resources_single_subject(instrument_agent_id, PRED.hasProcessDefinition, process_definition_id)

    def unlink_process_definition(self, instrument_agent_id='', process_definition_id=''):
        return self._unlink_resources(instrument_agent_id, PRED.hasProcessDefinition, process_definition_id)

    def find_having_process_definition(self, process_definition_id):
        return self._find_having_single(PRED.hasProcessDefinition, process_definition_id)

    def find_stemming_process_definition(self, instrument_agent_id):
        return self._find_stemming(instrument_agent_id, PRED.hasProcessDefinition, RT.InstrumentModel)

