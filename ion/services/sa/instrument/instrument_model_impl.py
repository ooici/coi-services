#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.instrument_model_impl
@author   Ian Katz
"""



#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT, LCS, PRED, LCE

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl
from ion.services.sa.instrument.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.instrument.instrument_agent_impl import InstrumentAgentImpl
from ion.services.sa.instrument.policy import ModelPolicy

class InstrumentModelImpl(ResourceSimpleImpl):
    """
    @brief resource management for InstrumentModel resources
    """

    def on_simpl_init(self):
        self.instrument_agent = InstrumentAgentImpl(self.clients)
        self.instrument_device = InstrumentDeviceImpl(self.clients)

        self.policy = ModelPolicy(self.clients)

        self.add_lce_precondition(LCE.PLAN, self.use_policy(self.policy.lce_precondition_plan))
        self.add_lce_precondition(LCE.DEVELOP, self.use_policy(self.policy.lce_precondition_develop))
        self.add_lce_precondition(LCE.INTEGRATE, self.use_policy(self.policy.lce_precondition_integrate))
        self.add_lce_precondition(LCE.DEPLOY, self.use_policy(self.policy.lce_precondition_deploy))
        self.add_lce_precondition(LCE.RETIRE, self.use_policy(self.policy.lce_precondition_retire))
        
        #self.add_lce_precondition(LCE.RETIRE, self.lcs_precondition_retired)


    def _primary_object_name(self):
        return RT.InstrumentModel

    def _primary_object_label(self):
        return "instrument_model"

    def lcs_precondition_retired(self, instrument_model_id):
        """
        can't retire if any devices or agents are using this model
        """
        if 0 < self.instrument_agent.find_having_model(instrument_model_id):
            return "Can't retire an instrument_model still associated to instrument agent(s)"
        
        if 0 < self.instrument_device.find_having_model(instrument_model_id):
            return "Can't retire an instrument_model still associated to instrument_device(s)"

        return ""
       

