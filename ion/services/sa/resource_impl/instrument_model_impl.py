#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.instrument_model_impl
@author   Ian Katz
"""



#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT, LCS

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl
from ion.services.sa.resource_impl.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.resource_impl.instrument_agent_impl import InstrumentAgentImpl

class InstrumentModelImpl(ResourceSimpleImpl):
    """
    @brief resource management for InstrumentModel resources
    """

    def on_simpl_init(self):
        self.instrument_agent = InstrumentAgentImpl(self.clients)
        self.instrument_device = InstrumentDeviceImpl(self.clients)

        self.add_lcs_precondition(LCS.RETIRED, self.lcs_precondition_retired)


    def _primary_object_name(self):
        return RT.InstrumentModel

    def _primary_object_label(self):
        return "instrument_model"

    def lcs_precondition_retired(self, instrument_model_id):
        """
        can't retire if any devices or agents are using this model
        """
        found, _ = self.instrument_agent.find_having(instrument_model_id)
        if 0 < len(found):
            return False
        
        found, _ = self.instrument_device.find_having(instrument_model_id)
        if 0 < len(found):
            return False

        return True
        

        
