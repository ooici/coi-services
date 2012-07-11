#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.platform_model_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT, LCE

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl
from ion.services.sa.instrument.resource_lcs_policy import ModelPolicy

class PlatformModelImpl(ResourceSimpleImpl):
    """
    @brief resource management for PlatformModel resources
    """

    def _primary_object_name(self):
        return RT.PlatformModel

    def _primary_object_label(self):
        return "platform_model"

    def on_simpl_init(self):
        self.policy = ModelPolicy(self.clients)

        self.add_lce_precondition(LCE.PLAN, self.use_policy(self.policy.lce_precondition_plan))
        self.add_lce_precondition(LCE.DEVELOP, self.use_policy(self.policy.lce_precondition_develop))
        self.add_lce_precondition(LCE.INTEGRATE, self.use_policy(self.policy.lce_precondition_integrate))
        self.add_lce_precondition(LCE.DEPLOY, self.use_policy(self.policy.lce_precondition_deploy))
        self.add_lce_precondition(LCE.RETIRE, self.use_policy(self.policy.lce_precondition_retire))
 
