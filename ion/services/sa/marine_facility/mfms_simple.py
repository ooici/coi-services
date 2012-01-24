#!/usr/bin/env python

"""
@package  ion.services.sa.marine_facility.mfms_simple
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
#from pyon.core.bootstrap import IonObject
from pyon.public import LCS

from ion.services.sa.resource_impl import ResourceImpl


class MFMSsimple(ResourceImpl):
    """
    @brief A base class for management of ION resources in MFMS that have a simple LCS
    """
    
    def on_impl_init(self):

        # lcs preconditions that are all operator discretion
        self.add_lcs_precondition(LCS.DISCOVERABLE,  self.lcs_precondition_always)
        self.add_lcs_precondition(LCS.AVAILABLE,     self.lcs_precondition_always)
        self.add_lcs_precondition(LCS.RETIRED,       self.lcs_precondition_always)

        self.add_lcs_precondition(LCS.PLANNED,       self.lcs_precondition_unimplemented)
        self.add_lcs_precondition(LCS.DEVELOPED,     self.lcs_precondition_unimplemented)
        self.add_lcs_precondition(LCS.INTEGRATED,    self.lcs_precondition_unimplemented)

        self.on_simpl_init()

    def on_simpl_init(self):
        """
        further init for simple resources
        """
        return

    def lcs_precondition_always(self, resource_id):
        return True

    def lcs_precondition_unimplemented(self, resource_id):
        raise NotImplementedError("Extender of the class must write this!")
    

    def on_post_create(self, obj_id, obj):
        """
        this is for simple resources ...
        they go into active lifecycle state immediately upon creation
        @param obj_id an object id
        @param obj the object itself (not needed in this case)
        """
        self.advance_lcs(obj_id, LCS.AVAILABLE)

        return


