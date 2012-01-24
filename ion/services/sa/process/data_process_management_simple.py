#!/usr/bin/env python

"""
@package  ion.services.sa.data_process_management_service.data_processs_management_simple
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
#from pyon.core.bootstrap import IonObject
from pyon.public import LCS

from ion.services.sa.resource_impl import ResourceImpl


class DataProductManagementSimple(ResourceImpl):
    """
    @brief A base class for management of ION resources in DPrMS that have a simple LCS
    """
    
    def on_impl_init(self):
        # no checks, simple resources just go straight to available on create
        self.add_lcs_precondition(LCS.AVAILABLE, (lambda s, r: True))
    
    def on_post_create(self, obj_id, obj):
        """
        this is for simple resources ...
        they go into active lifecycle state immediately upon creation
        @param obj_id an object id
        @param obj the object itself (not needed in this case)
        """
        self.advance_lcs(obj_id, LCS.AVAILABLE)

        return

