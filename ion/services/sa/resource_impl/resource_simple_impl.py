#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.resource_simple_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
#from pyon.core.bootstrap import IonObject
from pyon.ion.resource import LCE
from pyon.public import LCS

from ion.services.sa.resource_impl.resource_impl import ResourceImpl


class ResourceSimpleImpl(ResourceImpl):
    """
    @brief A base class for management of ION resources that have a simple LCS
    """


    def on_impl_init(self):
        # by default allow all transitions
        # args s, r are "self" and "resource"; retval = ok ? "" : "err msg"
        #for l in LCE.keys():
        #    self.add_lce_precondition(LCE.get(l), (lambda r: ""))

        # allow retiring under all cases
        self.add_lce_precondition(LCE.RETIRE, (lambda r: ""))

        self.on_simpl_init()


    def on_simpl_init(self):
        """
        further init for simple resources
        """
        return

