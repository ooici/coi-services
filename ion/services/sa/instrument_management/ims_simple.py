#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
#from pyon.core.bootstrap import IonObject
from pyon.public import LCS

from ion.services.sa.instrument_management.ims_worker import IMSworker



class IMSsimple(IMSworker):

    def on_post_create(self, obj_id, obj):
        # simple resources go into active lifecycle state immediately upon creation
        self.advance_lcs(obj_id, LCS.ACTIVE)

        return 

    def lcs_precondition_ACTIVE(self, resource_id):
        return True
