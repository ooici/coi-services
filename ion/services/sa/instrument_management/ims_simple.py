#!/usr/bin/env python

__author__ = 'Ian Katz'
__license__ = 'Apache 2.0'

#from pyon.core.exception import BadRequest, NotFound
#from pyon.core.bootstrap import IonObject

from ion.services.sa.instrument_management.ims_worker import IMSworker



class IMSsimple(IMSworker):
    
    def _post_create(self, obj_id, obj):
        self.RR.execute_lifecycle_transition(resource_id=obj_id, 
                                             lcstate='ACTIVE')

        return 
