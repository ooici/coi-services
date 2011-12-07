#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.cei.itaskable_resource_planner_service import BaseTaskableResourcePlannerService

class TaskableResourcePlannerService(BaseTaskableResourcePlannerService):


    def request_computation(self, computation_request={}):
        """ Should receive a ComputationRequest object
        """
        # Return Value
        # ------------
        # {computation_id: ''}
        #
        pass

    def cancel_computation(self, computation_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass