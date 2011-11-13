#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.sa.idata_process_management_service  import BaseDataProcessManagementService

class DataProcessManagementService(BaseDataProcessManagementService):

    def attach_process(self, process=''):
        # Connect the process to the input and output streams

        # Call DM:DataTransformMgmtSvc:DefineTransform to configure

        # Call DM:DataTransformMgmtSvc:BindTransform to connect transform and execute
        pass

    def define_data_process(self, process={}):
        # TODO: Coordinate orchestration with CEI:ProcessMgmtSvc to define a process

        # Bind transform: starts subscription to the input parameters?

        # Schedule transform: With a good transform that is bound, schedule its execution pattern

        pass

    def find_data_process(self, filters={}):
        # Locate a process based on metadata filters
        pass