#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.ans.ivisualization_workflow_management_service import BaseVisualizationWorkflowManagementService

class VisualizationWorkflowManagementService(BaseVisualizationWorkflowManagementService):

    """
    class docstring
    """

    def create_visualization_workflow(self, visualization_workflow={}):
        """Should take a VisualizationWorkflow object ans a parameter
        """
        # Return Value
        # ------------
        # {visualization_workflow_id: ''}
        #
        pass

    def update_visualization_workflow(self, visualization_workflow={}):
        """Should take a VisualizationWorkflow object ans a parameter
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_visualization_workflow(self, visualization_workflow_id=''):
        """method docstring
        """
        # Return Value - Should return a VisualizationWorkflow object
        # ------------
        # visualization_workflow: {}
        #
        pass

    def delete_visualization_workflow(self, visualization_workflow_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass