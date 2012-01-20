#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.ans.ivisualization_workflow_management_service import BaseVisualizationWorkflowManagementService

class VisualizationWorkflowManagementService(BaseVisualizationWorkflowManagementService):

    """
    The Visualization Workflow Management Service provides support for the definition, integration and enactment of defined workflows.
    Capabilities to enable the definition, instantiation, scheduling and execution control of developer provided workflows for the visualization of data products
    """

    def create_visualization_workflow(self, visualization_workflow=None):
        """Creates an Visualization Workflow resource from the parameter visualization_workflow object.

        @param visualization_workflow    VisualizationWorkflow
        @retval visualization_workflow_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

    def update_visualization_workflow(self, visualization_workflow=None):
        """Updates an existing Visualization Workflow resource.

        @param visualization_workflow    VisualizationWorkflow
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        pass

    def read_visualization_workflow(self, visualization_workflow_id=''):
        """Returns an existing Visualization Workflow resource.

        @param visualization_workflow_id    str
        @retval visualization_workflow    VisualizationWorkflow
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_visualization_workflow(self, visualization_workflow_id=''):
        """Deletes an existing Visualization Workflow resource.

        @param visualization_workflow_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def find_visualization_workflow(self, filters=None):
        """Returns a list of Visualization Workflow resources for the provided resource filter.

        @param filters    ResourceFilter
        @retval visualization_workflow_list    []
        """
        pass