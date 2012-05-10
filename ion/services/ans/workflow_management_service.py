#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.ans.iworkflow_management_service import BaseWorkflowManagementService

class WorkflowManagementService(BaseWorkflowManagementService):

    """
    The Workflow Management Service provides support for the definition, integration and enactment of
    defined workflows. Capabilities to enable the definition, instantiation, scheduling and execution
    control of developer provided workflows; specifically for the visualization of data products
    """

    def create_workflow_definition(self, workflow_definition=None):
        """Creates a Workflow Definition resource which specifies the steps involved in a workflow process.

        @param workflow_definition    WorkflowDefinition
        @retval workflow_definition_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

    def update_workflow_definition(self, workflow_definition=None):
        """Updates an existing Workflow Definition resource.

        @param workflow_definition    WorkflowDefinition
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        pass

    def read_workflow_definition(self, workflow_definition_id=''):
        """Returns an existing Workflow Definition resource.

        @param workflow_definition_id    str
        @retval workflow_definition    WorkflowDefinition
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_workflow_definition(self, workflow_definition_id=''):
        """Deletes an existing Workflow Definition resource.

        @param workflow_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def create_workflow(self, workflow_definition_id='', input_data_product_id=''):
        """Instantiates a Workflow specific by a Workflow Definition resource and an input data product id.
        Returns the data product id for the final output product.

        @param workflow_definition_id    str
        @param input_data_product_id    str
        @retval output_data_product_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

    def delete_workflow(self, workflow_definition_id=''):
        """Terminates a Workflow specific by a Workflow Definition resource which includes all internal processes.

        @param workflow_definition_id    str
        @retval data_product_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

