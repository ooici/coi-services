#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.ans.iworkflow_management_service import BaseWorkflowManagementService
from pyon.util.containers import is_basic_identifier
from pyon.core.exception import BadRequest, NotFound

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
        if not is_basic_identifier(workflow_definition.name):
            raise BadRequest("The workflow definition name '%s' can only contain alphanumeric and underscore characters" % workflow_definition.name)

        workflow_definition_id, version = self.clients.resource_registry.create(workflow_definition)
        return workflow_definition_id


    def update_workflow_definition(self, workflow_definition=None):
        """Updates an existing Workflow Definition resource.

        @param workflow_definition    WorkflowDefinition
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        if not is_basic_identifier(workflow_definition.name):
            raise BadRequest("The workflow definition name '%s' can only contain alphanumeric and underscore characters" % workflow_definition.name)

        self.clients.resource_registry.update(workflow_definition)

    def read_workflow_definition(self, workflow_definition_id=''):
        """Returns an existing Workflow Definition resource.

        @param workflow_definition_id    str
        @retval workflow_definition    WorkflowDefinition
        @throws NotFound    object with specified id does not exist
        """
        if not workflow_definition_id:
            raise BadRequest("The workflow_definition_id parameter is missing")

        workflow_definition = self.clients.resource_registry.read(workflow_definition_id)
        if not workflow_definition:
            raise NotFound("workflow_definition_id %s does not exist" % workflow_definition_id)

        return workflow_definition


    def delete_workflow_definition(self, workflow_definition_id=''):
        """Deletes an existing Workflow Definition resource.

        @param workflow_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        if not workflow_definition_id:
            raise BadRequest("The workflow_definition_id parameter is missing")

        workflow_definition = self.clients.resource_registry.read(workflow_definition_id)
        if not workflow_definition:
            raise NotFound("workflow_definition_id %s does not exist" % workflow_definition_id)
        self.clients.resource_registry.delete(workflow_definition_id)


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

