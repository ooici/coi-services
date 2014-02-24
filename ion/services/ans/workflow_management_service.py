#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.ans.iworkflow_management_service import BaseWorkflowManagementService
from pyon.util.containers import is_basic_identifier, create_unique_identifier, get_safe
from pyon.core.exception import BadRequest, NotFound, Inconsistent
from pyon.public import Container, log, IonObject, RT,PRED, OT
from ion.services.dm.utility.granule_utils import time_series_domain

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


        workflow_definition = self.read_workflow_definition(workflow_definition_id)
        self._update_workflow_associations(workflow_definition)


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

        self._update_workflow_associations(workflow_definition)

    def _delete_workflow_associations(self, workflow_definition_id):

        #Remove and existing associations
        aid_list = self.clients.resource_registry.find_associations(workflow_definition_id, PRED.hasDataProcessDefinition)
        for aid in aid_list:
            self.clients.resource_registry.delete_association(aid)

    def _update_workflow_associations(self, workflow_definition):

        #Remove and existing associations
        self._delete_workflow_associations(workflow_definition._id)

        #For each Data Process workflow step, create the appropriate associations
        for wf_step in workflow_definition.workflow_steps:
            if wf_step.type_ == OT.DataProcessWorkflowStep:
                self.clients.resource_registry.create_association(workflow_definition._id, PRED.hasDataProcessDefinition, wf_step.data_process_definition_id)


    def read_workflow_definition(self, workflow_definition_id=''):
        """Returns an existing Workflow Definition resource.

        @param workflow_definition_id    str
        @retval workflow_definition    WorkflowDefinition
        @throws BadRequest    if any of the required parameters are not set
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
        @throws BadRequest    if any of the required parameters are not set
        @throws NotFound    object with specified id does not exist
        """
        if not workflow_definition_id:
            raise BadRequest("The workflow_definition_id parameter is missing")

        workflow_definition = self.clients.resource_registry.read(workflow_definition_id)
        if not workflow_definition:
            raise NotFound("workflow_definition_id %s does not exist" % workflow_definition_id)


        self._delete_workflow_associations(workflow_definition_id)

        self.clients.resource_registry.delete(workflow_definition_id)




    def terminate_data_process_workflow(self, workflow_id='', delete_data_products=True):
        """Terminates a Workflow specific by a Workflow Definition resource which includes all internal processes.

        @param workflow_id    str
        @throws BadRequest    if any of the required parameters are not set
        @throws NotFound    object with specified id does not exist
        """

        if not workflow_id:
            raise BadRequest("The workflow_id parameter is missing")

        workflow = self.clients.resource_registry.read(workflow_id)
        if not workflow:
            raise NotFound("Workflow %s does not exist" % workflow_id)

        #Iterate through all of the data process associates and deactivate and delete them
        process_ids,_ = self.clients.resource_registry.find_objects(workflow_id, PRED.hasDataProcess, RT.DataProcess, True)
        for pid in process_ids:
            self.clients.data_process_management.deactivate_data_process(pid, timeout=30)
            self.clients.data_process_management.delete_data_process(pid, timeout=30)
            aid = self.clients.resource_registry.find_associations(workflow_id, PRED.hasDataProcess, pid)
            if aid:
                self.clients.resource_registry.delete_association(aid[0])


        #Iterate through all of the data product associations and suspend and delete them
        workflow_dp_ids,_ = self.clients.resource_registry.find_objects(workflow_id, PRED.hasDataProduct, RT.DataProduct, True)
        for dp_id in workflow_dp_ids:


            try:
                #This may fail if the dat data product was not persisted - which is ok.
                self.clients.data_product_management.suspend_data_product_persistence(dp_id, timeout=30)
            except Exception, e:
                log.warn(e.message)
                pass

            if delete_data_products:
                self.clients.data_product_management.delete_data_product(dp_id, timeout=30)

            aid = self.clients.resource_registry.find_associations(workflow_id, PRED.hasDataProduct, dp_id)
            if aid:
                self.clients.resource_registry.delete_association(aid[0])


        #Remove other associations
        aid = self.clients.resource_registry.find_associations(workflow_id, PRED.hasOutputProduct)
        if aid:
            self.clients.resource_registry.delete_association(aid[0])

        aid = self.clients.resource_registry.find_associations(workflow_id, PRED.hasInputProduct)
        if aid:
            self.clients.resource_registry.delete_association(aid[0])

        aid = self.clients.resource_registry.find_associations(workflow_id, PRED.hasDefinition)
        if aid:
            self.clients.resource_registry.delete_association(aid[0])

        #Finally remove the workflow object itself
        self.clients.resource_registry.delete(workflow_id)
