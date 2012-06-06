#!/usr/bin/env python


__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

from interface.services.ans.iworkflow_management_service import BaseWorkflowManagementService
from pyon.util.containers import is_basic_identifier
from pyon.core.exception import BadRequest, NotFound, Inconsistent
from pyon.public import Container, log, IonObject, RT,PRED

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

        if workflow_definition.type_ == RT.DataProcessWorkflowDefinition:
            workflow_definition = self.read_workflow_definition(workflow_definition_id)
            self._update_data_process_workflow_def_associations(workflow_definition)


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

        if workflow_definition.type_ == RT.DataProcessWorkflowDefinition:
            self._update_data_process_workflow_def_associations(workflow_definition)

    def _delete_data_process_workflow_def_associations(self, workflow_definition_id):

        #Remove and existing associations
        aid_list = self.clients.resource_registry.find_associations(workflow_definition_id, PRED.hasDataProcessDefinition, RT.DataProcessDefinition)
        for aid in aid_list:
            self.clients.resource_registry.delete_association(aid)

    def _update_data_process_workflow_def_associations(self, workflow_definition):

        #Remove and existing associations
        self._delete_data_process_workflow_def_associations(workflow_definition._id)

        #For Data Process workflows, create the appropriate associations
        for wf_step in workflow_definition.workflow_steps:
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


        if workflow_definition.type_ == RT.DataProcessWorkflowDefinition:
            self._delete_data_process_workflow_def_associations(workflow_definition_id)

        self.clients.resource_registry.delete(workflow_definition_id)


    def create_data_process_workflow(self, workflow_definition_id='', input_data_product_id=''):
        """Instantiates a Data Process Workflow specified by a Workflow Definition resource and an input data product id.
      Returns the id of the workflow and the data product id for the final output product.

        @param workflow_definition_id    str
        @param input_data_product_id    str
        @retval workflow_id str
        @retval output_data_product_id    str
        @throws BadRequest    if any of the required parameters are not set
        @throws NotFound    object with specified id does not exist
        """

        if not workflow_definition_id:
            raise BadRequest("The workflow_definition_id parameter is missing")

        workflow_definition = self.clients.resource_registry.read(workflow_definition_id)
        if not workflow_definition:
            raise NotFound("WorkflowDefinition %s does not exist" % workflow_definition_id)

        if not input_data_product_id:
            raise BadRequest("The input_data_product_id parameter is missing")

        input_data_product = self.clients.resource_registry.read(input_data_product_id)
        if not input_data_product:
            raise NotFound("The input data product %s does not exist" % input_data_product_id)


        #Create Workflow object and associations to track the instantiation of a work flow definition.
        workflow = IonObject(RT.Workflow, name=workflow_definition.name)
        workflow_id, _ = self.clients.resource_registry.create(workflow)
        self.clients.resource_registry.create_association(workflow_id, PRED.hasDefinition,workflow_definition_id )
        self.clients.resource_registry.create_association(workflow_id, PRED.hasInputProduct,input_data_product_id )

        #Setup the input data product id as the initial input product stream
        data_process_input_dp_id = input_data_product_id

        output_data_product_id = None

        #Iterate through the workflow steps to setup the data processes and connect them together.
        for wf_step in workflow_definition.workflow_steps:
            log.info("proc_def_id: " + wf_step.data_process_definition_id)

            data_process_definition = self.clients.resource_registry.read(wf_step.data_process_definition_id)

            # Find the link between the output Stream Definition resource and the Data Process Definition resource
            stream_ids,_ = self.clients.resource_registry.find_objects(data_process_definition._id, PRED.hasStreamDefinition, RT.StreamDefinition,  id_only=True)
            if not stream_ids:
                raise Inconsistent("The data process definition %s is missing an association to an output stream definition" % data_process_definition._id )
            process_output_stream_def_id = stream_ids[0]

            #Concatenate the name of the workflow and data process definition for the name of the data product output
            data_process_name = workflow_definition.name + '_' + data_process_definition.name

            # Create the output data product of the transform
            transform_dp_obj = IonObject(RT.DataProduct, name=data_process_name,description=data_process_definition.description)
            transform_dp_id = self.clients.data_product_management.create_data_product(transform_dp_obj, process_output_stream_def_id)
            if wf_step.persist_data:
                self.clients.data_product_management.activate_data_product_persistence(data_product_id=transform_dp_id, persist_data=wf_step.persist_data, persist_metadata=wf_step.persist_metadata)

            #Associate the intermediate data products with the workflow
            self.clients.resource_registry.create_association(workflow_id, PRED.hasDataProduct, transform_dp_id )

            # Create the  transform data process
            log.debug("create data_process and start it")
            data_process_id = self.clients.data_process_management.create_data_process(data_process_definition._id, [data_process_input_dp_id], {'output':transform_dp_id})
            self.clients.data_process_management.activate_data_process(data_process_id)

            #Track the the data process with an association to the workflow
            self.clients.resource_registry.create_association(workflow_id, PRED.hasDataProcess, data_process_id )

            #last one out of the for loop is the output product id
            output_data_product_id = transform_dp_id

            #Save the id of the output data stream for input to the next process in the workflow.
            data_process_input_dp_id = transform_dp_id


        #Track the output data product with an association
        self.clients.resource_registry.create_association(workflow_id, PRED.hasOutputProduct, output_data_product_id )

        return workflow_id, output_data_product_id


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
            self.clients.data_process_management.deactivate_data_process(pid)
            self.clients.data_process_management.delete_data_process(pid)
            aid = self.clients.resource_registry.find_associations(workflow_id, PRED.hasDataProcess, pid)
            if aid:
                self.clients.resource_registry.delete_association(aid[0])


        #Iterate through all of the data product associations and suspend and delete them
        workflow_dp_ids,_ = self.clients.resource_registry.find_objects(workflow_id, PRED.hasDataProduct, RT.DataProduct, True)
        for dp_id in workflow_dp_ids:

            if delete_data_products: #TODO - may have to revist this once the SA level stabilizes
                self.clients.data_product_management.suspend_data_product_persistence(dp_id)
                self.clients.data_product_management.delete_data_product(dp_id)

            aid = self.clients.resource_registry.find_associations(workflow_id, PRED.hasDataProduct, dp_id)
            if aid:
                self.clients.resource_registry.delete_association(aid[0])


        #Remove other associations
        aid = self.clients.resource_registry.find_associations(workflow_id, PRED.hasOutputProduct, RT.DataProduct)
        if aid:
            self.clients.resource_registry.delete_association(aid[0])

        aid = self.clients.resource_registry.find_associations(workflow_id, PRED.hasInputProduct, RT.DataProduct)
        if aid:
            self.clients.resource_registry.delete_association(aid[0])

        aid = self.clients.resource_registry.find_associations(workflow_id, PRED.hasDefinition, RT.DataProcessWorkflowDefinition)
        if aid:
            self.clients.resource_registry.delete_association(aid[0])

        #Finally remove the workflow object itself
        self.clients.resource_registry.delete(workflow_id)
