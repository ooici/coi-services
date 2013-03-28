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


    def create_data_process_workflow(self, workflow_definition_id='', input_data_product_id='', persist_workflow_data_product=True, output_data_product_name='', configuration={}):
        """Instantiates a Data Process Workflow specified by a Workflow Definition resource and an input data product id.
        Returns the id of the workflow and the data product id for the final output product.

        @param workflow_definition_id    str
        @param input_data_product_id    str
        @param persist_workflow_data_product    bool
        @param output_data_product_name    str
        @param configuration    IngestionConfiguration
        @retval workflow_id    str
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

        if output_data_product_name:
            workflow_name = '%s_%s' % (workflow_definition.name, output_data_product_name)
        else:
            workflow_name = create_unique_identifier('workflow_%s' % (workflow_definition.name))

         #Create Workflow object and associations to track the instantiation of a work flow definition.
        workflow = IonObject(RT.Workflow, name=workflow_name, persist_process_output_data=persist_workflow_data_product,
                                output_data_product_name=output_data_product_name, configuration=configuration)
        workflow_id, _ = self.clients.resource_registry.create(workflow)
        self.clients.resource_registry.create_association(workflow_id, PRED.hasDefinition,workflow_definition_id )
        self.clients.resource_registry.create_association(workflow_id, PRED.hasInputProduct,input_data_product_id )

        #Setup the input data product id as the initial input product stream
        data_process_input_dp_id = input_data_product_id

        output_data_products = {}
        output_data_product_id = None # Overall product id to return

        #Iterate through the workflow steps to setup the data processes and connect them together.
        for wf_step in workflow_definition.workflow_steps:
            log.debug("wf_step.data_process_definition_id: %s" , wf_step.data_process_definition_id)

            data_process_definition = self.clients.resource_registry.read(wf_step.data_process_definition_id)

            for binding, stream_definition_id in data_process_definition.output_bindings.iteritems():

                #--------------------------------------------------------------------------------
                # Create an output data product for each binding/stream definition
                #--------------------------------------------------------------------------------

                #Handle the last step differently as it is the output of the workflow.
                if wf_step == workflow_definition.workflow_steps[-1] and output_data_product_name:
                    data_product_name = output_data_product_name
                else:
                    data_product_name = create_unique_identifier('%s_%s_%s' % (workflow_name, data_process_definition.name, binding))

                tdom, sdom = time_series_domain()

                data_product_obj = IonObject(RT.DataProduct, 
                                             name            = data_product_name,
                                             description     = data_process_definition.description,
                                             temporal_domain = tdom.dump(),
                                             spatial_domain  = sdom.dump())
                data_product_id = self.clients.data_product_management.create_data_product(data_product_obj, stream_definition_id=stream_definition_id)


                # Persist if necessary - handle the last step of the workflow differently
                if wf_step == workflow_definition.workflow_steps[-1]:
                    if persist_workflow_data_product:
                        self.clients.data_product_management.activate_data_product_persistence(data_product_id=data_product_id)
                else:
                    # Persist intermediate steps independently
                    if wf_step.persist_process_output_data:
                        self.clients.data_product_management.activate_data_product_persistence(data_product_id=data_product_id)


                #Associate the intermediate data products with the workflow
                self.clients.resource_registry.create_association(workflow_id, PRED.hasDataProduct, data_product_id )
                output_data_products[binding] = data_product_id

            #May have to merge configuration blocks where the workflow entries will override the configuration in a step
            if configuration:
                process_config = dict(wf_step.configuration.items() + configuration.items())
            else:
                process_config = wf_step.configuration

            data_process_id = self.clients.data_process_management.create_data_process(data_process_definition._id, [data_process_input_dp_id], output_data_products, configuration=process_config)
            self.clients.data_process_management.activate_data_process(data_process_id)

            #Track the the data process with an association to the workflow
            self.clients.resource_registry.create_association(workflow_id, PRED.hasDataProcess, data_process_id )

            #last one out of the for loop is the output product id
            output_data_product_id = output_data_products.values()[0]

            #Save the id of the output data stream for input to the next process in the workflow.
            data_process_input_dp_id = output_data_products.values()[0]


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


            try:
                #This may fail if the dat data product was not persisted - which is ok.
                self.clients.data_product_management.suspend_data_product_persistence(dp_id)
            except Exception, e:
                log.warn(e.message)
                pass

            if delete_data_products:
                self.clients.data_product_management.delete_data_product(dp_id)

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
