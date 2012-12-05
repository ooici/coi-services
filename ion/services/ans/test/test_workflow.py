

import unittest, os
from nose.plugins.attrib import attr

from pyon.public import log, CFG, RT, LCS, PRED,IonObject

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceProcessClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceProcessClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceProcessClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceProcessClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceProcessClient
from interface.services.ans.iworkflow_management_service import WorkflowManagementServiceProcessClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceProcessClient

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition

from ion.services.ans.test.test_helper import VisualizationIntegrationTestHelper

from pyon.util.context import LocalContextMixin


class WorkflowServiceTestProcess(LocalContextMixin):
    name = 'workflow_test'
    id='workflow_int_test'
    process_type = 'simple'

@attr('INT', group='as')
class TestWorkflowManagementIntegration(VisualizationIntegrationTestHelper):

    def setUp(self):
        # Start container

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        #Instantiate a process to represent the test
        process=WorkflowServiceTestProcess()

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceProcessClient(node=self.container.node, process=process)
        self.damsclient = DataAcquisitionManagementServiceProcessClient(node=self.container.node, process=process)
        self.pubsubclient =  PubsubManagementServiceProcessClient(node=self.container.node, process=process)
        self.ingestclient = IngestionManagementServiceProcessClient(node=self.container.node, process=process)
        self.imsclient = InstrumentManagementServiceProcessClient(node=self.container.node, process=process)
        self.dataproductclient = DataProductManagementServiceProcessClient(node=self.container.node, process=process)
        self.dataprocessclient = DataProcessManagementServiceProcessClient(node=self.container.node, process=process)
        self.datasetclient =  DatasetManagementServiceProcessClient(node=self.container.node, process=process)
        self.workflowclient = WorkflowManagementServiceProcessClient(node=self.container.node, process=process)
        self.process_dispatcher = ProcessDispatcherServiceProcessClient(node=self.container.node, process=process)
        self.data_retriever = DataRetrieverServiceProcessClient(node=self.container.node, process=process)

        self.ctd_stream_def = SBE37_CDM_stream_definition()



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_SA_transform_components(self):

        assertions = self.assertTrue

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()
        data_product_stream_ids.append(ctd_stream_id)


        ###
        ###  Setup the first transformation
        ###

        # Salinity: Data Process Definition
        ctd_L2_salinity_dprocdef_id = self.create_salinity_data_process_definition()

        l2_salinity_all_data_process_id, ctd_l2_salinity_output_dp_id = self.create_transform_process(ctd_L2_salinity_dprocdef_id,ctd_parsed_data_product_id, 'salinity' )

        ## get the stream id for the transform outputs
        stream_ids, _ = self.rrclient.find_objects(ctd_l2_salinity_output_dp_id, PRED.hasStream, None, True)
        assertions(len(stream_ids) > 0 )
        sal_stream_id = stream_ids[0]
        data_product_stream_ids.append(sal_stream_id)


        ###
        ###  Setup the second transformation
        ###

        # Salinity Doubler: Data Process Definition
        salinity_doubler_dprocdef_id = self.create_salinity_doubler_data_process_definition()

        salinity_double_data_process_id, salinity_doubler_output_dp_id = self.create_transform_process(salinity_doubler_dprocdef_id, ctd_l2_salinity_output_dp_id, 'salinity' )

        stream_ids, _ = self.rrclient.find_objects(salinity_doubler_output_dp_id, PRED.hasStream, None, True)
        assertions(len(stream_ids) > 0 )
        sal_dbl_stream_id = stream_ids[0]
        data_product_stream_ids.append(sal_dbl_stream_id)


        #Start the output stream listener to monitor and collect messages
        results = self.start_output_stream_and_listen(ctd_stream_id, data_product_stream_ids)


        #Stop the transform processes
        self.dataprocessclient.deactivate_data_process(salinity_double_data_process_id)
        self.dataprocessclient.deactivate_data_process(l2_salinity_all_data_process_id)

        #Validate the data from each of the messages along the way
        self.validate_messages(results)


    @attr('LOCOINT')
    @attr('SMOKE')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_transform_workflow(self):

        assertions = self.assertTrue

        print "Building the workflow definition"
        workflow_def_obj = IonObject(RT.WorkflowDefinition,
                                     name='Salinity_Test_Workflow',
                                     description='tests a workflow of multiple transform data processes')

        workflow_data_product_name = 'TEST-Workflow_Output_Product' #Set a specific output product name

        #-------------------------------------------------------------------------------------------------------------------------
        print "Adding a transformation process definition for salinity"
        #-------------------------------------------------------------------------------------------------------------------------

        ctd_L2_salinity_dprocdef_id = self.create_salinity_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep',
                                      data_process_definition_id=ctd_L2_salinity_dprocdef_id,
                                      persist_process_output_data=False)  #Don't persist the intermediate data product
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #-------------------------------------------------------------------------------------------------------------------------
        print "Adding a transformation process definition for salinity doubler"
        #-------------------------------------------------------------------------------------------------------------------------

        salinity_doubler_dprocdef_id = self.create_salinity_doubler_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep',
                                      data_process_definition_id=salinity_doubler_dprocdef_id, )
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        print "Creating workflow def in the resource registry"
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)

        aids = self.rrclient.find_associations(workflow_def_id, PRED.hasDataProcessDefinition)
        assertions(len(aids) == 2 )

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        print "Creating the input data product"
        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()
        data_product_stream_ids.append(ctd_stream_id)

        print "Creating and starting the workflow"
        workflow_id, workflow_product_id = self.workflowclient.create_data_process_workflow(workflow_def_id,
                                                                                            ctd_parsed_data_product_id,
                persist_workflow_data_product=True, output_data_product_name=workflow_data_product_name, timeout=300)

        workflow_output_ids,_ = self.rrclient.find_subjects(RT.Workflow, PRED.hasOutputProduct, workflow_product_id, True)
        assertions(len(workflow_output_ids) == 1 )

        print "persisting the output product"
        #self.dataproductclient.activate_data_product_persistence(workflow_product_id)
        dataset_ids,_ = self.rrclient.find_objects(workflow_product_id, PRED.hasDataset, RT.DataSet, True)
        assertions(len(dataset_ids) == 1 )
        dataset_id = dataset_ids[0]

        print "Verifying the output data product name matches what was specified in the workflow definition"
        workflow_product = self.rrclient.read(workflow_product_id)
        assertions(workflow_product.name.startswith(workflow_data_product_name), 'Nope: %s != %s' % (workflow_product.name, workflow_data_product_name))

        print "Walking the associations to find the appropriate output data streams to validate the messages"
        workflow_dp_ids,_ = self.rrclient.find_objects(workflow_id, PRED.hasDataProduct, RT.DataProduct, True)
        assertions(len(workflow_dp_ids) == 2 )

        for dp_id in workflow_dp_ids:
            stream_ids, _ = self.rrclient.find_objects(dp_id, PRED.hasStream, None, True)
            assertions(len(stream_ids) == 1 )
            data_product_stream_ids.append(stream_ids[0])

        print "data_product_stream_ids: %s" % data_product_stream_ids

        print "Starting the output stream listener to monitor to collect messages"
        results = self.start_output_stream_and_listen(ctd_stream_id, data_product_stream_ids)

        print "results::: %s" % results

        print "Stopping the workflow processes"
        self.workflowclient.terminate_data_process_workflow(workflow_id, False, timeout=250)  # Should test true at some point

        print "Making sure the Workflow object was removed"
        objs, _ = self.rrclient.find_resources(restype=RT.Workflow)
        assertions(len(objs) == 0)

        print "Validating the data from each of the messages along the way"
        self.validate_messages(results)

        print "Checking to see if dataset id = %s, was persisted, and that it can be retrieved...." % dataset_id
        self.validate_data_ingest_retrieve(dataset_id)

        print "Cleaning up to make sure delete is correct."
        self.workflowclient.delete_workflow_definition(workflow_def_id)

        workflow_def_ids,_ = self.rrclient.find_resources(restype=RT.WorkflowDefinition)
        assertions(len(workflow_def_ids) == 0 )



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_google_dt_transform_workflow(self):

        assertions = self.assertTrue

        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='GoogleDT_Test_Workflow',description='Tests the workflow of converting stream data to Google DT')

        #Add a transformation process definition
        google_dt_procdef_id = self.create_google_dt_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=google_dt_procdef_id, persist_process_output_data=False)
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()
        data_product_stream_ids.append(ctd_stream_id)

        #Create and start the workflow
        workflow_id, workflow_product_id = self.workflowclient.create_data_process_workflow(workflow_def_id, ctd_parsed_data_product_id, timeout=20)

        workflow_output_ids,_ = self.rrclient.find_subjects(RT.Workflow, PRED.hasOutputProduct, workflow_product_id, True)
        assertions(len(workflow_output_ids) == 1 )

        #Walk the associations to find the appropriate output data streams to validate the messages
        workflow_dp_ids,_ = self.rrclient.find_objects(workflow_id, PRED.hasDataProduct, RT.DataProduct, True)
        assertions(len(workflow_dp_ids) == 1 )

        for dp_id in workflow_dp_ids:
            stream_ids, _ = self.rrclient.find_objects(dp_id, PRED.hasStream, None, True)
            assertions(len(stream_ids) == 1 )
            data_product_stream_ids.append(stream_ids[0])

        #Start the output stream listener to monitor and collect messages
        results = self.start_output_stream_and_listen(ctd_stream_id, data_product_stream_ids)

        #Stop the workflow processes
        self.workflowclient.terminate_data_process_workflow(workflow_id, False)  # Should test true at some point

        #Validate the data from each of the messages along the way
        self.validate_google_dt_transform_results(results)

        """
        # Check to see if ingestion worked. Extract the granules from data_retrieval.
        # First find the dataset associated with the output dp product
        ds_ids,_ = self.rrclient.find_objects(workflow_dp_ids[len(workflow_dp_ids) - 1], PRED.hasDataset, RT.DataSet, True)
        retrieved_granule = self.data_retriever.retrieve(ds_ids[0])

        #Validate the data from each of the messages along the way
        self.validate_google_dt_transform_results(retrieved_granule)
        """

        #Cleanup to make sure delete is correct.
        self.workflowclient.delete_workflow_definition(workflow_def_id)

        workflow_def_ids,_ = self.rrclient.find_resources(restype=RT.WorkflowDefinition)
        assertions(len(workflow_def_ids) == 0 )



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_mpl_graphs_transform_workflow(self):

        assertions = self.assertTrue

        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Mpl_Graphs_Test_Workflow',description='Tests the workflow of converting stream data to Matplotlib graphs')

        #Add a transformation process definition
        mpl_graphs_procdef_id = self.create_mpl_graphs_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=mpl_graphs_procdef_id, persist_process_output_data=False)
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()
        data_product_stream_ids.append(ctd_stream_id)

        #Create and start the workflow
        workflow_id, workflow_product_id = self.workflowclient.create_data_process_workflow(workflow_def_id, ctd_parsed_data_product_id,
            persist_workflow_data_product=True, timeout=20)

        workflow_output_ids,_ = self.rrclient.find_subjects(RT.Workflow, PRED.hasOutputProduct, workflow_product_id, True)
        assertions(len(workflow_output_ids) == 1 )

        #Walk the associations to find the appropriate output data streams to validate the messages
        workflow_dp_ids,_ = self.rrclient.find_objects(workflow_id, PRED.hasDataProduct, RT.DataProduct, True)
        assertions(len(workflow_dp_ids) == 1 )

        for dp_id in workflow_dp_ids:
            stream_ids, _ = self.rrclient.find_objects(dp_id, PRED.hasStream, None, True)
            assertions(len(stream_ids) == 1 )
            data_product_stream_ids.append(stream_ids[0])

        #Start the output stream listener to monitor and collect messages
        results = self.start_output_stream_and_listen(ctd_stream_id, data_product_stream_ids)

        #Stop the workflow processes
        self.workflowclient.terminate_data_process_workflow(workflow_id, False)  # Should test true at some point

        #Validate the data from each of the messages along the way
        self.validate_mpl_graphs_transform_results(results)

        # Check to see if ingestion worked. Extract the granules from data_retrieval.
        # First find the dataset associated with the output dp product
        ds_ids,_ = self.rrclient.find_objects(workflow_dp_ids[len(workflow_dp_ids) - 1], PRED.hasDataset, RT.DataSet, True)
        retrieved_granule = self.data_retriever.retrieve_last_granule(ds_ids[0])

        #Validate the data from each of the messages along the way
        self.validate_mpl_graphs_transform_results(retrieved_granule)

        #Cleanup to make sure delete is correct.
        self.workflowclient.delete_workflow_definition(workflow_def_id)

        workflow_def_ids,_ = self.rrclient.find_resources(restype=RT.WorkflowDefinition)
        assertions(len(workflow_def_ids) == 0 )


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_multiple_workflow_instances(self):

        assertions = self.assertTrue

        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Multiple_Test_Workflow',description='Tests the workflow of converting stream data')

        #Add a transformation process definition
        google_dt_procdef_id = self.create_google_dt_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=google_dt_procdef_id, persist_process_output_data=False)
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the first input data product
        ctd_stream_id1, ctd_parsed_data_product_id1 = self.create_ctd_input_stream_and_data_product('ctd_parsed1')
        data_product_stream_ids.append(ctd_stream_id1)

        #Create and start the first workflow
        workflow_id1, workflow_product_id1 = self.workflowclient.create_data_process_workflow(workflow_def_id, ctd_parsed_data_product_id1, timeout=20)

        #Create the second input data product
        ctd_stream_id2, ctd_parsed_data_product_id2 = self.create_ctd_input_stream_and_data_product('ctd_parsed2')
        data_product_stream_ids.append(ctd_stream_id2)

        #Create and start the second workflow
        workflow_id2, workflow_product_id2 = self.workflowclient.create_data_process_workflow(workflow_def_id, ctd_parsed_data_product_id2, timeout=20)

        #Walk the associations to find the appropriate output data streams to validate the messages
        workflow_ids,_ = self.rrclient.find_resources(restype=RT.Workflow)
        assertions(len(workflow_ids) == 2 )


        #Start the first input stream process
        ctd_sim_pid1 = self.start_sinusoidal_input_stream_process(ctd_stream_id1)

        #Start the second input stream process
        ctd_sim_pid2 = self.start_simple_input_stream_process(ctd_stream_id2)

        #Start the output stream listener to monitor a set number of messages being sent through the workflows
        results = self.start_output_stream_and_listen(None, data_product_stream_ids, message_count_per_stream=5)

        # stop the flow of messages...
        self.process_dispatcher.cancel_process(ctd_sim_pid1) # kill the ctd simulator process - that is enough data
        self.process_dispatcher.cancel_process(ctd_sim_pid2)

        #Stop the first workflow processes
        self.workflowclient.terminate_data_process_workflow(workflow_id1, False)  # Should test true at some point

        #Stop the second workflow processes
        self.workflowclient.terminate_data_process_workflow(workflow_id2, False)  # Should test true at some point

        workflow_ids,_ = self.rrclient.find_resources(restype=RT.Workflow)
        assertions(len(workflow_ids) == 0 )

        #Cleanup to make sure delete is correct.
        self.workflowclient.delete_workflow_definition(workflow_def_id)

        workflow_def_ids,_ = self.rrclient.find_resources(restype=RT.WorkflowDefinition)
        assertions(len(workflow_def_ids) == 0 )

        aid_list = self.rrclient.find_associations(workflow_def_id, PRED.hasDataProcessDefinition)
        assertions(len(aid_list) == 0 )
