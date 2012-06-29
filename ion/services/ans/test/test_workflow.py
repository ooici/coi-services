from pyon.core.object import IonObjectSerializer
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from interface.objects import CouchStorage, ProcessDefinition, StreamQuery
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.ans.iworkflow_management_service import WorkflowManagementServiceClient

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
from ion.processes.data.transforms.ctd.ctd_L2_salinity import SalinityTransform
from ion.processes.data.transforms.example_double_salinity import SalinityDoubler
from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDT
from ion.processes.data.transforms.viz.matplotlib_graphs import VizTransformMatplotlibGraphs

from pyon.public import log

from pyon.public import StreamSubscriberRegistrar

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG, RT, LCS, PRED
from pyon.core.exception import BadRequest, Inconsistent

from nose.plugins.attrib import attr
import unittest, os
import imghdr
import gevent, numpy
from prototype.sci_data.stream_parser import PointSupplementStreamParser

from interface.objects import Granule
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe

from pyon.util.context import LocalContextMixin


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='as')
class TestWorkflowManagementIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.workflowclient = WorkflowManagementServiceClient(node=self.container.node)
        self.process_dispatcher = ProcessDispatcherServiceClient(node=self.container.node)

        self.ctd_stream_def = SBE37_CDM_stream_definition()

    def _create_ctd_input_stream_and_data_product(self, data_product_name='ctd_parsed'):

        cc = self.container
        assertions = self.assertTrue

        #-------------------------------
        # Create CTD Parsed as the initial data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=self.ctd_stream_def, name='Simulated CTD data')


        log.debug('Creating new CDM data product with a stream definition')
        dp_obj = IonObject(RT.DataProduct,name=data_product_name,description='ctd stream test')
        try:
            ctd_parsed_data_product_id = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)
        except Exception as ex:
            self.fail("failed to create new data product: %s" %ex)

        log.debug('new ctd_parsed_data_product_id = %s' % ctd_parsed_data_product_id)

        #Only ever need one device for testing purposes.
        instDevice_obj,_ = self.rrclient.find_resources(restype=RT.InstrumentDevice, name='SBE37IMDevice')
        if instDevice_obj:
            instDevice_id = instDevice_obj[0]._id
        else:
            instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product_id)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product_id, persist_data=False, persist_metadata=False)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product_id, PRED.hasStream, None, True)
        assertions(len(stream_ids) > 0 )
        ctd_stream_id = stream_ids[0]

        return ctd_stream_id, ctd_parsed_data_product_id

    def _start_simple_input_stream_process(self, ctd_stream_id):
        return self._start_input_stream_process(ctd_stream_id)

    def _start_sinusoidal_input_stream_process(self, ctd_stream_id):
        return self._start_input_stream_process(ctd_stream_id, 'ion.processes.data.sinusoidal_stream_publisher', 'SinusoidalCtdPublisher')

    def _start_input_stream_process(self, ctd_stream_id, module = 'ion.processes.data.ctd_stream_publisher', class_name= 'SimpleCtdPublisher'):


        ###
        ### Start the process for producing the CTD data
        ###
        # process definition for the ctd simulator...
        producer_definition = ProcessDefinition()
        producer_definition.executable = {
            'module':module,
            'class':class_name
        }

        ctd_sim_procdef_id = self.process_dispatcher.create_process_definition(process_definition=producer_definition)

        # Start the ctd simulator to produce some data
        configuration = {
            'process':{
                'stream_id':ctd_stream_id,
                }
        }
        ctd_sim_pid = self.process_dispatcher.schedule_process(process_definition_id=ctd_sim_procdef_id, configuration=configuration)

        return ctd_sim_pid

    def _start_output_stream_and_listen(self, ctd_stream_id, data_product_stream_ids, message_count_per_stream=10):

        cc = self.container
        assertions = self.assertTrue

        ###
        ### Make a subscriber in the test to listen for transformed data
        ###
        salinity_subscription_id = self.pubsubclient.create_subscription(
            query=StreamQuery(data_product_stream_ids),
            exchange_name = 'workflow_test',
            name = "test workflow transformations",
        )

        pid = cc.spawn_process(name='dummy_process_for_test',
            module='pyon.ion.process',
            cls='SimpleProcess',
            config={})
        dummy_process = cc.proc_manager.procs[pid]

        subscriber_registrar = StreamSubscriberRegistrar(process=dummy_process, node=cc.node)

        result = gevent.event.AsyncResult()
        results = []
        message_count = len(data_product_stream_ids) * message_count_per_stream

        def message_received(message, headers):
            # Heads
            results.append(message)
            if len(results) >= message_count:   #Only wait for so many messages - per stream
                result.set(True)

        subscriber = subscriber_registrar.create_subscriber(exchange_name='workflow_test', callback=message_received)
        subscriber.start()

        # after the queue has been created it is safe to activate the subscription
        self.pubsubclient.activate_subscription(subscription_id=salinity_subscription_id)


        #Start the input stream process
        if ctd_stream_id is not None:
            ctd_sim_pid = self._start_simple_input_stream_process(ctd_stream_id)

        # Assert that we have received data
        assertions(result.get(timeout=30))

        # stop the flow parse the messages...
        if ctd_stream_id is not None:
            self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

        self.pubsubclient.deactivate_subscription(subscription_id=salinity_subscription_id)

        subscriber.stop()

        return results


    def _validate_messages(self, results):

        cc = self.container
        assertions = self.assertTrue

        first_salinity_values = None

        for message in results:
            rdt = RecordDictionaryTool.load_from_granule(message)
            rdt0 = rdt['coordinates']
            rdt1 = rdt['data']
            try:
                temp = get_safe(rdt1, 'temp')
#                psd = PointSupplementStreamParser(stream_definition=self.ctd_stream_def, stream_granule=message)
#                temp = psd.get_values('temperature')
#                log.info(psd.list_field_names())
            except KeyError as ke:
                temp = None

            if temp is not None:
                assertions(isinstance(temp, numpy.ndarray))

                log.info( 'temperature=' + str(numpy.nanmin(temp)))

                first_salinity_values = None

            else:
                #psd = PointSupplementStreamParser(stream_definition=SalinityTransform.outgoing_stream_def, stream_granule=message)
                #log.info( psd.list_field_names())

                # Test the handy info method for the names of fields in the stream def
                #assertions('salinity' in psd.list_field_names())

                # you have to know the name of the coverage in stream def
                salinity = get_safe(rdt1, 'salinity')
                #salinity = psd.get_values('salinity')
                log.info( 'salinity=' + str(numpy.nanmin(salinity)))

                assertions(isinstance(salinity, numpy.ndarray))

                assertions(numpy.nanmin(salinity) > 0.0) # salinity should always be greater than 0

                if first_salinity_values is None:
                    first_salinity_values = salinity.tolist()
                else:
                    second_salinity_values = salinity.tolist()
                    assertions(len(first_salinity_values) == len(second_salinity_values))
                    for idx in range(0,len(first_salinity_values)):
                        assertions(first_salinity_values[idx]*2.0 == second_salinity_values[idx])


    def _create_salinity_data_process_definition(self):

        # Salinity: Data Process Definition

        #First look to see if it exists and if not, then create it
        dpd,_ = self.rrclient.find_resources(restype=RT.DataProcessDefinition, name='ctd_salinity')
        if len(dpd) > 0:
            return dpd[0]

        log.debug("Create data process definition SalinityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_salinity',
            description='create a salinity data product',
            module='ion.processes.data.transforms.ctd.ctd_L2_salinity',
            class_name='SalinityTransform',
            process_source='SalinityTransform source code here...')
        try:
            ctd_L2_salinity_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Excpetion as ex:
            self.fail("failed to create new SalinityTransform data process definition: %s" %ex)

        # create a stream definition for the data from the salinity Transform
        sal_stream_def_id = self.pubsubclient.create_stream_definition(container=SalinityTransform.outgoing_stream_def,  name='Salinity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(sal_stream_def_id, ctd_L2_salinity_dprocdef_id )

        return ctd_L2_salinity_dprocdef_id

    def _create_salinity_doubler_data_process_definition(self):

        #First look to see if it exists and if not, then create it
        dpd,_ = self.rrclient.find_resources(restype=RT.DataProcessDefinition, name='salinity_doubler')
        if len(dpd) > 0:
            return dpd[0]

        # Salinity Doubler: Data Process Definition
        log.debug("Create data process definition SalinityDoublerTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='salinity_doubler',
            description='create a salinity doubler data product',
            module='ion.processes.data.transforms.example_double_salinity',
            class_name='SalinityDoubler',
            process_source='SalinityDoubler source code here...')
        try:
            salinity_doubler_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new SalinityDoubler data process definition: %s" %ex)


        # create a stream definition for the data from the salinity Transform
        salinity_double_stream_def_id = self.pubsubclient.create_stream_definition(container=SalinityDoubler.outgoing_stream_def,  name='SalinityDoubler')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(salinity_double_stream_def_id, salinity_doubler_dprocdef_id )

        return salinity_doubler_dprocdef_id


    def create_transform_process(self, data_process_definition_id, data_process_input_dp_id):

        data_process_definition = self.rrclient.read(data_process_definition_id)

        # Find the link between the output Stream Definition resource and the Data Process Definition resource
        stream_ids,_ = self.rrclient.find_objects(data_process_definition._id, PRED.hasStreamDefinition, RT.StreamDefinition,  id_only=True)
        if not stream_ids:
            raise Inconsistent("The data process definition %s is missing an association to an output stream definition" % data_process_definition._id )
        process_output_stream_def_id = stream_ids[0]

        #Concatenate the name of the workflow and data process definition for the name of the data product output
        data_process_name = data_process_definition.name

        # Create the output data product of the transform
        transform_dp_obj = IonObject(RT.DataProduct, name=data_process_name,description=data_process_definition.description)
        transform_dp_id = self.dataproductclient.create_data_product(transform_dp_obj, process_output_stream_def_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=transform_dp_id, persist_data=True, persist_metadata=True)

        #last one out of the for loop is the output product id
        output_data_product_id = transform_dp_id

        # Create the  transform data process
        log.debug("create data_process and start it")
        data_process_id = self.dataprocessclient.create_data_process(data_process_definition._id, [data_process_input_dp_id], {'output':transform_dp_id})
        self.dataprocessclient.activate_data_process(data_process_id)


        #Find the id of the output data stream
        stream_ids, _ = self.rrclient.find_objects(transform_dp_id, PRED.hasStream, None, True)
        if not stream_ids:
            raise Inconsistent("The data process %s is missing an association to an output stream" % data_process_id )

        return data_process_id, output_data_product_id


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    #@unittest.skip("Skipping for debugging ")
    def test_SA_transform_components(self):

        assertions = self.assertTrue

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self._create_ctd_input_stream_and_data_product()
        data_product_stream_ids.append(ctd_stream_id)


        ###
        ###  Setup the first transformation
        ###

        # Salinity: Data Process Definition
        ctd_L2_salinity_dprocdef_id = self._create_salinity_data_process_definition()

        l2_salinity_all_data_process_id, ctd_l2_salinity_output_dp_id = self.create_transform_process(ctd_L2_salinity_dprocdef_id,ctd_parsed_data_product_id )

        ## get the stream id for the transform outputs
        stream_ids, _ = self.rrclient.find_objects(ctd_l2_salinity_output_dp_id, PRED.hasStream, None, True)
        assertions(len(stream_ids) > 0 )
        sal_stream_id = stream_ids[0]
        data_product_stream_ids.append(sal_stream_id)


        ###
        ###  Setup the second transformation
        ###

        # Salinity Doubler: Data Process Definition
        salinity_doubler_dprocdef_id = self._create_salinity_doubler_data_process_definition()

        salinity_double_data_process_id, salinity_doubler_output_dp_id = self.create_transform_process(salinity_doubler_dprocdef_id, ctd_l2_salinity_output_dp_id )

        stream_ids, _ = self.rrclient.find_objects(salinity_doubler_output_dp_id, PRED.hasStream, None, True)
        assertions(len(stream_ids) > 0 )
        sal_dbl_stream_id = stream_ids[0]
        data_product_stream_ids.append(sal_dbl_stream_id)


        #Start the output stream listener to monitor and collect messages
        results = self._start_output_stream_and_listen(ctd_stream_id, data_product_stream_ids)

        #Stop the transform processes
        self.dataprocessclient.deactivate_data_process(salinity_double_data_process_id)
        self.dataprocessclient.deactivate_data_process(l2_salinity_all_data_process_id)

        #Validate the data from each of the messages along the way
        self._validate_messages(results)

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    #@unittest.skip("Skipping for debugging ")
    def test_transform_workflow(self):

        assertions = self.assertTrue

        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Salinity_Test_Workflow',description='tests a workflow of multiple transform data processes')

        workflow_data_product_name = 'TEST-Workflow_Output_Product' #Set a specific output product name

        #Add a transformation process definition
        ctd_L2_salinity_dprocdef_id = self._create_salinity_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=ctd_L2_salinity_dprocdef_id, persist_process_output_data=False)  #Don't persist the intermediate data product
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Add a transformation process definition
        salinity_doubler_dprocdef_id = self._create_salinity_doubler_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=salinity_doubler_dprocdef_id, output_data_product_name=workflow_data_product_name)
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)

        aids = self.rrclient.find_associations(workflow_def_id, PRED.hasDataProcessDefinition)
        assertions(len(aids) == 2 )

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self._create_ctd_input_stream_and_data_product()
        data_product_stream_ids.append(ctd_stream_id)

        #Create and start the workflow
        workflow_id, workflow_product_id = self.workflowclient.create_data_process_workflow(workflow_def_id, ctd_parsed_data_product_id, timeout=30)

        workflow_output_ids,_ = self.rrclient.find_subjects(RT.Workflow, PRED.hasOutputProduct, workflow_product_id, True)
        assertions(len(workflow_output_ids) == 1 )

        #Verify the output data product name matches what was specified in the workflow definition
        workflow_product = self.rrclient.read(workflow_product_id)
        assertions(workflow_product.name == workflow_data_product_name)

        #Walk the associations to find the appropriate output data streams to validate the messages
        workflow_dp_ids,_ = self.rrclient.find_objects(workflow_id, PRED.hasDataProduct, RT.DataProduct, True)
        assertions(len(workflow_dp_ids) == 2 )

        for dp_id in workflow_dp_ids:
            stream_ids, _ = self.rrclient.find_objects(dp_id, PRED.hasStream, None, True)
            assertions(len(stream_ids) == 1 )
            data_product_stream_ids.append(stream_ids[0])

        #Start the output stream listener to monitor and collect messages
        results = self._start_output_stream_and_listen(ctd_stream_id, data_product_stream_ids)

        #Stop the workflow processes
        self.workflowclient.terminate_data_process_workflow(workflow_id, False, timeout=15)  # Should test true at some point

        #Make sure the Workflow object was removed
        objs, _ = self.rrclient.find_resources(restype=RT.Workflow)
        assertions(len(objs) == 0)

        #Validate the data from each of the messages along the way
        self._validate_messages(results)

        #Cleanup to make sure delete is correct.
        self.workflowclient.delete_workflow_definition(workflow_def_id)

        workflow_def_ids,_ = self.rrclient.find_resources(restype=RT.WorkflowDefinition)
        assertions(len(workflow_def_ids) == 0 )


    def _create_google_dt_data_process_definition(self):

        #First look to see if it exists and if not, then create it
        dpd,_ = self.rrclient.find_resources(restype=RT.DataProcessDefinition, name='google_dt_transform')
        if len(dpd) > 0:
            return dpd[0]

        # Data Process Definition
        log.debug("Create data process definition GoogleDtTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='google_dt_transform',
            description='Convert data streams to Google DataTables',
            module='ion.processes.data.transforms.viz.google_dt',
            class_name='VizTransformGoogleDT',
            process_source='VizTransformGoogleDT source code here...')
        try:
            procdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new VizTransformGoogleDT data process definition: %s" %ex)


        # create a stream definition for the data from the
        stream_def_id = self.pubsubclient.create_stream_definition(container=VizTransformGoogleDT.outgoing_stream_def,  name='VizTransformGoogleDT')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id )

        return procdef_id


    def _validate_google_dt_results(self, results_stream_def, results):

        cc = self.container
        assertions = self.assertTrue

        for g in results:

            if isinstance(g,Granule):

                tx = TaxyTool.load_from_granule(g)
                rdt = RecordDictionaryTool.load_from_granule(g)
                #log.warn(tx.pretty_print())
                #log.warn(rdt.pretty_print())

                #gdt_component = rdt['google_dt_components'][0]
                gdt_component = get_safe(rdt, 'google_dt_components')

                assertions(gdt_component['viz_product_type'] == 'google_realtime_dt' )
                gdt_description = gdt_component['data_table_description']
                gdt_content = gdt_component['data_table_content']

                assertions(gdt_description[0][0] == 'time')
                assertions(len(gdt_description) > 1)
                assertions(len(gdt_content) >= 0)

        return


    def _create_mpl_graphs_data_process_definition(self):

        #First look to see if it exists and if not, then create it
        dpd,_ = self.rrclient.find_resources(restype=RT.DataProcessDefinition, name='mpl_graphs_transform')
        if len(dpd) > 0:
            return dpd[0]

        #Data Process Definition
        log.debug("Create data process definition MatplotlibGraphsTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='mpl_graphs_transform',
            description='Convert data streams to Matplotlib graphs',
            module='ion.processes.data.transforms.viz.matplotlib_graphs',
            class_name='VizTransformMatplotlibGraphs',
            process_source='VizTransformMatplotlibGraphs source code here...')
        try:
            procdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new VizTransformMatplotlibGraphs data process definition: %s" %ex)


        # create a stream definition for the data
        stream_def_id = self.pubsubclient.create_stream_definition(container=VizTransformMatplotlibGraphs.outgoing_stream_def,  name='VizTransformMatplotlibGraphs')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id )

        return procdef_id

    def _validate_mpl_graphs_results(self, results_stream_def, results):

        cc = self.container
        assertions = self.assertTrue


        for g in results:
            if isinstance(g,Granule):

                tx = TaxyTool.load_from_granule(g)
                rdt = RecordDictionaryTool.load_from_granule(g)
                #log.warn(tx.pretty_print())
                #log.warn(rdt.pretty_print())

                graphs = rdt['matplotlib_graphs']

                for graph in graphs:
                    assertions(graph['viz_product_type'] == 'matplotlib_graphs' )
                    # check to see if the list (numpy array) contians actual images
                    assertions(imghdr.what(graph['image_name'], graph['image_obj']) == 'png')


        return

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    #unittest.skip("Skipping for debugging ")
    def test_google_dt_transform_workflow(self):

        assertions = self.assertTrue

        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='GoogleDT_Test_Workflow',description='Tests the workflow of converting stream data to Google DT')

        #Add a transformation process definition
        google_dt_procdef_id = self._create_google_dt_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=google_dt_procdef_id)
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self._create_ctd_input_stream_and_data_product()
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
        results = self._start_output_stream_and_listen(ctd_stream_id, data_product_stream_ids)

        #Stop the workflow processes
        self.workflowclient.terminate_data_process_workflow(workflow_id, False)  # Should test true at some point

        #Validate the data from each of the messages along the way
        self._validate_google_dt_results(VizTransformGoogleDT.outgoing_stream_def, results)

        #Cleanup to make sure delete is correct.
        self.workflowclient.delete_workflow_definition(workflow_def_id)

        workflow_def_ids,_ = self.rrclient.find_resources(restype=RT.WorkflowDefinition)
        assertions(len(workflow_def_ids) == 0 )



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    #@unittest.skip("Skipping for debugging ")
    def test_mpl_graphs_transform_workflow(self):

        assertions = self.assertTrue

        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Mpl_Graphs_Test_Workflow',description='Tests the workflow of converting stream data to Matplotlib graphs')

        #Add a transformation process definition
        mpl_graphs_procdef_id = self._create_mpl_graphs_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=mpl_graphs_procdef_id)
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self._create_ctd_input_stream_and_data_product()
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
        results = self._start_output_stream_and_listen(ctd_stream_id, data_product_stream_ids)

        #Stop the workflow processes
        self.workflowclient.terminate_data_process_workflow(workflow_id, False)  # Should test true at some point

        #Validate the data from each of the messages along the way
        self._validate_mpl_graphs_results(VizTransformGoogleDT.outgoing_stream_def, results)

        #Cleanup to make sure delete is correct.
        self.workflowclient.delete_workflow_definition(workflow_def_id)

        workflow_def_ids,_ = self.rrclient.find_resources(restype=RT.WorkflowDefinition)
        assertions(len(workflow_def_ids) == 0 )


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    #@unittest.skip("Skipping for debugging ")
    def test_multiple_workflow_instances(self):

        assertions = self.assertTrue

        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Multiple_Test_Workflow',description='Tests the workflow of converting stream data')

        #Add a transformation process definition
        google_dt_procdef_id = self._create_google_dt_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=google_dt_procdef_id)
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the first input data product
        ctd_stream_id1, ctd_parsed_data_product_id1 = self._create_ctd_input_stream_and_data_product('ctd_parsed1')
        data_product_stream_ids.append(ctd_stream_id1)

        #Create and start the first workflow
        workflow_id1, workflow_product_id1 = self.workflowclient.create_data_process_workflow(workflow_def_id, ctd_parsed_data_product_id1, timeout=20)

        #Create the second input data product
        ctd_stream_id2, ctd_parsed_data_product_id2 = self._create_ctd_input_stream_and_data_product('ctd_parsed2')
        data_product_stream_ids.append(ctd_stream_id2)

        #Create and start the first workflow
        workflow_id2, workflow_product_id2 = self.workflowclient.create_data_process_workflow(workflow_def_id, ctd_parsed_data_product_id2, timeout=20)

        #Walk the associations to find the appropriate output data streams to validate the messages
        workflow_ids,_ = self.rrclient.find_resources(restype=RT.Workflow)
        assertions(len(workflow_ids) == 2 )


        #Start the first input stream process
        ctd_sim_pid1 = self._start_sinusoidal_input_stream_process(ctd_stream_id1)

        #Start the second input stream process
        ctd_sim_pid2 = self._start_simple_input_stream_process(ctd_stream_id2)

        #Start the output stream listener to monitor a set number of messages being sent through the workflows
        results = self._start_output_stream_and_listen(None, data_product_stream_ids, message_count_per_stream=5)

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
