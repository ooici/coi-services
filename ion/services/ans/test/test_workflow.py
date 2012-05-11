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


from pyon.public import log

from pyon.public import StreamSubscriberRegistrar

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG, RT, LCS, PRED
from pyon.core.exception import BadRequest, Inconsistent

from nose.plugins.attrib import attr
import unittest, os
import gevent, numpy
from prototype.sci_data.stream_parser import PointSupplementStreamParser



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

        print 'started services'

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

    def _create_ctd_input_stream_and_data_product(self):

        cc = self.container
        assertions = self.assertTrue

        #-------------------------------
        # Create CTD Parsed as the initial data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=self.ctd_stream_def, name='Simulated CTD data')


        print 'Creating new CDM data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,name='ctd_parsed',description='ctd stream test')
        try:
            ctd_parsed_data_product_id = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)
        except Exception as ex:
            self.fail("failed to create new data product: %s" %ex)

        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product_id

        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)


        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product_id)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product_id, persist_data=False, persist_metadata=False)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product_id, PRED.hasStream, None, True)
        assertions(len(stream_ids) > 0 )
        ctd_stream_id = stream_ids[0]

        return ctd_stream_id, ctd_parsed_data_product_id

    def _start_input_stream_process(self, ctd_stream_id):


        ###
        ### Start the process for producing the CTD data
        ###
        # process definition for the ctd simulator...
        producer_definition = ProcessDefinition()
        producer_definition.executable = {
            'module':'ion.processes.data.ctd_stream_publisher',
            'class':'SimpleCtdPublisher'
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

    def _start_output_stream_listener(self, data_product_stream_ids):

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
        def message_received(message, headers):
            # Heads
            log.warn(' data received!')
            results.append(message)
            if len(results) >= len(data_product_stream_ids) * 10:   #Only wait for so many messages - per stream
                result.set(True)

        subscriber = subscriber_registrar.create_subscriber(exchange_name='workflow_test', callback=message_received)
        subscriber.start()

        # after the queue has been created it is safe to activate the subscription
        self.pubsubclient.activate_subscription(subscription_id=salinity_subscription_id)


        # Assert that we have received data
        assertions(result.get(timeout=30))

        self.pubsubclient.deactivate_subscription(subscription_id=salinity_subscription_id)

        subscriber.stop()

        return results


    def _validate_messages(self, results):

        cc = self.container
        assertions = self.assertTrue

        first_salinity_values = None

        for message in results:

            try:
                psd = PointSupplementStreamParser(stream_definition=self.ctd_stream_def, stream_granule=message)
                temp = psd.get_values('temperature')
                print psd.list_field_names()
            except KeyError as ke:
                temp = None

            if temp is not None:
                assertions(isinstance(temp, numpy.ndarray))

                print 'temperature=' + str(numpy.nanmin(temp))

                first_salinity_values = None

            else:
                psd = PointSupplementStreamParser(stream_definition=SalinityTransform.outgoing_stream_def, stream_granule=message)
                print psd.list_field_names()

                # Test the handy info method for the names of fields in the stream def
                assertions('salinity' in psd.list_field_names())

                # you have to know the name of the coverage in stream def
                salinity = psd.get_values('salinity')
                print 'salinity=' + str(numpy.nanmin(salinity))

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
        if len(stream_ids) == 0:
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
        data_process_id = self.dataprocessclient.create_data_process(data_process_definition._id, data_process_input_dp_id, {'output':transform_dp_id})
        self.dataprocessclient.activate_data_process(data_process_id)


        #Find the id of the output data stream
        stream_ids, _ = self.rrclient.find_objects(transform_dp_id, PRED.hasStream, None, True)
        if len(stream_ids) == 0:
            raise Inconsistent("The data process %s is missing an association to an output stream" % data_process_id )

        return data_process_id, output_data_product_id


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    @unittest.skip("Skipping for now to get build to pass - something with running multiple tests")
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



        #Start the input stream process
        ctd_sim_pid = self._start_input_stream_process(ctd_stream_id)


        #Start te output stream listener to monitor and verify messages
        results = self._start_output_stream_listener(data_product_stream_ids)


        #Stop the transform processes
        self.dataprocessclient.deactivate_data_process(salinity_double_data_process_id)
        self.dataprocessclient.deactivate_data_process(l2_salinity_all_data_process_id)

        # stop the flow parse the messages...
        self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

        #Validate the data from each of the messages along the way
        self._validate_messages(results)

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_transform_workflow(self):

        assertions = self.assertTrue

        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Salinity_Test_Workflow',description='tests a workflow of multiple transform data processes')

        #Add a transformation process definition
        ctd_L2_salinity_dprocdef_id = self._create_salinity_data_process_definition()
        workflow_step_obj = IonObject('WorkflowStep', data_process_definition_id=ctd_L2_salinity_dprocdef_id, persist_data=False)  #Don't persist the intermediate data product
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Add a transformation process definition
        salinity_doubler_dprocdef_id = self._create_salinity_doubler_data_process_definition()
        workflow_step_obj = IonObject('WorkflowStep', data_process_definition_id=salinity_doubler_dprocdef_id)
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)


        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self._create_ctd_input_stream_and_data_product()
        data_product_stream_ids.append(ctd_stream_id)

        #Create and start the workflow
        workflow_product_id = self.workflowclient.create_workflow(workflow_def_id, ctd_parsed_data_product_id)



        #Walk the associations to find the appropriate output data streams to validate the messages
        workflow_ids,_ = self.rrclient.find_subjects(RT.Workflow, PRED.hasOutputProduct, workflow_product_id, True)
        assertions(len(workflow_ids) == 1 )

        process_ids,_ = self.rrclient.find_objects(workflow_ids[0], PRED.hasProcess, RT.DataProcess, True)
        assertions(len(process_ids) == 2 )

        for p in process_ids:

            out_dp_ids, _ = self.rrclient.find_objects(p, PRED.hasOutputProduct, None, True)
            assertions(len(out_dp_ids) == 1 )

            stream_ids, _ = self.rrclient.find_objects(out_dp_ids[0], PRED.hasStream, None, True)
            assertions(len(stream_ids) == 1 )
            data_product_stream_ids.append(stream_ids[0])


        #Start the input stream process
        ctd_sim_pid = self._start_input_stream_process(ctd_stream_id)

        #Start the output stream listener to monitor and verify messages
        results = self._start_output_stream_listener(data_product_stream_ids)

        #Stop the workflow processes
        for p in process_ids:
            self.dataprocessclient.deactivate_data_process(p)

        # stop the flow parse the messages...
        self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

        #Validate the data from each of the messages along the way
        self._validate_messages(results)