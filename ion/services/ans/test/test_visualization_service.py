
from pyon.net.endpoint import Subscriber
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
from interface.services.ans.ivisualization_service import VisualizationServiceClient

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition


from pyon.public import log, IonObject, RT, PRED


from ion.services.ans.test.test_helper import VisualizationIntegrationTestHelper

from nose.plugins.attrib import attr
import unittest, os
import imghdr
import gevent, numpy
from mock import Mock, sentinel, patch, call
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe

from pyon.util.context import LocalContextMixin
import logging


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='as')
class TestVisualizationServiceIntegration(VisualizationIntegrationTestHelper):

    def setUp(self):
        # Start container

        logging.disable(logging.ERROR)
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        logging.disable(logging.NOTSET)

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.workflowclient = WorkflowManagementServiceClient(node=self.container.node)
        self.process_dispatcher = ProcessDispatcherServiceClient(node=self.container.node)
        self.vis_client = VisualizationServiceClient(node=self.container.node)
        self.ingestion_management = IngestionManagementServiceClient(node=self.container.node)

        self.ctd_stream_def = SBE37_CDM_stream_definition()

        self.queues = []

    def tearDown(self):
        for queue in self.queues:
            xn = self.container.ex_manager.create_xn_queue(queue)
            xn.delete()

        super(TestVisualizationServiceIntegration,self).tearDown()

    def validate_messages(self, msgs):
        msg = msgs


        rdt = RecordDictionaryTool.load_from_granule(msg.body)

        vardict = {}
        vardict['temp'] = get_safe(rdt, 'temp')
        vardict['time'] = get_safe(rdt, 'time')
        print vardict['time']
        print vardict['temp']




    @attr('LOCOINT')
    #@patch.dict('pyon.ion.exchange.CFG', {'container':{'exchange':{'auto_register': False}}})
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_visualization_queue(self):

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()
        data_product_stream_ids.append(ctd_stream_id)

        user_queue_name = 'user_queue'

        xq = self.container.ex_manager.create_xn_queue(user_queue_name)

        salinity_subscription_id = self.pubsubclient.create_subscription(
            stream_ids=data_product_stream_ids,
            exchange_name = user_queue_name,
            name = "user visualization queue"
        )

        subscriber = Subscriber(from_name=xq)
        subscriber.initialize()

        # after the queue has been created it is safe to activate the subscription
        self.pubsubclient.activate_subscription(subscription_id=salinity_subscription_id)

        #Start the output stream listener to monitor and collect messages
        #results = self.start_output_stream_and_listen(None, data_product_stream_ids)

        #Not sure why this is needed - but it is
        #subscriber._chan.stop_consume()

        ctd_sim_pid = self.start_simple_input_stream_process(ctd_stream_id)
        gevent.sleep(10.0)  # Send some messages - don't care how many

        msg_count,_ = xq.get_stats()
        log.info('Messages in user queue 1: %s ' % msg_count)

        #Validate the data from each of the messages along the way
        #self.validate_messages(results)

#        for x in range(msg_count):
#            mo = subscriber.get_one_msg(timeout=1)
#            print mo.body
#            mo.ack()

        msgs = subscriber.get_all_msgs(timeout=2)
        for x in range(len(msgs)):
            msgs[x].ack()
            self.validate_messages(msgs[x])
           # print msgs[x].body



        #Should be zero after pulling all of the messages.
        msg_count,_ = xq.get_stats()
        log.info('Messages in user queue 2: %s ' % msg_count)


        #Trying to continue to receive messages in the queue
        gevent.sleep(5.0)  # Send some messages - don't care how many


        #Turning off after everything - since it is more representative of an always on stream of data!
        self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data



        #Should see more messages in the queue
        msg_count,_ = xq.get_stats()
        log.info('Messages in user queue 3: %s ' % msg_count)

        msgs = subscriber.get_all_msgs(timeout=2)
        for x in range(len(msgs)):
            msgs[x].ack()
            self.validate_messages(msgs[x])

        #Should be zero after pulling all of the messages.
        msg_count,_ = xq.get_stats()
        log.info('Messages in user queue 4: %s ' % msg_count)

        subscriber.close()


    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_multiple_visualization_queue(self):

        # set up a workflow with the salinity transform and the doubler. We will direct the original stream and the doubled stream to queues
        # and test to make sure the subscription to the queues is working correctly
        assertions = self.assertTrue

        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Viz_Test_Workflow',description='A workflow to test collection of multiple data products in queues')

        workflow_data_product_name = 'TEST-Workflow_Output_Product' #Set a specific output product name
        #-------------------------------------------------------------------------------------------------------------------------
        #Add a transformation process definition for salinity
        #-------------------------------------------------------------------------------------------------------------------------

        ctd_L2_salinity_dprocdef_id = self.create_salinity_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=ctd_L2_salinity_dprocdef_id, persist_process_output_data=False)  #Don't persist the intermediate data product
        configuration = {'stream_name' : 'salinity'}
        workflow_step_obj.configuration = configuration
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)

        aids = self.rrclient.find_associations(workflow_def_id, PRED.hasDataProcessDefinition)
        assertions(len(aids) == 1 )

        #The list of data product streams to monitor
        data_product_stream_ids = list()

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()
        data_product_stream_ids.append(ctd_stream_id)

        #Create and start the workflow
        workflow_id, workflow_product_id = self.workflowclient.create_data_process_workflow(workflow_def_id, ctd_parsed_data_product_id, timeout=30)

        workflow_output_ids,_ = self.rrclient.find_subjects(RT.Workflow, PRED.hasOutputProduct, workflow_product_id, True)
        assertions(len(workflow_output_ids) == 1 )

        #Walk the associations to find the appropriate output data streams to validate the messages
        workflow_dp_ids,_ = self.rrclient.find_objects(workflow_id, PRED.hasDataProduct, RT.DataProduct, True)
        assertions(len(workflow_dp_ids) == 1 )

        for dp_id in workflow_dp_ids:
            stream_ids, _ = self.rrclient.find_objects(dp_id, PRED.hasStream, None, True)
            assertions(len(stream_ids) == 1 )
            data_product_stream_ids.append(stream_ids[0])

        # Now for each of the data_product_stream_ids create a queue and pipe their data to the queue


        user_queue_name1 = 'user_queue_1'
        user_queue_name2 = 'user_queue_2'

        # use idempotency to create queues
        xq1 = self.container.ex_manager.create_xn_queue(user_queue_name1)
        self.queues.append(user_queue_name1)
        xq2 = self.container.ex_manager.create_xn_queue(user_queue_name2)
        self.queues.append(user_queue_name2)
        self.container.ex_manager.purge_queue(xq1.queue)
        self.container.ex_manager.purge_queue(xq2.queue)

        # the create_subscription call takes a list of stream_ids so create temp ones

        dp_stream_id1 = list()
        dp_stream_id1.append(data_product_stream_ids[0])
        dp_stream_id2 = list()
        dp_stream_id2.append(data_product_stream_ids[1])

        salinity_subscription_id1 = self.pubsubclient.create_subscription( stream_ids=dp_stream_id1,
            exchange_name = user_queue_name1, name = "user visualization queue1")

        salinity_subscription_id2 = self.pubsubclient.create_subscription( stream_ids=dp_stream_id2,
            exchange_name = user_queue_name2, name = "user visualization queue2")

        # Create subscribers for the output of the queue
        subscriber1 = Subscriber(from_name=xq1)
        subscriber1.initialize()
        subscriber2 = Subscriber(from_name=xq2)
        subscriber2.initialize()

        # after the queue has been created it is safe to activate the subscription
        self.pubsubclient.activate_subscription(subscription_id=salinity_subscription_id1)
        self.pubsubclient.activate_subscription(subscription_id=salinity_subscription_id2)

        # Start input stream and wait for some time
        ctd_sim_pid = self.start_simple_input_stream_process(ctd_stream_id)
        gevent.sleep(5.0)  # Send some messages - don't care how many

        msg_count,_ = xq1.get_stats()
        log.info('Messages in user queue 1: %s ' % msg_count)
        msg_count,_ = xq2.get_stats()
        log.info('Messages in user queue 2: %s ' % msg_count)

        msgs1 = subscriber1.get_all_msgs(timeout=2)
        msgs2 = subscriber2.get_all_msgs(timeout=2)

        for x in range(min(len(msgs1), len(msgs2))):
            msgs1[x].ack()
            msgs2[x].ack()
            self.validate_multiple_vis_queue_messages(msgs1[x].body, msgs2[x].body)

        #Turning off after everything - since it is more representative of an always on stream of data!
        self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

        return


    #@unittest.skip('Skipped because of broken record dictionary work-around')
    def test_realtime_visualization(self):

        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='GoogleDT_Test_Workflow',description='Tests the workflow of converting stream data to Google DT')

        #Add a transformation process definition
        google_dt_procdef_id = self.create_google_dt_data_process_definition()
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=google_dt_procdef_id, persist_process_output_data=False)
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()

        #Create and start the workflow
        workflow_id, workflow_product_id = self.workflowclient.create_data_process_workflow(workflow_def_id, ctd_parsed_data_product_id, timeout=20)

        ctd_sim_pid = self.start_sinusoidal_input_stream_process(ctd_stream_id)


        #TODO - Need to add workflow creation for google data table
        vis_params ={}
        vis_token = self.vis_client.initiate_realtime_visualization(data_product_id=workflow_product_id, visualization_parameters=vis_params)

        #Trying to continue to receive messages in the queue
        gevent.sleep(10.0)  # Send some messages - don't care how many

        #TODO - find out what the actual return data type should be
        vis_data = self.vis_client.get_realtime_visualization_data(vis_token)
        if (vis_data):
            self.validate_google_dt_transform_results(vis_data)

        #Trying to continue to receive messages in the queue
        gevent.sleep(5.0)  # Send some messages - don't care how many

        #Turning off after everything - since it is more representative of an always on stream of data!
        #todo remove the try except
        try:
            self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data
        except:
            log.warning("cancelling process did not work")
        vis_data = self.vis_client.get_realtime_visualization_data(vis_token)

        if vis_data:
            self.validate_google_dt_transform_results(vis_data)

        self.vis_client.terminate_realtime_visualization_data(vis_token)

        #Stop the workflow processes
        self.workflowclient.terminate_data_process_workflow(workflow_id, False)  # Should test true at some point

        #Cleanup to make sure delete is correct.
        self.workflowclient.delete_workflow_definition(workflow_def_id)


    #@unittest.skip('Skipped because of broken record dictionary work-around')
    def test_google_dt_overview_visualization(self):

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()

        # start producing data
        ctd_sim_pid = self.start_sinusoidal_input_stream_process(ctd_stream_id)

        # Generate some data for a few seconds
        gevent.sleep(5.0)

        #Turning off after everything - since it is more representative of an always on stream of data!
        self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

        # Use the data product to test the data retrieval and google dt generation capability of the vis service
        vis_data = self.vis_client.get_visualization_data(ctd_parsed_data_product_id)

        # validate the returned data
        self.validate_vis_service_google_dt_results(vis_data)


    #@unittest.skip('Skipped because of broken record dictionary work-around')
    def test_mpl_graphs_overview_visualization(self):

        #Create the input data product
        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()
        ctd_sim_pid = self.start_sinusoidal_input_stream_process(ctd_stream_id)

        # Generate some data for a few seconds
        gevent.sleep(5.0)

        #Turning off after everything - since it is more representative of an always on stream of data!
        self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

        # Use the data product to test the data retrieval and google dt generation capability of the vis service
        vis_data = self.vis_client.get_visualization_image(ctd_parsed_data_product_id)

        # validate the returned data
        self.validate_vis_service_mpl_graphs_results(vis_data)

        return

