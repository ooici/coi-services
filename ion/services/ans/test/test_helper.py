from pyon.public import Container, log, IonObject
from interface.objects import CouchStorage, ProcessDefinition
from pyon.ion.stream import StandaloneStreamSubscriber
from ion.services.dm.utility.granule_utils import time_series_domain
from seawater.gibbs import SP_from_cndr
from seawater.gibbs import cte

from numpy.testing import assert_array_almost_equal

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG, RT, LCS, PRED
from pyon.core.exception import BadRequest, Inconsistent

import imghdr
import gevent
import numpy
import simplejson
import base64
import ast

from interface.objects import Granule
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe
from seawater.gibbs import SP_from_cndr
from seawater.gibbs import cte


class VisualizationIntegrationTestHelper(IonIntegrationTestCase):

    def on_start(self):
        super(VisualizationIntegrationTestHelper, self).on_start()


    def create_ctd_input_stream_and_data_product(self, data_product_name='ctd_parsed'):

        cc = self.container
        assertions = self.assertTrue

        #-------------------------------
        # Create CTD Parsed as the initial data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_pdict_id = self.datasetclient.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(name='Simulated CTD data', parameter_dictionary_id=ctd_pdict_id)


        log.debug('Creating new CDM data product with a stream definition')

        tdom, sdom = time_series_domain()

        dp_obj = IonObject(RT.DataProduct,
            name=data_product_name,
            description='ctd stream test',
            temporal_domain = tdom.dump(),
            spatial_domain = sdom.dump())

        ctd_parsed_data_product_id = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)
        self.addCleanup(self.dataproductclient.delete_data_product, ctd_parsed_data_product_id)

        log.debug('new ctd_parsed_data_product_id = %s' % ctd_parsed_data_product_id)

        #Only ever need one device for testing purposes.
        instDevice_obj,_ = self.rrclient.find_resources(restype=RT.InstrumentDevice, name='SBE37IMDevice')
        if instDevice_obj:
            instDevice_id = instDevice_obj[0]._id
        else:
            instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product_id)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product_id)
        self.addCleanup(self.dataproductclient.suspend_data_product_persistence, ctd_parsed_data_product_id)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product_id, PRED.hasStream, None, True)
        assertions(len(stream_ids) > 0 )
        ctd_stream_id = stream_ids[0]

        return ctd_stream_id, ctd_parsed_data_product_id

    def start_simple_input_stream_process(self, ctd_stream_id):
        return self.start_input_stream_process(ctd_stream_id)

    def start_sinusoidal_input_stream_process(self, ctd_stream_id):
        return self.start_input_stream_process(ctd_stream_id, 'ion.processes.data.sinusoidal_stream_publisher', 'SinusoidalCtdPublisher')

    def start_input_stream_process(self, ctd_stream_id, module = 'ion.processes.data.ctd_stream_publisher', class_name= 'SimpleCtdPublisher'):
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

    def start_output_stream_and_listen(self, ctd_stream_id, data_product_stream_ids, message_count_per_stream=10):
        assertions = self.assertTrue
        exchange_name = 'workflow_test'

        ###
        ### Make a subscriber in the test to listen for transformed data
        ###

        salinity_subscription_id = self.pubsubclient.create_subscription(
                name = 'test workflow transformations',
                exchange_name = exchange_name,
                stream_ids = data_product_stream_ids
                )

        result = gevent.event.AsyncResult()
        results = []
        message_count = len(data_product_stream_ids) * message_count_per_stream

        def message_received(message, stream_route, stream_id):
            # Heads
            results.append(message)
            if len(results) >= message_count:   #Only wait for so many messages - per stream
                result.set(True)

        subscriber = StandaloneStreamSubscriber(exchange_name='workflow_test', callback=message_received)
        subscriber.xn.purge()
        self.addCleanup(subscriber.xn.delete)
        subscriber.start()

        # after the queue has been created it is safe to activate the subscription
        self.pubsubclient.activate_subscription(subscription_id=salinity_subscription_id)

        #Start the input stream process
        if ctd_stream_id is not None:
            ctd_sim_pid = self.start_simple_input_stream_process(ctd_stream_id)

        # Assert that we have received data
        assertions(result.get(timeout=60))

        # stop the flow parse the messages...
        if ctd_stream_id is not None:
            self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

        self.pubsubclient.deactivate_subscription(subscription_id=salinity_subscription_id)

        subscriber.stop()

        return results


    def validate_messages(self, results):
        bin1 = numpy.array([])
        bin2 = numpy.array([])
        for message in results:
            rdt = RecordDictionaryTool.load_from_granule(message)
            if 'salinity' in message.data_producer_id:
                if 'double' in message.data_producer_id:
                    bin2 = numpy.append(bin2, rdt['salinity'])
                else:
                    bin1 = numpy.append(bin1, rdt['salinity'])



        assert_array_almost_equal(bin2, bin1 * 2.0)



    def validate_data_ingest_retrieve(self, dataset_id):

        assertions = self.assertTrue

        #validate that data was ingested
        replay_granule = self.data_retriever.retrieve_last_data_points(dataset_id, 10)
        rdt = RecordDictionaryTool.load_from_granule(replay_granule)
        salinity = get_safe(rdt, 'salinity')
        assertions(salinity != None)

        #retrieve all the granules from the database and check the values
        replay_granule_all = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(replay_granule_all)
        for k, v in rdt.iteritems():
            if k == 'salinity':
                for val in numpy.nditer(v):
                    assertions(val > 0)


    def create_salinity_doubler_data_process_definition(self):

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
            class_name='SalinityDoubler')
        try:
            salinity_doubler_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new SalinityDoubler data process definition: %s" %ex)


        # create a stream definition for the data from the salinity Transform
        ctd_pdict_id = self.datasetclient.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        salinity_double_stream_def_id = self.pubsubclient.create_stream_definition(name='SalinityDoubler', parameter_dictionary_id=ctd_pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(salinity_double_stream_def_id, salinity_doubler_dprocdef_id, binding='salinity' )

        return salinity_doubler_dprocdef_id


    def create_transform_process(self, data_process_definition_id, data_process_input_dp_id, stream_name):

        data_process_definition = self.rrclient.read(data_process_definition_id)

        # Find the link between the output Stream Definition resource and the Data Process Definition resource
        stream_ids,_ = self.rrclient.find_objects(data_process_definition._id, PRED.hasStreamDefinition, RT.StreamDefinition,  id_only=True)
        if not stream_ids:
            raise Inconsistent("The data process definition %s is missing an association to an output stream definition" % data_process_definition._id )
        process_output_stream_def_id = stream_ids[0]

        #Concatenate the name of the workflow and data process definition for the name of the data product output
        data_process_name = data_process_definition.name

        # Create the output data product of the transform

        tdom, sdom = time_series_domain()

        transform_dp_obj = IonObject(RT.DataProduct,
            name=data_process_name,
            description=data_process_definition.description,
            temporal_domain = tdom.dump(),
            spatial_domain = sdom.dump())

        transform_dp_id = self.dataproductclient.create_data_product(transform_dp_obj, process_output_stream_def_id)

        self.dataproductclient.activate_data_product_persistence(data_product_id=transform_dp_id)

        #last one out of the for loop is the output product id
        output_data_product_id = transform_dp_id

        # Create the  transform data process
        log.debug("create data_process and start it")
        data_process_id = self.dataprocessclient.create_data_process(
            data_process_definition_id = data_process_definition._id,
            in_data_product_ids = [data_process_input_dp_id],
            out_data_product_ids = [transform_dp_id])

        self.dataprocessclient.activate_data_process(data_process_id)


        #Find the id of the output data stream
        stream_ids, _ = self.rrclient.find_objects(transform_dp_id, PRED.hasStream, None, True)
        if not stream_ids:
            raise Inconsistent("The data process %s is missing an association to an output stream" % data_process_id )

        return data_process_id, output_data_product_id



    def create_highcharts_data_process_definition(self):
        return helper_create_highcharts_data_process_definition(self.container)


    def validate_highcharts_transform_results(self, results):

        assertions = self.assertTrue

        # if its just one granule, wrap it up in a list so we can use the following for loop for a couple of cases
        if isinstance(results,Granule):
            results =[results]

        for g in results:

            if isinstance(g,Granule):

                rdt = RecordDictionaryTool.load_from_granule(g)
                hc_data_arr = get_safe(rdt, 'hc_data')

                if hc_data_arr == None:
                    log.debug("hc_data in granule is None")
                    continue

                assertions(len(hc_data_arr) >= 0) # Need to come up with a better check

                hc_data = hc_data_arr[0]
                assertions(len(hc_data) >= 0)

                assertions(len(hc_data[0]["name"]) >= 0)
                assertions(len(hc_data[0]["data"]) >= 0)



    def create_mpl_graphs_data_process_definition(self):

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
            class_name='VizTransformMatplotlibGraphs')
        try:
            procdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new VizTransformMatplotlibGraphs data process definition: %s" %ex)


        pdict_id = self.datasetclient.read_parameter_dictionary_by_name('graph_image_param_dict',id_only=True)
        # create a stream definition for the data
        stream_def_id = self.pubsubclient.create_stream_definition(name='VizTransformMatplotlibGraphs', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id, binding='graph_image_param_dict' )

        return procdef_id

    def validate_mpl_graphs_transform_results(self, results):

        cc = self.container
        assertions = self.assertTrue

        # if its just one granule, wrap it up in a list so we can use the following for loop for a couple of cases
        if isinstance(results,Granule):
            results =[results]

        found_data = False
        for g in results:
            if isinstance(g,Granule):
                rdt = RecordDictionaryTool.load_from_granule(g)

                graphs = get_safe(rdt, 'matplotlib_graphs')

                if graphs == None:
                    continue

                for graph in graphs[0]:

                    # At this point only dictionaries containing image data should be passed
                    # For some reason non dictionary values are filtering through.
                    if not isinstance(graph, dict):
                        continue

                    assertions(graph['viz_product_type'] == 'matplotlib_graphs' )
                    # check to see if the list (numpy array) contains actual images
                    assertions(imghdr.what(graph['image_name'], h = graph['image_obj']) == 'png')
                    found_data = True
        return found_data



    def validate_vis_service_highcharts_results(self, results):

        assertions = self.assertTrue
        assertions(results)

        hc_data = simplejson.loads(results)

        for series in hc_data:
            assertions(series["name"])
            assertions(series["data"])

        return

    def validate_vis_service_mpl_graphs_results(self, results_str):

        # convert incoming string to dict
        results = ast.literal_eval(results_str)

        assertions = self.assertTrue
        assertions(results)

        # check to see if the object passed is a dictionary with a valid image object in it
        image_format = results["content_type"].lstrip("image/")

        assertions(imghdr.what(results['image_name'], h = base64.decodestring(results['image_obj'])) == image_format)

        return

    def validate_multiple_vis_queue_messages(self, msg1, msg2):

        assertions = self.assertTrue

        # verify that the salinity in msg2 is a result of content from msg1
        rdt1 = RecordDictionaryTool.load_from_granule(msg1)
        rdt2 = RecordDictionaryTool.load_from_granule(msg2)

        # msg1 should not have salinity
        # assertions(rdt1['salinity'] == None)

        conductivity = rdt1['conductivity']
        pressure = rdt1['pressure']
        temperature = rdt1['temp']

        msg1_sal_value = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=pressure)
        msg2_sal_value = rdt2['salinity']
        b = msg1_sal_value == msg2_sal_value

        if isinstance(b,bool):
            assertions(b)
        else:
            assertions(b.all())

        return

    def create_highcharts_workflow_def(self):

        return helper_create_highcharts_workflow_def(self.container)



def helper_create_highcharts_data_process_definition(container):

    from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
    rrclient = ResourceRegistryServiceClient(node=container.node)

    #First look to see if it exists and if not, then create it
    dpd,_ = rrclient.find_resources(restype=RT.DataProcessDefinition, name='highcharts_transform')
    if len(dpd) > 0:
        return dpd[0]

    # Data Process Definition
    log.debug("Create data process definition for highcharts transform")
    dpd_obj = IonObject(RT.DataProcessDefinition,
        name='highcharts_transform',
        description='Convert data streams to Highcharts data',
        module='ion.processes.data.transforms.viz.highcharts',
        class_name='VizTransformHighCharts')

    from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
    dataprocessclient = DataProcessManagementServiceClient(node=container.node)

    procdef_id = dataprocessclient.create_data_process_definition(dpd_obj)

    from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
    datasetclient = DatasetManagementServiceClient(node=container.node)

    pdict_id = datasetclient.read_parameter_dictionary_by_name('highcharts', id_only=True)

    from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
    pubsubclient = PubsubManagementServiceClient(node=container.node)

    # create a stream definition for the data from the
    stream_def_id = pubsubclient.create_stream_definition(name='VizTransformHighCharts', parameter_dictionary_id=pdict_id)
    dataprocessclient.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id, binding='highcharts' )

    return procdef_id


def helper_create_highcharts_workflow_def(container):

    from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
    rrclient = ResourceRegistryServiceClient(node=container.node)

    # Check to see if the workflow defnition already exist
    workflow_def_ids,_ = rrclient.find_resources(restype=RT.WorkflowDefinition, name='Realtime_HighCharts', id_only=True)

    if len(workflow_def_ids) > 0:
        workflow_def_id = workflow_def_ids[0]
    else:
        # Build the workflow definition
        workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Realtime_HighCharts',description='Convert stream data to HighCharts data')

        #Add a transformation process definition
        procdef_id = helper_create_highcharts_data_process_definition(container)
        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=procdef_id)
        workflow_def_obj.workflow_steps.append(workflow_step_obj)

        #Create it in the resource registry
        from interface.services.ans.iworkflow_management_service import WorkflowManagementServiceClient
        workflowclient = WorkflowManagementServiceClient(node=container.node)

        workflow_def_id = workflowclient.create_workflow_definition(workflow_def_obj)

    return workflow_def_id


def preload_ion_params(container):

    log.info("Preloading ...")
    # load_parameter_scenarios
    container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=dict(
        op="load",
        scenario="BETA",
        #path="master",
        path="https://docs.google.com/spreadsheet/pub?key=0ArYknstLVPe7dDZleTRRZzVfaFowSEpzaGVLTU9hUnc&output=xls",
        categories="ParameterFunctions,ParameterDefs,ParameterDictionary,StreamDefinition,DataProcessDefinition,WorkflowDefinition",
        clearcols="owner_id,org_ids",
        #assets="res/preload/r2_ioc/ooi_assets",
        parseooi="True",
    ))