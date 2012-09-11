from pyon.public import Container, log, IonObject
from interface.objects import CouchStorage, ProcessDefinition
from pyon.ion.stream import StandaloneStreamSubscriber
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
from ion.processes.data.transforms.ctd.ctd_L2_salinity import SalinityTransform
from ion.processes.data.transforms.example_double_salinity import SalinityDoubler
from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDT
from ion.processes.data.transforms.viz.matplotlib_graphs import VizTransformMatplotlibGraphs
from ion.services.dm.utility.granule_utils import CoverageCraft
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.ans.iworkflow_management_service import WorkflowManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.ans.ivisualization_service import VisualizationServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

from pyon.public import log


from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG, RT, LCS, PRED
from pyon.core.exception import BadRequest, Inconsistent

import imghdr
import gevent, numpy
import simplejson
import base64

from interface.objects import Granule
from ion.services.dm.utility.granule.taxonomy import TaxyTool
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe


class VisualizationIntegrationTestHelper(IonIntegrationTestCase):

    def create_ctd_input_stream_and_data_product(self, data_product_name='ctd_parsed'):

        cc = self.container
        assertions = self.assertTrue

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.dataset_management =  DatasetManagementServiceClient(node=self.container.node)
        self.workflowclient = WorkflowManagementServiceClient(node=self.container.node)
        self.process_dispatcher = ProcessDispatcherServiceClient(node=self.container.node)
        self.vis_client = VisualizationServiceClient(node=self.container.node)


        #-------------------------------
        # Create CTD Parsed as the initial data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(name='Simulated CTD data')


        log.debug('Creating new CDM data product with a stream definition')

        craft = CoverageCraft
        sdom, tdom = craft.create_domains()
        sdom = sdom.dump()
        tdom = tdom.dump()
        parameter_dictionary = craft.create_parameters()
        parameter_dictionary = parameter_dictionary.dump()

        dp_obj = IonObject(RT.DataProduct,
            name=data_product_name,
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_parsed_data_product_id = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id, parameter_dictionary)

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
        subscriber.start()

        # after the queue has been created it is safe to activate the subscription
        self.pubsubclient.activate_subscription(subscription_id=salinity_subscription_id)

        #Start the input stream process
        if ctd_stream_id is not None:
            ctd_sim_pid = self.start_simple_input_stream_process(ctd_stream_id)

        # Assert that we have received data
        assertions(result.get(timeout=30))

        # stop the flow parse the messages...
        if ctd_stream_id is not None:
            self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

        self.pubsubclient.deactivate_subscription(subscription_id=salinity_subscription_id)

        subscriber.stop()

        return results


    def validate_messages(self, results):

        assertions = self.assertTrue

        first_salinity_values = None

        for message in results:
            rdt = RecordDictionaryTool.load_from_granule(message)

            try:
                temp = get_safe(rdt, 'temp')
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
                salinity = get_safe(rdt, 'salinity')
                #salinity = psd.get_values('salinity')
                log.info( 'salinity=' + str(numpy.nanmin(salinity)))

                # Check to see if salinity has values
                assertions(salinity != None)

                assertions(isinstance(salinity, numpy.ndarray))
                assertions(numpy.nanmin(salinity) > 0.0) # salinity should always be greater than 0

                if first_salinity_values is None:
                    first_salinity_values = salinity.tolist()
                else:
                    second_salinity_values = salinity.tolist()
                    assertions(len(first_salinity_values) == len(second_salinity_values))
                    for idx in range(0,len(first_salinity_values)):
                        assertions(first_salinity_values[idx]*2.0 == second_salinity_values[idx])


    def validate_data_ingest_retrieve(self, dataset_id):

        assertions = self.assertTrue
        self.data_retriever = DataRetrieverServiceClient(node=self.container.node)

        #validate that data was ingested
        replay_granule = self.data_retriever.retrieve_last_granule(dataset_id)
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

    def create_salinity_data_process_definition(self):

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
        sal_stream_def_id = self.pubsubclient.create_stream_definition(name='Salinity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(sal_stream_def_id, ctd_L2_salinity_dprocdef_id )

        return ctd_L2_salinity_dprocdef_id

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
            class_name='SalinityDoubler',
            process_source='SalinityDoubler source code here...')
        try:
            salinity_doubler_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new SalinityDoubler data process definition: %s" %ex)


        # create a stream definition for the data from the salinity Transform
        salinity_double_stream_def_id = self.pubsubclient.create_stream_definition(name='SalinityDoubler')
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

        craft = CoverageCraft
        sdom, tdom = craft.create_domains()
        sdom = sdom.dump()
        tdom = tdom.dump()
        parameter_dictionary = craft.create_parameters()
        parameter_dictionary = parameter_dictionary.dump()

        transform_dp_obj = IonObject(RT.DataProduct,
            name=data_process_name,
            description=data_process_definition.description,
            temporal_domain = tdom,
            spatial_domain = sdom)

        transform_dp_id = self.dataproductclient.create_data_product(transform_dp_obj, process_output_stream_def_id, parameter_dictionary)

        self.dataproductclient.activate_data_product_persistence(data_product_id=transform_dp_id)

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



    def create_google_dt_data_process_definition(self):

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
        stream_def_id = self.pubsubclient.create_stream_definition(name='VizTransformGoogleDT')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id )

        return procdef_id


    def validate_google_dt_transform_results(self, results):

        assertions = self.assertTrue

        # if its just one granule, wrap it up in a list so we can use the following for loop for a couple of cases
        if isinstance(results,Granule):
            results =[results]

        for g in results:

            if isinstance(g,Granule):

                gdt = RecordDictionaryTool.load_from_granule(g)

                viz_product_type = get_safe(gdt, "viz_product_type")

                if not viz_product_type:
                    continue

                assertions( viz_product_type == 'google_dt' )
                assertions(len(gdt['data_description'][0]) >= 0) # Need to come up with a better check
                assertions(len(gdt['data_content'][0]) >= 0)



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
            class_name='VizTransformMatplotlibGraphs',
            process_source='VizTransformMatplotlibGraphs source code here...')
        try:
            procdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new VizTransformMatplotlibGraphs data process definition: %s" %ex)


        # create a stream definition for the data
        stream_def_id = self.pubsubclient.create_stream_definition(name='VizTransformMatplotlibGraphs')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id )

        return procdef_id

    def validate_mpl_graphs_transform_results(self, results):

        cc = self.container
        assertions = self.assertTrue

        # if its just one granule, wrap it up in a list so we can use the following for loop for a couple of cases
        if isinstance(results,Granule):
            results =[results]

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



    def validate_vis_service_google_dt_results(self, results):
        assertions = self.assertTrue

        assertions(results)
        gdt_str = (results.lstrip("google.visualization.Query.setResponse(")).rstrip(")")

        assertions(len(gdt_str) > 0)

        return

    def validate_vis_service_mpl_graphs_results(self, results):

        assertions = self.assertTrue
        assertions(results)

        # check to see if the object passed is a dictionary with a valid image object in it
        image_format = results["content_type"].lstrip("image/")

        assertions(imghdr.what(results['image_name'], h = base64.decodestring(results['image_obj'])) == image_format)

        return
