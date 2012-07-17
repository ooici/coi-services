from pyon.public import Container, log, IonObject
from interface.objects import CouchStorage, ProcessDefinition, StreamQuery

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

import imghdr
import gevent, numpy

from interface.objects import Granule
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe



class VisualizationIntegrationTestHelper(IonIntegrationTestCase):

    def create_ctd_input_stream_and_data_product(self, data_product_name='ctd_parsed'):

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

        subscriber_registrar = StreamSubscriberRegistrar(process=dummy_process, container=cc)

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

        cc = self.container
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
        sal_stream_def_id = self.pubsubclient.create_stream_definition(container=SalinityTransform.outgoing_stream_def,  name='Salinity')
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
        stream_def_id = self.pubsubclient.create_stream_definition(container=VizTransformGoogleDT.outgoing_stream_def,  name='VizTransformGoogleDT')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id )

        return procdef_id


    def validate_google_dt_results(self, results):

        cc = self.container
        assertions = self.assertTrue

        # if its just one granule, wrap it up in a list so we can use the following for loop for a couple of cases
        if isinstance(results,Granule):
            results =[results]

        for g in results:

            if isinstance(g,Granule):

                tx = TaxyTool.load_from_granule(g)
                rdt = RecordDictionaryTool.load_from_granule(g)

                gdt_data = get_safe(rdt, 'google_dt')

                # IF this granule does not contains google dt, skip
                if gdt_data == None:
                    continue

                gdt = gdt_data[0]

                assertions(gdt['viz_product_type'] == 'google_dt' )
                assertions(len(gdt['data_table']) >= 0) # Need to come up with a better check




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
        stream_def_id = self.pubsubclient.create_stream_definition(container=VizTransformMatplotlibGraphs.outgoing_stream_def,  name='VizTransformMatplotlibGraphs')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id )

        return procdef_id

    def validate_mpl_graphs_results(self, results):

        cc = self.container
        assertions = self.assertTrue

        # if its just one granule, wrap it up in a list so we can use the following for loop for a couple of cases
        if isinstance(results,Granule):
            results =[results]

        for g in results:
            if isinstance(g,Granule):

                tx = TaxyTool.load_from_granule(g)
                rdt = RecordDictionaryTool.load_from_granule(g)

                graphs = get_safe(rdt, 'matplotlib_graphs')

                if graphs == None:
                    continue

                for graph in graphs:
                    assertions(graph['viz_product_type'] == 'matplotlib_graphs' )
                    # check to see if the list (numpy array) contians actual images
                    assertions(imghdr.what(graph['image_name'], graph['image_obj']) == 'png')


