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

from interface.objects import Granule, DataProcessTypeEnum
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


        dp_obj = IonObject(RT.DataProduct,
            name=data_product_name,
            description='ctd stream test')

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





def preload_ion_params(container):

    log.info("Preloading ...")
    # load_parameter_scenarios
    container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=dict(
        op="load",
        scenario="BETA",
        #path="master",
        path="https://docs.google.com/spreadsheet/pub?key=0ArYknstLVPe7dDZleTRRZzVfaFowSEpzaGVLTU9hUnc&output=xls",
        categories="ParameterFunctions,ParameterDefs,ParameterDictionary,StreamDefinition",
        clearcols="owner_id,org_ids",
        #assets="res/preload/r2_ioc/ooi_assets",
        parseooi="True",
    ))
