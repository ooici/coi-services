#!/usr/bin/env python
from gevent.coros import RLock
from gevent.greenlet import Greenlet

__author__ = 'Raj Singh'
__license__ = 'Apache 2.0'

# Pyon imports
# Note pyon imports need to be first for monkey patching to occur
from pyon.ion.transform import TransformDataProcess
from pyon.public import IonObject, RT, log, PRED
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient, ResourceRegistryServiceProcessClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.objects import StreamQuery

from datetime import datetime
import string
import random
import StringIO
import simplejson
import gevent
from gevent.greenlet import Greenlet



from interface.services.ans.ivisualization_service import BaseVisualizationService
from interface.services.ans.ivisualization_service import VisualizationServiceClient
from interface.objects import HdfStorage, CouchStorage
from pyon.event.event import EventSubscriber
from pyon.util.async import spawn
from interface.objects import ResourceModificationType
from prototype.sci_data.stream_parser import PointSupplementStreamParser


# Matplotlib related imports
import matplotlib as mpl
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

# Google viz library for google charts
import ion.services.ans.gviz_api as gviz_api

class VisualizationService(BaseVisualizationService):
    """

    """

    def on_start(self):

        # The data dictionary object holds a copy of all the viz products created by the service. The viz
        # products are indexed by the viz_product_type and data_product_id (which could be google_datatables or
        # mpl_graphs
        self.viz_data_dictionary = {}
        self.viz_data_dictionary['google_dt'] = {}
        self.viz_data_dictionary['google_realtime_dt'] = {}
        self.viz_data_dictionary['matplotlib_graphs'] = {}
        # Kind of redundant but we will maintain a separate list of data product_ids registered with the viz_service
        self.data_products = []

        # Create clients to interface with PubSub, Transform Management Service and Resource Registry
        self.pubsub_cli = PubsubManagementServiceClient(node=self.container.node)
        self.tms_cli = TransformManagementServiceClient(node=self.container.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.container.node)

        #The following code might go away in the future but check for an existing ingestion configuration in the system
        # and if it does not exist, create one. It will be used by the data producers to persist data
        self.IngestClient = IngestionManagementServiceClient(node=self.container.node)
        ingestion_cfgs, _ = self.rr_cli.find_resources(RT.IngestionConfiguration, None, None, True)

        if len(ingestion_cfgs) == 0:
            # ingestion configuration parameters
            self.exchange_point_id = 'science_data'
            self.number_of_workers = 2
            self.hdf_storage = HdfStorage(relative_path='ingest')
            self.couch_storage = CouchStorage(datastore_name='test_datastore')
            self.XP = 'science_data'
            self.exchange_name = 'ingestion_queue'

            # Create ingestion configuration and activate it
            self.ingestion_configuration_id =  self.IngestClient.create_ingestion_configuration(
                exchange_point_id=self.exchange_point_id,
                couch_storage=self.couch_storage,
                hdf_storage=self.hdf_storage,
                number_of_workers=self.number_of_workers
            )
            print 'test_activateInstrument: ingestion_configuration_id', self.ingestion_configuration_id

            # activate an ingestion configuration
            ret = self.IngestClient.activate_ingestion_configuration(self.ingestion_configuration_id)
            log.debug("test_activateInstrument: activate = %s"  % str(ret))


        # Create process definitions which will used to spawn off the tranform processes
        self.process_definition1 = IonObject(RT.ProcessDefinition, name='viz_transform_process'+'.'+self.random_id_generator())
        self.process_definition1.executable = {
            'module': 'ion.services.ans.visualization_service',
            'class':'VizTransformProcForMatplotlibGraphs'
        }
        self.process_definition_id1, _ = self.rr_cli.create(self.process_definition1)

        self.process_definition2 = IonObject(RT.ProcessDefinition, name='viz_transform_process'+'.'+self.random_id_generator())
        self.process_definition2.executable = {
            'module': 'ion.services.ans.visualization_service',
            'class':'VizTransformProcForGoogleDT'
        }
        self.process_definition_id2, _ = self.rr_cli.create(self.process_definition2)


        # Discover the existing data_product_ids active in the system
        sys_prod_ids, _ = self.rr_cli.find_resources(RT.DataProduct, None, None, True)

        # Register all the streams in the system, which will in turn start transform processes
        for dp_id in sys_prod_ids:
            self.register_new_data_product(dp_id)


        # listen for events when new data_products show up
        self.event_subscriber = EventSubscriber(
            callback=self.receive_new_dataproduct_event
        )

        self.gl = spawn(self.event_subscriber.listen)
        self.event_subscriber._ready_event.wait(timeout=5)


        return

    def on_stop(self):
        self.ingestion_service.deactivate_ingestion_configuration(self.ingestion_configuration_id)

        super(VisualizationService, self).on_stop()
        return

    def get_google_dt(self, data_product_id='', query=''):
        """Request to fetch the datatable for a data product as specified in the query. Query will also specify whether its a realtime view or one-shot

        @param data_product_id    str
        @param query    str
        @retval datatable    str
        @throws NotFound    object with specified id, query does not exist
        """

        # extract the stream_id associated with the data_product_id
        viz_stream_id,_ = self.rr_cli.find_objects(data_product_id, PRED.hasStream, None, True)

        if viz_stream_id == []:
            log.warn ("Visualization_service: get_google_dt(): viz_stream_id is empty")
            return None

        viz_stream_def_id = self.pubsub_cli.find_stream_definition(stream_id = viz_stream_id[0], id_only = True)

        try:
            # send the resultant datatable back
            if data_product_id in self.viz_data_dictionary['google_dt']:
                return self.viz_data_dictionary['google_dt'][data_product_id]
            else:
                return None

        except AttributeError:
            return None

    def get_google_realtime_dt(self, data_product_id='', query=''):
        """Request to fetch the datatable for a data product as specified in the query. Query will also specify whether its a realtime view or one-shot

        @param data_product_id    str
        @param query    str
        @retval datatable    str
        @throws NotFound    object with specified id, query does not exist
        """
        try:
            if data_product_id in self.viz_data_dictionary['google_realtime_dt']:
                return self.viz_data_dictionary['google_realtime_dt'][data_product_id]
            else:
                return None

        except AttributeError:
            return None

    def get_list_of_mpl_images(self, data_product_id=''):
        """Request to fetch the list of Matplotlib generated images associated with a data product

        @param data_product_id    str
        @retval image_list    str
        @throws NotFound    object with specified id does not exist
        """

        # return a json version of the array stored in the data_dict
        try:
            if data_product_id in self.viz_data_dictionary['matplotlib_graphs']:
                json_img_list = simplejson.dumps({'data': self.viz_data_dictionary['matplotlib_graphs'][data_product_id]['list_of_images']})
                return "image_list_callback("+json_img_list+")"
            else:
                return None

        except AttributeError:
            return None

    def get_image(self, data_product_id = '', image_name=''):
        """Request to fetch a file object from within the Visualization Service

        @param file_name    str
        @retval file_obj    str
        @throws NotFound    object with specified id does not exist
        """
        try:
            if data_product_id in self.viz_data_dictionary['matplotlib_graphs']:
                if image_name in self.viz_data_dictionary['matplotlib_graphs'][data_product_id]:
                    return self.viz_data_dictionary['matplotlib_graphs'][data_product_id][image_name]
                else:
                    return None
            else:
                return None

        except AttributeError:
            return None


    def submit_google_dt(self, data_product_id='', data_table=''):
        """Send the rendered image to

        @param data_product_id    str
        @param data_table    str
        @param description    str
        @throws BadRequest    check data_product_id

        """

        # Just copy the datatable in to the data dictionary
        self.viz_data_dictionary['google_dt'][data_product_id] = data_table

        return

    def submit_google_realtime_dt(self, data_product_id='', data_table=''):
        """Send the rendered image to

        @param data_product_id    str
        @param data_table    str
        @param description    str
        @throws BadRequest    check data_product_id

        """

        # Just copy the datatable in to the data dictionary
        self.viz_data_dictionary['google_realtime_dt'][data_product_id] = data_table

        return

    def submit_mpl_image(self, data_product_id='', image_file_obj='', image_name=''):
        """Send the rendered image to the visualization service

        @param data_product_id    str
        @param image_file_obj    str
        @param description    str
        @throws BadRequest    check data_product_id
        """

        # Store the image object in the data dictionary and note the image in the list.

        # Check for existing entries before updating the list_of_images
        found = False
        for name in self.viz_data_dictionary['matplotlib_graphs'][data_product_id]['list_of_images']:
            if name == image_name:
                found = True
                break

        if not found:
            list_len = len(self.viz_data_dictionary['matplotlib_graphs'][data_product_id]['list_of_images'])
            self.viz_data_dictionary['matplotlib_graphs'][data_product_id]['list_of_images'].append(image_name)

        # Add binary data from the image to the dictionary
        self.viz_data_dictionary['matplotlib_graphs'][data_product_id][image_name] = image_file_obj

        return

    def register_new_data_product(self, data_product_id=''):


        """Apprise the Visualization service of a new data product in the system. This function inits transform
        processes for generating the matplotlib graphs of the new data product. It also creates transform processes which
        generate Google data-tables for the real-time streams (sliding window) coming in from the instruments.

        @param data_product_id    str
        @throws BadRequest    check data_product_id for duplicates
        """

        # extract the stream_id associated with the data_product_id
        viz_stream_id,_ = self.rr_cli.find_objects(data_product_id, PRED.hasStream, None, True)

        if viz_stream_id == []:
            log.warn ("Visualization_service: viz_stream_id is empty")
            return

        viz_stream_def_id = self.pubsub_cli.find_stream_definition(stream_id = viz_stream_id[0], id_only = True)

        # Go ahead only if the data product is unique
        if data_product_id in self.data_products:
            raise BadRequest
        self.data_products[len(self.data_products):] = data_product_id

        # init the space needed to store matplotlib_graphs and realtime Google data tables

        # For the matplotlib graphs, the list_of_images stores the names of the image files. The actual binary data for the
        # images is also stored in the same dictionary as {img_name1: binary_data1, img_name2: binary_data2 .. etc}
        self.viz_data_dictionary['matplotlib_graphs'][data_product_id] = {'transform_proc': "", 'list_of_images': []}
        # The 'data_table' key points to a JSON string
        self.viz_data_dictionary['google_realtime_dt'][data_product_id] = {'transform_proc': "", 'data_table': []}

        ###############################################################################
        # Create transform process for the matplotlib graphs.
        ###############################################################################

        # Create the subscription to the stream. This will be passed as parameter to the transform worker
        #query1 = StreamQuery(stream_ids=[viz_stream_id,])
        query1 = StreamQuery(viz_stream_id)
        viz_subscription_id1 = self.pubsub_cli.create_subscription(query=query1, exchange_name='viz_data_exchange.'+self.random_id_generator())

        # maybe this is a good place to pass the couch DB table to use and other parameters
        configuration1 = {"stream_def_id": viz_stream_def_id, "data_product_id": data_product_id}

        # Launch the viz transform process
        viz_transform_id1 = self.tms_cli.create_transform( name='viz_transform_matplotlib_'+ self.random_id_generator() + '.'+data_product_id,
            in_subscription_id=viz_subscription_id1,
            process_definition_id=self.process_definition_id1,
            configuration=configuration1)
        self.tms_cli.activate_transform(viz_transform_id1)

        # keep a record of the the viz_transform_id
        #self.viz_data_dictionary['matplotlib_graphs'][data_product_id]['transform_proc'] = viz_transform_id1


        ###############################################################################
        # Create transform process for the Google realtime datatables
        ###############################################################################

        # Create the subscription to the stream. This will be passed as parameter to the transform worker
        #query2 = StreamQuery(stream_ids=[viz_stream_id,])
        query2 = StreamQuery(viz_stream_id)
        viz_subscription_id2 = self.pubsub_cli.create_subscription(query=query2, exchange_name='viz_data_exchange.'+self.random_id_generator())

        # maybe this is a good place to pass the couch DB table to use and other parameters
        configuration2 = {"stream_def_id": viz_stream_def_id, "data_product_id": data_product_id, "realtime_flag": "True"}

        # Launch the viz transform process
        viz_transform_id2 = self.tms_cli.create_transform( name='viz_transform_realtime_google_dt_' + self.random_id_generator()+'.'+data_product_id,
            in_subscription_id=viz_subscription_id2,
            process_definition_id=self.process_definition_id2,
            configuration=configuration2)
        self.tms_cli.activate_transform(viz_transform_id2)


        # keep a record of the the viz_transform_id
        #self.viz_data_dictionary['google_realtime_dt'][data_product_id]['transform_proc'] = viz_transform_id2


    def random_id_generator(self, size=8, chars=string.ascii_uppercase + string.digits):
        id = ''.join(random.choice(chars) for x in range(size))
        return id

    def receive_new_dataproduct_event(self, event_msg, headers):
        """
        For now we will start a thread that emulates an event handler
        """

        # Only handle data product creation events
        if not (event_msg.origin_type == "DataProduct" and
                (event_msg.mod_type == ResourceModificationType.CREATE or event_msg.mod_type == ResourceModificationType.UPDATE)):
            return

        # register the new data product
        self.register_new_data_product(event_msg.origin)


class VizTransformProcForGoogleDT(TransformDataProcess):

    """
    This class is used for instantiating worker processes that have subscriptions to data streams and convert
    incoming data from CDM format to JSON style Google DataTables

    """
    def on_start(self):
        super(VizTransformProcForGoogleDT,self).on_start()
        self.initDataTableFlag = True

        # Create client to interface with the viz service
        self.vs_cli = VisualizationServiceClient(node=self.container.node)
        self.rr_cli = ResourceRegistryServiceProcessClient(process = self, node = self.container.node)

        # extract the data_product_id from the transform name. Should be appended with a '.'
        self.data_product_id = self.CFG.get('data_product_id')
        self.stream_def_id = self.CFG.get("stream_def_id")
        self.stream_def = self.rr_cli.read(self.stream_def_id)
        self.realtime_flag = False
        if self.CFG.get("realtime_flag") == "True":
            self.realtime_flag = True

        self.dataDescription = []
        self.dataTableContent = []
        self.varTuple = []


    def process(self, packet):

        log.warn('(%s): Received Viz Data Packet' % (self.name) )
        #log.debug('(%s):   - Processing: %s' % (self.name,packet))

        psd = PointSupplementStreamParser(stream_definition=self.stream_def.container, stream_granule=packet)
        vardict = {}
        arrLen = None
        for varname in psd.list_field_names():
            vardict[varname] = psd.get_values(varname)
            arrLen = len(vardict[varname])


        #if its the first time, init the dataTable
        if self.initDataTableFlag:
            # create data description from the variables in the message
            self.dataDescription = [('time', 'datetime', 'time')]

            # split the data string to extract variable names
            for varname in psd.list_field_names():

                if varname == 'time':
                    continue

                self.dataDescription.append((varname, 'number', varname))

            #self.dataTableContent = [[None] * len(self.dataDescription)]
            self.initDataTableFlag = False


        for i in xrange(arrLen):
            varTuple = []

            for varname,_,_ in self.dataDescription:
                val = float(vardict[varname][i])
                if varname == 'time':
                    #varTuple.append(num2date(val,'days since 1970-01-01T00:00:00Z', 'standard'))
                    varTuple.append(datetime.fromtimestamp(val))
                else:
                    varTuple.append(val)

            self.dataTableContent.append (varTuple)

            # Maintain a sliding window for realtime transform processes
            realtime_window_size = 100
            if self.realtime_flag and len(self.dataTableContent) > realtime_window_size:
                #self.dataTableContent.pop(-(realtime_window_size + 1)) # always pop the first element
                self.dataTableContent.pop(0)

        # create the google viz data table
        data_table = gviz_api.DataTable(self.dataDescription)
        data_table.LoadData(self.dataTableContent)

        # submit the Json version of the datatable to the viz service
        self.vs_cli.submit_google_realtime_dt(self.data_product_id, data_table.ToJSonResponse())

        # clear the tuple for future use
        self.varTuple[:] = []



class VizTransformProcForMatplotlibGraphs(TransformDataProcess):

    """
    This class is used for instantiating worker processes that have subscriptions to data streams and convert
    incoming data from CDM format to Matplotlib graphs

    """
    def on_start(self):
        super(VizTransformProcForMatplotlibGraphs,self).on_start()
        #assert len(self.streams)==1
        self.initDataFlag = True
        self.graph_data = {} # Stores a dictionary of variables : [List of values]

        # Create client to interface with the viz service

        self.rr_cli = ResourceRegistryServiceProcessClient(process = self, node = self.container.node)

        # extract the data_product_id from the transform name. Should be appended with a '.'
        self.data_product_id = self.CFG.get('data_product_id')
        self.stream_def_id = self.CFG.get("stream_def_id")
        self.stream_def = self.rr_cli.read(self.stream_def_id)

        # Start the thread responsible for keeping track of time and generating graphs
        self.rendering_proc = Greenlet(self.rendering_thread)
        self.rendering_proc.start()

        # Create a stringIO object to hold image files in memory


        # Create a mutex for shared data
        self.lock = RLock()



    def process(self, packet):
        log.warn('(%s): Received Viz Data Packet' % self.name )
        #log.debug('(%s):   - Processing: %s' % (self.name,packet))

        # parse the incoming data
        psd = PointSupplementStreamParser(stream_definition=self.stream_def.container, stream_granule=packet)

        # re-arrange incoming data into an easy to parse dictionary
        vardict = {}
        arrLen = None
        for varname in psd.list_field_names():
            vardict[varname] = psd.get_values(varname)
            arrLen = len(vardict[varname])

        if self.initDataFlag:
            # look at the incoming packet and store
            for varname in psd.list_field_names():
                self.lock.acquire()
                self.graph_data[varname] = []
                self.lock.release()

            self.initDataFlag = False

        # If code reached here, the graph data storage has been initialized. Just add values
        # to the list
        self.lock.acquire()
        for varname in psd.list_field_names():
            self.graph_data[varname].extend(vardict[varname])
        self.lock.release()

    def rendering_thread(self):
        from copy import deepcopy
        # Service Client
        vs_cli = VisualizationServiceClient()

        # init Matplotlib
        fig = Figure()
        ax = fig.add_subplot(111)
        canvas = FigureCanvas(fig)
        imgInMem = StringIO.StringIO()
        while True:

            # Sleep for a pre-decided interval. Should be specifiable in a YAML file
            gevent.sleep(20)

            # If there's no data, wait
            # Lock is used here to make sure the entire vector exists start to finish, this assures that the data won
            working_set=None
            self.lock.acquire()
            if len(self.graph_data) == 0:
                continue
            else:
                working_set = deepcopy(working_set)
            self.lock.release()

            # For the simple case of testing, lets plot all time variant variables one at a time
            xAxisVar = 'time'
            xAxisFloatData = working_set[xAxisVar]

            for varName, varData in working_set.iteritems():
                if varName == 'time' or varName == 'height' or varName == 'longitude' or varName == 'latitude':
                    continue

                yAxisVar = varName
                yAxisFloatData = working_set[varName]

                # Generate the plot
                ax.plot(xAxisFloatData, yAxisFloatData, 'ro')
                ax.set_xlabel(xAxisVar)
                ax.set_ylabel(yAxisVar)
                ax.set_title(yAxisVar + ' vs ' + xAxisVar)
                ax.set_autoscale_on(False)

                # generate filename for the output image
                fileName = yAxisVar + '_vs_' + xAxisVar + '.png'
                # Save the figure to the in memory file
                canvas.print_figure(imgInMem, format="png")
                imgInMem.seek(0)

                # submit the image object to the visualization service
                vs_cli.submit_mpl_image(self.data_product_id, imgInMem.getvalue(), fileName)

                #clear the canvas for the next image
                ax.clear()
