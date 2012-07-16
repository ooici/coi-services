#!/usr/bin/env python



__author__ = 'Raj Singh, Stephen Henrie'
__license__ = 'Apache 2.0'

"""
Note:
[1]
"""

# Pyon imports
# Note pyon imports need to be first for monkey patching to occur
from pyon.ion.transform import TransformDataProcess
from pyon.public import IonObject, RT, log, PRED, StreamSubscriberRegistrar, StreamPublisherRegistrar, Container
from pyon.util.containers import create_unique_identifier, get_safe
from interface.objects import StreamQuery
from pyon.core.exception import Inconsistent
from datetime import datetime
import string
import random
import StringIO
import simplejson
import math
import gevent
from gevent.greenlet import Greenlet

from interface.services.ans.ivisualization_service import BaseVisualizationService
from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDT
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.net.endpoint import Subscriber

# Matplotlib related imports
import matplotlib as mpl
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

# Google viz library for google charts
import ion.services.ans.gviz_api as gviz_api



class VisualizationService(BaseVisualizationService):

    def on_start(self):

        # init services needed
        self.rrclient = self.clients.resource_registry
        self.pubsubclient =  self.clients.pubsub_management
        self.workflowclient = self.clients.workflow_management
        self.tmsclient = self.clients.transform_management
        self.data_retriever = self.clients.data_retriever

        return

    def on_stop(self):

        return


    def initiate_realtime_visualization(self, data_product_id='', query=''):
        """Initial request required to start a realtime chart for the specified data product. Returns a user specific token associated
        with the request that will be required for subsequent requests when polling data.

        @param data_product_id    str
        @param query    str
        @retval query_token    str
        @throws NotFound    Throws if specified data product id or its visualization product does not exist
        """

        # Perform a look up to check and see if the DP is indeed a realtime GDT stream
        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")

        data_product = self.clients.resource_registry.read(data_product_id)
        if not data_product:
            raise NotFound("Data product %s does not exist" % data_product_id)

        # TODO check if is a real time GDT stream

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStream, None, True)
        if not stream_ids:
            raise Inconsistent("Could not find Stream Id for Data Product %s" % data_product_id)

        data_product_stream_id = stream_ids

        # Create a queue to collect the stream granules

        #TODO - figure out how to determine if a queue already exists for this user - maybe a session ID?

        query_token = create_unique_identifier('user_queue')

        xq = Container.instance.ex_manager.create_xn_queue(query_token)

        #TODO - figure out how to make this instance specific
        self.subscription_id = self.pubsubclient.create_subscription(
            query=StreamQuery(data_product_stream_id),
            exchange_name = query_token,
            exchange_point = 'science_data',
            name = "user visualization queue",
        )

        # after the queue has been created it is safe to activate the subscription
        self.clients.pubsub_management.activate_subscription(self.subscription_id)

        return query_token

    def _process_messages(self, msgs):

        rdt = RecordDictionaryTool.load_from_granule(msgs.body)

        vardict = {}
        vardict['temp'] = get_safe(rdt, 'temp')
        vardict['time'] = get_safe(rdt, 'time')
        print vardict['time']
        print vardict['temp']

        return (vardict['time'], vardict['temp'])

    def get_realtime_visualization_data(self, query_token=''):
        """This operation returns a block of visualization data for displaying data product in real time. This operation requires a
        user specific token which was provided from a previsou request to the init_realtime_visualization operation.

        @param query_token    str
        @retval datatable    str
        @throws NotFound    Throws if specified query_token or its visualization product does not exist
        """

        if not query_token:
            raise BadRequest("The query_token parameter is missing")

        #TODO -should I keep doing this? DOes it matter since it is idempotent
        xq = Container.instance.ex_manager.create_xn_queue(query_token)


        subscriber = Subscriber(from_name=xq)
        subscriber.initialize()

        msg_count,_ = subscriber._chan.get_stats()
        print 'Messages in user queue 1: ' + str(msg_count)

        ret_val = []
        msgs = subscriber.get_n_msgs(msg_count, timeout=2)
        for x in range(len(msgs)):
            msgs[x].ack()
            ret_val.append(self._process_messages(msgs[x]))

        msg_count,_ = subscriber._chan.get_stats()
        print 'Messages in user queue 2: ' + str(msg_count)

        subscriber.close()

        return str(ret_val)


    def terminate_realtime_visualization_data(self, query_token=''):
        """This operation terminates and cleans up resources associated with realtime visualization data. This operation requires a
        user specific token which was provided from a previsou request to the init_realtime_visualization operation.

        @param query_token    str
        @throws NotFound    Throws if specified query_token or its visualization product does not exist
        """

        if not query_token:
            raise BadRequest("The query_token parameter is missing")

        self.clients.pubsub_management.deactivate_subscription(self.subscription_id)

        self.clients.pubsub_management.delete_subscription(self.subscription_id)

        #TODO -should I keep doing this? DOes it matter since it is idempotent
        xq = Container.instance.ex_manager.create_xn_queue(query_token)

        self.container.ex_manager.delete_xn(xq)

    def init_google_dt(self, data_product_id='', query=''):
        """Retrieves the data for the specified DP and sends a token back which can be checked in
            a non-blocking fashion till data is ready

        @param data_product_id    str
        @param query    str
        @retval query_token  str
        @throws NotFound    object with specified id, query does not exist
        """

        #try:
        # get the dataset_id associated with the data_product. Need it to do the data retrieval
        ds_ids,_ = self.rrclient.find_objects(data_product_id, PRED.hasDataset, RT.DataSet, True)

        if ds_ids == None or len(ds_ids) == 0:
            print ">>>>>> COULD NOT LOCATE DATASET ID"
            return []

        # Ideally just need the latest granule to figure out the list of images
        #replay_granule = self.data_retriever.retrieve(ds_ids[0],{'start_time':0,'end_time':2})
        retrieve_granule = self.data_retriever.retrieve(ds_ids[0])

        print ">>>>>>>>>>>> REPLAY_GRANULE = ", retrieve_granule

        # send the granule through the transform to get the google datatable
        gt_transform = VizTransformGoogleDT()
        #gt_transform.on_start()
        gt_data_granule = gt_transform.execute(retrieve_granule)

        gt_tx = TaxyTool.load_from_granule(gt_data_granule)
        gt_rdt = RecordDictionaryTool.load_from_granule(gt_data_granule)
        print " >>>>>>>>>  GT_DATA_GRANULE = ", gt_data_granule

        #except:
        #    return []


        return query_token

    def get_google_dt(self, query_token=''):

        datatable = None


        return datatable

    def get_image(self, data_product_id='', image_id=''):

        image_obj = None

        return image_obj

    def get_list_of_mpl_images(self, data_product_id='', query=''):

        # return a json version of the array stored in the data_dict
        try:
            # get the dataset_id associated with the data_product. Need it to do the data retrieval
            #ds_ids, = self.rrclient.find_resources(data_product_id, PRED.hasDataset, None, True)
            ds_ids,_ = self.rrclient.find_objects(data_product_id, PRED.hasDataset, RT.DataSet, True)

            if ds_ids == None or len(ds_ids) == 0:
                print ">>>>>> COULD NOT LOCATE DATASET ID"
                return []

            # Ideally just need the latest granule to figure out the list of images
            #replay_granule = self.data_retriever.retrieve(ds_ids[0],{'start_time':0,'end_time':2})
            retrieve_granule = self.data_retriever.retrieve(ds_ids[0])

            print ">>>>>>>>>>>> REPLAY_GRANULE = ", retrieve_granule

            # iterate over the granule

            image_list = []

            # return Json list
            return image_list

        except:
            return []
