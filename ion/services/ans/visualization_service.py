#!/usr/bin/env python

__author__ = 'Raj Singh'
__license__ = 'Apache 2.0'

"""
Note:
[1]
"""

# Pyon imports
# Note pyon imports need to be first for monkey patching to occur
from pyon.ion.transform import TransformDataProcess
from pyon.public import IonObject, RT, log, PRED, StreamSubscriberRegistrar, StreamPublisherRegistrar
from interface.objects import StreamQuery

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

    def random_id_generator(self, size=12, chars=string.ascii_uppercase + string.digits):
        id = ''.join(random.choice(chars) for x in range(size))
        return id

    def init_google_dt_realtime(self, data_product_id='', query=''):
        """Request to search for the system transform handling the GDT conversion for the specified DP.
            Prepares the queue to collect the data, manages the subscription and returns a token to access
            the data queue. Subsequent request to access the queue should contain this token.

        @param data_product_id    str
        @param query    str
        @retval query_token  str
        @throws NotFound    object with specified id, query does not exist
        """

        # Perform a look up to check and see if the DP is indeed a realtime GDT stream

        # Create a queue to collect the stream granules


        return query_token

    def get_google_dt_realtime(self, query_token=''):

        datatable = None


        return datatable

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
