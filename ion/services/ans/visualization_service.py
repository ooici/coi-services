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
from pyon.event.event import EventSubscriber
from pyon.util.async import spawn
#from interface.objects import ResourceModificationType
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

    def on_start(self):

        # init services needed
        self.rrclient = self.clients.resource_registry
        self.pubsubclient =  self.clients.pubsub_management
        self.workflowclient = self.clients.workflow_management
        self.tmsclient = self.clients.transform_management

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

    def get_image(self, data_product_id='', query=''):

        image_obj = None

        return image_obj
