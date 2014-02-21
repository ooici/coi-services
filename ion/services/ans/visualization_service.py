#!/usr/bin/env python



__author__ = 'Raj Singh, Stephen Henrie'
__license__ = 'Apache 2.0'

"""
Note:
[1]
"""

# Pyon imports
# Note pyon imports need to be first for monkey patching to occur
from pyon.public import IonObject, RT, log, PRED, EventPublisher, OT
from pyon.util.containers import create_unique_identifier, get_safe
from pyon.core.exception import Inconsistent, BadRequest, NotFound, ServiceUnavailable
from datetime import datetime

import simplejson, json
import gevent
import base64
import sys, traceback

from interface.services.ans.ivisualization_service import BaseVisualizationService
#from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDTAlgorithm
from ion.processes.data.transforms.viz.highcharts import VizTransformHighChartsAlgorithm
from ion.processes.data.transforms.viz.matplotlib_graphs import VizTransformMatplotlibGraphsAlgorithm
from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from pyon.net.endpoint import Subscriber
from interface.objects import Granule, RealtimeVisualization, ProcessDefinition, ProcessSchedule
from interface.objects import ProcessRestartMode, ProcessQueueingMode, ProcessStateEnum
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from pyon.util.containers import get_safe
# for direct hdf access
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from pyon.util.containers import DotDict

# PIL
#import Image
#import StringIO


# Google viz library for google charts
#import ion.services.ans.gviz_api as gviz_api

USER_VISUALIZATION_QUEUE = 'viz-out'
PERSIST_REALTIME_DATA_PRODUCTS = False

class VisualizationService(BaseVisualizationService):

    def on_init(self):
        self.create_workflow_timeout = get_safe(self.CFG, 'create_workflow_timeout', 60)
        self.terminate_workflow_timeout = get_safe(self.CFG, 'terminate_workflow_timeout', 60)
#        self.monitor_timeout = get_safe(self.CFG, 'user_queue_monitor_timeout', 300)
#        self.monitor_queue_size = get_safe(self.CFG, 'user_queue_monitor_size', 100)
#
#        #Setup and event object for use by the queue monitoring greenlet
#        self.monitor_event = gevent.event.Event()
#        self.monitor_event.clear()
#
#
#        #Start up queue monitor
#        self._process.thread_manager.spawn(self.user_vis_queue_monitor)
        return


    def on_quit(self):
#        self.monitor_event.set()
        return



    def initiate_realtime_visualization_data(self, data_product_id='', visualization_parameters=None):
        """Initial request required to start a realtime chart for the specified data product. Returns a user specific token associated
        with the request that will be required for subsequent requests when polling data.
        
        Note : The in_product_type is a temporary fix for specifying the type of data product to the the service

        @param data_product_id    str
        @param in_product_type str
        @param query        str
        @throws NotFound    Throws if specified data product id or its visualization product does not exist
        """
        log.debug( "initiate_realtime_visualization Vis worker: %s " , self.id)

        # Load the visualization_parameter, which is passed as  string in to its representative dict
        query = None
        vp = None
        if visualization_parameters:
            vp = simplejson.loads(visualization_parameters)
            if vp:
                if vp.has_key('query'):
                    query=vp['query']

        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")
        data_product = self.clients.data_product_management.read_data_product(data_product_id)
        
        if not data_product:
            raise NotFound("Data product %s does not exist" % data_product_id)


        # Create and associate the visualization
        viz = RealtimeVisualization(name='Visualization for %s' % data_product_id,
                                    visualization_parameters=visualization_parameters)
        viz_id, rev = self.clients.resource_registry.create(viz)
        query_token = viz_id
        self.clients.resource_registry.create_association(data_product_id, PRED.hasRealtimeVisualization, viz_id)

        stream_def_id = self._get_highcharts_streamdef()

        stream_id, route = self.clients.pubsub_management.create_stream(name="viz_%s" % viz_id, exchange_point="visualization", stream_definition_id=stream_def_id)

        # Create a queue for the visualization subscriber and bind the stream to it.
        # The stream will be used by the visualization process to publish on
        queue_name = '-'.join([USER_VISUALIZATION_QUEUE, query_token])
        self.container.ex_manager.create_xn_queue(queue_name)
        subscription_id = self.clients.pubsub_management.create_subscription(
                stream_ids=[stream_id],
                exchange_name = queue_name,
                name = query_token)

        # after the queue has been created it is safe to activate the subscription
        self.clients.pubsub_management.activate_subscription(subscription_id)

        # Associate the subscription with the viz so we can clean it up later
        self.clients.resource_registry.create_association(viz_id, PRED.hasSubscription, subscription_id)
        # Associate the stream
        self.clients.resource_registry.create_association(viz_id, PRED.hasStream, stream_id)

        # Launch the worker
        pid = self._launch_highcharts(viz_id, data_product_id, stream_id)
        viz._id = viz_id
        viz._rev = rev
        viz.pid = pid
        self.clients.resource_registry.update(viz)


        ret_dict = {'rt_query_token': query_token}
        return simplejson.dumps(ret_dict)

    def _get_highcharts_streamdef(self):
        '''
        A somewhat idempotent retrieve of the stream definition
        Returns the high charts stream definition
        '''

        streamdefs, _ = self.clients.resource_registry.find_resources(restype=RT.StreamDefinition,name='HighCharts_out', id_only=True)
        if streamdefs: # If there's not one create it
            stream_def_id = streamdefs[0]
        else:
            pdict_id = self.clients.dataset_management.read_parameter_dictionary_by_name('highcharts')
            stream_def_id = self.clients.pubsub_management.create_stream_definition('HighCharts_out', parameter_dictionary_id=pdict_id)
        return stream_def_id

    def _get_highcharts_procdef(self):
        '''
        A somewhat idempotent retrieve of the process definition
        Returns the high charts process definition id 
        '''
        procdefs, _ = self.clients.resource_registry.find_resources(
                    name='HighChartsDefinition',
                    restype=RT.ProcessDefinition, 
                    id_only=True)
        if procdefs:
            return procdefs[0]

        procdef = ProcessDefinition(name='HighChartsDefinition')
        procdef.executable['module'] = 'ion.processes.data.transforms.viz.highcharts'
        procdef.executable['class'] = 'VizTransformHighCharts'

        procdef_id = self.clients.process_dispatcher.create_process_definition(procdef)
        return procdef_id



    def _launch_highcharts(self, viz_id, data_product_id, out_stream_id):
        '''
        Launches the high-charts transform
        '''
        stream_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStream, id_only=True)
        if not stream_ids:
            raise BadRequest("Can't launch high charts streaming: data product doesn't have associated stream (%s)" % data_product_id)

        queue_name = 'viz_%s' % data_product_id
        sub_id = self.clients.pubsub_management.create_subscription(
                    name='viz transform for %s' % data_product_id, 
                    exchange_name=queue_name,
                    stream_ids=stream_ids)

        self.clients.pubsub_management.activate_subscription(sub_id)

        self.clients.resource_registry.create_association(viz_id, PRED.hasSubscription, sub_id)
        
        config = DotDict()
        config.process.publish_streams.highcharts = out_stream_id
        config.process.queue_name = queue_name

        # This process MUST be launched the first time or fail so the user
        # doesn't wait there for nothing to happen.
        schedule = ProcessSchedule()
        schedule.restart_mode = ProcessRestartMode.NEVER
        schedule.queueing_mode = ProcessQueueingMode.ALWAYS

        # Launch the process
        procdef_id = self._get_highcharts_procdef()
        pid = self.clients.process_dispatcher.schedule_process(
                process_definition_id=procdef_id,
                schedule=schedule,
                configuration=config)

        # Make sure it launched or raise an error

        process_gate = ProcessStateGate(self.clients.process_dispatcher.read_process, pid, ProcessStateEnum.RUNNING)
        if not process_gate.await(self.CFG.get_safe('endpoint.receive.timeout', 10)):
            raise ServiceUnavailable("Failed to launch high charts realtime visualization")

        return pid

        
    def _process_visualization_message(self, messages):

        final_hc_data = []
        final_hc_data_no_numpy = []

        for message in messages:

            if message == None:
                continue

            message_data = message.body

            if isinstance(message_data,Granule):

                rdt = RecordDictionaryTool.load_from_granule(message_data)
                hc_data_arr = get_safe(rdt, 'hc_data')

                # IF this granule does not contain google dt, skip
                if hc_data_arr is None:
                    continue

                hc_data = hc_data_arr[0]

                for series in hc_data:
                    if not series.has_key("name"):
                        continue
                    series_name = series["name"]

                    # find index in final data. If it does not exist, create it
                    final_hc_data_idx = -1
                    idx = 0
                    for _s in final_hc_data:
                        if _s["name"] == series_name:
                            final_hc_data_idx = idx
                            break
                        idx += 1


                    # create an entry in the final hc structure if there's none for this series
                    if final_hc_data_idx == -1:
                        final_hc_data.append({})
                        final_hc_data_idx = len(final_hc_data) - 1
                        final_hc_data[final_hc_data_idx]["name"] = series_name
                        final_hc_data[final_hc_data_idx]["data"] = []

                        if series.has_key("visible"):
                            final_hc_data[final_hc_data_idx]["visible"] = series["visible"]
                        if series.has_key("tooltip"):
                            final_hc_data[final_hc_data_idx]["tooltip"] = series["tooltip"]

                    # Append the series data to the final hc structure
                    final_hc_data[final_hc_data_idx]["data"] += series["data"].tolist()

        return json.dumps(final_hc_data)


    def get_realtime_visualization_data(self, query_token=''):
        """This operation returns a block of visualization data for displaying data product in real time. This operation requires a
        user specific token which was provided from a previous request to the init_realtime_visualization operation.

        @param query_token    str
        @retval datatable    str
        @throws NotFound    Throws if specified query_token or its visualization product does not exist
        """
        log.debug( "get_realtime_visualization_data Vis worker: %s", self.id)

        ret_val = []
        if not query_token:
            raise BadRequest("The query_token parameter is missing")

        try:
            #Taking advantage of idempotency
            queue_name = '-'.join([USER_VISUALIZATION_QUEUE, query_token])
            xq = self.container.ex_manager.create_xn_queue(queue_name)

            subscriber = Subscriber(from_name=xq)
            subscriber.initialize()

        except:
            # Close the subscriber if it exists
            if subscriber:
                subscriber.close()

            raise BadRequest("Could not subscribe to the real-time queue")

        msgs = subscriber.get_all_msgs(timeout=2)
        for x in range(len(msgs)):
            msgs[x].ack()

        subscriber.close()

        # Different messages should get processed differently. Ret val will be decided by the viz product type
        ret_val = self._process_visualization_message(msgs)

        return ret_val


    def terminate_realtime_visualization_data(self, query_token=''):
        """This operation terminates and cleans up resources associated with realtime visualization data. This operation requires a
        user specific token which was provided from a previous request to the init_realtime_visualization operation.

        @param query_token    str
        @throws NotFound    Throws if specified query_token or its visualization product does not exist
        """
        log.debug( "terminate_realtime_visualization_data Vis worker: %s" , self.id)

        if not query_token:
            raise BadRequest("The query_token parameter is missing")

        viz = self.clients.resource_registry.read(query_token)

        subscriptions, _ = self.clients.resource_registry.find_objects(query_token, PRED.hasSubscription, id_only=True)
        if not subscriptions:
            raise NotFound("A Subscription object for the query_token parameter %s is not found" % query_token)
        for subscription in subscriptions:
            try:
                self.clients.pubsub_management.deactivate_subscription(subscription)
            except BadRequest as m:
                if 'Subscription is not active.' != m.message:
                    raise
            self.clients.pubsub_management.delete_subscription(subscription)

        streams, _ = self.clients.resource_registry.find_objects(query_token, PRED.hasStream, id_only=True)
        if not streams:
            raise NotFound("A stream object for the query_token %s was not found" % query_token)
        for stream in streams:
            self.clients.pubsub_management.delete_stream(stream)

        pid = viz.pid
        self.clients.process_dispatcher.cancel_process(pid)
        self.clients.resource_registry.delete(query_token)


    def get_visualization_data(self, data_product_id='', visualization_parameters=None):

        if visualization_parameters is None:
            return self._get_google_dt(data_product_id)
        
        vp_dict = simplejson.loads(visualization_parameters)

        # The visualization parameters must contain a query type. Based on this the processing methods will be chosen
        if (vp_dict['query_type'] == 'metadata'):
            return self._get_data_product_metadata(data_product_id)

        if (vp_dict['query_type'] == 'highcharts_data'):
            return self._get_highcharts_data(data_product_id, vp_dict)

        if (vp_dict['query_type'] == 'mpl_image'):
            image_info = self.get_visualization_image(data_product_id, vp_dict)
            # Encode actual image object as base64 string so that it can be passed as json string back.
            image_info['image_obj'] = base64.encodestring(image_info["image_obj"])
            return simplejson.dumps(image_info)

    def _publish_access_event(self, access_type, data_product_id=None, access_params=None):
        try:
            pub = EventPublisher(OT.InformationContentAccessedEvent, process=self)
            event_data = dict(origin_type=RT.DataProduct,
                              origin=data_product_id or "",
                              sub_type=access_type,
                              access_params=access_params or {})
            pub.publish_event(**event_data)
        except Exception as ex:
            log.exception("Error publishing InformationContentAccessedEvent for data product: %s", data_product_id)

    def _get_highcharts_data(self, data_product_id='', visualization_parameters=None):
        """Retrieves the data for the specified DP

        @param data_product_id    str
        @param visualization_parameters    str
        @retval jsonp_visualization_data str
        @throws NotFound    object with specified id, query does not exist
        """

        # An empty dict is returned in case there is no data in coverage
        empty_hc = []

        # error check
        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")

        use_direct_access = False
        if visualization_parameters == {}:
            visualization_parameters = None

        # Extract the parameters. Definitely init first
        query = None
        if visualization_parameters:
            #query = {'parameters':[]}
            query = {}
            # Error check and damage control. Definitely need time
            if 'parameters' in visualization_parameters and len(visualization_parameters['parameters']) > 0:
                if not 'time' in visualization_parameters['parameters']:
                    visualization_parameters['parameters'].append('time')

                query['parameters'] = visualization_parameters['parameters']

            # The times passed from UI are system times so convert them to NTP
            if 'start_time' in visualization_parameters:
                query['start_time'] = int(visualization_parameters['start_time'])

            if 'end_time' in visualization_parameters:
                query['end_time'] = int((visualization_parameters['end_time']))

            # stride time
            if 'stride_time' in visualization_parameters:
                try:
                    query['stride_time'] = int(visualization_parameters['stride_time'])
                except TypeError: 
                    # There are some (rare) situations where the AJAX request has 'null' in the request
                    # Example:
                    # {"query_type":"google_dt","parameters":[],"start_time":-2208988800,"end_time":-2208988800,"stride_time":null,"use_direct_access":0}
                    query['stride_time'] = 1
            else:
                query['stride_time'] = 1

            # direct access parameter
            if 'use_direct_access' in visualization_parameters:
                if (int(visualization_parameters['use_direct_access']) == 1):
                    use_direct_access = True
                else:
                    use_direct_access = False

        # get the dataset_id and objs associated with the data_product. Need it to do the data retrieval
        ds_ids,_ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.Dataset, True)

        if ds_ids is None or not ds_ids:
            raise NotFound("Could not find dataset associated with data product")
        stream_def_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=True)
        if not stream_def_ids:
            raise NotFound('Could not find stream definition associated with data product')
        stream_def_id = stream_def_ids[0]
        try:

            if use_direct_access:
                retrieved_granule = DataRetrieverService.retrieve_oob(ds_ids[0], query=query, delivery_format=stream_def_id)
            else:
                #replay_granule = self.clients.data_retriever.retrieve(ds_ids[0],{'start_time':0,'end_time':2})
                retrieved_granule = self.clients.data_retriever.retrieve(ds_ids[0], query=query, delivery_format=stream_def_id)
        except BadRequest:
            dp = self.container.resource_registry.read(data_product_id)
            log.exception('Problem visualizing data product: %s (%s).\n_get_highcharts_data(data_product_id="%s", visualization_parameters=%s)', dp.name, data_product_id, data_product_id, visualization_parameters)
            raise

        # If thereis no data, return an empty dict
        if retrieved_granule is None:
            return simplejson.dumps(empty_hc)

        # send the granule through the transform to get the google datatable
        hc_pdict_id = self.clients.dataset_management.read_parameter_dictionary_by_name('highcharts',id_only=True)
        hc_stream_def = self.clients.pubsub_management.create_stream_definition('HighCharts_out', parameter_dictionary_id=hc_pdict_id)

        hc_data_granule = VizTransformHighChartsAlgorithm.execute(retrieved_granule, params=hc_stream_def, config=visualization_parameters)

        if hc_data_granule == None:
            return simplejson.dumps(empty_hc)

        hc_rdt = RecordDictionaryTool.load_from_granule(hc_data_granule)
        # Now go through this redundant step of converting the hc_data into a non numpy version
        hc_data_np = (get_safe(hc_rdt, "hc_data"))[0]
        hc_data = []

        for series in hc_data_np:
            s = {}
            for key in series:
                if key == "data":
                    s["data"] = series["data"].tolist()
                    continue
                s[key] = series[key]
            hc_data.append(s)

        # return the json version of the table
        return json.dumps(hc_data)


    def get_visualization_image(self, data_product_id='', visualization_parameters=None):

        # Error check
        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")
        if visualization_parameters == {}:
            visualization_parameters = None
        # Extract the retrieval related parameters. Definitely init all parameters first
        query = None
        image_name = None
        if visualization_parameters :
            #query = {'parameters':[]}
            query = {}

            # Error check and damage control. Definitely need time
            if 'parameters' in visualization_parameters:
                if not 'time' in visualization_parameters['parameters']:
                    visualization_parameters['parameters'].append('time')
                query['parameters'] = visualization_parameters['parameters']

            if 'stride_time' in visualization_parameters:
                query['stride_time'] = visualization_parameters['stride_time']
            if 'start_time' in visualization_parameters:
                query['start_time'] = visualization_parameters['start_time']
            if 'end_time' in visualization_parameters:
                query['end_time'] = visualization_parameters['end_time']

            # If an image_name was given, it means the image is already in the storage. Just need the latest granule.
            # ignore other paramters passed
            if 'image_name' in visualization_parameters:
                image_name = visualization_parameters['image_name']

        # get the dataset_id associated with the data_product. Need it to do the data retrieval
        ds_ids,_ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.Dataset, True)

        if ds_ids is None or not ds_ids:
            log.warn("Specified data_product does not have an associated dataset")
            return None

        # Ideally just need the latest granule to figure out the list of images
        if image_name:
            retrieved_granule = self.clients.data_retriever.retrieve_last_data_points(ds_ids[0], 10)
        else:
            retrieved_granule = self.clients.data_retriever.retrieve(ds_ids[0], query=query)

        if retrieved_granule is None:
            return None

        # no need to pass data through transform if its already a pre-computed image
        if image_name:
            mpl_data_granule = retrieved_granule
        else:
            # send the granule through the transform to get the matplotlib graphs
            mpl_pdict_id = self.clients.dataset_management.read_parameter_dictionary_by_name('graph_image_param_dict',id_only=True)
            mpl_stream_def = self.clients.pubsub_management.create_stream_definition('mpl', parameter_dictionary_id=mpl_pdict_id)
            mpl_data_granule = VizTransformMatplotlibGraphsAlgorithm.execute(retrieved_granule, config=visualization_parameters, params=mpl_stream_def)

        if mpl_data_granule == None:
            return None

        mpl_rdt = RecordDictionaryTool.load_from_granule(mpl_data_granule)

        ret_dict = dict()
        ret_dict['content_type'] = (get_safe(mpl_rdt, "content_type"))[0]
        ret_dict['image_name'] = (get_safe(mpl_rdt, "image_name"))[0]
        # reason for encoding as base64 string is otherwise message pack complains about the bit stream
        #ret_dict['image_obj'] = base64.encodestring((get_safe(mpl_rdt, "image_obj"))[0])
        ret_dict['image_obj'] = (get_safe(mpl_rdt, "image_obj"))[0]

        return ret_dict



    def _get_data_product_metadata(self, data_product_id=""):

        dp_meta_data = {}
        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")

        # DEBUG *** Try to locate the processing_level_code variable for a data product. Get the object first
        dp_obj = self.clients.resource_registry.read(data_product_id)
        if dp_obj and hasattr(dp_obj, 'processing_level_code'):
            dp_meta_data['processing_level_code'] = dp_obj.processing_level_code
        else:
            dp_meta_data['processing_level_code'] = ""

        # get the dataset_id and dataset associated with the data_product. Need it to do the data retrieval
        ds_ids,_ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.Dataset, True)

        if not ds_ids:
            log.warn("Specified data_product does not have an associated dataset")
            return None

        # Start collecting the data to populate the output dictionary
        #time_bounds = self.clients.dataset_management.dataset_bounds(ds_ids[0])['time']
        #dp_meta_data['time_bounds'] = [float(ntplib.ntp_to_system_time(i)) for i in time_bounds]
        time_bounds = self.clients.dataset_management.dataset_temporal_bounds(ds_ids[0])
        dp_meta_data['time_bounds'] = time_bounds
        dp_meta_data['time_steps'] = self.clients.dataset_management.dataset_extents_by_axis(ds_ids[0], 'time')

        # use the associations to get to the parameter dict
        parameter_display_names = {}
        for stream_def in self.clients.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=True)[0]:
            for pdict in self.clients.resource_registry.find_objects(stream_def, PRED.hasParameterDictionary, id_only=True)[0]:
                for context in self.clients.resource_registry.find_objects(pdict, PRED.hasParameterContext, id_only=False)[0]:
                    #display_name = context.display_name
                    parameter_display_names[context.name] = context.display_name

        dp_meta_data['parameter_display_names'] = parameter_display_names

        return simplejson.dumps(dp_meta_data)

