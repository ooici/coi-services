#!/usr/bin/env python



__author__ = 'Raj Singh, Stephen Henrie'
__license__ = 'Apache 2.0'

"""
Note:
[1]
"""

# Pyon imports
# Note pyon imports need to be first for monkey patching to occur
from pyon.public import IonObject, RT, log, PRED
from pyon.util.containers import create_unique_identifier, get_safe
from pyon.core.exception import Inconsistent, BadRequest, NotFound
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
from interface.objects import Granule
from pyon.util.containers import get_safe
# for direct hdf access
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

# PIL
#import Image
#import StringIO


# Google viz library for google charts
#import ion.services.ans.gviz_api as gviz_api

USER_VISUALIZATION_QUEUE = 'UserVisQueue'
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

        # Perform a look up to check and see if the DP is indeed a realtime GDT stream
        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")
        data_product = self.clients.resource_registry.read(data_product_id)
        
        if not data_product:
            raise NotFound("Data product %s does not exist" % data_product_id)

        #Look for a workflow which is already executing for this data product
        workflow_ids, _ = self.clients.resource_registry.find_subjects(subject_type=RT.Workflow, predicate=PRED.hasInputProduct, object=data_product_id)

        #If an existing workflow is not found then create one
        if len(workflow_ids) == 0:
            #TODO - Add this workflow definition to preload data
            # Check to see if the workflow defnition already exist
            workflow_def_ids,_ = self.clients.resource_registry.find_resources(restype=RT.WorkflowDefinition, name='Realtime_HighCharts', id_only=True)

            if len(workflow_def_ids) > 0:
                workflow_def_id = workflow_def_ids[0]
            else:
                raise NotFound(" Workflow definition named 'Realtime_HighCharts' does not exist")

            #Create and start the workflow
            workflow_id, workflow_product_id = self.clients.workflow_management.create_data_process_workflow(workflow_definition_id=workflow_def_id,
                input_data_product_id=data_product_id, persist_workflow_data_product=PERSIST_REALTIME_DATA_PRODUCTS, timeout=self.create_workflow_timeout)
        else:
            workflow_id = workflow_ids[0]._id

            # detect the output data product of the workflow
            workflow_dp_ids,_ = self.clients.resource_registry.find_objects(subject=workflow_id, predicate=PRED.hasOutputProduct, object_type=RT.DataProduct, id_only=True)
            if len(workflow_dp_ids) < 1:
                log.error("Could not find output data product for existing workflow : ", workflow_id)
                raise NotFound("Could not find output data product for existing workflow : ", workflow_id)

            workflow_product_id = workflow_dp_ids[0]

        # find associated stream id with the output
        workflow_output_stream_ids, _ = self.clients.resource_registry.find_objects(subject=workflow_product_id, predicate=PRED.hasStream, object_type=None, id_only=True)

        # Create a queue to collect the stream granules - idempotency saves the day!
        query_token = create_unique_identifier(USER_VISUALIZATION_QUEUE)

        xq = self.container.ex_manager.create_xn_queue(query_token)
        subscription_id = self.clients.pubsub_management.create_subscription(
            stream_ids=workflow_output_stream_ids,
            exchange_name = query_token,
            name = query_token
        )

        # after the queue has been created it is safe to activate the subscription
        self.clients.pubsub_management.activate_subscription(subscription_id)

        ret_dict = {'rt_query_token': query_token}
        return simplejson.dumps(ret_dict)


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
            xq = self.container.ex_manager.create_xn_queue(query_token)

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

        subscriptions, _ = self.clients.resource_registry.find_resources(restype=RT.Subscription, name=query_token, id_only=False)

        if not subscriptions:
            raise NotFound("A Subscription object for the query_token parameter %s is not found" % query_token)

        if len(subscriptions) > 1:
            log.warn("An inconsistent number of Subscription resources associated with the name: %s - will use the first one in the list",query_token )

        if not subscriptions[0].activated:
            log.warn("Subscription and Visualization queue %s is deactivated, so another worker may be cleaning this up already - skipping", query_token)
            return

        #Get the Subscription id
        subscription_id = subscriptions[0]._id

        #need to find the associated data product BEFORE deleting the subscription
        stream_ids, _  = self.clients.resource_registry.find_objects(object_type=RT.Stream, predicate=PRED.hasStream, subject=subscription_id, id_only=True)

        sub_stream_id = ''
        if len(stream_ids) == 0:
            log.error("Cannot find Stream for Subscription id %s", subscription_id)
        else:
            sub_stream_id = stream_ids[0]

        #Now delete the subscription if activated
        try:
            self.clients.pubsub_management.deactivate_subscription(subscription_id)

            #This pubsub operation deletes the underlying echange queues as well, so no need to manually delete.
            self.clients.pubsub_management.delete_subscription(subscription_id)

        #Need to continue on even if this fails
        except BadRequest, e:
            #It is okay if the workflow is no longer available as it may have been cleaned up by another worker
            if 'Subscription is not active' not in e.message:
                log.exception(e)
                sub_stream_id = ''

        except Exception, e:
            log.exception(e)
            sub_stream_id = ''


        log.debug("Real-time queue %s cleaned up", query_token)

        if sub_stream_id != '':
            try:
                #Can only clean up if we have all of the data
                workflow_data_product_id, _  = self.clients.resource_registry.find_subjects(subject_type=RT.DataProduct, predicate=PRED.hasStream, object=sub_stream_id, id_only=True)

                #Need to clean up associated workflow from the data product as long as they are going to be created for each user
                if len(workflow_data_product_id) == 0:
                    log.error("Cannot find Data Product for Stream id %s", sub_stream_id )
                else:

                    #Get the total number of subscriptions
                    total_subscriptions, _  = self.clients.resource_registry.find_subjects(subject_type=RT.Subscription, predicate=PRED.hasStream, object=sub_stream_id, id_only=False)

                    #Filter the list down to just those that are subscriptions for user real-time queues
                    total_subscriptions = [subs for subs in total_subscriptions if subs.name.startswith(USER_VISUALIZATION_QUEUE)]

                    #Only cleanup data product and workflow when the last subscription has been deleted
                    if len(total_subscriptions) == 0:

                        workflow_id, _  = self.clients.resource_registry.find_subjects(subject_type=RT.Workflow, predicate=PRED.hasOutputProduct, object=workflow_data_product_id[0], id_only=True)
                        if len(workflow_id) == 0:
                            log.error("Cannot find Workflow for Data Product id %s", workflow_data_product_id[0] )
                        else:
                            self.clients.workflow_management.terminate_data_process_workflow(workflow_id=workflow_id[0], delete_data_products=True, timeout=self.terminate_workflow_timeout)

            except NotFound, e:
                #It is okay if the workflow is no longer available as it may have been cleaned up by another worker
                pass

            except Exception, e:
                log.exception(e)


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
        dp_meta_data['time_steps'] = self.clients.dataset_management.dataset_extents(ds_ids[0])['time'][0]

        # use the associations to get to the parameter dict
        parameter_display_names = {}
        for stream_def in self.clients.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=True)[0]:
            for pdict in self.clients.resource_registry.find_objects(stream_def, PRED.hasParameterDictionary, id_only=True)[0]:
                for context in self.clients.resource_registry.find_objects(pdict, PRED.hasParameterContext, id_only=False)[0]:
                    #display_name = context.display_name
                    parameter_display_names[context.name] = context.display_name

        dp_meta_data['parameter_display_names'] = parameter_display_names

        return simplejson.dumps(dp_meta_data)

