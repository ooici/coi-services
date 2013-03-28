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

import simplejson
import gevent
import base64

from interface.services.ans.ivisualization_service import BaseVisualizationService
from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDTAlgorithm
from ion.processes.data.transforms.viz.matplotlib_graphs import VizTransformMatplotlibGraphsAlgorithm
from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from pyon.net.endpoint import Subscriber
from interface.objects import Granule
from pyon.util.containers import get_safe
# for direct hdf access
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService

# PIL
#import Image
#import StringIO


# Google viz library for google charts
import ion.services.ans.gviz_api as gviz_api

USER_VISUALIZATION_QUEUE = 'UserVisQueue'
PERSIST_REALTIME_DATA_PRODUCTS = False

class VisualizationService(BaseVisualizationService):

    def on_init(self):

        self.create_workflow_timeout = get_safe(self.CFG, 'create_workflow_timeout', 60)
        self.terminate_workflow_timeout = get_safe(self.CFG, 'terminate_workflow_timeout', 30)
        self.monitor_timeout = get_safe(self.CFG, 'user_queue_monitor_timeout', 60)
        self.monitor_queue_size = get_safe(self.CFG, 'user_queue_monitor_size', 100)

        #Setup and event object for use by the queue monitoring greenlet
        self.monitor_event = gevent.event.Event()
        self.monitor_event.clear()


        #Start up queue monitor
        self._process.thread_manager.spawn(self.user_vis_queue_monitor)


    def on_quit(self):
        self.monitor_event.set()


    def user_vis_queue_monitor(self, **kwargs):

        log.debug("Starting Monitor Loop worker: " + self.id + " timeout=" + str(self.monitor_timeout))

        while not self.monitor_event.wait(timeout=self.monitor_timeout):

            if self.container.is_terminating():
                break

            #get the list of queues and message counts on the broker for the user vis queues
            queues = []
            try:
                queues = self.container.ex_manager.list_queues(name=USER_VISUALIZATION_QUEUE, return_columns=['name', 'messages'])
            except Exception, e:
                log.warn('Unable to get queue information from broker management plugin: ' + e.message)
                pass

            log.debug( "In Monitor Loop worker: " + self.id)
            for queue in queues:

                log.debug(queue['name'], queue['messages'])

                #Check for queues which are getting too large and clean them up if need be.
                if queue['messages'] > self.monitor_queue_size:
                    vis_token = queue['name'][queue['name'].index('UserVisQueue'):]

                    try:
                        log.warn("Real-time visualization queue %s had too many messages %d, so terminating this queue and associated resources." % (queue['name'], queue['messages'] ))

                        #Clear out the queue
                        msgs = self.get_realtime_visualization_data(query_token=vis_token)

                        #Now terminate it
                        self.terminate_realtime_visualization_data(query_token=vis_token)
                    except NotFound, e:
                        log.warn("The token %s could not not be found by the terminate_realtime_visualization_data operation; another worked may have cleaned it up already", vis_token)
                    except Exception, e1:
                        #Log errors and keep going!
                        log.exception(e1)

        log.debug('Exiting user_vis_queue_monitor')


    def initiate_realtime_visualization(self, data_product_id='', visualization_parameters=None, callback=""):
        """Initial request required to start a realtime chart for the specified data product. Returns a user specific token associated
        with the request that will be required for subsequent requests when polling data.
        
        Note : The in_product_type is a temporary fix for specifying the type of data product to the the service

        @param data_product_id    str
        @param in_product_type str
        @param query        str
        @param callback     str
        @throws NotFound    Throws if specified data product id or its visualization product does not exist
        """
        log.debug( "initiate_realtime_visualization Vis worker: " + self.id)

        query = None
        if visualization_parameters:
            if visualization_parameters.has_key('query'):
                query=visualization_parameters['query']

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
            workflow_def_ids,_ = self.clients.resource_registry.find_resources(restype=RT.WorkflowDefinition, name='Realtime_Google_DT', id_only=True)

            if len(workflow_def_ids) > 0:
                workflow_def_id = workflow_def_ids[0]
            else:
                workflow_def_id = self._create_google_dt_workflow_def()

            #Create and start the workflow. Take about 4 secs .. wtf
            workflow_id, workflow_product_id = self.clients.workflow_management.create_data_process_workflow(workflow_definition_id=workflow_def_id,
                input_data_product_id=data_product_id, persist_workflow_data_product=PERSIST_REALTIME_DATA_PRODUCTS, timeout=self.create_workflow_timeout)
        else:
            workflow_id = workflow_ids[0]._id

            # detect the output data product of the workflow
            workflow_dp_ids,_ = self.clients.resource_registry.find_objects(subject=workflow_id, predicate=PRED.hasOutputProduct, object_type=RT.DataProduct, id_only=True)
            if len(workflow_dp_ids) != 1:
                log.warn("Workflow Data Product %s has multiple output data products (%d) - using first one found " % (workflow_id, len(workflow_dp_ids)))

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

        if callback == "":
            return query_token
        else:
            return callback + "(\"" + query_token + "\")"




    def _process_visualization_message(self, messages, callback, reqId):

        gdt_description = []
        gdt_content = []
        viz_product_type = 'google_dt'  # defaults to google_dt unless overridden by the message

        for message in messages:

            if message == None:
                continue

            message_data = message.body

            if isinstance(message_data,Granule):

                rdt = RecordDictionaryTool.load_from_granule(message_data)
                gdt_components = get_safe(rdt, 'google_dt_components')

                # IF this granule does not contain google dt, skip
                if gdt_components is None:
                    continue

                gdt_component = gdt_components[0]
                viz_product_type = gdt_component['viz_product_type']

                # Process Google DataTable messages
                if viz_product_type == 'google_dt':

                    # If the data description is being put together for the first time,
                    # switch the time format from float to datetime
                    if (gdt_description == []):
                        temp_gdt_description = gdt_component['data_description']
                        gdt_description = [('time', 'datetime', 'time')]

                        for idx in range(1,len(temp_gdt_description)):
                            # for some weird reason need to force convert to tuples
                            temp_arr = temp_gdt_description[idx]
                            if temp_arr != None and temp_arr[0] != 'time':
                                gdt_description.append((temp_arr[0], temp_arr[1], temp_arr[2]))

                    # append all content to one big array
                    temp_gdt_content = gdt_component['data_content']
                    for tempTuple in temp_gdt_content:
                        # sometimes there are inexplicable empty tuples in the content. Drop them
                        if tempTuple == [] or len(tempTuple) == 0:
                            continue

                        varTuple = []

                        #varTuple.append(datetime.fromtimestamp(tempTuple[0]))
                        varTuple.append(datetime.fromtimestamp(tempTuple[0]))

                        for idx in range(1,len(tempTuple)):
                            varTuple.append(tempTuple[idx])

                        gdt_content.append(varTuple)


                #TODO - what to do if this is not a valid visualization message?


        # Now that all the messages have been parsed, any last processing should be done here
        if viz_product_type == "google_dt":
            # Using the description and content, build the google data table
            if (gdt_description):
                gdt = gviz_api.DataTable(gdt_description)
                gdt.LoadData(gdt_content)

                # return the json version of the table
                if callback == '':
                    return gdt.ToJSonResponse(req_id = reqId)
                else:
                    return callback + "(\"" + gdt.ToJSonResponse(req_id = reqId) + "\")"

            # Handle case where there is no data for constructing a GDT
            else:
                if callback == '':
                    return None
                else:
                    return callback + "(\"" + None + "\")"





        return None


    def get_realtime_visualization_data(self, query_token='', callback='', tqx=""):
        """This operation returns a block of visualization data for displaying data product in real time. This operation requires a
        user specific token which was provided from a previous request to the init_realtime_visualization operation.

        @param query_token    str
        @retval datatable    str
        @throws NotFound    Throws if specified query_token or its visualization product does not exist
        """
        log.debug( "get_realtime_visualization_data Vis worker: " + self.id)

        log.debug("Query token : " + query_token + " CB : " + callback + "TQX : " + tqx)

        reqId = 0
        # If a reqId was passed in tqx, extract it
        if tqx:
            tqx_param_list = tqx.split(";")
            for param in tqx_param_list:
                key, value = param.split(":")
                if key == 'reqId':
                    reqId = value

        ret_val = []
        if not query_token:
            raise BadRequest("The query_token parameter is missing")


        #Taking advantage of idempotency
        xq = self.container.ex_manager.create_xn_queue(query_token)


        subscriber = Subscriber(from_name=xq)
        subscriber.initialize()

        msgs = subscriber.get_all_msgs(timeout=2)
        for x in range(len(msgs)):
            msgs[x].ack()

        # Different messages should get processed differently. Ret val will be decided by the viz product type
        ret_val = self._process_visualization_message(msgs, callback, reqId)


        #TODO - replace as need be to return valid GDT data
        #return {'viz_data': ret_val}

        return ret_val


    def terminate_realtime_visualization_data(self, query_token=''):
        """This operation terminates and cleans up resources associated with realtime visualization data. This operation requires a
        user specific token which was provided from a previous request to the init_realtime_visualization operation.

        @param query_token    str
        @throws NotFound    Throws if specified query_token or its visualization product does not exist
        """
        log.debug( "terminate_realtime_visualization_data Vis worker: " + self.id)

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
                    log.error("Cannot find Data Product for Stream id %s", ( sub_stream_id ))
                else:

                    #Get the total number of subscriptions
                    total_subscriptions, _  = self.clients.resource_registry.find_subjects(subject_type=RT.Subscription, predicate=PRED.hasStream, object=sub_stream_id, id_only=False)

                    #Filter the list down to just those that are subscriptions for user real-time queues
                    total_subscriptions = [subs for subs in total_subscriptions if subs.name.startswith(USER_VISUALIZATION_QUEUE)]

                    #Only cleanup data product and workflow when the last subscription has been deleted
                    if len(total_subscriptions) == 0:

                        workflow_id, _  = self.clients.resource_registry.find_subjects(subject_type=RT.Workflow, predicate=PRED.hasOutputProduct, object=workflow_data_product_id[0], id_only=True)
                        if len(workflow_id) == 0:
                            log.error("Cannot find Workflow for Data Product id %s", ( workflow_data_product_id[0] ))
                        else:
                            self.clients.workflow_management.terminate_data_process_workflow(workflow_id=workflow_id[0], delete_data_products=True, timeout=self.terminate_workflow_timeout)

            except NotFound, e:
                #It is okay if the workflow is no longer available as it may have been cleaned up by another worker
                pass

            except Exception, e:
                log.exception(e)



    #THese definition creation functions should really only be used for tests and should be moved to the test helper and created
    # as part of test setup. The definitions should be in the preload data

    def _create_google_dt_data_process_definition(self):

        dpd,_ = self.clients.resource_registry.find_resources(restype=RT.DataProcessDefinition, name='google_dt_transform', id_only=True)
        if len(dpd) > 0:
            return dpd[0]

        # Data Process Definition
        log.debug("Create data process definition GoogleDtTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='google_dt_transform',
            description='Convert data streams to Google DataTables',
            module='ion.processes.data.transforms.viz.google_dt',
            class_name='VizTransformGoogleDT')
        try:
            procdef_id = self.clients.data_process_management.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new VizTransformGoogleDT data process definition: %s" %ex)

        pdict_id = self.clients.dataset_management.read_parameter_dictionary_by_name('google_dt', id_only=True)

        # create a stream definition for the data from the
        stream_def_id = self.clients.pubsub_management.create_stream_definition(name='VizTransformGoogleDT', parameter_dictionary_id=pdict_id)
        self.clients.data_process_management.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id, binding='google_dt' )

        return procdef_id


    def _create_google_dt_workflow_def(self):
        # Check to see if the workflow defnition already exist
        workflow_def_ids,_ = self.clients.resource_registry.find_resources(restype=RT.WorkflowDefinition, name='Realtime_Google_DT', id_only=True)

        if len(workflow_def_ids) > 0:
            workflow_def_id = workflow_def_ids[0]
        else:
            # Build the workflow definition
            workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Realtime_Google_DT',description='Convert stream data to Google Datatable')

            #Add a transformation process definition
            google_dt_procdef_id = self._create_google_dt_data_process_definition()
            workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=google_dt_procdef_id)
            workflow_def_obj.workflow_steps.append(workflow_step_obj)

            #Create it in the resource registry
            workflow_def_id = self.clients.workflow_management.create_workflow_definition(workflow_def_obj)

        return workflow_def_id



    def get_visualization_data(self, data_product_id='', visualization_parameters=None, callback='', tqx=""):
        """Retrieves the data for the specified DP

        @param data_product_id    str
        @param visualization_parameters    str
        @param callback     str
        @retval jsonp_visualization_data str
        @throws NotFound    object with specified id, query does not exist
        """

        # An empty dict is returned in case there is no data in coverage
        empty_gdt = gviz_api.DataTable([('time', 'datetime', 'time')])

        # error check
        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")

        use_direct_access = False
        if visualization_parameters == {}:
            visualization_parameters = None

        reqId = 0
        # If a reqId was passed in tqx, extract it
        if tqx:
            tqx_param_list = tqx.split(";")
            for param in tqx_param_list:
                key, value = param.split(":")
                if key == 'reqId':
                    reqId = value


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
                #query['start_time'] = int(ntplib.system_to_ntp_time(float(visualization_parameters['start_time'])))
                query['start_time'] = int(visualization_parameters['start_time'])

            if 'end_time' in visualization_parameters:
                #query['end_time'] = int(ntplib.system_to_ntp_time(float(visualization_parameters['end_time'])))
                query['end_time'] = int((visualization_parameters['end_time']))

            # stride time
            if 'stride_time' in visualization_parameters:
                query['stride_time'] = int(visualization_parameters['stride_time'])
            else:
                query['stride_time'] == 1

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

        if use_direct_access:
            retrieved_granule = DataRetrieverService.retrieve_oob(ds_ids[0], query=query)
        else:
            #replay_granule = self.clients.data_retriever.retrieve(ds_ids[0],{'start_time':0,'end_time':2})
            retrieved_granule = self.clients.data_retriever.retrieve(ds_ids[0], query=query)

        # If thereis no data, return an empty dict
        if retrieved_granule is None:
            if callback == '':
                return empty_gdt.ToJSonResponse(req_id = reqId)
            else:
                return callback + "(\"" + empty_gdt.ToJSonResponse(req_id = reqId) + "\")"

        # Need the parameter dictionary for the precision entry
        #rdt = RecordDictionaryTool.load_from_granule(retrieved_granule)

        # send the granule through the transform to get the google datatable
        gdt_pdict_id = self.clients.dataset_management.read_parameter_dictionary_by_name('google_dt',id_only=True)
        gdt_stream_def = self.clients.pubsub_management.create_stream_definition('gdt', parameter_dictionary_id=gdt_pdict_id)

        gdt_data_granule = VizTransformGoogleDTAlgorithm.execute(retrieved_granule, params=gdt_stream_def, config=visualization_parameters)
        if gdt_data_granule == None:
            if callback == '':
                return empty_gdt.ToJSonResponse(req_id = reqId)
            else:
                return callback + "(\"" + empty_gdt.ToJSonResponse(req_id = reqId) + "\")"

        gdt_rdt = RecordDictionaryTool.load_from_granule(gdt_data_granule)
        gdt_components = get_safe(gdt_rdt, 'google_dt_components')
        gdt_component = gdt_components[0]
        temp_gdt_description = gdt_component["data_description"]
        temp_gdt_content = gdt_component["data_content"]

        # adjust the 'float' time to datetime in the content
        gdt_description = [('time', 'datetime', 'time')]
        gdt_content = []
        for idx in range(1,len(temp_gdt_description)):
            temp_arr = temp_gdt_description[idx]
            if temp_arr != None and temp_arr[0] != 'time':
                gdt_description.append((temp_arr[0], temp_arr[1], temp_arr[2]))

        for tempTuple in temp_gdt_content:
            # sometimes there are inexplicable empty tuples in the content. Drop them
            if tempTuple == [] or len(tempTuple) == 0:
                continue

            varTuple = []
            varTuple.append(datetime.fromtimestamp(tempTuple[0]))
            for idx in range(1,len(tempTuple)):
                varTuple.append(tempTuple[idx])

            gdt_content.append(varTuple)

        # now generate the Google datatable out of the description and content
        gdt = gviz_api.DataTable(gdt_description)
        gdt.LoadData(gdt_content)

        # return the json version of the table
        if callback == '':
            return gdt.ToJSonResponse(req_id = reqId)
        else:
            return callback + "(\"" + gdt.ToJSonResponse(req_id = reqId) + "\")"



    def get_visualization_image(self, data_product_id='', visualization_parameters=None, callback=''):

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

        #print " >>>>>>>>>>>> RETRIEVED GRANULE : " , retrieved_granule
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
        ret_dict['image_obj'] = base64.encodestring((get_safe(mpl_rdt, "image_obj"))[0])

        if callback == '':
            return ret_dict
        else:
            return callback + "(" + simplejson.dumps(ret_dict) + ")"



    def get_data_product_kml(self, visualization_parameters = None):

        kml_content = ""
        ui_server = "http://localhost:3000" # This server hosts the UI and is used for creating all embedded links within KML
        #observatory_icon_file = "/static/img/r2/li.observatories.png"
        starting_altitude = "20000"
        observatory_icon_file = "/static/img/Observatory-icon1.png"

        if visualization_parameters:
            if "ui_server" in visualization_parameters:
                ui_server = visualization_parameters["ui_server"]

        # First step. Discover all Data products in the system
        dps,_ = self.clients.resource_registry.find_resources(RT.DataProduct, None, None, False)

        # Start creating the kml in memory
        # Common KML tags
        kml_content += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        kml_content += "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n"
        kml_content += "<Document>\n"
        kml_content += "<name>DataProduct geo-reference information, ID: " + str(create_unique_identifier('google_flush_key')) + "</name>\n"
        # define line styles. Used for polygons
        kml_content += "<Style id=\"yellowLine\">\n<LineStyle>\n<color>ff61f2f2</color>\n<width>4</width>\n</LineStyle>\n</Style>"

        # Embed the icon images. Each as a separate style
        kml_content += "<Style id=\"observatory-icon\">\n<IconStyle>\n<Icon>\n<href>"
        kml_content += ui_server + observatory_icon_file
        kml_content += "</href>\n</Icon>\n</IconStyle>\n</Style>\n"

        dp_cluster = {}
        # Several Dataproducts are basically coming from the same geo-location. Start clustering them
        for dp in dps:
            _lat_center = _lon_center = 0.0
            bounds = dp.geospatial_bounds
            if bounds == None:
                continue

            # approximate placemark position in the middle of the geo spatial bounds
            _lon_center = bounds.geospatial_longitude_limit_west + (bounds.geospatial_longitude_limit_east - bounds.geospatial_longitude_limit_west) / 2.0
            _lat_center = bounds.geospatial_latitude_limit_south + (bounds.geospatial_latitude_limit_north - bounds.geospatial_latitude_limit_south) / 2.0

            # generate a key from lon-lat info. Truncate floats to one digit after decimal to keep things sane
            key = ("%.1f"%_lon_center) + "," + ("%.1f"%_lat_center)

            # All co-located data products are put in a list
            if not key in dp_cluster:
                dp_cluster[key] = []
            dp_cluster[key].append(dp)


        # Enter all DP in to KML placemarks
        for set_key in dp_cluster:
            # create only one placemark per set but populate information from all entries within the set
            _lat_center = _lon_center = 0.0
            bounds = dp_cluster[set_key][0].geospatial_bounds
            if bounds == None:
                continue

            # approximate placemark position in the middle of the geo spatial bounds
            _lon_center = bounds.geospatial_longitude_limit_west + (bounds.geospatial_longitude_limit_east - bounds.geospatial_longitude_limit_west) / 2.0
            _lat_center = bounds.geospatial_latitude_limit_south + (bounds.geospatial_latitude_limit_north - bounds.geospatial_latitude_limit_south) / 2.0

            # Start Placemark tag for point
            kml_content += "<Placemark>\n"

            # name of placemark
            kml_content += "<name>"
            kml_content += "Data Products at : " + str(_lat_center) + "," + str(_lon_center)
            kml_content += "</name>\n"

            # style /icon for placemark
            kml_content += "<styleUrl>#observatory-icon</styleUrl>\n"

            # Description
            kml_content += "<description>\n<![CDATA[\n"
            # insert HTML here as a table containing info about the data products
            html_description = "<table border='1' align='center'>\n"
            html_description += "<tr> <td><b>Data Product</b></td> <td> <b>ID</b> </b> <td> <b>Preview</b> </td> </tr>\n"
            for dp in dp_cluster[set_key]:
                html_description += "<tr>"
                html_description += "<td>" + dp.name + "</td>"
                html_description += "<td><a class=\"external\" href=\"" + ui_server + "/DataProduct/face/" + str(dp._id) + "/\">" + str(dp._id) + "</a> </td> "
                html_description += "<td> </td>"

                html_description += "</tr>"

            # Close the table
            html_description += "</table>"

            kml_content += html_description
            kml_content += "\n]]>\n</description>\n"

            # Point information
            #_lat_center = _lon_center = 0 ############# REMOVE THIS *************
            kml_content += "<Point>\n<coordinates>" + str(_lon_center) + "," + str(_lat_center) + "," + starting_altitude + "</coordinates>\n</Point>\n"

            # Close Placemark
            kml_content += "</Placemark>\n"

        # ------
        kml_content += "</Document>\n"
        kml_content += "</kml>\n"

        return kml_content



    def get_data_product_metadata(self, data_product_id="", callback=""):

        dp_meta_data = {}
        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")

        # get the dataset_id associated with the data_product. Need it to do the data retrieval
        ds_ids,_ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.Dataset, True)

        if ds_ids is None or not ds_ids:
            log.warn("Specified data_product does not have an associated dataset")
            return None

        # Start collecting the data to populate the output dictionary
        #time_bounds = self.clients.dataset_management.dataset_bounds(ds_ids[0])['time']
        #dp_meta_data['time_bounds'] = [float(ntplib.ntp_to_system_time(i)) for i in time_bounds]
        time_bounds = self.clients.dataset_management.dataset_temporal_bounds(ds_ids[0])
        dp_meta_data['time_bounds'] = time_bounds
        dp_meta_data['time_steps'] = self.clients.dataset_management.dataset_extents(ds_ids[0])['time'][0]

        dp_meta_data_json = simplejson.dumps(dp_meta_data)

        if callback:
            return callback + "(" + dp_meta_data_json + ")"
        else:
            return dp_meta_data_json

