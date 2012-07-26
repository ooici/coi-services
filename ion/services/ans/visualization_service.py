#!/usr/bin/env python



__author__ = 'Raj Singh, Stephen Henrie'
__license__ = 'Apache 2.0'

"""
Note:
[1]
"""

# Pyon imports
# Note pyon imports need to be first for monkey patching to occur
from pyon.public import IonObject, RT, log, PRED, StreamSubscriberRegistrar, StreamPublisherRegistrar, Container
from pyon.util.containers import create_unique_identifier, get_safe
from interface.objects import StreamQuery
from pyon.core.exception import Inconsistent, BadRequest
from datetime import datetime

import simplejson
import math
import gevent
import base64
import string
import random
from gevent.greenlet import Greenlet


from interface.services.ans.ivisualization_service import BaseVisualizationService
from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDT
from ion.processes.data.transforms.viz.matplotlib_graphs import VizTransformMatplotlibGraphs
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.net.endpoint import Subscriber
from interface.objects import Granule
from pyon.util.containers import get_safe

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
        self.dataprocessclient = self.clients.data_process_management

        return

    def on_stop(self):

        return


    def initiate_realtime_visualization(self, data_product_id='', in_product_type='', query='', callback=""):
        """Initial request required to start a realtime chart for the specified data product. Returns a user specific token associated
        with the request that will be required for subsequent requests when polling data.
        
        Note : The in_product_type is a temporary fix for specifying the type of data product to the the service

        @param data_product_id    str
        @param in_product_type str
        @param query        str
        @param callback     str
        @retval query_token    str
        @throws NotFound    Throws if specified data product id or its visualization product does not exist
        """

        # Perform a look up to check and see if the DP is indeed a realtime GDT stream
        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")
        data_product = self.clients.resource_registry.read(data_product_id)
        
        if not data_product:
            raise NotFound("Data product %s does not exist" % data_product_id)

        data_product_stream_id = None
        workflow_def_id = None

        # If not in_product_type was specified, assume the data product passed represents a pure data stream
        # and needs to be converted to some form of visualization .. Google DataTable by default unless specified
        # in the future by additional parameters. Create appropriate workflow to do the conversion
        if in_product_type == '':

            # Check to see if the workflow defnition already exist
            workflow_def_ids,_ = self.rrclient.find_resources(restype=RT.WorkflowDefinition, name='Realtime_Google_DT', id_only=True)

            if len(workflow_def_ids) > 0:
                workflow_def_id = workflow_def_ids[0]
            else:
                # Build the workflow definition
                workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Realtime_Google_DT',description='Convert stream data to Google Datatable')

                #Add a transformation process definition
                google_dt_procdef_id = self.create_google_dt_data_process_definition()
                workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=google_dt_procdef_id)
                workflow_def_obj.workflow_steps.append(workflow_step_obj)

                #Create it in the resource registry
                workflow_def_id = self.workflowclient.create_workflow_definition(workflow_def_obj)


            #Create and start the workflow
            workflow_id, workflow_product_id = self.workflowclient.create_data_process_workflow(workflow_def_id, data_product_id, timeout=20)

            # detect the output data product of the workflow
            workflow_dp_ids,_ = self.rrclient.find_objects(workflow_id, PRED.hasDataProduct, RT.DataProduct, True)
            if len(workflow_dp_ids) != 1:
                raise ValueError("Workflow Data Product ids representing output DP must contain exactly one entry")

            # find associated stream id with the output
            workflow_output_stream_ids, _ = self.rrclient.find_objects(workflow_dp_ids[len(workflow_dp_ids) - 1], PRED.hasStream, None, True)
            data_product_stream_id = workflow_output_stream_ids


        # TODO check if is a real time GDT stream automatically
        if in_product_type == 'google_dt':

            # Retrieve the id of the OUTPUT stream from the out Data Product
            stream_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStream, None, True)
            if not stream_ids:
                raise Inconsistent("Could not find Stream Id for Data Product %s" % data_product_id)

            data_product_stream_id = stream_ids


        # Create a queue to collect the stream granules - idempotency saves the day!
        query_token = create_unique_identifier('user_queue')

        xq = self.container.ex_manager.create_xn_queue(query_token)

        subscription_id = self.pubsubclient.create_subscription(
            query=StreamQuery(data_product_stream_id),
            exchange_name = query_token,
            exchange_point = 'science_data',
            name = query_token
        )

        # after the queue has been created it is safe to activate the subscription
        self.clients.pubsub_management.activate_subscription(subscription_id)

        if callback == "":
            return query_token
        else:
            return callback + "(\"" + query_token + "\")"




    def _process_visualization_message(self, messages):

        gdt_description = None
        gdt_content = []
        viz_product_type = ''

        for message in messages:

            message_data = message.body

            if isinstance(message_data,Granule):

                tx = TaxyTool.load_from_granule(message_data)
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
                    if (gdt_description == None):
                        temp_gdt_description = gdt_component['data_description']
                        gdt_description = [('time', 'datetime', 'time')]

                        for idx in range(1,len(temp_gdt_description)):
                            # for some weird reason need to force convert to tuples
                            temp_arr = temp_gdt_description[idx]
                            if temp_arr != None:
                                gdt_description.append((temp_arr[0], temp_arr[1], temp_arr[2]))

                    # append all content to one big array
                    temp_gdt_content = gdt_component['data_content']
                    for tempTuple in temp_gdt_content:
                        # sometimes there are inexplicable empty tuples in the content. Drop them
                        if tempTuple == [] or len(tempTuple) == 0:
                            continue

                        varTuple = []
                        varTuple.append(datetime.fromtimestamp(tempTuple[0]))
                        for idx in range(1,len(tempTuple)):
                            varTuple.append(tempTuple[idx])
                        gdt_content.append(varTuple)


                #TODO - what to do if this is not a valid visualization message?


        # Now that all the messages have been parsed, any last processing should be done here
        if viz_product_type == "google_dt":

            # Using the description and content, build the google data table
            gdt = gviz_api.DataTable(gdt_description)
            gdt.LoadData(gdt_content)

            return gdt.ToJSonResponse()

        return None


    def get_realtime_visualization_data(self, query_token=''):
        """This operation returns a block of visualization data for displaying data product in real time. This operation requires a
        user specific token which was provided from a previsou request to the init_realtime_visualization operation.

        @param query_token    str
        @retval datatable    str
        @throws NotFound    Throws if specified query_token or its visualization product does not exist
        """

        if not query_token:
            raise BadRequest("The query_token parameter is missing")

        try:

            #Taking advantage of idempotency
            xq = self.container.ex_manager.create_xn_queue(query_token)

            subscriber = Subscriber(from_name=xq)
            subscriber.initialize()

            msg_count,_ = subscriber._chan.get_stats()
            log.info('Messages in user queue 1: ' + str(msg_count))

            ret_val = []
            msgs = subscriber.get_n_msgs(msg_count, timeout=2)
            for x in range(len(msgs)):
                msgs[x].ack()

            # Different messages should get processed differently. Ret val will be decided by the viz product type
            ret_val = self._process_visualization_message(msgs)

            msg_count,_ = subscriber._chan.get_stats()
            log.info('Messages in user queue 2: ' + str(msg_count))

        except Exception, e:
            raise e

        finally:
            subscriber.close()

        #TODO - replace as need be to return valid GDT data
        #return {'viz_data': ret_val}

        return ret_val


    def terminate_realtime_visualization_data(self, query_token=''):
        """This operation terminates and cleans up resources associated with realtime visualization data. This operation requires a
        user specific token which was provided from a previsou request to the init_realtime_visualization operation.

        @param query_token    str
        @throws NotFound    Throws if specified query_token or its visualization product does not exist
        """

        if not query_token:
            raise BadRequest("The query_token parameter is missing")


        subscription_ids = self.clients.resource_registry.find_resources(restype=RT.Subscription, name=query_token, id_only=True)

        if not subscription_ids:
            raise BadRequest("A Subscription object for the query_token parameter %s is not found" % query_token)
            return

        if len(subscription_ids[0]) > 1:
            log.warn("An inconsistent number of Subscription resources associated with the name: %s - using the first one in the list",query_token )

        subscription_id = subscription_ids[0][0]

        self.clients.pubsub_management.deactivate_subscription(subscription_id)

        self.clients.pubsub_management.delete_subscription(subscription_id)

        #Taking advantage of idempotency
        xq = self.container.ex_manager.create_xn_queue(query_token)

        self.container.ex_manager.delete_xn(xq)



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
        stream_def_id = self.pubsubclient.create_stream_definition(container=VizTransformGoogleDT.outgoing_stream_def, name='VizTransformGoogleDT')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id )

        return procdef_id


    def get_visualization_image(self, data_product_id=''):

        images = None

        return images



    def get_visualization_data(self, data_product_id='', out_product_type='', query='', callback=''):
        """Retrieves the data for the specified DP and sends a token back which can be checked in
            a non-blocking fashion till data is ready

        @param data_product_id    str
        @param query    str
        @retval query_token  str
        @throws NotFound    object with specified id, query does not exist
        """

        # error check
        if data_product_id == '' or out_product_type == '':
            return None

        query_token = None
        gdt = None

        # get the dataset_id associated with the data_product. Need it to do the data retrieval
        ds_ids,_ = self.rrclient.find_objects(data_product_id, PRED.hasDataset, RT.DataSet, True)

        if ds_ids == None or len(ds_ids) == 0:
            return None

        # Ideally just need the latest granule to figure out the list of images
        #replay_granule = self.data_retriever.retrieve(ds_ids[0],{'start_time':0,'end_time':2})
        retrieved_granule = self.data_retriever.retrieve(ds_ids[0])
        if retrieved_granule == None:
            return None

        if out_product_type == 'google_dt':
            # send the granule through the transform to get the google datatable
            gdt_transform = VizTransformGoogleDT()
            gdt_data_granule = gdt_transform.execute(retrieved_granule)

            gdt_rdt = RecordDictionaryTool.load_from_granule(gdt_data_granule)
            gdt_components = get_safe(gdt_rdt, "google_dt_components")
            temp_gdt_description = gdt_components[0]["data_description"]
            temp_gdt_content = gdt_components[0]["data_content"]


            # adjust the 'float' time to datetime in the content
            gdt_description = [('time', 'datetime', 'time')]
            gdt_content = []
            for idx in range(1,len(temp_gdt_description)):
                gdt_description.append(temp_gdt_description[idx])

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
                return gdt.ToJSonResponse()
            else:
                return callback + "(\"" + gdt.ToJSonResponse() + "\")"

        if out_product_type == 'matplotlib_graphs':

            mpl_transform = VizTransformMatplotlibGraphs()
            mpl_data_granule = mpl_transform.execute(retrieved_granule)

            mpl_rdt = RecordDictionaryTool.load_from_granule(mpl_data_granule)
            temp_mpl_graph_list = get_safe(mpl_rdt, "matplotlib_graphs")
            mpl_graphs = temp_mpl_graph_list[0]

            # restructure the mpl graphs in to a simpler dict that will be passed through
            ret_dict = {}
            for graph in mpl_graphs:
                ret_dict[graph['image_name']] = base64.encodestring(graph['image_obj'])

            if callback == '':
                return ret_dict
            else:
                return callback + "(" + simplejson.dumps(ret_dict) + ")"


