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
import math
import gevent
import base64
import string
import random
from gevent.greenlet import Greenlet

from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.ans.ivisualization_service import BaseVisualizationService
from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDTAlgorithm
from ion.processes.data.transforms.viz.matplotlib_graphs import VizTransformMatplotlibGraphsAlgorithm
from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from pyon.net.endpoint import Subscriber
from interface.objects import Granule
from pyon.util.containers import get_safe

# Google viz library for google charts
import ion.services.ans.gviz_api as gviz_api


class VisualizationService(BaseVisualizationService):


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

        data_product_stream_id = None
        workflow_def_id = None

        # Check to see if the workflow defnition already exist
        workflow_def_ids,_ = self.clients.resource_registry.find_resources(restype=RT.WorkflowDefinition, name='Realtime_Google_DT', id_only=True)

        if len(workflow_def_ids) > 0:
            workflow_def_id = workflow_def_ids[0]
        else:
            workflow_def_id = self._create_google_dt_workflow_def()

        #Create and start the workflow
        workflow_id, workflow_product_id = self.clients.workflow_management.create_data_process_workflow(workflow_def_id, data_product_id, timeout=20)

        # detect the output data product of the workflow
        workflow_dp_ids,_ = self.clients.resource_registry.find_objects(workflow_id, PRED.hasDataProduct, RT.DataProduct, True)
        if len(workflow_dp_ids) != 1:
            raise ValueError("Workflow Data Product ids representing output DP must contain exactly one entry")

        # find associated stream id with the output
        workflow_output_stream_ids, _ = self.clients.resource_registry.find_objects(workflow_dp_ids[len(workflow_dp_ids) - 1], PRED.hasStream, None, True)
        data_product_stream_id = workflow_output_stream_ids

        # Create a queue to collect the stream granules - idempotency saves the day!
        query_token = create_unique_identifier('user_queue')

        xq = self.container.ex_manager.create_xn_queue(query_token)
        subscription_id = self.clients.pubsub_management.create_subscription(
            stream_ids=data_product_stream_id,
            exchange_name = query_token,
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
                    if (gdt_description == None):
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
                        varTuple.append(datetime.fromtimestamp(tempTuple[0]))
                        for idx in range(1,len(tempTuple)):
                            # some silly numpy format won't go away so need to cast numbers to floats
                            if(gdt_description[idx][1] == 'number'):
                                varTuple.append((float)(tempTuple[idx]))
                            else:
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
        user specific token which was provided from a previous request to the init_realtime_visualization operation.

        @param query_token    str
        @retval datatable    str
        @throws NotFound    Throws if specified query_token or its visualization product does not exist
        """

        ret_val = []
        if not query_token:
            raise BadRequest("The query_token parameter is missing")

        #try:

        #Taking advantage of idempotency
        xq = self.container.ex_manager.create_xn_queue(query_token)


        subscriber = Subscriber(from_name=xq)
        subscriber.initialize()

        msgs = subscriber.get_all_msgs(timeout=2)
        for x in range(len(msgs)):
            msgs[x].ack()

        # Different messages should get processed differently. Ret val will be decided by the viz product type
        ret_val = self._process_visualization_message(msgs)

        #except Exception, e:
        #    raise e

        #finally:
        #    subscriber.close()

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


        if len(subscription_ids[0]) > 1:
            log.warn("An inconsistent number of Subscription resources associated with the name: %s - using the first one in the list",query_token )

        subscription_id = subscription_ids[0][0]

        self.clients.pubsub_management.deactivate_subscription(subscription_id)

        self.clients.pubsub_management.delete_subscription(subscription_id)

        #Taking advantage of idempotency
        xq = self.container.ex_manager.create_xn_queue(query_token)

        self.container.ex_manager.delete_xn(xq)


    def _create_google_dt_data_process_definition(self):

        #First look to see if it exists and if not, then create it
        self.dataset_management =  DatasetManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.pubsubclient = PubsubManagementServiceClient(node=self.container.node)
        dpd,_ = self.rrclient.find_resources(restype=RT.DataProcessDefinition, name='google_dt_transform')
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
            procdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new VizTransformGoogleDT data process definition: %s" %ex)

        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('google_dt', id_only=True)

        # create a stream definition for the data from the
        stream_def_id = self.pubsubclient.create_stream_definition(name='VizTransformGoogleDT', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id, binding='google_dt' )

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



    def get_visualization_data(self, data_product_id='', visualization_parameters=None, callback=''):
        """Retrieves the data for the specified DP and sends a token back which can be checked in
            a non-blocking fashion till data is ready

        @param data_product_id    str
        @param visualization_parameters    str
        @param callback     str
        @retval jsonp_visualization_data str
        @throws NotFound    object with specified id, query does not exist
        """

        # error check
        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")

        gdt = None
        query = None

        # get the dataset_id associated with the data_product. Need it to do the data retrieval
        ds_ids,_ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.DataSet, True)

        if ds_ids is None or not ds_ids:
            raise NotFound("Could not find dataset associated with data product")

        # Ideally just need the latest granule to figure out the list of images
        #replay_granule = self.clients.data_retriever.retrieve(ds_ids[0],{'start_time':0,'end_time':2})
        retrieved_granule = self.clients.data_retriever.retrieve(ds_ids[0], query)
        if retrieved_granule is None:
            return None

        # send the granule through the transform to get the google datatable
        gdt_pdict_id = self.clients.dataset_management.read_parameter_dictionary_by_name('google_dt',id_only=True)
        gdt_stream_def = self.clients.pubsub_management.create_stream_definition('gdt', parameter_dictionary_id=gdt_pdict_id)

        gdt_data_granule = VizTransformGoogleDTAlgorithm.execute(retrieved_granule, params=gdt_stream_def)
        if gdt_data_granule == None:
            return None

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
                # some silly numpy format won't go away so need to cast numbers to floats
                if(gdt_description[idx][1] == 'number'):
                    if tempTuple[idx] == None:
                        varTuple.append(0.0)
                    else:
                        varTuple.append(float(tempTuple[idx]))
                else:
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



    def get_visualization_image(self, data_product_id='', visualization_parameters=None, callback=''):

        # Error check
        if not data_product_id:
            raise BadRequest("The data_product_id parameter is missing")
        if visualization_parameters == {}:
            visualization_parameters = None

        # Extract the retrieval related parameters. Definitely init all parameters first
        query = None
        if visualization_parameters :
            query = {'parameters':[], 'start_time':0,'end_time':0, 'stride_time':1}
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

        # get the dataset_id associated with the data_product. Need it to do the data retrieval
        ds_ids,_ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.DataSet, True)
        if ds_ids is None or not ds_ids:
            return None

        # Ideally just need the latest granule to figure out the list of images
        #replay_granule = self.clients.data_retriever.retrieve(ds_ids[0],{'start_time':0,'end_time':2})
        retrieved_granule = self.clients.data_retriever.retrieve(ds_ids[0], query=query)

        if retrieved_granule is None:
            return None

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
