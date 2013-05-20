#!/usr/bin/env python
'''
@author Luke Campbell <Use the force Luke>
@file ion/services/dm/test/test_dm_extended.py
@description Resting place for tests that validate the extended functionalities of DM
'''

from ion.services.dm.test.dm_test_case import DMTestCase, Streamer
from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDTAlgorithm
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from nose.plugins.attrib import attr
from pyon.util.breakpoint import breakpoint
from pyon.public import IonObject, RT, PRED
from pyon.util.containers import DotDict
import numpy as np

@attr('INT',group='dm')
class TestDMExtended(DMTestCase):
    '''
    ion/services/dm/test/test_dm_extended.py:TestDMExtended
    '''
    def setUp(self):
        DMTestCase.setUp(self)
        self.ph = ParameterHelper(self.dataset_management, self.addCleanup)

    def make_array_data_product(self):
        pdict_id = self.ph.crete_simple_array_pdict()
        stream_def_id = self.create_stream_definition('test_array_flow_paths', parameter_dictionary_id=pdict_id)

        data_product_id = self.create_data_product('test_array_flow_paths', stream_def_id)
        self.activate_data_product(data_product_id)
        return data_product_id, stream_def_id

    def preload_beta(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "https://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA'
        config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)
    
    def create_google_dt_workflow_def(self):
        # Check to see if the workflow defnition already exist
        workflow_def_ids,_ = self.resource_registry.find_resources(restype=RT.WorkflowDefinition, name='Realtime_Google_DT', id_only=True)

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
            workflow_def_id = self.workflow_management.create_workflow_definition(workflow_def_obj)

        return workflow_def_id
   
    def create_google_dt_data_process_definition(self):

        #First look to see if it exists and if not, then create it
        dpd,_ = self.resource_registry.find_resources(restype=RT.DataProcessDefinition, name='google_dt_transform')
        if len(dpd) > 0:
            return dpd[0]

        # Data Process Definition
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='google_dt_transform',
            description='Convert data streams to Google DataTables',
            module='ion.processes.data.transforms.viz.google_dt',
            class_name='VizTransformGoogleDT')
        try:
            procdef_id = self.data_process_management.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new VizTransformGoogleDT data process definition: %s" %ex)

        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('google_dt', id_only=True)

        # create a stream definition for the data from the
        stream_def_id = self.pubsub_management.create_stream_definition(name='VizTransformGoogleDT', parameter_dictionary_id=pdict_id)
        self.data_process_management.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id, binding='google_dt' )

        return procdef_id

    def test_dm_realtime_visualization(self):
        self.preload_beta()

        # Create the google_dt workflow definition since there is no preload for the test
        workflow_def_id = self.create_google_dt_workflow_def()

        #Create the input data product
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_simulator', id_only=True)
        stream_def_id = self.create_stream_definition('ctd sim L2', parameter_dictionary_id=pdict_id, available_fields=['time','salinity','density'])
        data_product_id = self.create_data_product('ctd simulator', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)

        #viz_token = self.visualization.initiate_realtime_visualization_data(data_product_id=data_product_id)

        streamer = Streamer(data_product_id)
        self.addCleanup(streamer.stop)

        breakpoint(locals())
#        ctd_stream_id, ctd_parsed_data_product_id = self.create_ctd_input_stream_and_data_product()
#        ctd_sim_pid = self.start_sinusoidal_input_stream_process(ctd_stream_id)
#
#        vis_params ={}
#        vis_token_resp = self.vis_client.initiate_realtime_visualization_data(data_product_id=ctd_parsed_data_product_id, visualization_parameters=simplejson.dumps(vis_params))
#        print ">>>>>>>>>>>>>>>>>>> vis_token_resp : ", vis_token_resp
#
#        import ast
#        vis_token = ast.literal_eval(vis_token_resp)["rt_query_token"]
#
#        result = gevent.event.AsyncResult()
#
#        def get_vis_messages(get_data_count=7):  #SHould be an odd number for round robbin processing by service workers
#
#
#            get_cnt = 0
#            while get_cnt < get_data_count:
#
#                vis_data = self.vis_client.get_realtime_visualization_data(vis_token)
#                if (vis_data):
#                    self.validate_google_dt_transform_results(vis_data)
#
#                get_cnt += 1
#                gevent.sleep(5) # simulates the polling from UI
#
#            result.set(get_cnt)
#
#        gevent.spawn(get_vis_messages)
#
#        result.get(timeout=90)
#
#        #Trying to continue to receive messages in the queue
#        gevent.sleep(2.0)  # Send some messages - don't care how many
#
#
#        # Cleanup
#        self.vis_client.terminate_realtime_visualization_data(vis_token)
#
#
#        #Turning off after everything - since it is more representative of an always on stream of data!
#        self.process_dispatcher.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

    
    def test_array_visualization(self):
        data_product_id, stream_def_id = self.make_array_data_product()

        # Make a granule with an array type, give it a few values
        # Send it to google_dt transform, verify output

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(2208988800, 2208988810)
        rdt['temp_sample'] = np.arange(10*4).reshape(10,4)
        granule = rdt.to_granule()

        gdt_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('google_dt',id_only=True)
        gdt_stream_def = self.create_stream_definition('gdt', parameter_dictionary_id=gdt_pdict_id)

        gdt_data_granule = VizTransformGoogleDTAlgorithm.execute(granule, params=gdt_stream_def)

        rdt = RecordDictionaryTool.load_from_granule(gdt_data_granule)
        testval = {'data_content': [[0.0, 0.0, 1.0, 2.0, 3.0],
              [1.0, 4.0, 5.0, 6.0, 7.0],
              [2.0, 8.0, 9.0, 10.0, 11.0],
              [3.0, 12.0, 13.0, 14.0, 15.0],
              [4.0, 16.0, 17.0, 18.0, 19.0],
              [5.0, 20.0, 21.0, 22.0, 23.0],
              [6.0, 24.0, 25.0, 26.0, 27.0],
              [7.0, 28.0, 29.0, 30.0, 31.0],
              [8.0, 32.0, 33.0, 34.0, 35.0],
              [9.0, 36.0, 37.0, 38.0, 39.0]],
             'data_description': [('time', 'number', 'time'),
              ('temp_sample[0]', 'number', 'temp_sample[0]', {'precision': '5'}),
              ('temp_sample[1]', 'number', 'temp_sample[1]', {'precision': '5'}),
              ('temp_sample[2]', 'number', 'temp_sample[2]', {'precision': '5'}),
              ('temp_sample[3]', 'number', 'temp_sample[3]', {'precision': '5'})],
             'viz_product_type': 'google_dt'}
        self.assertEquals(rdt['google_dt_components'][0], testval)



    def test_array_flow_paths(self):
        data_product_id, stream_def_id = self.make_array_data_product()

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dm = DatasetMonitor(dataset_id)


        # I need to make sure that we can fill the RDT with its values
        # Test for one timestep
        # Test for multiple timesteps
        # Publishes 
        # Ingests correctly
        # Retrieves correctly

        #--------------------------------------------------------------------------------
        # Ensure that the RDT can be filled with ArrayType values
        #--------------------------------------------------------------------------------
        
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = [0]
        rdt['temp_sample'] = [[0,1,2,3,4]]
        np.testing.assert_array_equal(rdt['temp_sample'], np.array([[0,1,2,3,4]]))

        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dm.event.wait(10))
        dm.event.clear()

        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['temp_sample'], np.array([[0,1,2,3,4]]))

        #--------------------------------------------------------------------------------
        # Ensure that it deals with multiple values
        #--------------------------------------------------------------------------------

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = [1,2,3]
        rdt['temp_sample'] = [[0,1,2,3,4],[1],[5,5,5,5,5]]

        m = rdt.fill_value('temp_sample') or np.finfo(np.float32).max
        np.testing.assert_equal(m,np.finfo(np.float32).max)
        np.testing.assert_array_equal(rdt['temp_sample'], [[0,1,2,3,4],[1,m,m,m,m],[5,5,5,5,5]])
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dm.event.wait(10))
        dm.event.clear()


        #--------------------------------------------------------------------------------
        # Retrieve and Verify
        #--------------------------------------------------------------------------------

        retrieved_granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(retrieved_granule)
        np.testing.assert_array_equal(rdt['time'], np.array([0,1,2,3]))
        np.testing.assert_array_equal(rdt['temp_sample'], np.array([[0,1,2,3,4],[0,1,2,3,4],[1,m,m,m,m],[5,5,5,5,5]]))
        
