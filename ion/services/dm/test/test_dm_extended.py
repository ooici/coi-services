#!/usr/bin/env python
'''
@author Luke Campbell <Use the force Luke>
@file ion/services/dm/test/test_dm_extended.py
@description Resting place for tests that validate the extended functionalities of DM
'''

from ion.services.dm.test.dm_test_case import DMTestCase, Streamer
from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDTAlgorithm
from ion.processes.data.replay.replay_process import RetrieveProcess
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.services.dm.utility.tmpsf_simulator import TMPSFSimulator
from ion.services.dm.utility.bad_simulator import BadSimulator
from ion.util.direct_coverage_utils import DirectCoverageAccess
from ion.services.dm.utility.hydrophone_simulator import HydrophoneSimulator
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.provenance import graph
from ion.processes.data.registration.registration_process import RegistrationProcess
from coverage_model import ParameterFunctionType, ParameterDictionary, PythonFunction, ParameterContext
from ion.processes.data.transforms.transform_worker import TransformWorker
from interface.objects import DataProcessDefinition, InstrumentDevice, ParameterFunction, ParameterFunctionType as PFT 
from nose.plugins.attrib import attr
from pyon.util.breakpoint import breakpoint
from pyon.core.exception import NotFound
from pyon.event.event import EventSubscriber
from pyon.util.file_sys import FileSystem
from pyon.public import IonObject, RT, CFG, PRED, OT
from pyon.util.containers import DotDict
from pydap.client import open_url
from shutil import rmtree
from datetime import datetime, timedelta
from pyon.net.endpoint import RPCClient
from pyon.util.log import log
import lxml.etree as etree
import simplejson as json
import pkg_resources
import tempfile
import os
import unittest
import numpy as np
import time
import gevent
from gevent.event import Event
import calendar

class TestDMExtended(DMTestCase):
    '''
    ion/services/dm/test/test_dm_extended.py:TestDMExtended
    '''

    @attr('UTIL')
    def test_pydap_handlers(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        stream_def_id = self.create_stream_definition('ctd', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('ctd', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)


        rdt = self.ph.get_rdt(stream_def_id)
        t = np.arange(3600)
        np.random.shuffle(t)
        rdt['time'] = t
        rdt['temp'] = np.arange(3600)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)
        self.ph.publish_rdt_to_data_product(data_product_id,rdt)
        dataset_monitor.wait()

        #from pydap.client import open_url
        pydap_host = CFG.get_safe('server.pydap.host','localhost')
        pydap_port = CFG.get_safe('server.pydap.port',8001)
        url = 'http://%s:%s/%s' %(pydap_host, pydap_port, dataset_id)
        ds = open_url(url)

        breakpoint(locals(), globals())
        #ds['temp']['temp'][:]

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
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA'
        config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary'
        config.path = 'master'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    def preload_full_beta(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA'
        config.path = 'master'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    def preload_alpha(self):
        config = DotDict()
        config.cfg = 'res/preload/r2_ioc/config/ooi_alpha.yml'
        config.path = 'master'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    
    def preload_tmpsf(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA,TMPSF'
        config.path = 'master'
        #config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)
    
    def preload_example1(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA,EXAMPLE1'
        config.path = 'master'
        #config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary,StreamDefinition,DataProduct'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)
    
    def preload_example2(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA,EXAMPLE2'
        config.path = 'master'
        #config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary,StreamDefinition,DataProduct'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    def preload_prest(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA,PREST'
        config.path = 'master'
        #config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary,StreamDefinition,DataProduct'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    def preload_ctdpf(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA,CTDPF'
        config.path = 'master'
        #config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary,StreamDefinition,DataProduct'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)
        self.container.spawn_process('import_dataset', 'ion.processes.data.import_dataset', 'ImportDataset', {'op':'load', 'instrument':'CTDPF'})

    def preload_lctest(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA,LC_TEST'
        config.path = 'master'
        config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    def preload_sptest(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA,SP_TEST'
        config.path = 'master'
        config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    def preload_ctdgv(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA,GLIDER,CTDGV,CTDGV01'
        config.path = 'master'
        #config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary,StreamDefinition,DataProduct'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)
        self.container.spawn_process('import_dataset', 'ion.processes.data.import_dataset', 'ImportDataset', {'op':'load', 'instrument':'CTDGV'})

    def preload_vel3d_cd(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA,VEL3D_C'
        config.path = 'master'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    def preload_mflm(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'BETA,SP_MFLM'
        config.path = 'master'
        #config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    def stop_ctdgv(self):
        self.container.spawn_process('import_dataset', 'ion.processes.data.import_dataset', 'ImportDataset', {'op':'stop', 'instrument':'CTDGV'})



    def preload_indexes(self):
        pass


    def launch_device_facepage(self, instrument_device_id):
        '''
        Opens the UI face page on localhost for a particular instrument device
        '''
        from subprocess import call
        call(['open', 'http://localhost:3000/InstrumentDevice/face/%s/' % instrument_device_id])


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
    
    def create_highcharts_workflow_def(self):
        # Check to see if the workflow defnition already exist
        workflow_def_ids,_ = self.resource_registry.find_resources(restype=RT.WorkflowDefinition, name='Realtime_HighCharts', id_only=True)

        if len(workflow_def_ids) > 0:
            workflow_def_id = workflow_def_ids[0]
        else:
            # Build the workflow definition
            workflow_def_obj = IonObject(RT.WorkflowDefinition, name='Realtime_HighCharts',description='Convert stream data to highcharts Datatable')

            #Add a transformation process definition
            highcharts_dt_procdef_id = self.create_highcharts_data_process_definition()
            workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id=highcharts_dt_procdef_id)
            workflow_def_obj.workflow_steps.append(workflow_step_obj)

            #Create it in the resource registry
            workflow_def_id = self.workflow_management.create_workflow_definition(workflow_def_obj)

        return workflow_def_id
   
    def create_highcharts_data_process_definition(self):

        #First look to see if it exists and if not, then create it
        dpd,_ = self.resource_registry.find_resources(restype=RT.DataProcessDefinition, name='HIGHCHARTS')
        if len(dpd) > 0:
            return dpd[0]

        # Data Process Definition
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='HIGHCHARTS',
            description='HighCharts charts',
            module='ion.processes.data.transforms.viz.highcharts',
            class_name='VizTransformHighCharts')
        try:
            procdef_id = self.data_process_management.create_data_process_definition(dpd_obj)
        except Exception as ex:
            self.fail("failed to create new VizTransformhighchartsDT data process definition: %s" %ex)

        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('highcharts', id_only=True)

        # create a stream definition for the data from the
        stream_def_id = self.pubsub_management.create_stream_definition(name='VizTransformhighchartsDT', parameter_dictionary_id=pdict_id)
        self.data_process_management.assign_stream_definition_to_data_process_definition(stream_def_id, procdef_id, binding='highcharts' )

        return procdef_id

    @attr('UTIL')
    def test_tmpsf_arrays(self):
        self.preload_tmpsf()
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('tmpsf_sample', id_only=True)
        stream_def_id = self.create_stream_definition('tmpsf', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('tmpsf', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)

        rdt = ParameterHelper.rdt_for_data_product(data_product_id)
        tomato = {'quality_flag': 'ok', 'preferred_timestamp':
                'port_timestamp', 'internal_timestamp': 3223662780.0,
                'stream_name': 'tmpsf_sample', 'values': [{'value_id':
                    'timestamp', 'value': 3223662780.0}, {'value_id':
                        'temperature', 'value': [21.4548, 21.0132, 20.9255,
                            21.1266, 21.1341, 21.5606, 21.2156, 21.4749,
                            21.3044, 21.132, 21.1798, 21.2352, 21.3488,
                            21.1214, 21.6426, 21.1479, 21.0069, 21.5426,
                            21.3204, 21.2402, 21.3968, 21.4371, 21.0411,
                            21.4361]}, {'value_id': 'battery_voltage', 'value':
                                11.5916}, {'value_id': 'serial_number',
                                    'value': '021964'}], 'port_timestamp':
                                1378230448.439269, 'driver_timestamp':
                                3587219248.444593, 'pkt_format_id':
                                'JSON_Data', 'pkt_version': 1}
        from ion.agents.populate_rdt import populate_rdt
        rdt = populate_rdt(rdt, [tomato])
        ParameterHelper.publish_rdt_to_data_product(data_product_id, rdt)
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)


        breakpoint(locals(), globals())


    @attr('UTIL')
    def test_hydrophone_simulator(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.crete_simple_array_pdict()
        stream_def_id = self.create_stream_definition('ctd parsed', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('ctd hydrophone', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)

        s = HydrophoneSimulator(data_product_id, interval=4)
        breakpoint(locals())

        s.stop()

    @attr('INT')
    def test_realtime_visualization(self):
        self.preload_beta()

        # Create the input data product
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_simulator', id_only=True)
        stream_def_id = self.create_stream_definition('ctd sim L2', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('ctd simulator', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)

        # Launch the realtime visualization process
        viz_token = self.visualization.initiate_realtime_visualization_data(data_product_id=data_product_id)
        dataset_monitor = DatasetMonitor(data_product_id=data_product_id)
        self.addCleanup(dataset_monitor.stop)
        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = [1, 2]
        rdt['temp'] = [10, 20]
        rdt['conductivity'] = [30, 40]
        rdt['pressure'] = [40, 50]
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())

        # Get the token from the JSON str
        viz_id = json.loads(viz_token)['rt_query_token']

        # Poll the queue until we get our data from realtime
        from pyon.util.poller import poll_wrapper
        @poll_wrapper(timeout=CFG.get_safe('endpoint.receive.timeout',10))
        def poller(inst, viz_id):
            r = inst.visualization.get_realtime_visualization_data(viz_id)
            if r == '[]':
                return False
            return r
        r = poller(self, viz_id)
        r = json.loads(r)

        # Decode the json string and assert that the data is correct
        result_set = { k['name'] : k for k in r }
        np.testing.assert_almost_equal(result_set['temp']['data'][0][1], 10.0)
        np.testing.assert_almost_equal(result_set['conductivity']['data'][0][1], 30.0)
        np.testing.assert_almost_equal(result_set['pressure']['data'][0][1], 40.0)

        # Make sure that we can terminate
        self.visualization.terminate_realtime_visualization_data(viz_id)

        viz, _ = self.resource_registry.find_resources(restype=RT.RealtimeVisualization, id_only=True)
        self.assertFalse(viz)

    @attr('UTIL')
    def test_dm_realtime_visualization(self):
        self.preload_beta()

        #Create the input data product
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_simulator', id_only=True)
        stream_def_id = self.create_stream_definition('ctd sim L2', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('ctd simulator', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)

        #viz_token = self.visualization.initiate_realtime_visualization_data(data_product_id=data_product_id)

        streamer = Streamer(data_product_id)
        self.addCleanup(streamer.stop)

        
        #self.preload_ui()
        #self.strap_erddap(data_product_id)
        self.launch_ui_facepage(data_product_id)
        breakpoint(locals(), globals())

    @attr('UTIL')
    def test_dm_metadata(self):
        self.preload_mflm()
        data_product_id = self.data_product_by_id('DPROD142')
        self.strap_erddap(data_product_id)
        breakpoint(locals(), globals())

    
    @attr('INT',group='dm')
    def test_array_visualization(self):
        data_product_id, stream_def_id = self.make_array_data_product()

        # Make a granule with an array type, give it a few values
        # Send it to google_dt transform, verify output

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(2208988800, 2208988810)
        rdt['temp_sample'] = np.arange(10*4).reshape(10,4)
        rdt['cond_sample'] = np.arange(10*4).reshape(10,4)

        granule = rdt.to_granule()
        dataset_monitor = DatasetMonitor(self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id))
        self.addCleanup(dataset_monitor.stop)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        dataset_monitor.wait()

        gdt_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('google_dt',id_only=True)
        gdt_stream_def = self.create_stream_definition('gdt', parameter_dictionary_id=gdt_pdict_id)

        gdt_data_granule = VizTransformGoogleDTAlgorithm.execute(granule, params=gdt_stream_def)

        rdt = RecordDictionaryTool.load_from_granule(gdt_data_granule)
        testval = {'data_content': [
            [0.0 , 0.0  , 1.0  , 2.0  , 3.0  , 0.0  , 2.0  , 4.0  , 6.0  , 0.0  , 1.0  , 2.0  , 3.0]   ,
            [1.0 , 4.0  , 5.0  , 6.0  , 7.0  , 8.0  , 10.0 , 12.0 , 14.0 , 4.0  , 5.0  , 6.0  , 7.0]   ,
            [2.0 , 8.0  , 9.0  , 10.0 , 11.0 , 16.0 , 18.0 , 20.0 , 22.0 , 8.0  , 9.0  , 10.0 , 11.0]  ,
            [3.0 , 12.0 , 13.0 , 14.0 , 15.0 , 24.0 , 26.0 , 28.0 , 30.0 , 12.0 , 13.0 , 14.0 , 15.0]  ,
            [4.0 , 16.0 , 17.0 , 18.0 , 19.0 , 32.0 , 34.0 , 36.0 , 38.0 , 16.0 , 17.0 , 18.0 , 19.0]  ,
            [5.0 , 20.0 , 21.0 , 22.0 , 23.0 , 40.0 , 42.0 , 44.0 , 46.0 , 20.0 , 21.0 , 22.0 , 23.0]  ,
            [6.0 , 24.0 , 25.0 , 26.0 , 27.0 , 48.0 , 50.0 , 52.0 , 54.0 , 24.0 , 25.0 , 26.0 , 27.0]  ,
            [7.0 , 28.0 , 29.0 , 30.0 , 31.0 , 56.0 , 58.0 , 60.0 , 62.0 , 28.0 , 29.0 , 30.0 , 31.0]  ,
            [8.0 , 32.0 , 33.0 , 34.0 , 35.0 , 64.0 , 66.0 , 68.0 , 70.0 , 32.0 , 33.0 , 34.0 , 35.0]  ,
            [9.0 , 36.0 , 37.0 , 38.0 , 39.0 , 72.0 , 74.0 , 76.0 , 78.0 , 36.0 , 37.0 , 38.0 , 39.0]] ,
                 'data_description': [('time', 'number', 'time'),
              ('temp_sample[0]', 'number', 'temp_sample[0]', {'precision': '5'}),
              ('temp_sample[1]', 'number', 'temp_sample[1]', {'precision': '5'}),
              ('temp_sample[2]', 'number', 'temp_sample[2]', {'precision': '5'}),
              ('temp_sample[3]', 'number', 'temp_sample[3]', {'precision': '5'}),
              ('temp_offset[0]', 'number', 'temp_offset[0]', {'precision': '5'}),
              ('temp_offset[1]', 'number', 'temp_offset[1]', {'precision': '5'}),
              ('temp_offset[2]', 'number', 'temp_offset[2]', {'precision': '5'}),
              ('temp_offset[3]', 'number', 'temp_offset[3]', {'precision': '5'}),
              ('cond_sample[0]', 'number', 'cond_sample[0]', {'precision': '5'}),
              ('cond_sample[1]', 'number', 'cond_sample[1]', {'precision': '5'}),
              ('cond_sample[2]', 'number', 'cond_sample[2]', {'precision': '5'}),
              ('cond_sample[3]', 'number', 'cond_sample[3]', {'precision': '5'})],
             'viz_product_type': 'google_dt'}
        self.assertEquals(rdt['google_dt_components'][0], testval)

    @attr('INT',group='dm')
    def test_array_flow_paths(self):
        data_product_id, stream_def_id = self.make_array_data_product()

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dm = DatasetMonitor(dataset_id)
        self.addCleanup(dm.stop)


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
        self.assertTrue(dm.wait())
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
        self.assertTrue(dm.wait())
        dm.event.clear()


        #--------------------------------------------------------------------------------
        # Retrieve and Verify
        #--------------------------------------------------------------------------------

        retrieved_granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(retrieved_granule)
        np.testing.assert_array_equal(rdt['time'], np.array([0,1,2,3]))
        np.testing.assert_array_equal(rdt['temp_sample'], np.array([[0,1,2,3,4],[0,1,2,3,4],[1,m,m,m,m],[5,5,5,5,5]]))
        
    @attr('UTIL')
    def test_creation_args(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        stream_def_id = self.create_stream_definition('ctd', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('ctd', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)

        breakpoint(locals())

        rdt = self.ph.get_rdt(stream_def_id)
        rdt['time'] = np.arange(20)
        rdt['temp'] = np.arange(20)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)
        self.ph.publish_rdt_to_data_product(data_product_id,rdt)
        dataset_monitor.wait()

        breakpoint(locals())

        granule = self.data_retriever.retrieve(dataset_id)

        breakpoint(locals())

    @attr("UTIL")
    def test_example_preload(self):
        print 'preloading...'
        self.preload_example1()

        data_product_ids, _ = self.container.resource_registry.find_resources_ext(alt_id='DPROD102', alt_id_ns='PRE')
        data_product_id = data_product_ids[0]
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)


        with DirectCoverageAccess() as dca:
            dca.upload_calibration_coefficients(dataset_id, 'test_data/sbe16coeffs.csv', 'test_data/sbe16coeffs.yml')

        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        rdt = ph.rdt_for_data_product(data_product_id)
        rdt['time'] = [time.time() + 2208988800]
        rdt['temperature'] = [248471]
        rdt['pressure'] = [528418]
        rdt['conductivity'] = [1673175]
        rdt['thermistor_temperature']=[24303]

        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)
        ph.publish_rdt_to_data_product(data_product_id, rdt)
        dataset_monitor.wait()
        g = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(g)

        breakpoint(locals())

    @attr("UTIL")
    def test_example2_preload(self):
        print 'preloading...'
        self.preload_example2()

        data_product_ids, _ = self.container.resource_registry.find_resources_ext(alt_id='DPROD104', alt_id_ns='PRE')
        data_product_id = data_product_ids[0]
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)


        with DirectCoverageAccess() as dca:
            dca.upload_calibration_coefficients(dataset_id, 'test_data/vel3d_coeff.csv', 'test_data/vel3d_coeff.yml')

        from ion_functions.data.test.test_vel_functions import TS, VE, VN, VU

        rdt = ParameterHelper.rdt_for_data_product(data_product_id)
        rdt['time'] = [time.time() + 2208988800]
        rdt['velocity_east'] = [VE[0]]
        rdt['velocity_north'] = [VN[0]]
        rdt['velocity_up'] = [VU[0]]

        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)
        ParameterHelper.publish_rdt_to_data_product(data_product_id, rdt)
        dataset_monitor.wait()
        g = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(g)

        breakpoint(locals())

    def extract_static_dataset(self, key):
        dsatest_dir = '/tmp/dsatest'
        static_files = {
                'ctdpf':'test_data/ctdpf_example.zip',
                'ctdgv':'test_data/glider_data_files.zip'
                }
        import os
        #import shutil
        from zipfile import ZipFile
        if not os.path.exists(dsatest_dir):
            os.makedirs(dsatest_dir)

        with ZipFile(static_files[key],'r') as zf:
            for f in zf.infolist():
                zf.extract(f, dsatest_dir)

    @attr("UTIL")
    def test_ctdpf(self):
        self.extract_static_dataset('ctdpf')
        self.preload_ctdpf()
        data_product_ids, _ = self.container.resource_registry.find_resources_ext(alt_id='DPROD100', alt_id_ns='PRE')
        data_product_id = data_product_ids[0]
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        breakpoint(locals(), globals())

    @attr("UTIL")
    def test_ctdgv(self):
        self.extract_static_dataset('ctdgv')
        self.preload_ctdgv()
        data_product_ids, _ = self.container.resource_registry.find_resources_ext(alt_id='DPROD118', alt_id_ns='PRE')
        data_product_id = data_product_ids[0]
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        breakpoint(locals(), globals())

    @attr("UTIL")
    def test_out_of_order(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        stream_def_id = self.create_stream_definition('ctd', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('ctd', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)

        s = BadSimulator(data_product_id)

        breakpoint(locals())

        s.stop()

    @attr("UTIL")
    def test_lctest_preload(self):
        self.preload_lctest()


        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('sparse_dict', id_only=True)
        stream_def_id = self.create_stream_definition('sparse_example', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('sparse_example', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)


        rdt = ParameterHelper.rdt_for_data_product(data_product_id)
        rdt['time'] = [time.time() + 2208988800]
        rdt['sparse_float'] = [3.14159265358979323]
        rdt['sparse_double'] = [2.7182818284590452353602874713526624977572470936999595]
        rdt['sparse_int'] = [131071] # 6th mersenne prime
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)
        ParameterHelper.publish_rdt_to_data_product(data_product_id, rdt)
        dataset_monitor.wait()

        for i in xrange(10):
            dataset_monitor.event.clear()
            rdt = ParameterHelper.rdt_for_data_product(data_product_id)
            rdt['time'] = [time.time() + 2208988800]
            ParameterHelper.publish_rdt_to_data_product(data_product_id, rdt)
            dataset_monitor.wait()


        g = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(g)
            
        breakpoint(locals())

    @attr("INT")
    def test_ccov_visualization(self):
        '''
        Tests Complex Coverage aggregation of array types and proper splitting of coverages
        tests pydap and the visualization
        '''

        data_product_id, stream_def_id = self.make_array_data_product()

        # Make a granule with an array type, give it a few values
        # Send it to google_dt transform, verify output

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(2208988800, 2208988801, .1)
        rdt['temp_sample'] = np.arange(10*4).reshape(10,4)
        rdt['cond_sample'] = np.arange(10*4).reshape(10,4)

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='abc1', connection_index='1')
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()


        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(2208988810, 2208988820)
        rdt['temp_sample'] = np.arange(10*4).reshape(10,4)
        rdt['cond_sample'] = np.arange(10*4).reshape(10,4)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='abc2', connection_index='1')
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()

        qstring = '{"stride_time": 1, "parameters": [], "query_type": "highcharts_data", "start_time": 0, "use_direct_access": 0, "end_time": 19}'
        graph = self.visualization.get_visualization_data(data_product_id, qstring)
        self.assertIn('temp_sample[3]', graph)

        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)

        np.testing.assert_array_equal(rdt['temp_sample'][0], np.arange(4))

        pydap_host = CFG.get_safe('server.pydap.host','localhost')
        pydap_port = CFG.get_safe('server.pydap.port',8001)
        url = 'http://%s:%s/%s' %(pydap_host, pydap_port, data_product_id)

        ds = open_url(url)

        temp_sample = list(ds['data']['temp_sample'])[0]
        self.assertEquals(temp_sample, '0.0,1.0,2.0,3.0')

    @attr('INT')
    def test_ingest_metadata(self):
        data_product_id = self.make_ctd_data_product()
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.arange(30)
        rdt['temp'] = np.arange(30)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()

        object_store = self.container.object_store
        metadata_doc = object_store.read_doc(dataset_id)
        self.assertIn('bounds', metadata_doc)
        bounds = metadata_doc['bounds']
        self.assertEquals(bounds['time'], [0, 29])
        self.assertEquals(bounds['temp'], [0, 29])

        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = [-15, -1, 20, 40]
        rdt['temp'] = [-1, 0, 0, 0]
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()
        
        metadata_doc = object_store.read_doc(dataset_id)
        self.assertIn('bounds', metadata_doc)
        bounds = metadata_doc['bounds']
        self.assertEquals(bounds['time'], [-15, 40])
        self.assertEquals(bounds['temp'], [-1, 29])

        bounds = self.dataset_management.dataset_bounds(dataset_id)
        self.assertEquals(bounds['time'], [-15, 40])
        self.assertEquals(bounds['temp'], [-1, 29])
        bounds = self.dataset_management.dataset_bounds(dataset_id, ['temp'])
        self.assertEquals(bounds['temp'], [-1, 29])
        assert 'time' not in bounds
        tmin, tmax = self.dataset_management.dataset_bounds_by_axis(dataset_id, 'temp')
        self.assertEquals([tmin,tmax], [-1, 29])
        tmin, tmax = self.dataset_management.dataset_temporal_bounds(dataset_id)
        self.assertEquals([tmin,tmax], [-15 - 2208988800, 40 - 2208988800])

        extents = self.dataset_management.dataset_extents(dataset_id)
        self.assertEquals(extents['time'], 34)
        self.assertEquals(extents['temp'], 34)

        extent = self.dataset_management.dataset_extents_by_axis(dataset_id, 'time')
        self.assertEquals(extent, 34)

    @unittest.skip("Complex Coverages aren't used for the time being")
    @attr('INT')
    def test_ccov_domain_slicing(self):
        '''
        Verifies that the complex coverage can handle slicing across the domain instead of the range
        '''
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        stream_def_id = self.create_stream_definition('ctd', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('ctd', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(20,40)
        rdt['temp'] = np.arange(20)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='1', connection_index='1')
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()
        
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(60,80)
        rdt['temp'] = np.arange(20)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='2', connection_index='1')
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()
        
        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(100,120)
        rdt['temp'] = np.arange(20)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='3', connection_index='1')
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()

        cov = DatasetManagementService._get_coverage(dataset_id)
        ccov = cov.reference_coverage
        #self.assertEquals(len(ccov._reference_covs), 3)

        # Completely within the first coverage
        testval = ccov.get_value_dictionary(param_list=['time', 'temp'], domain_slice=(0,5))
        np.testing.assert_array_equal(testval['time'], np.arange(20,25))

        # Completely within a different coverage
        testval = ccov.get_value_dictionary(param_list=['time', 'temp'], domain_slice=(20,25))
        np.testing.assert_array_equal(testval['time'], np.arange(60,65))

        # Intersecting two coverages
        testval = ccov.get_value_dictionary(param_list=['time', 'temp'], domain_slice=(15,25))
        np.testing.assert_array_equal(testval['time'], np.array([35, 36, 37, 38, 39, 60, 61, 62, 63, 64]))

        # Union of entire domain
        testval = ccov.get_value_dictionary(param_list=['time', 'temp'], domain_slice=(0,60))
        np.testing.assert_array_equal(testval['time'], np.concatenate([np.arange(20,40), np.arange(60,80), np.arange(100,120)]))

        # Exceeding domain
        testval = ccov.get_value_dictionary(param_list=['time', 'temp'], domain_slice=(0,120))
        np.testing.assert_array_equal(testval['time'], np.concatenate([np.arange(20,40), np.arange(60,80), np.arange(100,120)]))
        
    @attr("UTIL")
    def test_locking_contention(self):
        pass

    @attr("UTIL")
    def test_large_perf(self):
        self.preload_ui()
        data_product_id = self.make_ctd_data_product()
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)

        cov = DatasetManagementService._get_simplex_coverage(dataset_id, mode='w')
        size = 3600 * 24 * 7
        cov.insert_timesteps(size)
        value_array = np.arange(size)
        random_array = np.arange(size)
        np.random.shuffle(random_array)
        cov.set_parameter_values('time', random_array)
        cov.set_parameter_values('temp', value_array)
        cov.set_parameter_values('conductivity', value_array)
        cov.set_parameter_values('pressure', value_array)


        #self.data_retriever.retrieve(dataset_id)
        self.strap_erddap()
        breakpoint(locals(), globals())


    @attr("INT")
    def test_prest(self):
        '''
        Tests the prest configuration data product to make sure that the 
        presssure_sensor_range is compatible with the coverage model
        '''
        self.preload_prest()
        data_products, _ = self.container.resource_registry.find_resources_ext(alt_id='DPROD69', alt_id_ns='PRE')
        data_product_id = data_products[0]._id
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = [0]
        rdt['pressure_sensor_range'] = [(6000,6000)]
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())

        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['pressure_sensor_range'], np.array([[6000, 6000]]))
    
    @attr("UTIL")
    def test_overlapping(self):

        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        stream_def_id = self.create_stream_definition('ctd', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('ctd', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)

        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(20,40)
        rdt['temp'] = np.arange(20)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='1', connection_index='1')
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()

        self.preload_ui()
        self.strap_erddap(data_product_id)
        self.launch_ui_facepage(data_product_id)
        breakpoint(locals(), globals())

    @attr("UTIL")
    def test_sptest(self):
        self.preload_sptest()
        breakpoint(locals(), globals())
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctdbp_no_sample', id_only=True)
        stream_def_id = self.create_stream_definition('ctdbp_no_sample', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('CTDBP-NO Parsed', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)
        breakpoint(locals(), globals())

    @attr("INT")
    def test_empty_dataset(self):
        data_product_id = self.make_ctd_data_product()

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)

        bounds = self.dataset_management.dataset_temporal_bounds(dataset_id)
        self.assertEquals(bounds, {})

    @attr("UTIL")
    def test_overlapping_repeating(self):
        data_product_id, stream_def_id = self.make_array_data_product()
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        
        # Throw some data 
        rdt['time'] = np.arange(20, 40) 
        rdt['temp_sample'] = np.random.random(20 * 20).reshape(20,20)
        rdt['cond_sample'] = np.array(range(20) * 20).reshape(20,20)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='abc1', connection_index='1')

        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()

        # Throw some overlapping data and preceeding on the same coverage

        rdt['time'] = np.arange(5,25)
        rdt['temp_sample'] = np.random.random(20 * 20).reshape(20,20)
        rdt['cond_sample'] = np.array(range(20) * 20).reshape(20,20)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='abc1', connection_index='2')

        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()

        rdt['temp_sample'] = np.random.random(20 * 20).reshape(20,20)
        rdt['cond_sample'] = np.array(range(20) * 20).reshape(20,20)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='abc2', connection_index='1')

        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()

        self.strap_erddap(data_product_id)
        self.preload_ui()
        breakpoint(locals(), globals())

    @attr("UTIL")
    def test_illegal_char(self):
        pdict_id = self.ph.create_illegal_char_pdict()
        stream_def_id = self.create_stream_definition('illegal_char', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('ICE Cream', stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)

        rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
        rdt['time'] = np.arange(10)
        rdt['ice_cream'] = np.arange(10)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())

        self.strap_erddap()
        breakpoint(locals(), globals())


    @attr("UTIL")
    def test_vel3d_cd(self):
        self.preload_vel3d_cd()
        instrument_devices, _ = self.container.resource_registry.find_resources_ext(alt_id='ID21', alt_id_ns='PRE')
        instrument_device_id = instrument_devices[0]._id
        data_products, _ = self.container.resource_registry.find_resources_ext(alt_id='DPROD109', alt_id_ns='PRE')
        data_product_id = data_products[0]._id
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        self.launch_device_facepage(instrument_device_id)
        breakpoint(locals(), globals())

    @attr("INT")
    def test_calibration_injection(self):
        self.preload_vel3d_cd()
        data_products, _ = self.container.resource_registry.find_resources_ext(alt_id='DPROD109', alt_id_ns='PRE')
        data_product_id = data_products[0]._id
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        rdt = ParameterHelper.rdt_for_data_product(data_product_id)
        rdt['time'] = np.array([  3.598370100e+09,   3.598370101e+09,   3.598370102e+09,
                                  3.598370103e+09,   3.598370104e+09,   3.598370105e+09,
                                  3.598370106e+09,   3.598370107e+09,   3.598370108e+09,
                                  3.598370109e+09]) 
        rdt['amplitude_beam_1'] = np.array([48, 47, 47, 47, 47, 47, 47, 47, 47, 47], dtype=np.int16)
        rdt['amplitude_beam_2'] = np.array([49, 48, 49, 50, 49, 48, 49, 50, 50, 49], dtype=np.int16)
        rdt['amplitude_beam_3'] = np.array([50, 49, 49, 49, 49, 49, 49, 49, 49, 49], dtype=np.int16)

        rdt['correlation_beam_1'] = np.array([ 6,  7,  4,  7, 23, 14, 24, 18, 23, 15], dtype=np.int16)
        rdt['correlation_beam_2'] = np.array([11, 13, 18,  7,  7, 18,  5, 27, 18, 18], dtype=np.int16)
        rdt['correlation_beam_3'] = np.array([27, 48, 37, 50, 31, 45, 44, 42, 37, 42], dtype=np.int16)

        rdt['ensemble_counter'] = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype=np.int16)

        rdt['seawater_pressure'] = np.array([ 5009.,  4239.,  3468.,  3082.,  3468.,  3468.,  3853.,  4624., 3468.,  4624.], dtype=np.float32)
        rdt['turbulent_velocity_east'] = np.array([  2629.,   4334.,   1272.,    546.,  64299.,    765.,    960., 64392.,  65205.,  63270.], dtype=np.float32)
        rdt['turbulent_velocity_north'] = np.array([   548.,  65409.,    395.,  65216.,   2105.,  64558.,  64841., 460.,   1485.,    789.], dtype=np.float32)
        rdt['turbulent_velocity_vertical'] = np.array([  4.60000000e+02,   6.68000000e+02,   7.00000000e+01,
                                                         6.53850000e+04,   1.89000000e+02,   6.54920000e+04,
                                                         5.00000000e+00,   1.43000000e+02,   2.55000000e+02,
                                                         6.53480000e+04], dtype=np.float32)
        rdt['upward_turbulent_velocity'] = np.array([4.60000008e-01,   6.67999983e-01,   7.00000003e-02,
                                                     6.53850021e+01,   1.88999996e-01,   6.54919968e+01,
                                                     4.99999989e-03,   1.43000007e-01,   2.54999995e-01,
                                                     6.53479996e+01], dtype=np.float32)

        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        self.ph.publish_rdt_to_data_product(data_product_id, rdt)

        dataset_monitor.wait()
    
        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['eastward_turbulent_velocity'], 
                np.array([-9999999., -9999999., -9999999., -9999999., -9999999., 
                          -9999999., -9999999., -9999999., -9999999., -9999999.], dtype=np.float32))
        np.testing.assert_array_equal(rdt['northward_turbulent_velocity'], 
                np.array([-9999999., -9999999., -9999999., -9999999., -9999999., 
                          -9999999., -9999999., -9999999., -9999999., -9999999.], dtype=np.float32))
        self.container.spawn_process(
                'injector', 
                'ion.util.direct_coverage_utils', 
                'CoverageAgent', 
                {  'data_product_preload_id': 'DPROD109',
                   'data_path' : 'test_data/vel3d_coeff.csv',
                   'config_path':'test_data/vel3d_coeff.yml' 
                })
        # req-tag: L4-CI-SA-RQ-365
        # req-tag: L4-CI-SA-RQ-181 
        # Evaluates the L1a data product for eastward_turbulent_velocity
        # the same as a client downloading the data product would.
        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['eastward_turbulent_velocity'], 
                 np.array([  2349.11694336, -15214.62695312,   1098.15771484, -18775.88671875,
                            60796.74609375, -18371.9921875 , -18269.46289062,  61372.3359375 ,
                            61845.6328125 ,  60203.23046875], dtype=np.float32))

        # Evaluates the L1a data product for eastward_turbulent_velocity
        np.testing.assert_array_equal(rdt['northward_turbulent_velocity'], 
                 np.array([  1301.38183594,  63762.33984375,    753.69665527,  62457.11328125,
                            21036.83203125,  61893.37890625,  62221.40625   ,  19493.015625  ,
                            20712.68164062,  19475.28125   ], dtype=np.float32))

    @attr("UTIL")
    def test_alpha(self):
        self.preload_alpha()
        try:
            from growl import growl
            growl("Alpha", "Loaded")
        except ImportError:
            pass
        breakpoint(locals(), globals())
    
    @attr("UTIL")
    def test_beta(self):
        self.preload_full_beta()
        try:
            from growl import growl
            growl("Beta", "Loaded")
        except ImportError:
            pass
        breakpoint(locals(), globals())

    @attr("INT")
    def test_catalog_repair(self):
        data_product_id = self.make_ctd_data_product()
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.arange(30)
        rdt['temp'] = np.arange(30)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()

        datasets_xml_path = RegistrationProcess.get_datasets_xml_path(CFG)
        with open(datasets_xml_path, 'w'):
            pass # Corrupt the file


        self.container.spawn_process('reregister', 'ion.processes.bootstrap.registration_bootstrap', 'RegistrationBootstrap', {'op':'register_datasets'})

        with open(datasets_xml_path, 'r') as f:
            buf = f.read()
        self.assertIn(data_product_id, buf)

    @attr("INT")
    def test_ctdmo(self):
        self.preload_mflm()
        #2014-01-24 08:16:00,373 INFO Dummy-391 ion.agents.data.dataset_agent:289 Particle received: {"quality_flag": "ok", "preferred_timestamp": "internal_timestamp", "stream_name": "ctdmo_parsed", "pkt_format_id": "JSON_Data", "pkt_version": 1, "internal_timestamp": 3587292001.0, "values": [{"value_id": "inductive_id", "value": 55}, {"value_id": "temperature", "value": 205378}, {"value_id": "conductivity", "value": 410913}, {"value_id": "pressure", "value": 3939}, {"value_id": "ctd_time", "value": 431618401}], "driver_timestamp": 3599568956.723209, "new_sequence": false}
        #2014-01-24 08:16:00,408 INFO Dummy-391 ion.agents.data.dataset_agent:289 Particle received: {"quality_flag": "ok", "preferred_timestamp": "internal_timestamp", "stream_name": "ctdmo_parsed", "pkt_format_id": "JSON_Data", "pkt_version": 1, "internal_timestamp": 3663184963.0, "values": [{"value_id": "inductive_id", "value": 55}, {"value_id": "temperature", "value": 389972}, {"value_id": "conductivity", "value": 417588}, {"value_id": "pressure", "value": 13616}, {"value_id": "ctd_time", "value": 507511363}], "driver_timestamp": 3599568956.724784, "new_sequence": false}
        data_product_id = self.data_product_by_id('DPROD142')
        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = [1, 2]
        rdt['temperature'] = [205378, 289972]
        rdt['conductivity'] = [410913, 417588]
        rdt['pressure'] = [3939, 13616]
        rdt['cc_p_range'] = 1000.
        rdt['cc_lat'] = 40.
        rdt['cc_lon'] = -70.

        np.testing.assert_array_equal(rdt['seawater_pressure'], np.array([  20.71102333,  194.42785645], dtype=np.float32))
        np.testing.assert_array_equal(rdt['seawater_conductivity'], np.array([3.5, 3.5], dtype=np.float32))
        np.testing.assert_array_equal(rdt['seawater_temperature'], np.array([10., 18.], dtype=np.float32))
        np.testing.assert_array_equal(rdt['sci_water_pracsal'], np.array([31.84717941,  25.82336998], dtype=np.float32))
        np.testing.assert_array_equal(rdt['seawater_density'], np.array([1024.58862305,  1019.12799072], dtype=np.float32))

    @attr("UTIL")
    def test_egg_packaging(self):

        # req-tag: NEW SA - 2
        file_source = """
#!/usr/bin/env python
'''
@author Luke Campbell
@file vec/rotate.py
@description Vectorized 2d-rotation methods
'''

import numpy as np

def rotate(u,v, theta):
    '''
    Rotates the vectors u and v by theta radians 
    clockwise.
    '''
    c = np.cos(theta)
    s = np.sin(theta)
    M = np.array([[ c, s],
                  [-s, c]])
    uv = np.array([v, u])
    v_, u_ = np.dot(M, uv)
    return u_, v_

def rotate_u(u,v,theta):
    '''
    Returns the u-component of a rotation
    '''
    u_ = np.empty_like(u)
    for i in xrange(u.shape[0]):
        u_[i] = rotate(u[i], v[i], theta[i])[0]
    return u_

def rotate_v(u,v,theta):
    '''
    Returns the v-component of a rotation
    '''
    v_ = np.empty_like(u)
    for i in xrange(u.shape[0]):
        v_[i] = rotate(u[i], v[i], theta[i])[1]
    return v_
"""
        tempdir = tempfile.mkdtemp()
        package = os.path.join(tempdir, 'rotate')
        os.makedirs(package)
        with open(os.path.join(package, '__init__.py'), 'w'):
            pass # touch __init__.py to make it a package

        with open(os.path.join(package, 'rotate.py'),'w') as f:
            f.write(file_source)

        from ion.util.package import main
        main('rotate', '0.1', [package])

        egg = 'rotate-0.1-py2.7.egg'
        pkg_resources.working_set.add_entry(egg)

        from rotate_0_1.rotate import rotate_u, rotate_v

        u = np.arange(10, dtype=np.float32)
        v = np.arange(10, dtype=np.float32)
        theta = np.ones(10, dtype=np.float32) * np.pi / 2
        u_ = rotate_u(u, v, theta)
        v_ = rotate_v(u, v, theta)

        np.testing.assert_almost_equal(u_, -u, 4)
        np.testing.assert_almost_equal(v_, v, 4)

        rmtree(tempdir)
        os.remove(egg)


    @attr("INT")
    def test_provenance_graph(self):
        # Preload MFLM to get the CTDMO data product
        self.preload_mflm()
        data_product_id = self.data_product_by_id('DPROD142')

        # Get the parameter dictionary for this data product
        dataset_id = self.dataset_of_data_product(data_product_id)
        dataset = self.dataset_management.read_dataset(dataset_id)
        param_dict_dump = dataset.parameter_dictionary
        pdict = ParameterDictionary.load(param_dict_dump)

        density_dependencies = graph(pdict, 'seawater_density')
        what_it_should_be = {'cc_lat': {},
                             'cc_lon': {},
                             'sci_water_pracsal': {
                                 'seawater_conductivity': {'conductivity': {}},
                                  'seawater_pressure': {'cc_p_range': {}, 'pressure': {}},
                                  'seawater_temperature': {'temperature': {}}},
                             'seawater_pressure': {'cc_p_range': {}, 'pressure': {}},
                             'seawater_temperature': {'temperature': {}}}
        self.assertEquals(density_dependencies, what_it_should_be)


    @attr("INT")
    def test_data_product_assocs(self):
        # req-tag: L4-CI-SA-RQ-364 
        self.preload_mflm()
        # Grab the CTDMO Data Product
        data_product_id = self.data_product_by_id('DPROD142')

        sources, _ = self.resource_registry.find_objects(data_product_id, PRED.hasSource, id_only=False)
        # Assert that this data product has one and only one source
        self.assertEquals(len(sources), 1)
        for source in sources:
            # Assert it's an instrument device
            self.assertIsInstance(source, InstrumentDevice)
            # Assert that it's the 'SP MFLM A CTDMO-01'
            self.assertIn('PRE:ID36', source.alt_ids)

        # req-tag: NEW SA-3
        output_data_products, _ = self.resource_registry.find_subjects(
                    object=data_product_id, 
                    predicate=PRED.hasDataProductParent,
                    subject_type=RT.DataProduct)
        # Assert that we have children data products and they are associated
        self.assertTrue(output_data_products)

    @attr('INT')
    def test_data_product_spatiotemporal_search(self):
        data_product_id = self.make_ctd_data_product()
        dataset_monitor = DatasetMonitor(data_product_id=data_product_id)
        self.addCleanup(dataset_monitor.stop)

        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.array([ 3.602342268e+09,   3.602342269e+09,   3.602342270e+09,
                                 3.602342271e+09,   3.602342272e+09,   3.602342273e+09,
                                 3.602342274e+09,   3.602342275e+09,   3.602342276e+09,
                                 3.602342277e+09])

        rdt['lat'] = np.array([40.0] * 10)
        rdt['lon'] = np.array([-70.0] * 10)

        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())

        # The data product resource should now include the temporal range
        data_product = self.resource_registry.read(data_product_id)
        np.testing.assert_equal(data_product.nominal_datetime.start_datetime, 3.602342268e+09 - 2208988800) # Shifted for NTP epoch difference
        np.testing.assert_equal(data_product.nominal_datetime.end_datetime, 3.602342277e+09 - 2208988800) # Shifted for NTP epoch difference

        # We should also be able to search for it
        search_string = "SEARCH 'nominal_datetime' TIME FROM '2014-02-01' TO '2014-03-01' FROM 'resources_index' AND SEARCH 'type_' IS 'DataProduct' FROM 'resources_index'"
        dp_ids = self.discovery.parse(search_string)
        self.assertIn(data_product_id, dp_ids)

        search_query = {'and': [], 'query': {'field': 'geospatial_bounds', 'top_left': [-72.5208, 41.9595], 'bottom_right': [-67.6208, 38.333], 'index': 'data_products_index', 'cmpop':'overlaps'}, 'limit': 100, 'or': []}
        dp_ids = self.discovery.query(search_query)
        self.assertIn(data_product_id, dp_ids)

        # Now make changes
        dataset_monitor = DatasetMonitor(data_product_id=data_product_id)
        self.addCleanup(dataset_monitor.stop)

        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.arange(3.602342277e+09,3.602342277e+09+10)
        rdt['lat'] = np.array([45.0] * 10)
        rdt['lon'] = np.array([-70.0] * 10)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())

        # The data product resource should now include the temporal range
        data_product = self.resource_registry.read(data_product_id)
        np.testing.assert_equal(data_product.nominal_datetime.start_datetime, 3.602342268e+09 - 2208988800) # Shifted for NTP epoch difference
        np.testing.assert_equal(data_product.nominal_datetime.end_datetime, 3.602342277e+09+9 - 2208988800) # Shifted for NTP epoch difference

        search_query = {'and': [], 'query': {'field': 'geospatial_bounds', 'top_left': [-72.5208, 41.9595], 'bottom_right': [-67.6208, 38.333], 'index': 'data_products_index', 'cmpop':'overlaps'}, 'limit': 100, 'or': []}
        dp_ids = self.discovery.query(search_query)
        self.assertNotIn(data_product_id, dp_ids)

        search_query = {'and': [], 'query': {'field': 'geospatial_bounds', 'top_left': [-72.5208, 45.9595], 'bottom_right': [-67.6208, 38.333], 'index': 'data_products_index', 'cmpop':'overlaps'}, 'limit': 100, 'or': []}
        dp_ids = self.discovery.query(search_query)
        self.assertIn(data_product_id, dp_ids)

    @attr("INT")
    def test_ingestion_eval(self):
        '''
        This test verifies that ingestion does NOT try to evaluate the values coming in
        '''

        # Make a new function for failure
        owner = 'ion.util.functions'
        func = 'fail'
        arg_list = ['x']
        pf = ParameterFunction(name='fail', function_type=PFT.PYTHON, owner=owner, function=func, args=arg_list)
        expr_id = self.dataset_management.create_parameter_function(pf)
        self.addCleanup(self.dataset_management.delete_parameter_function, expr_id)
        expr = DatasetManagementService.get_coverage_function(pf)
        expr.param_map = {'x':'temp'}
        failure_ctx = ParameterContext('failure', param_type=ParameterFunctionType(expr))
        failure_ctx.uom = '1'
        failure_ctxt_id = self.dataset_management.create_parameter_context(name='failure', parameter_context=failure_ctx.dump(), parameter_function_id=expr_id)
        self.addCleanup(self.dataset_management.delete_parameter_context, failure_ctxt_id)

        # I add the new parameter to the ctd_parsed_param_dict parameter dictionary by creating an association for it
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        self.resource_registry.create_association(pdict_id, PRED.hasParameterContext, failure_ctxt_id)

        # I make a standard CTDBP data product using the new parameter dictionary
        data_product_id = self.make_ctd_data_product()

        # The goal with this part is to make an event subscriber that will listen to the events published by ion.util.functions:fail
        # if it's run then it will publish an event. If I receive the event then I know ingestion is still evaluating the functions
        # when it shouldn't.
        verified = Event()

        event_subscriber = EventSubscriber(event_type=OT.GranuleIngestionErrorEvent, callback=lambda *args, **kwargs : verified.set(), auto_delete=True)
        event_subscriber.start()
        self.addCleanup(event_subscriber.stop)
        
        # We also need to synchronize on when the data has made it through ingestion
        dataset_monitor = DatasetMonitor(data_product_id=data_product_id)
        self.addCleanup(dataset_monitor.stop)
        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = [0]
        rdt['temp'] = [1]
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())

        # We'll give it about ten seconds, after that it *probably* didn't get run. It would be nice to be certain
        # but, I don't know of any pattern that ensures this.
        self.assertFalse(verified.wait(10))

    @attr('UTIL')
    def test_cov_access(self):
        ''' What happens when we access a coverage with no ingestion? '''
        # Create a data product
        data_product_id = self.create_data_product('uningested', param_dict_name='ctd_parsed_param_dict')
        # initialize the dataset but don't launch ingestion
        self.data_product_management.create_dataset_for_data_product(data_product_id)
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        # Get raw access to the coverage
        with DirectCoverageAccess() as dca:
            cov = dca.get_editable_coverage(dataset_id)
            cov.insert_timesteps(10)
            cov.set_parameter_values('time', np.arange(10))
            cov.set_parameter_values('temp', np.arange(10))

        # Verify that what we did is in there
        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_allclose(rdt['time'], np.arange(10))
        np.testing.assert_allclose(rdt['temp'], np.arange(10))

    @attr("INT")
    def test_data_product_catalog(self):
        with self.assertRaises(NotFound):
            self.data_product_management.read_catalog_entry('fakeid1')
        data_product_id = self.make_ctd_data_product()
        dp = self.resource_registry.read(data_product_id)
        dp.name = 'Pioneer CTDBP Imaginary TEMPWAT L1'
        dp.comment = 'An imaginary dataset'
        dp.ooi_short_name = 'TEMPWAT'
        dp.ooi_product_name = 'TEMPWAT'
        dp.regime = 'Surface Water'
        dp.qc_glblrng = 'applicable'
        dp.flow_diagram_dcn = '1342-00010'
        dp.dps_dcn = '1341-00010'
        dp.synonyms = ['sst', 'sea-surface-temperature', 'sea_surface_temperature']
        dp.acknowledgement = "To someone's darling wife?"
        dp.iso_topic_category = ['isocat1', 'isocat2']
        dp.ioos_category = 'temperature'
        dp.iso_spatial_representation_type = 'timeSeries'
        dp.processing_level_code = "L1"
        dp.license_uri = "http://lmgtfy.com/?q=Open+Source"
        dp.exclusive_rights_status = 'THERE CAN BE ONLY ONE!'
        dp.reference_urls = ['https://confluence.oceanobservatories.org/display/instruments/TEMPWAT', 'https://confluence.oceanobservatories.org/display/instruments/CTDBP']
        dp.provenance_description = 'Nope'
        dp.citation_description = 'Consider this a warning'
        dp.lineage_description = 'I am Connor MacLeod of the Clan MacLeod. I was born in 1518 in the village of Glenfinnan on the shores of Loch Shiel. And I am immortal.'
        self.data_product_management.update_data_product(dp)

        entry = self.data_product_management.read_catalog_entry(data_product_id)
        ele = etree.fromstring(entry)
        d = { child.attrib['name'] : child.text for child in ele.find('addAttributes') }

        from ion.processes.data.registration.registration_process import RegistrationProcess
        for required in RegistrationProcess.catalog_metadata:
            if required in ['iso_topic_category', 'synonyms', 'reference_urls', 'name']:
                continue
            if getattr(dp, required):
                self.assertEquals(d[required], getattr(dp, required))

        self.make_ctd_data_product() # Make another one so we have two catalog entries

        self.data_product_management.delete_catalog_entry(data_product_id)

        with self.assertRaises(NotFound):
            self.data_product_management.read_catalog_entry(data_product_id)

