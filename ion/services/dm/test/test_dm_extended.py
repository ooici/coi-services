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
from coverage_model import ParameterFunctionType, ParameterDictionary, PythonFunction, ParameterContext as CovParameterContext
from coverage_model import NumpyParameterData
from ion.processes.data.transforms.transform_worker import TransformWorker
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
from pyon.ion.event import EventPublisher
from interface.objects import DataProcessDefinition, InstrumentDevice, ParameterFunction, ParameterFunctionType as PFT, ParameterContext
from interface.objects import InstrumentSite, InstrumentModel, PortTypeEnum, Deployment, CabledInstrumentDeploymentContext, DataProductTypeEnum, DataProduct
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
from ion.services.coi.service_gateway_service import service_gateway_app
from webtest import TestApp

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
        pdict_id = self.ph.create_simple_array_pdict()
        stream_def_id = self.create_stream_definition('test_array_flow_paths', parameter_dictionary_id=pdict_id)

        data_product_id = self.create_data_product('test_array_flow_paths', stream_def_id)
        self.activate_data_product(data_product_id)
        return data_product_id, stream_def_id

    def to_data_dict(self,data_dict):
        if 'time' in data_dict:
            time_array = data_dict['time']
        else:
            elements = data_dict.values()[0]
            time_array = np.arange(len(elements))

        for k,v in data_dict.iteritems():
            data_dict[k] = NumpyParameterData(k, v, time_array)
        return data_dict



    def stop_ctdgv(self):
        self.container.spawn_process('import_dataset', 'ion.processes.data.import_dataset', 'ImportDataset', {'op':'stop', 'instrument':'CTDGV'})



    def launch_device_facepage(self, instrument_device_id):
        '''
        Opens the UI face page on localhost for a particular instrument device
        '''
        from subprocess import call
        call(['open', 'http://localhost:3000/InstrumentDevice/face/%s/' % instrument_device_id])
    
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

    @attr('SMOKE')
    def test_realtime_visualization(self):
        data_product_id = self.make_ctd_data_product()

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
        rdt['temp_sample'] = [[0,1,2,3,4],[1,3,3,3,3],[5,5,5,5,5]]

        m = rdt.fill_value('temp_sample') or np.finfo(np.float32).max
        np.testing.assert_equal(m,np.finfo(np.float32).max)
        np.testing.assert_array_equal(rdt['temp_sample'], [[0,1,2,3,4],[1,3,3,3,3],[5,5,5,5,5]])
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dm.wait())
        dm.event.clear()


        #--------------------------------------------------------------------------------
        # Retrieve and Verify
        #--------------------------------------------------------------------------------

        retrieved_granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(retrieved_granule)
        np.testing.assert_array_equal(rdt['time'], np.array([0,1,2,3]))
        np.testing.assert_array_equal(rdt['temp_sample'], np.array([[0,1,2,3,4],[0,1,2,3,4],[1,3,3,3,3],[5,5,5,5,5]]))
        
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

        self.strap_erddap(data_product_id)
        breakpoint(locals(), globals())

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

    @unittest.skip("Array types return objects temporarily")
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
        rdt['time'] = [15, 1, 20, 40]
        rdt['temp'] = [-1, 0, 0, 0]
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()
        
        metadata_doc = object_store.read_doc(dataset_id)
        self.assertIn('bounds', metadata_doc)
        bounds = metadata_doc['bounds']
        self.assertEquals(bounds['time'], [0, 40])
        self.assertEquals(bounds['temp'], [-1, 29])

        bounds = self.dataset_management.dataset_bounds(dataset_id)
        self.assertEquals(bounds['time'], [0, 40])
        self.assertEquals(bounds['temp'], [-1, 29])
        bounds = self.dataset_management.dataset_bounds(dataset_id, ['temp'])
        self.assertEquals(bounds['temp'], [-1, 29])
        assert 'time' not in bounds
        tmin, tmax = self.dataset_management.dataset_bounds_by_axis(dataset_id, 'temp')
        self.assertEquals([tmin,tmax], [-1, 29])
        tmin, tmax = self.dataset_management.dataset_temporal_bounds(dataset_id)
        self.assertEquals([tmin,tmax], [0 - 2208988800, 40 - 2208988800])

        extents = self.dataset_management.dataset_extents(dataset_id)
        self.assertEquals(extents['time'], 34)
        self.assertEquals(extents['temp'], 34)

        extent = self.dataset_management.dataset_extents_by_axis(dataset_id, 'time')
        self.assertEquals(extent, 34)

    @attr("UTIL")
    def test_large_perf(self):
        from coverage_model import NumpyParameterData
        self.preload_ui()
        data_product_id = self.make_ctd_data_product()
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)

        cov = DatasetManagementService._get_simplex_coverage(dataset_id, mode='w')
        size = 3600 * 24 * 7
        cov.insert_timesteps(size)
        value_array = np.arange(size)
        random_array = np.arange(size)
        np.random.shuffle(random_array)

        cov.set_parameter_values({'time': random_array, 
                                  'temp': NumpyParameterData('temp', value_array)})
        cov.set_parameter_values({'time': random_array, 
                                  'conductivity': NumpyParameterData('conductivity', value_array)})
        cov.set_parameter_values({'time': random_array, 
                                  'pressure': NumpyParameterData('pressure', value_array)})


        #self.data_retriever.retrieve(dataset_id)
        self.strap_erddap(data_product_id)
        breakpoint(locals(), globals())


    @attr("PRELOAD")
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

        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.arange(20,40)
        rdt['temp'] = np.arange(20)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='1', connection_index='1')
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.reset()


        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.arange(30,50)
        rdt['temp'] = np.arange(20)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt, connection_id='1', connection_index='1')
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.reset()

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

    @attr("UTIL")
    def test_dataset_endpoints(self):
        data_product_id = self.make_ctd_data_product()

        sparse_value = ParameterContext(name='sparseness', 
                                      parameter_type='sparse',
                                      value_encoding='float32',
                                      display_name='Sparseness',
                                      description='Example of sparseness',
                                      fill_value=-9999,
                                      units='1')
        sparse_value_id = self.dataset_management.create_parameter(sparse_value)
        self.data_product_management.add_parameter_to_data_product(sparse_value_id, data_product_id)

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)

        bounds = self.dataset_management.dataset_temporal_bounds(dataset_id)
        self.assertEquals(bounds, {})
        rdt = self.ph.rdt_for_data_product(data_product_id)
        dataset_monitor = DatasetMonitor(data_product_id=data_product_id)
        self.addCleanup(dataset_monitor.stop)

        rdt['time'] = np.arange(20,40)
        rdt['temp'] = np.arange(20)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait(500))
        dataset_monitor.reset()
        
        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['time'], np.arange(20,40))
        np.testing.assert_array_equal(rdt['temp'], np.arange(20))

        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.arange(40,50)
        rdt['sparseness'] = [20] * 10
        rdt['preferred_timestamp'] = ['driver_timestamp'] * 10
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait(500))
        dataset_monitor.reset()

        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_allclose(rdt['sparseness'], np.array([-9999] * 20 + [20] * 10))



        array_value = ParameterContext(name='arrayness',
                                       parameter_type='array',
                                       value_encoding='float32',
                                       units='K',
                                       display_name='Array-ness',
                                       description='Parameterization of arrays')
        array_value_id = self.dataset_management.create_parameter(array_value)
        self.data_product_management.add_parameter_to_data_product(array_value_id, data_product_id)


        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.arange(50,60)
        rdt['arrayness'] = np.arange(40).reshape(10,4)

        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait(10))

        self.preload_ui()
        self.launch_ui_facepage(data_product_id)
        self.strap_erddap(data_product_id, False)
        breakpoint(locals(), globals())

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

    @unittest.skip("Depends on M086")
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


    @attr("PRELOAD")
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
        param_dict_id = self.resource_registry.find_objects(dataset_id, PRED.hasParameterDictionary, id_only=True)[0][0]
        pdict = DatasetManagementService.get_parameter_dictionary(param_dict_id)

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


    @attr("PRELOAD")
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
        failure_ctx = CovParameterContext('failure', param_type=ParameterFunctionType(expr))
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
            data_dict = self.to_data_dict({
                'time' : np.arange(10),
                'temp' : np.arange(10)})
            cov.set_parameter_values(data_dict)

        # Verify that what we did is in there
        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        breakpoint(locals(), globals())
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


        self.verify_bad_xml()

    def verify_bad_xml(self):
        with self.assertRaises(NotFound):
            self.data_product_management.read_catalog_entry('fakeid1')
        data_product_id = self.make_ctd_data_product()
        dp = self.resource_registry.read(data_product_id)
        dp.name = 'Pioneer CTDBP <Imaginary> & TEMPWAT L1'
        self.data_product_management.update_data_product(dp)

    def make_tempwat(self, data_product_id):
        from interface.objects import DataProduct
        stream_def_id = self.resource_registry.find_objects(data_product_id,PRED.hasStreamDefinition,id_only=True)[0][0]
        pdict_id = self.resource_registry.find_objects(stream_def_id,PRED.hasParameterDictionary,id_only=True)[0][0]
        stream_def_id = self.pubsub_management.create_stream_definition(name='tempwat l0', parameter_dictionary_id=pdict_id, available_fields=['temp','time'])
        dp = DataProduct(name='TEMPWAT L0', qc_trndtst='applicable', category=DataProductTypeEnum.DERIVED)
        data_product_id = self.data_product_management.create_data_product(dp, stream_definition_id=stream_def_id, parent_data_product_id=data_product_id)
        return data_product_id

    def add_tempwat_qc(self, data_product_id):
        #--------------------------------------------------------------------------------
        # Global Range
        #--------------------------------------------------------------------------------
        tempwat_qc = ParameterContext(name='tempwat_glblrng_qc', 
                                      parameter_type='quantity',
                                      value_encoding='int8',
                                      units='1',
                                      ooi_short_name='TEMPWAT_GLBLRNG_QC',
                                      fill_value=-88)
        tempwat_qc_id = self.dataset_management.create_parameter(tempwat_qc)
        self.data_product_management.add_parameter_to_data_product(tempwat_qc_id, data_product_id)
        #--------------------------------------------------------------------------------
        # Trend Test
        #--------------------------------------------------------------------------------
        tempwat_qc = ParameterContext(name='tempwat_trndtst_qc', 
                                      parameter_type='quantity',
                                      value_encoding='int8',
                                      units='1',
                                      ooi_short_name='TEMPWAT_TRNDTST_QC',
                                      fill_value=-88)
        tempwat_qc_id = self.dataset_management.create_parameter(tempwat_qc)
        self.data_product_management.add_parameter_to_data_product(tempwat_qc_id, data_product_id)
        #--------------------------------------------------------------------------------
        # Spike Test
        #--------------------------------------------------------------------------------
        tempwat_qc = ParameterContext(name='tempwat_spketst_qc', 
                                      parameter_type='quantity',
                                      value_encoding='int8',
                                      units='1',
                                      ooi_short_name='TEMPWAT_SPKETST_QC',
                                      fill_value=-88)
        tempwat_qc_id = self.dataset_management.create_parameter(tempwat_qc)
        self.data_product_management.add_parameter_to_data_product(tempwat_qc_id, data_product_id)
        #--------------------------------------------------------------------------------
        # Gradient Test
        #--------------------------------------------------------------------------------
        tempwat_qc = ParameterContext(name='tempwat_gradtst_qc', 
                                      parameter_type='quantity',
                                      value_encoding='int8',
                                      units='1',
                                      ooi_short_name='TEMPWAT_GRADTST_QC',
                                      fill_value=-88)
        tempwat_qc_id = self.dataset_management.create_parameter(tempwat_qc)
        self.data_product_management.add_parameter_to_data_product(tempwat_qc_id, data_product_id)
        #--------------------------------------------------------------------------------
        # Local Range Test
        #--------------------------------------------------------------------------------
        tempwat_qc = ParameterContext(name='tempwat_loclrng_qc', 
                                      parameter_type='quantity',
                                      value_encoding='int8',
                                      units='1',
                                      ooi_short_name='TEMPWAT_LOCLRNG_QC',
                                      fill_value=-88)
        tempwat_qc_id = self.dataset_management.create_parameter(tempwat_qc)
        self.data_product_management.add_parameter_to_data_product(tempwat_qc_id, data_product_id)
        #--------------------------------------------------------------------------------
        # Stuck Value
        #--------------------------------------------------------------------------------
        preswat_qc = ParameterContext(name='preswat_stuckvl_qc', 
                                      parameter_type='quantity',
                                      value_encoding='int8',
                                      units='1',
                                      ooi_short_name='PRESWAT_STUCKVL_QC',
                                      fill_value=-88)
        preswat_qc_id = self.dataset_management.create_parameter(preswat_qc)
        self.data_product_management.add_parameter_to_data_product(preswat_qc_id, data_product_id)

    @attr("UTIL")
    def test_multi_deployment_qc(self):
        data_product_id = self.make_ctd_data_product()
        instrument_device = InstrumentDevice(name='Test CTDBP')
        instrument_device_id, _ = self.resource_registry.create(instrument_device)

        site_1 = InstrumentSite(name='Site 1')
        site_1_id, _ = self.resource_registry.create(site_1)

        site_2 = InstrumentSite(name='Site 2')
        site_2_id, _ = self.resource_registry.create(site_2)


    def local_range_upload(self):
        content = {'TEMPWAT': {'local_range': [{'author': 'Bodhi',
            'table': {'datlim1': [0, 1, 2, 3, 4],
             'datlim2': [30, 31, 32, 33, 34],
             'month': [0, 2, 6, 8, 10]},
            'ts_created': 1399319183.604609,
            'units': 'deg_C'}]},
         'PRACSAL': {'local_range': [{'author': 'Johnny Utah',
            'table': {'datlim1': [0.0, 10.0, 10.0, 20.0],
             'datlim2': [30.0, 40.0, 40.0, 50.0],
             'lat': [46.436926, 46.436926, 46.436926, 46.436926],
             'lon': [-124.832179, -124.832179, -125.35965425, -125.35965425],
             'pressure': [0.0, 37.5, 0.0, 37.5]},
            'ts_created': 1399319183.604609,
            'units': '1'}]},
         '_type': 'QC'}

        self.container.object_store.create_doc(content, "CP01CNSM-MFD37-03-CTDBPD000")


        
    def make_instrument_data_product(self):
        data_product_id = self.make_ctd_data_product()

        data_product = self.resource_registry.read(data_product_id)
        data_product.qc_applications['TEMPWAT'] = ['qc_glblrng', 'qc_trndtst', 'qc_spketst', 'qc_gradtst', 'qc_loclrng', 'qc_stuckvl']
        data_product.qc_applications['PRESWAT'] = ['qc_stuckvl']
        self.data_product_management.update_data_product(data_product)
        site = InstrumentSite(name='example site', reference_designator='CP01CNSM-MFD37-03-CTDBPD000')
        site_id, _ = self.resource_registry.create(site)
        device = InstrumentDevice(name='a deployable device')
        device.ooi_property_number = 'ooi-inst-1'
        device_id = self.instrument_management.create_instrument_device(device)
        model = InstrumentModel(name='SBE37')
        model_id = self.instrument_management.create_instrument_model(model)
        self.instrument_management.assign_instrument_model_to_instrument_device(model_id, device_id)
        # for some reason this isn't a service method

        self.resource_registry.create_association(site_id, 'hasModel', model_id)
        #self.instrument_management.assign_instrument_model_to_instrument_site(model_id, site_id)

        pp_obj = IonObject(OT.PlatformPort, reference_designator='CP01CNSM-MFD37-03-CTDBPD000', port_type= PortTypeEnum.PAYLOAD, ip_address='1' )
        port_assignments = {device_id : pp_obj}

        deployment = Deployment(name='Operation Malabar', type="Cabled", context=CabledInstrumentDeploymentContext(), port_assignments=port_assignments)
        deployment_id = self.observatory_management.create_deployment(deployment, site_id, device_id)
        self.observatory_management.activate_deployment(deployment_id)
        
        self.data_acquisition_management.register_instrument(device_id)
        self.data_acquisition_management.assign_data_product(device_id, data_product_id)
        return data_product_id

    @attr("UTIL")
    def test_qc_stuff(self):
        testapp = TestApp(service_gateway_app)
        data_product_id = self.make_instrument_data_product()


        # Post the lookup tables
        self.local_range_upload()
        upload_files = [('file', 'test_data/sample_qc_upload.csv')]
        result = testapp.post('/ion-service/upload/qc', upload_files=upload_files, status=200)
        

        # Now lets make a derived data product for tempwat

        self.container.spawn_process('qc', 'ion.processes.data.transforms.qc_post_processing', 'QCProcessor', {})

        #self.container.object_store.create_doc(doc, 'CP01CNSM-MFD37-03-CTDBPD000')
        streamer = Streamer(data_product_id)
        self.addCleanup(streamer.stop)


        event_publisher = EventPublisher(OT.ResetQCEvent)
        self.addCleanup(event_publisher.close)
        event = Event()

        def annoying_thread(event_publisher, event):
            log.error("Started annoying thread")
            while not event.wait(1):
                log.error("annoying thread")
                event_publisher.publish_event(origin='refdes')
                

        g = gevent.spawn(annoying_thread, event_publisher, event)


        breakpoint(locals(), globals())
        event.set()
        g.join()

    @attr("INT")
    def test_complex_stubs(self):
        params = {
            "time" : {
                "parameter_type" : "quantity",
                "value_encoding" : "float64",
                "display_name" : "Time",
                "description" : "Timestamp",
                "units" : "seconds since 1900-01-01"
            },
            "data" : {
                "parameter_type" : "quantity",
                "value_encoding" : "float32",
                "display_name" : "Data",
                "description" : "The Red Pill",
                "units" : "1"
            }
        }
        # Make the device data product
        device_data_product = DataProduct('The Gibson') # Category defaults to device
        device_data_product_id = self.data_product_from_params(device_data_product, params)
        # Creates the dataset and the ingestion worker
        self.data_product_management.activate_data_product_persistence(device_data_product_id)
        device_dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(device_data_product_id)

        # Make the site data product
        data_product = DataProduct('Garbage File', category=DataProductTypeEnum.SITE)
        data_product_id = self.data_product_from_params(data_product, params)
        self.data_product_management.create_dataset_for_data_product(data_product_id)
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)


        # Make some fake data
        rdt = self.ph.rdt_for_data_product(device_data_product_id)
        rdt['time'] = np.arange(60)
        rdt['data'] = np.arange(60)
        monitor = DatasetMonitor(data_product_id=device_data_product_id)
        self.ph.publish_rdt_to_data_product(device_data_product_id, rdt)
        self.assertTrue(monitor.wait())
        self.dataset_management.add_dataset_window_to_complex(device_dataset_id, (20, 40), dataset_id)

        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_allclose(rdt['time'], np.arange(20,41))



    def data_product_from_params(self, data_product, param_struct):

        param_dict = {}
        for name,param in param_struct.iteritems():
            ctx = ParameterContext(name=name, **param)
            p_id = self.dataset_management.create_parameter(ctx)
            param_dict[name] = p_id

        pdict_id = self.dataset_management.create_parameter_dictionary(data_product.name, param_dict.values(), 'time')
        stream_def_id = self.pubsub_management.create_stream_definition(data_product.name, parameter_dictionary_id=pdict_id)
        data_product_id = self.data_product_management.create_data_product(data_product, stream_definition_id=stream_def_id)
        
        return data_product_id




    @attr("UTIL")
    def test_cal_stuff(self):
        testapp = TestApp(service_gateway_app)
        data_product_id = self.make_instrument_data_product()
        parameter = ParameterContext(name='cc_a0',
                                     display_name='Calibration a0',
                                     description='Calibration for a0',
                                     value_encoding='float32',
                                     parameter_type='sparse')
        parameter_id = self.dataset_management.create_parameter(parameter)
        self.data_product_management.add_parameter_to_data_product(parameter_id, data_product_id)


        verified = Event()
        event_subscriber = EventSubscriber(event_type=OT.DatasetCalibrationEvent, callback=lambda *args, **kwargs : verified.set(), auto_delete=True)
        event_subscriber.start()
        self.addCleanup(event_subscriber.stop)


        upload_files = [('file', 'test_data/sample_calibrations.csv')]
        result = testapp.post('/ion-service/upload/calibration', upload_files=upload_files, status=200)
        python_dict = result.json
        self.assertIn('fuc_id', python_dict['data']['GatewayResponse'])

        self.assertTrue(verified.wait(10))


        dataset_monitor = DatasetMonitor(data_product_id=data_product_id)
        rdt = self.ph.rdt_for_data_product(data_product_id)
        feb1 = calendar.timegm(datetime(2014,2,1).timetuple()) + 2208988800
        rdt['time'] = np.arange(feb1, feb1+20)
        rdt['temp'] = np.arange(20)

        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())


        dataset_id = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)[0][0]
        granule = self.data_retriever.retrieve(dataset_id)
        rdt_out = RecordDictionaryTool.load_from_granule(granule)

        np.testing.assert_allclose(rdt_out['time'], rdt['time'])
        np.testing.assert_allclose(rdt_out['temp'], rdt['temp'])
        np.testing.assert_allclose(rdt_out['cc_a0'], np.array([1.13e2] * 20))

    def make_calibration_params(self, data_product_id):

        parameter_functions = DotDict({
            'identity' : {
                'function_type' : PFT.PYTHON,
                'owner' : 'ion_functions.data.interpolation',
                'function' : 'identity',
                'args':['x']
            },
            'interpolate' : {
                'function_type' : PFT.PYTHON,
                'owner' : 'ion_functions.data.interpolation',
                'function' : 'secondary_interpolation',
                'args':['x', 'range0', 'range1', 'starts', 'ends']
            },
            'ne_offset' : {
                'function_type' : PFT.NUMEXPR,
                'function' : 'x + 2',
                'args' : ['x']
            },
            'polyval_calibration' : {
                'function_type' : PFT.PYTHON,
                'owner' : 'ion_functions.data.interpolation',
                'function' : 'polyval_calibration',
                'args':['coefficients', 'x']
            }
        })

        for pf_name, pf in parameter_functions.iteritems():
            parameter_function = ParameterFunction(name=pf_name, **pf)
            parameter_function_id = self.dataset_management.create_parameter_function(parameter_function)
            parameter_functions[pf_name]['_id'] = parameter_function_id



        parameters = DotDict({
            'tempwat_sum' : {
                'display_name' : 'Fake Data',
                'description' : 'Not a real parameter',
                'parameter_type' : 'function',
                'parameter_function_id' : parameter_functions.ne_offset._id,
                'units':'1',
                'value_encoding':'float32',
                'parameter_function_map' : {
                    'x' : 'temp'
                }
            }
        })

        for param_name, param_def in parameters.iteritems():
            param = ParameterContext(name=param_name, **param_def)
            param_id = self.dataset_management.create_parameter(param)
            self.data_product_management.add_parameter_to_data_product(param_id, data_product_id)

    @attr("UTIL")
    def test_secondary_cals(self):
        data_product_id = self.make_instrument_data_product()
        self.make_calibration_params(data_product_id)
        from ion.services.dm.utility.secondary_calibrations import SecondaryCalibrations
        sc = SecondaryCalibrations(self.data_product_management, self.dataset_management)
        sc.add_post_deployment(data_product_id, 'conductivity')
        sc.add_post_recovery(data_product_id, 'conductivity')
        sc.add_post_interpolated(data_product_id, 'conductivity')

        dataset_monitor = DatasetMonitor(data_product_id=data_product_id)
        rdt = self.ph.rdt_for_data_product(data_product_id)
        ntp_now = time.time() + 2208988800
        rdt['time'] = np.arange(ntp_now, ntp_now+20)
        rdt['temp'] = np.arange(20)
        rdt['conductivity'] = np.arange(20)

        self.ph.publish_rdt_to_data_product(data_product_id,rdt)
        self.assertTrue(dataset_monitor.wait())


        from coverage_model import ConstantOverTime


        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        cov = DatasetManagementService._get_coverage(dataset_id, mode='r+')
        np_dict = {
            'condwat_l1b_pd_cals' : ConstantOverTime('condwat_l1b_pd_cals', (0.0, 0.0, 0.0, 1.2, 1.0)),
            'condwat_l1b_pr_cals' : ConstantOverTime('condwat_l1b_pd_cals', (0.0, 0.0, 0.0, 1.2, 2.0)),
            'condwat_l1b_start' : ConstantOverTime('condwat_l1b_start', ntp_now),
            'condwat_l1b_end' : ConstantOverTime('condwat_l1b_end', ntp_now+10),
        }
        cov.set_parameter_values(np_dict)


        breakpoint(locals(), globals()) 
