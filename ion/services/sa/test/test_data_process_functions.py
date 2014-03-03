#!/usr/bin/env python
__author__ = 'Luke'
from ion.services.dm.test.dm_test_case import DMTestCase
from ion.processes.data.replay.replay_process import RetrieveProcess
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.services.dm.utility.provenance import graph
from coverage_model import ParameterFunctionType, ParameterDictionary, PythonFunction, ParameterContext
from ion.processes.data.transforms.transform_worker import TransformWorker
from interface.objects import DataProcessDefinition
from nose.plugins.attrib import attr
from pyon.util.breakpoint import breakpoint
from datetime import datetime, timedelta
from pyon.util.containers import DotDict
from pyon.util.log import log
from pyon.public import RT, PRED, IonObject
from interface.objects import TransformFunctionType, DataProcessTypeEnum, ParameterFunction, ParameterFunctionType as PFT
import os
import unittest
import numpy as np
import calendar

class TestDataProcessFunctions(DMTestCase):

    egg_url = 'http://sddevrepo.oceanobservatories.org/releases/ion_example-0.1-py2.7.egg' 
    def preload_units(self):
        config = DotDict()
        config.op = 'load'
        config.attachments = "res/preload/r2_ioc/attachments"
        config.scenario = 'LC_UNITS'
        config.categories='ParameterFunctions,ParameterDefs,ParameterDictionary'
        config.path = 'master'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    @attr('INT')
    def test_retrieve_process(self):
        data_product_id = self.make_ctd_data_product()
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        rdt = self.ph.rdt_for_data_product(data_product_id)
        date0 = datetime(2014, 1, 1, 0, 0) # 2014-01-01T00:00Z
        time0 = calendar.timegm(date0.timetuple()) + 2208988800 # NTP
        rdt['time'] = np.arange(time0, time0+30)
        rdt['temp'] = np.arange(30)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()
        retrieve_process = RetrieveProcess(dataset_id)
        rdt = retrieve_process.retrieve(date0, date0 + timedelta(hours=1))
        np.testing.assert_array_equal(rdt['temp'], np.arange(30))

    @attr('INT')
    def test_append_parameter(self):
        # Make a CTDBP Data Product
        data_product_id = self.make_ctd_data_product()
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        # Throw some data in it
        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.arange(30)
        rdt['temp'] = np.arange(30)
        rdt['pressure'] = np.arange(30)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()

        # Grab the egg
        egg_url = self.egg_url
        egg_path = TransformWorker.download_egg(egg_url)
        import pkg_resources
        pkg_resources.working_set.add_entry(egg_path)
        self.addCleanup(os.remove, egg_path)

        # Make a parameter function
        owner = 'ion_example.add_arrays'
        func = 'add_arrays'
        arglist = ['a', 'b']
        pf = ParameterFunction(name='add_arrays', function_type=PFT.PYTHON, owner=owner, function=func, args=arglist)
        pfunc_id = self.dataset_management.create_parameter_function(pf)
        self.addCleanup(self.dataset_management.delete_parameter_function, pfunc_id)

        # Make a context (instance of the function)
        pfunc = DatasetManagementService.get_coverage_function(pf)
        pfunc.param_map = {'a':'temp', 'b':'pressure'}
        ctxt = ParameterContext('array_sum', param_type=ParameterFunctionType(pfunc))
        ctxt_dump = ctxt.dump()
        ctxt_id = self.dataset_management.create_parameter_context('array_sum', ctxt_dump)
        self.dataset_management.add_parameter_to_dataset(ctxt_id, dataset_id)

        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['array_sum'], np.arange(0,60,2))

    @attr('INT')
    def test_add_parameter_function(self):
        # req-tag: NEW SA - 31
        # Make a CTDBP Data Product
        data_product_id = self.make_ctd_data_product()
        self.data_product_id = data_product_id
        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)

        # Throw some data in it
        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.arange(30)
        rdt['temp'] = np.arange(30)
        rdt['pressure'] = np.arange(30)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())
        dataset_monitor.event.clear()

        #--------------------------------------------------------------------------------
        # This is what the user defines either via preload or through the UI
        #--------------------------------------------------------------------------------
        # Where the egg is
        egg_url = self.egg_url

        # Make a parameter function
        owner = 'ion_example.add_arrays'
        func = 'add_arrays'
        arglist = ['a', 'b']
        pf = ParameterFunction(name='add_arrays', function_type=PFT.PYTHON, owner=owner, function=func, args=arglist, egg_uri=egg_url)
        pfunc_id = self.dataset_management.create_parameter_function(pf)
        #--------------------------------------------------------------------------------
        self.addCleanup(self.dataset_management.delete_parameter_function, pfunc_id)

        # Make a data process definition
        dpd = DataProcessDefinition(name='add_arrays', description='Sums two arrays')
        dpd_id = self.data_process_management.create_data_process_definition(dpd, pfunc_id)

        # TODO: assert assoc exists
        argmap = {'a':'temp', 'b':'pressure'}
        dp_id = self.data_process_management.create_data_process(dpd_id, [data_product_id], argument_map=argmap, out_param_name='array_sum')

        # Verify that the function worked!
        granule = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_array_equal(rdt['array_sum'], np.arange(0,60,2))
    
        # Verify that we can inspect it as well
        source_code = self.data_process_management.inspect_data_process_definition(dpd_id)
        self.assertEquals(source_code, 'def add_arrays(a, b):\n    return a+b\n')

    @attr("UTIL")
    def test_ui_functionality(self):
        '''
        Tests the service implementations and UI compliance through the service gateway
        '''
        # Get some initial dpds
        # There's one specifically for converting from C to F
        self.preload_units()
        
        # User clicks create data process
        # User is presented with a dropdown of data process definitions
        dpds, _ = self.resource_registry.find_resources(restype=RT.DataProcessDefinition)
        # User selects the c_to_f data process definition
        relevant = filter(lambda x: 'c_to_f' in x.name, dpds)
        breakpoint(locals(), globals())


        # User can select an existing data


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_validate_argument_input(self):
        self.test_add_parameter_function()
        data_product_id = self.data_product_id

        dpms = self.container.proc_manager.procs_by_name['data_process_management']
        self.assertTrue(dpms.validate_argument_input(data_product_id, {'a':'temp', 'b':'pressure'}))
        self.assertFalse(dpms.validate_argument_input(data_product_id, {'a':'temp', 'b':'not_pressure'}))

        stream_defs, _ = self.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=False)
        stream_def = stream_defs[0]
        stream_def.available_fields = ['time', 'temp']
        self.resource_registry.update(stream_def)

        params = dpms.parameters_for_data_product(data_product_id, True)
        self.assertEquals(len(params), 2)

    @attr('UTIL')
    def test_logger(self):
        data_product_id = self.make_ctd_data_product()
        # Clone the data product so we have an output
        clone_id = self.clone_data_product(data_product_id)

        data_process_id = self.create_data_process_logger(data_product_id, clone_id, {'x':'temp'})
        
        dataset_monitor = DatasetMonitor(data_product_id=data_product_id)
        self.addCleanup(dataset_monitor.stop)

        # Put some data into the the data product
        rdt = self.ph.rdt_for_data_product(data_product_id)
        rdt['time'] = np.arange(40)
        rdt['temp'] = np.arange(40)
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())

        # Watch the output
        dataset_monitor = DatasetMonitor(data_product_id=clone_id)
        self.addCleanup(dataset_monitor.stop)
        # Run the replay
        self.data_process_management.activate_data_process(data_process_id)

        # Make sure data came out
        self.assertTrue(dataset_monitor.wait())

    def create_data_process_logger(self, data_product_id, clone_id, argument_map):
        '''
        Launches a data process that just prints input
        '''
        out_name = argument_map.values()[0]

        # Make the transfofm function
        tf_obj = IonObject(RT.TransformFunction,
                           name='stream_logger',
                           description='',
                           function='stream_logger',
                           module='ion.services.sa.test.test_data_process_functions',
                           arguments=['x'],
                           function_type=TransformFunctionType.TRANSFORM)
        func_id = self.data_process_management.create_transform_function(tf_obj)
        self.addCleanup(self.data_process_management.delete_transform_function, func_id)
        
        # Make the data process definition
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='stream_logger',
                            description='logs some stream stuff',
                            data_process_type=DataProcessTypeEnum.RETRIEVE_PROCESS)
        configuration = DotDict()
        configuration.publish_limit = 40
        dpd_id = self.data_process_management.create_data_process_definition(dpd_obj, func_id)
        data_process_id = self.data_process_management.create_data_process(
                            data_process_definition_id=dpd_id, 
                            inputs=[data_product_id], 
                            outputs=[clone_id], 
                            configuration=configuration,
                            argument_map=argument_map, 
                            out_param_name=out_name) 
        return data_process_id

    def clone_data_product(self, data_product_id):
        '''
        Clones a data product but gives it a different name and a new id
        '''
        stream_def_ids, _ = self.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=True)
        dp = self.data_product_management.read_data_product(data_product_id)
        del dp._id
        del dp._rev
        dp.name += '_clone'

        clone_id = self.data_product_management.create_data_product(dp, stream_def_ids[0])
        self.addCleanup(self.data_product_management.delete_data_product, clone_id)

        self.data_product_management.activate_data_product_persistence(clone_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, clone_id)

        return clone_id

def stream_logger(x):
    log.info(repr(x))
    return x
