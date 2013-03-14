#!/usr/bin/env python

"""
@brief Test to check CTDBP chain of transforms: L0, L1 and L2 (density and salinity)
@author Swarbhanu Chatterjee
"""



from pyon.ion.stream import  StandaloneStreamPublisher
from pyon.public import log, IonObject, RT, PRED
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.containers import DotDict
from pyon.ion.stream import StandaloneStreamSubscriber
from nose.plugins.attrib import attr

from interface.objects import ProcessDefinition
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule_utils import time_series_domain
from coverage_model import ParameterContext, AxisTypeEnum, QuantityType
from coverage_model.parameter import ParameterDictionary

import gevent
import numpy, random


@attr('INT', group='dm')
class TestCTDPChain(IonIntegrationTestCase):
    def setUp(self):
        super(TestCTDPChain, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.pubsub            = PubsubManagementServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.data_process_management = DataProcessManagementServiceClient()
        self.dataproduct_management = DataProductManagementServiceClient()
        self.resource_registry = ResourceRegistryServiceClient()

        # This is for the time values inside the packets going into the transform
        self.i = 0
        self.cnt = 0

        # Cleanup of queue created by the subscriber
        self.queue_cleanup = []
        self.data_process_cleanup = []

    def _get_new_ctd_L0_packet(self, stream_definition_id, length):

        rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)
        rdt['time'] = numpy.arange(self.i, self.i+length)

        for field in rdt:
            if isinstance(rdt._pdict.get_context(field).param_type, QuantityType):
                rdt[field] = numpy.array([random.uniform(0.0,75.0)  for i in xrange(length)])

        g = rdt.to_granule()
        self.i+=length

        return g

    def clean_queues(self):
        for queue in self.queue_cleanup:
            xn = self.container.ex_manager.create_xn_queue(queue)
            xn.delete()

    def cleaning_operations(self):
        for dproc_id in self.data_process_cleanup:
            self.data_process_management.delete_data_process(dproc_id)

    def test_ctdp_chain(self):
        """
        Test that packets are processed by a chain of CTDP transforms: L0, L1 and L2
        """

        #-------------------------------------------------------------------------------------
        # Prepare the stream def to be used for transform chain
        #-------------------------------------------------------------------------------------

        #todo Check whether the right parameter dictionary is being used
        self._prepare_stream_def_for_transform_chain()

        #-------------------------------------------------------------------------------------
        # Prepare the data proc defs and in and out data products for the transforms
        #-------------------------------------------------------------------------------------

        # list_args_L0 = [data_proc_def_id, input_dpod_id, output_dpod_id]
        list_args_L0 = self._prepare_things_you_need_to_launch_transform(name_of_transform='L0')

        list_args_L1 = self._prepare_things_you_need_to_launch_transform(name_of_transform='L1')

        list_args_L2_density = self._prepare_things_you_need_to_launch_transform(name_of_transform='L2_density')

        list_args_L2_salinity = self._prepare_things_you_need_to_launch_transform(name_of_transform='L2_salinity')

        log.debug("Got the following args: L0 = %s, L1 = %s, L2 density = %s, L2 salinity = %s", list_args_L0, list_args_L1, list_args_L2_density, list_args_L2_salinity )

        #-------------------------------------------------------------------------------------
        # Launch the CTDP transforms
        #-------------------------------------------------------------------------------------

        L0_data_proc_id = self._launch_transform('L0', *list_args_L0)

        L1_data_proc_id = self._launch_transform('L1', *list_args_L1)

        L2_density_data_proc_id = self._launch_transform('L2_density', *list_args_L2_density)

        L2_salinity_data_proc_id = self._launch_transform('L2_salinity', *list_args_L2_salinity)

        log.debug("Launched the transforms: L0 = %s, L1 = %s", L0_data_proc_id, L1_data_proc_id)

        #-------------------------------------------------------------------------
        # Start a subscriber listening to the output of each of the transforms
        #-------------------------------------------------------------------------

        ar_L0 = self.start_subscriber_listening_to_L0_transform(out_data_prod_id=list_args_L0[2])
        ar_L1 = self.start_subscriber_listening_to_L1_transform(out_data_prod_id=list_args_L1[2])

        ar_L2_density = self.start_subscriber_listening_to_L2_density_transform(out_data_prod_id=list_args_L2_density[2])
        ar_L2_salinity = self.start_subscriber_listening_to_L2_density_transform(out_data_prod_id=list_args_L2_salinity[2])


        #-------------------------------------------------------------------
        # Publish the parsed packets that the L0 transform is listening for
        #-------------------------------------------------------------------

        stream_id, stream_route = self.get_stream_and_route_for_data_prod(data_prod_id= list_args_L0[1])
        self._publish_for_L0_transform(stream_id, stream_route)

        #-------------------------------------------------------------------
        # Check the granules being outputted by the transforms
        #-------------------------------------------------------------------
        self._check_granule_from_L0_transform(ar_L0)
        self._check_granule_from_L1_transform(ar_L1)
        self._check_granule_from_L2_density_transform(ar_L2_density)
        self._check_granule_from_L2_salinity_transform(ar_L2_salinity)


    def _prepare_stream_def_for_transform_chain(self):

        # Get the stream definition for the stream using the parameter dictionary
#        pdict_id = self.dataset_management.read_parameter_dictionary_by_name(parameter_dict_name, id_only=True)
        pdict_id = self._create_input_param_dict_for_test(parameter_dict_name = 'input_param_for_L0')
        self.in_stream_def_id_for_L0 = self.pubsub.create_stream_definition(name='stream_def_for_L0', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub.delete_stream_definition, self.in_stream_def_id_for_L0)

        pdict_id = self._create_input_param_dict_for_test(parameter_dict_name = 'params_for_other_transforms')
        self.stream_def_id = self.pubsub.create_stream_definition(name='stream_def_for_CTDBP_transforms', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub.delete_stream_definition, self.stream_def_id)

        log.debug("Got the parsed parameter dictionary: id: %s", pdict_id)
        log.debug("Got the stream def for parsed input to L0: %s", self.in_stream_def_id_for_L0)
        log.debug("Got the stream def for other other streams: %s", self.stream_def_id)

    def _prepare_things_you_need_to_launch_transform(self, name_of_transform = ''):

        module, class_name = self._get_class_module(name_of_transform)

        #-------------------------------------------------------------------------
        # Data Process Definition
        #-------------------------------------------------------------------------

        dpd_obj = IonObject(RT.DataProcessDefinition,
            name= 'CTDBP_%s_Transform' % name_of_transform,
            description= 'Data Process Definition for the CTDBP %s transform.' % name_of_transform,
            module= module,
            class_name=class_name)

        data_proc_def_id = self.data_process_management.create_data_process_definition(dpd_obj)
        self.addCleanup(self.data_process_management.delete_data_process_definition, data_proc_def_id)
        log.debug("created data process definition: id = %s", data_proc_def_id)

        #-------------------------------------------------------------------------
        # Construct temporal and spatial Coordinate Reference System objects for the data product objects
        #-------------------------------------------------------------------------

        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()

        #-------------------------------------------------------------------------
        # Get the names of the input and output data products
        #-------------------------------------------------------------------------

        input_dpod_id = ''
        output_dpod_id = ''

        if name_of_transform == 'L0':

            input_dpod_id = self._create_input_data_product('parsed', tdom, sdom)
            output_dpod_id = self._create_output_data_product('L0', tdom, sdom)

            self.in_prod_for_L1 = output_dpod_id

        elif name_of_transform == 'L1':

            input_dpod_id = self.in_prod_for_L1
            output_dpod_id = self._create_output_data_product('L1', tdom, sdom)

            self.in_prod_for_L2 = output_dpod_id

        elif name_of_transform == 'L2_density':

            input_dpod_id = self.in_prod_for_L2
            output_dpod_id = self._create_output_data_product('L2_density', tdom, sdom)

        elif name_of_transform == 'L2_salinity':

            input_dpod_id = self.in_prod_for_L2
            output_dpod_id = self._create_output_data_product('L2_salinity', tdom, sdom)

        else:
            self.fail("something bad happened")

        return [data_proc_def_id, input_dpod_id, output_dpod_id]

    def _get_class_module(self, name_of_transform):

        options = {'L0' : self._class_module_L0,
                   'L1' : self._class_module_L1,
                   'L2_density' : self._class_module_L2_density,
                   'L2_salinity' : self._class_module_L2_salinity}

        return options[name_of_transform]()

    def _class_module_L0(self):
        module = 'ion.processes.data.transforms.ctdbp.ctdbp_L0'
        class_name = 'CTDBP_L0_all'
        return module, class_name

    def _class_module_L1(self):

        module = 'ion.processes.data.transforms.ctdbp.ctdbp_L1'
        class_name = 'CTDBP_L1_Transform'
        return module, class_name

    def _class_module_L2_density(self):

        module = 'ion.processes.data.transforms.ctdbp.ctdbp_L2_density'
        class_name = 'CTDBP_DensityTransform'

        return module, class_name

    def _class_module_L2_salinity(self):

        module = 'ion.processes.data.transforms.ctdbp.ctdbp_L2_salinity'
        class_name = 'CTDBP_SalinityTransform'

        return module, class_name

    def _create_input_data_product(self, name_of_transform = '', tdom = None, sdom = None):

        dpod_obj = IonObject(RT.DataProduct,
            name='dprod_%s' % name_of_transform,
            description='for_%s' % name_of_transform,
            temporal_domain = tdom,
            spatial_domain = sdom)

        log.debug("the stream def id: %s", self.stream_def_id)

        if name_of_transform == 'L0':
            stream_def_id = self.in_stream_def_id_for_L0
        else:
            stream_def_id = self.stream_def_id


        dpod_id = self.dataproduct_management.create_data_product(data_product=dpod_obj,
            stream_definition_id= stream_def_id
        )
        self.addCleanup(self.dataproduct_management.delete_data_product, dpod_id)

        log.debug("got the data product out. id: %s", dpod_id)

        return dpod_id

    def _create_output_data_product(self, name_of_transform = '', tdom = None, sdom = None):

        dpod_obj = IonObject(RT.DataProduct,
            name='dprod_%s' % name_of_transform,
            description='for_%s' % name_of_transform,
            temporal_domain = tdom,
            spatial_domain = sdom)

        if name_of_transform == 'L0':
            stream_def_id = self.in_stream_def_id_for_L0
        else:
            stream_def_id = self.stream_def_id

        dpod_id = self.dataproduct_management.create_data_product(data_product=dpod_obj,
            stream_definition_id=stream_def_id
        )
        self.addCleanup(self.dataproduct_management.delete_data_product, dpod_id)

        return dpod_id

    def _launch_transform(self, name_of_transform = '', data_proc_def_id = None, input_dpod_id = None, output_dpod_id = None):

        # We need the key name here to be "L2_stream", since when the data process is launched, this name goes into
        # the config as in config.process.publish_streams.L2_stream when the config is used to launch the data process

        if name_of_transform in ['L0', 'L1']:
            binding = '%s_stream' % name_of_transform
        elif name_of_transform == 'L2_salinity':
            binding = 'salinity'
        elif name_of_transform == 'L2_density':
            binding = 'density'

        output_products = {binding : output_dpod_id}

        config = None
        if name_of_transform == 'L1':
            config = self._create_calibration_coefficients_dict()
        elif name_of_transform == 'L2_density':
            config = DotDict()
            config.process = {'lat' : 32.7153, 'lon' : 117.1564}

        data_proc_id = self.data_process_management.create_data_process( data_proc_def_id, [input_dpod_id], output_products, config)
        self.addCleanup(self.data_process_management.delete_data_process, data_proc_id)

        self.data_process_management.activate_data_process(data_proc_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, data_proc_id)

        log.debug("Created a data process for ctdbp %s transform: id = %s", name_of_transform, data_proc_id)

        return data_proc_id

    def get_stream_and_route_for_data_prod(self, data_prod_id = ''):

        stream_ids, _ = self.resource_registry.find_objects(data_prod_id, PRED.hasStream, RT.Stream, True)
        stream_id = stream_ids[0]
        input_stream = self.resource_registry.read(stream_id)
        stream_route = input_stream.stream_route

        return stream_id, stream_route

    def start_subscriber_listening_to_L0_transform(self, out_data_prod_id = ''):

        #----------- Create subscribers to listen to the two transforms --------------------------------

        stream_ids, _ = self.resource_registry.find_objects(out_data_prod_id, PRED.hasStream, RT.Stream, True)
        output_stream_id_of_transform = stream_ids[0]

        ar_L0 = self._start_subscriber_to_transform( name_of_transform = 'L0',stream_id=output_stream_id_of_transform)

        return ar_L0


    def start_subscriber_listening_to_L1_transform(self, out_data_prod_id = ''):

        #----------- Create subscribers to listen to the two transforms --------------------------------

        stream_ids, _ = self.resource_registry.find_objects(out_data_prod_id, PRED.hasStream, RT.Stream, True)
        output_stream_id_of_transform = stream_ids[0]

        ar_L1 = self._start_subscriber_to_transform( name_of_transform = 'L1',stream_id=output_stream_id_of_transform)

        return ar_L1


    def start_subscriber_listening_to_L2_density_transform(self, out_data_prod_id = ''):

        #----------- Create subscribers to listen to the two transforms --------------------------------

        stream_ids, _ = self.resource_registry.find_objects(out_data_prod_id, PRED.hasStream, RT.Stream, True)
        output_stream_id_of_transform = stream_ids[0]

        ar_L2_density = self._start_subscriber_to_transform( name_of_transform = 'L2_density', stream_id=output_stream_id_of_transform)

        return ar_L2_density

    def start_subscriber_listening_to_L2_salinity_transform(self, out_data_prod_id = ''):

        #----------- Create subscribers to listen to the two transforms --------------------------------

        stream_ids, _ = self.resource_registry.find_objects(out_data_prod_id, PRED.hasStream, RT.Stream, True)
        output_stream_id_of_transform = stream_ids[0]

        ar_L2_density = self._start_subscriber_to_transform( name_of_transform = 'L2_salinity',stream_id=output_stream_id_of_transform)

        return ar_L2_density

    def _start_subscriber_to_transform(self,  name_of_transform = '', stream_id = ''):

        ar = gevent.event.AsyncResult()
        def subscriber(m,r,s):
            ar.set(m)

        sub = StandaloneStreamSubscriber(exchange_name='sub_%s' % name_of_transform, callback=subscriber)

        # Note that this running the below line creates an exchange since none of that name exists before
        sub_id = self.pubsub.create_subscription('subscriber_to_transform_%s' % name_of_transform,
            stream_ids=[stream_id],
            exchange_name='sub_%s' % name_of_transform)
        self.addCleanup(self.pubsub.delete_subscription, sub_id)

        self.pubsub.activate_subscription(sub_id)
        self.addCleanup(self.pubsub.deactivate_subscription, sub_id)

        sub.start()
        self.addCleanup(sub.stop)

        return ar

    def _check_granule_from_L0_transform(self, ar = None):

        granule_from_transform = ar.get(timeout=20)
        log.debug("Got the following granule from the L0 transform: %s", granule_from_transform)

        # Check the algorithm being applied
        self._check_application_of_L0_algorithm(granule_from_transform)


    def _check_granule_from_L1_transform(self, ar = None):

        granule_from_transform = ar.get(timeout=20)
        log.debug("Got the following granule from the L1 transform: %s", granule_from_transform)

        # Check the algorithm being applied
        self._check_application_of_L1_algorithm(granule_from_transform)

    def _check_granule_from_L2_density_transform(self, ar = None):

        granule_from_transform = ar.get(timeout=20)
        log.debug("Got the following granule from the L2 transform: %s", granule_from_transform)

        # Check the algorithm being applied
        self._check_application_of_L2_density_algorithm(granule_from_transform)

    def _check_granule_from_L2_salinity_transform(self, ar = None):

        granule_from_transform = ar.get(timeout=20)
        log.debug("Got the following granule from the L2 transform: %s", granule_from_transform)

        # Check the algorithm being applied
        self._check_application_of_L2_salinity_algorithm(granule_from_transform)


    def _check_application_of_L0_algorithm(self, granule = None):
        """ Check the algorithm applied by the L0 transform """

        rdt = RecordDictionaryTool.load_from_granule(granule)

        list_of_expected_keys = ['time', 'pressure', 'conductivity', 'temperature']

        for key in list_of_expected_keys:
            self.assertIn(key, rdt)
            self.assertIsNotNone(rdt[key])

    def _check_application_of_L1_algorithm(self, granule = None):
        """ Check the algorithm applied by the L1 transform """
        rdt = RecordDictionaryTool.load_from_granule(granule)

        list_of_expected_keys = [ 'time', 'pressure', 'conductivity', 'temp']

        for key in list_of_expected_keys:
            self.assertIn(key, rdt)
            self.assertIsNotNone(rdt[key])

    def _check_application_of_L2_density_algorithm(self, granule = None):
        """ Check the algorithm applied by the L2 transform """
        rdt = RecordDictionaryTool.load_from_granule(granule)

        list_of_expected_keys = ['time', 'density']

        for key in list_of_expected_keys:
            self.assertIn(key, rdt)
            self.assertIsNotNone(rdt[key])

    def _check_application_of_L2_salinity_algorithm(self, granule = None):
        """ Check the algorithm applied by the L2 transform """
        rdt = RecordDictionaryTool.load_from_granule(granule)

        list_of_expected_keys = ['time', 'salinity']

        for key in list_of_expected_keys:
            self.assertIn(key, rdt)
            self.assertIsNotNone(rdt[key])


    def _publish_for_L0_transform(self, input_stream_id = None, stream_route = None):

        #----------- Publish on that stream so that the transform can receive it --------------------------------
        self._publish_to_transform(input_stream_id, stream_route )

    def _publish_to_transform(self, stream_id = '', stream_route = None):

        pub = StandaloneStreamPublisher(stream_id, stream_route)
        publish_granule = self._get_new_ctd_L0_packet(stream_definition_id=self.in_stream_def_id_for_L0, length = 5)
        pub.publish(publish_granule)

        log.debug("Published the following granule: %s", publish_granule)


    def _create_input_param_dict_for_test(self, parameter_dict_name = ''):

        pdict = ParameterDictionary()

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=numpy.dtype('float64')))
        t_ctxt.axis = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 01-01-1900'
        pdict.add_context(t_ctxt)

        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        cond_ctxt.uom = ''
        pdict.add_context(cond_ctxt)

        pres_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        pres_ctxt.uom = ''
        pdict.add_context(pres_ctxt)

        if parameter_dict_name == 'input_param_for_L0':
            temp_ctxt = ParameterContext('temperature', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        else:
            temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=numpy.dtype('float32')))

        temp_ctxt.uom = ''
        pdict.add_context(temp_ctxt)

        dens_ctxt = ParameterContext('density', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        dens_ctxt.uom = ''
        pdict.add_context(dens_ctxt)

        sal_ctxt = ParameterContext('salinity', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        sal_ctxt.uom = ''
        pdict.add_context(sal_ctxt)

        #create temp streamdef so the data product can create the stream
        pc_list = []
        for pc_k, pc in pdict.iteritems():
            ctxt_id = self.dataset_management.create_parameter_context(pc_k, pc[1].dump())
            pc_list.append(ctxt_id)
            if parameter_dict_name == 'input_param_for_L0':
                self.addCleanup(self.dataset_management.delete_parameter_context,ctxt_id)
            elif pc[1].name == 'temp':
                self.addCleanup(self.dataset_management.delete_parameter_context,ctxt_id)

        pdict_id = self.dataset_management.create_parameter_dictionary(parameter_dict_name, pc_list)
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)

        return pdict_id



    def _create_calibration_coefficients_dict(self):
        config = DotDict()
        config.process.calibration_coeffs = {
                  'temp_calibration_coeffs': {
                      'TA0' : 1.561342e-03,
                      'TA1' : 2.561486e-04,
                      'TA2' : 1.896537e-07,
                      'TA3' : 1.301189e-07,
                      'TOFFSET' : 0.000000e+00
                  },

                  'cond_calibration_coeffs':  {
                      'G' : -9.896568e-01,
                      'H' : 1.316599e-01,
                      'I' : -2.213854e-04,
                      'J' : 3.292199e-05,
                      'CPCOR' : -9.570000e-08,
                      'CTCOR' : 3.250000e-06,
                      'CSLOPE' : 1.000000e+00
                  },

                  'pres_calibration_coeffs' : {
                      'PA0' : 4.960417e-02,
                      'PA1' : 4.883682e-04,
                      'PA2' : -5.687309e-12,
                      'PTCA0' : 5.249802e+05,
                      'PTCA1' : 7.595719e+00,
                      'PTCA2' : -1.322776e-01,
                      'PTCB0' : 2.503125e+01,
                      'PTCB1' : 5.000000e-05,
                      'PTCB2' : 0.000000e+00,
                      'PTEMPA0' : -6.431504e+01,
                      'PTEMPA1' : 5.168177e+01,
                      'PTEMPA2' : -2.847757e-01,
                      'POFFSET' : 0.000000e+00
                  }

              }


        return config
















#    def check_cond_algorithm_execution(self, publish_granule, granule_from_transform):
#
#        input_rdt_to_transform = RecordDictionaryTool.load_from_granule(publish_granule)
#        output_rdt_transform = RecordDictionaryTool.load_from_granule(granule_from_transform)
#
#        output_data = output_rdt_transform['conductivity']
#        input_data = input_rdt_to_transform['conductivity']
#
#        self.assertTrue(numpy.array_equal(((input_data / 100000.0) - 0.5), output_data))
#
#
#    def check_granule_splitting(self, publish_granule, out_dict):
#        """
#        This checks that the ctd_L2_all transform is able to split out one of the
#        granules from the whole granule
#        fed into the transform
#        """
#
#        input_rdt_to_transform = RecordDictionaryTool.load_from_granule(publish_granule)
#
#        in_cond = input_rdt_to_transform['conductivity']
#        in_pressure = input_rdt_to_transform['pressure']
#        in_temp = input_rdt_to_transform['temp']
#
#        out_cond = out_dict['c']
#        out_pres = out_dict['p']
#        out_temp = out_dict['t']
#
#        self.assertTrue(numpy.array_equal(in_cond,out_cond))
#        self.assertTrue(numpy.array_equal(in_pressure, out_pres))
#        self.assertTrue(numpy.array_equal(in_temp,out_temp))
