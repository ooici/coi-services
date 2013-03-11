#!/usr/bin/env python

"""
@brief Test to check CTD
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

from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

@attr('INT', group='dm')
class CtdTransformsIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(CtdTransformsIntTest, self).setUp()

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

        # Cleanup of queue created by the subscriber
        self.queue_cleanup = []
        self.data_process_cleanup = []

    def _create_input_param_dict_for_test(self, parameter_dict_name = ''):

        pdict = ParameterDictionary()

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=numpy.dtype('int32')))
        t_ctxt.axis = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 01-01-1970'
        pdict.add_context(t_ctxt)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=numpy.dtype('int32')))
        lat_ctxt.axis = AxisTypeEnum.LAT
        lat_ctxt.uom = ''
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=numpy.dtype('int32')))
        lon_ctxt.axis = AxisTypeEnum.LON
        lon_ctxt.uom = ''
        pdict.add_context(lon_ctxt)

        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=numpy.dtype('int32')))
        cond_ctxt.uom = ''
        pdict.add_context(cond_ctxt)

        pres_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=numpy.dtype('int32')))
        pres_ctxt.uom = ''
        pdict.add_context(pres_ctxt)

        if parameter_dict_name == 'input_param_dict':
            temp_ctxt = ParameterContext('temperature', param_type=QuantityType(value_encoding=numpy.dtype('int32')))
        else:
            temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=numpy.dtype('int32')))

        temp_ctxt.uom = ''
        pdict.add_context(temp_ctxt)

        dens_ctxt = ParameterContext('density', param_type=QuantityType(value_encoding=numpy.dtype('int32')))
        dens_ctxt.uom = ''
        pdict.add_context(dens_ctxt)

        sal_ctxt = ParameterContext('salinity', param_type=QuantityType(value_encoding=numpy.dtype('int32')))
        sal_ctxt.uom = ''
        pdict.add_context(sal_ctxt)

        #create temp streamdef so the data product can create the stream
        pc_list = []
        for pc_k, pc in pdict.iteritems():
            ctxt_id = self.dataset_management.create_parameter_context(pc_k, pc[1].dump())
            pc_list.append(ctxt_id)
            if parameter_dict_name == 'input_param_dict':
                self.addCleanup(self.dataset_management.delete_parameter_context,ctxt_id)
            elif  parameter_dict_name == 'output_param_dict' and pc[1].name == 'temp':
                self.addCleanup(self.dataset_management.delete_parameter_context,ctxt_id)

        pdict_id = self.dataset_management.create_parameter_dictionary(parameter_dict_name, pc_list)

        return pdict_id

    def _get_new_ctd_L0_packet(self, stream_definition_id, length):

        rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)
        rdt['time'] = numpy.arange(self.i, self.i+length)

        for field in rdt:
            if isinstance(rdt._pdict.get_context(field).param_type, QuantityType):
                rdt[field] = numpy.array([random.uniform(0.0,75.0)  for i in xrange(length)])

        g = rdt.to_granule()
        self.i+=length

        return g

    def _create_calibration_coefficients_dict(self):

#        calibration_coeffs = {}
#        calibration_coeffs['temp_calibration_coeffs'] = self.CFG.process.calibration_coeffs.temp_calibration_coeffs
#        calibration_coeffs['pres_calibration_coeffs'] = self.CFG.process.calibration_coeffs.pres_calibration_coeffs
#        calibration_coeffs['cond_calibration_coeffs'] = self.CFG.process.calibration_coeffs.cond_calibration_coeffs

        config = DotDict()
        config.process = DotDict()
        config.process.calibration_coeffs = {}
        config.process.calibration_coeffs['temp_calibration_coeffs'] = {'a0' : 1, 'a1' : 1, 'a2' : 1, 'a3' : 1}

        config.process.calibration_coeffs['pres_calibration_coeffs'] = { 'PTEMPA0' : 1,
                                                          'PTEMPA1' : 1,
                                                          'PTEMPA2' : 1,
                                                          'PTCA0' : 0.1,
                                                          'PTCA1' : 0.1,
                                                          'PTCA2' : 0.1,
                                                          'PTCB0' : 1,
                                                          'PTCB1' : 1,
                                                          'PTCB2' : 1,
                                                          'PA0' : 1,
                                                          'PA1' : 1,
                                                          'PA2' : 1}

        config.process.calibration_coeffs['cond_calibration_coeffs'] = {'g' : 1, 'h' : 1, 'I'  :1, 'j' : 1,
                                                         'CTcor' : 0.1, 'CPcor' : 0.2}

        return config

    def clean_queues(self):
        for queue in self.queue_cleanup:
            xn = self.container.ex_manager.create_xn_queue(queue)
            xn.delete()

    def cleaning_operations(self):
        for dproc_id in self.data_process_cleanup:
            self.data_process_management.delete_data_process(dproc_id)

    def test_ctd_L1_all(self):
        """
        Test that packets are processed by the ctd_L1_all transform
        """

        #----------- Data Process Definition --------------------------------

        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='CTDBP_L1_Transform',
            description='Take granules on the L0 stream which have the C, T and P data and separately apply algorithms and output on the L1 stream.',
            module='ion.processes.data.transforms.ctdbp.ctdbp_L1',
            class_name='CTDBP_L1_Transform')

        dprocdef_id = self.data_process_management.create_data_process_definition(dpd_obj)

        log.debug("created data process definition: id = %s", dprocdef_id)

        #----------- Data Products --------------------------------

        # Construct temporal and spatial Coordinate Reference System objects
        tdom, sdom = time_series_domain()

        sdom = sdom.dump()
        tdom = tdom.dump()

        # Get the stream definition for the stream using the parameter dictionary
        L0_pdict_id = self._create_input_param_dict_for_test(parameter_dict_name = 'input_param_dict')

        L0_stream_def_id = self.pubsub.create_stream_definition(name='parsed', parameter_dictionary_id=L0_pdict_id)

        L1_pdict_id = self._create_input_param_dict_for_test(parameter_dict_name = 'output_param_dict')
        L1_stream_def_id = self.pubsub.create_stream_definition(name='L1_out', parameter_dictionary_id=L1_pdict_id)


        log.debug("Got the parsed parameter dictionary: id: %s", L0_pdict_id)
        log.debug("Got the stream def for parsed input: %s", L0_stream_def_id)

        log.debug("got the stream def for the output: %s", L1_stream_def_id)

        # Input data product
        L0_stream_dp_obj = IonObject(RT.DataProduct,
            name='L0_stream',
            description='L0 stream input to CTBP L1 transform',
            temporal_domain = tdom,
            spatial_domain = sdom)

        input_dp_id = self.dataproduct_management.create_data_product(data_product=L0_stream_dp_obj,
            stream_definition_id=L0_stream_def_id
        )

        # output data product
        L1_stream_dp_obj = IonObject(RT.DataProduct,
            name='L1_stream',
            description='L1_stream output of CTBP L1 transform',
            temporal_domain = tdom,
            spatial_domain = sdom)

        L1_stream_dp_id = self.dataproduct_management.create_data_product(data_product=L1_stream_dp_obj,
            stream_definition_id=L1_stream_def_id
        )

        # We need the key name here to be "L1_stream", since when the data process is launched, this name goes into
        # the config as in config.process.publish_streams.L1_stream when the config is used to launch the data process
        self.output_products = {'L1_stream' : L1_stream_dp_id}
        out_stream_ids, _ = self.resource_registry.find_objects(L1_stream_dp_id, PRED.hasStream, RT.Stream, True)
        output_stream_id = out_stream_ids[0]

        config = self._create_calibration_coefficients_dict()
        dproc_id = self.data_process_management.create_data_process( dprocdef_id, [input_dp_id], self.output_products, config)
        self.data_process_cleanup.append(dproc_id)
        log.debug("Created a data process for ctdbp_L1. id: %s", dproc_id)

        # Activate the data process
        self.data_process_management.activate_data_process(dproc_id)

        #----------- Find the stream that is associated with the input data product when it was created by create_data_product() --------------------------------

        stream_ids, _ = self.resource_registry.find_objects(input_dp_id, PRED.hasStream, RT.Stream, True)

        input_stream_id = stream_ids[0]
        input_stream = self.resource_registry.read(input_stream_id)
        stream_route = input_stream.stream_route

        log.debug("The input stream for the L1 transform: %s", input_stream_id)

        #----------- Create a subscriber that will listen to the transform's output --------------------------------

        ar = gevent.event.AsyncResult()
        def subscriber(m,r,s):
            ar.set(m)

        sub = StandaloneStreamSubscriber(exchange_name='sub', callback=subscriber)

        sub_id = self.pubsub.create_subscription('subscriber_to_transform',
            stream_ids=[output_stream_id],
            exchange_name='sub')

        self.pubsub.activate_subscription(sub_id)

        self.addCleanup(sub.stop)
        self.queue_cleanup.append(sub.xn.queue)
        self.addCleanup(self.clean_queues)

        sub.start()

        #----------- Publish on that stream so that the transform can receive it --------------------------------

        pub = StandaloneStreamPublisher(input_stream_id, stream_route)
        publish_granule = self._get_new_ctd_L0_packet(stream_definition_id=L0_stream_def_id, length = 5)

        pub.publish(publish_granule)

        log.debug("Published the following granule: %s", publish_granule)

        granule_from_transform = ar.get(timeout=20)

        log.debug("Got the following granule from the transform: %s", granule_from_transform)

        # Check that the granule published by the L1 transform has the right properties
        self._check_granule_from_transform(granule_from_transform)


    def _check_granule_from_transform(self, granule):
        """
        An internal method to check if a granule has the right properties
        """

        rdt = RecordDictionaryTool.load_from_granule(granule)

        self.assertTrue(rdt.__contains__('pressure') and rdt.__contains__('temp') and rdt.__contains__('conductivity'))
        self.assertTrue(rdt.__contains__('time') and rdt.__contains__('lat') and rdt.__contains__('lon'))

        #todo: need to check the algorithms here for the granule






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
#        This checks that the ctd_L1_all transform is able to split out one of the
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
