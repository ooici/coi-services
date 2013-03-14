#!/usr/bin/env python

"""
@brief Test to check CTD
@author Swarbhanu Chatterjee
"""


from pyon.ion.stream import  StandaloneStreamPublisher
from pyon.public import log, IonObject, RT, PRED
from pyon.util.int_test import IonIntegrationTestCase
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
class CtdbpTransformsIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(CtdbpTransformsIntTest, self).setUp()

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

    def _get_new_ctd_packet(self, stream_definition_id, length):

        rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)
        rdt['time'] = numpy.arange(self.i, self.i+length)

        for field in rdt:
            if isinstance(rdt._pdict.get_context(field).param_type, QuantityType):
                rdt[field] = numpy.array([random.uniform(0.0,75.0)  for i in xrange(length)])

        g = rdt.to_granule()
        self.i+=length

        return g

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

        temp_ctxt = ParameterContext('temperature', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
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
            self.addCleanup(self.dataset_management.delete_parameter_context,ctxt_id)

        pdict_id = self.dataset_management.create_parameter_dictionary(parameter_dict_name, pc_list)
        self.addCleanup(self.dataset_management.delete_parameter_dictionary, pdict_id)

        return pdict_id

    def test_ctdbp_L0_all(self):
        """
        Test packets processed by the ctdbp_L0_all transform
        """

        #----------- Data Process Definition --------------------------------

        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='CTDBP_L0_all',
            description='Take parsed stream and put the C, T and P into three separate L0 streams.',
            module='ion.processes.data.transforms.ctdbp.ctdbp_L0',
            class_name='CTDBP_L0_all')

        dprocdef_id = self.data_process_management.create_data_process_definition(dpd_obj)
        self.addCleanup(self.data_process_management.delete_data_process_definition, dprocdef_id)

        log.debug("created data process definition: id = %s", dprocdef_id)

        #----------- Data Products --------------------------------

        # Construct temporal and spatial Coordinate Reference System objects
        tdom, sdom = time_series_domain()

        sdom = sdom.dump()
        tdom = tdom.dump()

        input_param_dict = self._create_input_param_dict_for_test(parameter_dict_name = 'fictitious_ctdp_param_dict')

        # Get the stream definition for the stream using the parameter dictionary
#        input_param_dict = self.dataset_management.read_parameter_dictionary_by_name('ctdbp_cdef_sample', id_only=True)
        input_stream_def_dict = self.pubsub.create_stream_definition(name='parsed', parameter_dictionary_id=input_param_dict)
        self.addCleanup(self.pubsub.delete_stream_definition, input_stream_def_dict)

        log.debug("Got the parsed parameter dictionary: id: %s", input_param_dict)
        log.debug("Got the stream def for parsed input: %s", input_stream_def_dict)

        # Input data product
        parsed_stream_dp_obj = IonObject(RT.DataProduct,
            name='parsed_stream',
            description='Parsed stream input to CTBP L0 transform',
            temporal_domain = tdom,
            spatial_domain = sdom)

        input_dp_id = self.dataproduct_management.create_data_product(data_product=parsed_stream_dp_obj,
            stream_definition_id=input_stream_def_dict
        )
        self.addCleanup(self.dataproduct_management.delete_data_product, input_dp_id)

        # output data product
        L0_stream_dp_obj = IonObject(RT.DataProduct,
            name='L0_stream',
            description='L0_stream output of CTBP L0 transform',
            temporal_domain = tdom,
            spatial_domain = sdom)

        L0_stream_dp_id = self.dataproduct_management.create_data_product(data_product=L0_stream_dp_obj,
                                                                    stream_definition_id=input_stream_def_dict
                                                                    )
        self.addCleanup(self.dataproduct_management.delete_data_product, L0_stream_dp_id)

        # We need the key name here to be "L0_stream", since when the data process is launched, this name goes into
        # the config as in config.process.publish_streams.L0_stream when the config is used to launch the data process
        self.output_products = {'L0_stream' : L0_stream_dp_id}
        out_stream_ids, _ = self.resource_registry.find_objects(L0_stream_dp_id, PRED.hasStream, RT.Stream, True)
        self.assertTrue(len(out_stream_ids))
        output_stream_id = out_stream_ids[0]

        dproc_id = self.data_process_management.create_data_process( dprocdef_id, [input_dp_id], self.output_products)
        self.addCleanup(self.data_process_management.delete_data_process, dproc_id)

        log.debug("Created a data process for ctdbp_L0. id: %s", dproc_id)

        # Activate the data process
        self.data_process_management.activate_data_process(dproc_id)
        self.addCleanup(self.data_process_management.deactivate_data_process, dproc_id)

        #----------- Find the stream that is associated with the input data product when it was created by create_data_product() --------------------------------

        stream_ids, _ = self.resource_registry.find_objects(input_dp_id, PRED.hasStream, RT.Stream, True)
        self.assertTrue(len(stream_ids))

        input_stream_id = stream_ids[0]
        stream_route = self.pubsub.read_stream_route(input_stream_id)

        log.debug("The input stream for the L0 transform: %s", input_stream_id)

        #----------- Create a subscriber that will listen to the transform's output --------------------------------

        ar = gevent.event.AsyncResult()
        def subscriber(m,r,s):
            ar.set(m)

        sub = StandaloneStreamSubscriber(exchange_name='sub', callback=subscriber)

        sub_id = self.pubsub.create_subscription('subscriber_to_transform',
            stream_ids=[output_stream_id],
            exchange_name='sub')
        self.addCleanup(self.pubsub.delete_subscription, sub_id)

        self.pubsub.activate_subscription(sub_id)
        self.addCleanup(self.pubsub.deactivate_subscription, sub_id)


        sub.start()
        self.addCleanup(sub.stop)

        #----------- Publish on that stream so that the transform can receive it --------------------------------

        pub = StandaloneStreamPublisher(input_stream_id, stream_route)
        publish_granule = self._get_new_ctd_packet(stream_definition_id=input_stream_def_dict, length = 5)

        pub.publish(publish_granule)

        log.debug("Published the following granule: %s", publish_granule)

        granule_from_transform = ar.get(timeout=20)

        log.debug("Got the following granule from the transform: %s", granule_from_transform)

        # Check that the granule published by the L0 transform has the right properties
        self._check_granule_from_transform(granule_from_transform)


    def _check_granule_from_transform(self, granule):
        """
        An internal method to check if a granule has the right properties
        """

        pass


