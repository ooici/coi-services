#!/usr/bin/env python

"""
@brief Test to check CTDBP chain of transforms: L0, L1 and L2 (density and salinity)
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
from coverage_model import QuantityType

import gevent
import numpy, random

from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

@attr('INT', group='dm')
class TestCTDPChain(IonIntegrationTestCase):
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
        self.cnt = 0

        # Cleanup of queue created by the subscriber
        self.queue_cleanup = []

    def _get_new_ctd_L1_packet(self, stream_definition_id, length):

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


    def test_ctdp_chain(self):
        """
        Test that packets are processed by a chain of CTDP transforms: L0, L1 and L2
        """

        #-------------------------------------------------------------------------------------
        # Prepare the stream def to be used for transform chain
        #-------------------------------------------------------------------------------------

        #todo Check whether the right parameter dictionary is being used
        self._prepare_stream_def_for_transform_chain(parameter_dict_name='ctd_parsed_param_dict')

        #-------------------------------------------------------------------------------------
        # Prepare the data proc defs and in and out data products for the transforms
        #-------------------------------------------------------------------------------------

        # list_args_L0 = [data_proc_def_id, input_dpod_id, output_dpod_id]
        list_args_L0 = self._prepare_things_you_need_to_launch_transform(name_of_transform='L0')

        list_args_L1 = self._prepare_things_you_need_to_launch_transform(name_of_transform='L1')

        list_args_L2_density = self._prepare_things_you_need_to_launch_transform(name_of_transform='L2_density')

        list_args_L2_salinity = self._prepare_things_you_need_to_launch_transform(name_of_transform='L2_salinity')

        #-------------------------------------------------------------------------------------
        # Launch the CTDP transforms
        #-------------------------------------------------------------------------------------

        L0_data_proc_id = self._launch_transform('L0', *list_args_L0)

        L1_data_proc_id = self._launch_transform('L1', *list_args_L1)

        L2_density_data_proc_id = self._launch_transform('L2_density', *list_args_L2_density)

        L2_salinity_data_proc_id = self._launch_transform('L2_salinity', *list_args_L2_salinity)

        #-------------------------------------------------------------------------
        # Start a subscriber listening to the output of each of the transforms
        #-------------------------------------------------------------------------

        ar_L0 = self.start_subscriber_listening_to_L0_transform(output_stream_id_of_transform=list_args_L0[2])
        ar_L1 = self.start_subscriber_listening_to_L1_transform(output_stream_id_of_transform=list_args_L1[2])
        ar_L2 = self.start_subscriber_listening_to_L2_transform(output_stream_id_of_transform=list_args_L1[2])

        #-------------------------------------------------------------------
        # Publish the parsed packets that the L0 transform is listening for
        #-------------------------------------------------------------------

        stream_id, stream_route = self.get_stream_and_route_for_data_prod(data_prod_id= list_args_L0[1])
        self._publish_for_L0_transform(input_stream_id, stream_route)

        #-------------------------------------------------------------------
        # Check the granules being outputted by the transforms
        #-------------------------------------------------------------------
        self._check_granule_from_L0_transform(ar_L0)
        self._check_granule_from_L1_transform(ar_L1)
        self._check_granule_from_L2_transform(ar_L2)


    def _prepare_stream_def_for_transform_chain(self, parameter_dict_name = ''):

        # Get the stream definition for the stream using the parameter dictionary
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name(parameter_dict_name, id_only=True)
        self.stream_def_id = self.pubsub.create_stream_definition(name='stream_def_for_CTDBP_transforms', parameter_dictionary_id=pdict_id)

        log.debug("Got the parsed parameter dictionary: id: %s", pdict_id)
        log.debug("Got the stream def for parsed input: %s", self.stream_def_id)

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
        log.debug("created data process definition: id = %s", data_proc_def_id)

        #-------------------------------------------------------------------------
        # Construct temporal and spatial Coordinate Reference System objects for the data product objects
        #-------------------------------------------------------------------------

        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()

        #-------------------------------------------------------------------------
        # Input data product
        #-------------------------------------------------------------------------
        input_dpod_id = self._create_input_data_product(tdom, sdom)

        #-------------------------------------------------------------------------
        # output data product
        #-------------------------------------------------------------------------
        output_dpod_id = self._create_output_data_product(tdom, sdom)

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
            name='in_data_prod_for_%s' % name_of_transform,
            description='in_data_prod_for_%s' % name_of_transform,
            temporal_domain = tdom,
            spatial_domain = sdom)

        dpod_id = self.dataproduct_management.create_data_product(data_product=dpod_obj,
            stream_definition_id=self.stream_def_id
        )

        return dpod_id

    def _create_output_data_product(self, name_of_transform = '', tdom = None, sdom = None):

        dpod_obj = IonObject(RT.DataProduct,
            name='out_data_prod_for_%s' % name_of_transform,
            description='out_data_prod_for_%s' % name_of_transform,
            temporal_domain = tdom,
            spatial_domain = sdom)

        dpod_id = self.dataproduct_management.create_data_product(data_product=dpod_obj,
            stream_definition_id=self.stream_def_id
        )

        return dpod_id

    def _launch_transform(self, name_of_transform = '', data_proc_def_id = None, input_dpod_id = None, output_dpod_id = None):

        # We need the key name here to be "L2_stream", since when the data process is launched, this name goes into
        # the config as in config.process.publish_streams.L2_stream when the config is used to launch the data process
        output_products = {'L0_stream' : output_dpod_id}

        data_proc_id = self.data_process_management.create_data_process( data_proc_def_id, [input_dpod_id], output_products)

        # Activate the data process
        self.data_process_management.activate_data_process(data_proc_id)

        log.debug("Created a data process for ctdbp %s transform: id = %s", name_of_transform, data_proc_id)

        return data_proc_id

    def get_stream_and_route_for_data_prod(self, data_prod_id = ''):

        stream_ids, _ = self.resource_registry.find_objects(data_prod_id, PRED.hasStream, RT.Stream, True)
        stream_id = stream_ids[0]
        input_stream = self.resource_registry.read(input_stream_id)
        stream_route = input_stream.stream_route

        return stream_id, stream_route

    def _launch_L2_transform(self):

        pass


    def start_subscriber_listening_to_L0_transform(self, output_stream_id_of_transform = ''):

        #----------- Create subscribers to listen to the two transforms --------------------------------

        ar_L0 = self._start_subscriber_to_transform( name_of_transform = 'L0',stream_id=output_stream_id_of_transform)

        return ar_L0


    def start_subscriber_listening_to_L1_transform(self, output_stream_id_of_transform = ''):

        #----------- Create subscribers to listen to the two transforms --------------------------------

        ar_L1 = self._start_subscriber_to_transform( name_of_transform = 'L1',stream_id=output_stream_id_of_transform)

        return ar_L1


    def start_subscriber_listening_to_L2_transform(self, output_stream_id_of_transform = ''):

        #----------- Create subscribers to listen to the two transforms --------------------------------

        ar_L2 = self._start_subscriber_to_transform( name_of_transform = 'L2',stream_id=output_stream_id_of_transform)

        return ar_L2

    def _start_subscriber_to_transform(self,  name_of_transform = '', stream_id = ''):

        ar = gevent.event.AsyncResult()
        def subscriber(m,r,s):
            ar.set(m)

        sub = StandaloneStreamSubscriber(exchange_name='sub_%s' % name_of_transform, callback=subscriber)

        # Note that this running the below line creates an exchange since none of that name exists before
        sub_id = self.pubsub.create_subscription('subscriber_to_transform_%s' % name_of_transform,
            stream_ids=[stream_id],
            exchange_name='sub_%s' % name_of_transform)

        self.pubsub.activate_subscription(sub_id)

        # Cleanups for the subscriber
        self.addCleanup(sub.stop)
        self.queue_cleanup.append(sub.xn.queue)
        self.addCleanup(self.clean_queues)

        sub.start()

        return ar

    def _check_granule_from_L0_transform(self, ar = None):

        granule_from_transform = ar.get(timeout=20)
        log.debug("Got the following granule from the L0 transform: %s", granule_from_transform)

        # Check the algorithm being applied
        self._check_application_of_L0_algorithm(granule)


    def _check_granule_from_L1_transform(self, ar = None):

        granule_from_transform = ar.get(timeout=20)
        log.debug("Got the following granule from the L1 transform: %s", granule_from_transform)

        # Check the algorithm being applied
        self._check_application_of_L1_algorithm(granule_from_transform)

    def _check_granule_from_L2_transform(self, ar = None):

        granule_from_transform = ar.get(timeout=20)
        log.debug("Got the following granule from the L2 transform: %s", granule_from_transform)

        # Check the algorithm being applied
        self._check_application_of_L2_algorithm(granule_from_transform)

    def _check_application_of_L0_algorithm(self, granule = None):
        """ Check the algorithm applied by the L0 transform """
        pass

    def _check_application_L1_algorithm(self, granule = None):
        """ Check the algorithm applied by the L1 transform """
        pass

    def _check_application_L2_algorithm(self, granule = None):
        """ Check the algorithm applied by the L2 transform """
        pass

    def _publish_for_L0_transform(self, input_stream_id = None, stream_route = None):

        #----------- Publish on that stream so that the transform can receive it --------------------------------
        self._publish_to_transform(input_stream_id, stream_route )

    def _publish_to_transform(self, stream_id = '', stream_route = None):

        pub = StandaloneStreamPublisher(stream_id, stream_route)
        publish_granule = self._get_new_ctd_L1_packet(stream_definition_id=L1_self.stream_def_id, length = 5)
        pub.publish(publish_granule)

        log.debug("Published the following granule: %s", publish_granule)
























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
