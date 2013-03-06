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
from coverage_model import QuantityType

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
        self.queue_cleanup = []

    def _get_new_ctd_packet(self, stream_definition_id, length):

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

        log.debug("created data process definition: id = %s", dprocdef_id)

        #----------- Data Products --------------------------------

        # Construct temporal and spatial Coordinate Reference System objects
        tdom, sdom = time_series_domain()

        sdom = sdom.dump()
        tdom = tdom.dump()

        # Get the stream definition for the stream using the parameter dictionary
        parsed_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctdbp_cdef_sample', id_only=True)
        parsed_stream_def_id = self.pubsub.create_stream_definition(name='parsed', parameter_dictionary_id=parsed_pdict_id)

        log.debug("Got the parsed parameter dictionary: id: %s", parsed_pdict_id)
        log.debug("Got the stream def for parsed input: %s", parsed_stream_def_id)

        # Input data product
        parsed_stream_dp_obj = IonObject(RT.DataProduct,
            name='parsed_stream',
            description='Parsed stream input to CTBP L0 transform',
            temporal_domain = tdom,
            spatial_domain = sdom)

        input_dp_id = self.dataproduct_management.create_data_product(data_product=parsed_stream_dp_obj,
            stream_definition_id=parsed_stream_def_id
        )

        # output data product
        L0_stream_dp_obj = IonObject(RT.DataProduct,
            name='L0_stream',
            description='L0_stream output of CTBP L0 transform',
            temporal_domain = tdom,
            spatial_domain = sdom)

        L0_stream_dp_id = self.dataproduct_management.create_data_product(data_product=L0_stream_dp_obj,
                                                                    stream_definition_id=parsed_stream_def_id
                                                                    )

        # We need the key name here to be "L0_stream", since when the data process is launched, this name goes into
        # the config as in config.process.publish_streams.L0_stream when the config is used to launch the data process
        self.output_products = {'L0_stream' : L0_stream_dp_id}
        out_stream_ids, _ = self.resource_registry.find_objects(L0_stream_dp_id, PRED.hasStream, RT.Stream, True)
        output_stream_id = out_stream_ids[0]

        dproc_id = self.data_process_management.create_data_process( dprocdef_id, [input_dp_id], self.output_products)

        log.debug("Created a data process for ctdbp_L0. id: %s", dproc_id)

        # Activate the data process
        self.data_process_management.activate_data_process(dproc_id)

        #----------- Find the stream that is associated with the input data product when it was created by create_data_product() --------------------------------

        stream_ids, _ = self.resource_registry.find_objects(input_dp_id, PRED.hasStream, RT.Stream, True)

        input_stream_id = stream_ids[0]
        input_stream = self.resource_registry.read(input_stream_id)
        stream_route = input_stream.stream_route

        log.debug("The input stream for the L0 transform: %s", input_stream_id)

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
        publish_granule = self._get_new_ctd_packet(stream_definition_id=parsed_stream_def_id, length = 5)

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


