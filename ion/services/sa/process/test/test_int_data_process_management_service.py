#!/usr/bin/env python

'''
@file ion/services/sa/process/test/test_int_data_process_management_service.py
@author Maurice Manning
@test ion.services.sa.process.DataProcessManagementService integration test
'''

from nose.plugins.attrib import attr
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.public import Container, log, IonObject
from pyon.util.containers import DotDict
from pyon.public import CFG, RT, LCS, PRED, StreamPublisher, StreamSubscriber
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.util.context import LocalContextMixin
from interface.services.icontainer_agent import ContainerAgentClient
import time
from pyon.util.int_test import IonIntegrationTestCase
import unittest

class FakeProcess(LocalContextMixin):
    name = ''

@attr('INT', group='mmm')
#@unittest.skip('need to fix...')
class TestIntDataProcessManagementService(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2sa.yml')


        # Now create client to DataProcessManagementService
        self.Processclient = DataProcessManagementServiceClient(node=self.container.node)
        self.RRclient = ResourceRegistryServiceClient(node=self.container.node)
        self.DAMSclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.DPMSclient = DataProductManagementServiceClient(node=self.container.node)
        self.PubSubClient = PubsubManagementServiceClient(node=self.container.node)

    def test_createDataProcess(self):


        #-------------------------------
        # Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessManagementService: create data process definition")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='data_process_definition',
                            description='some new dpd',
                            process_source='some_source_reference')
        try:
            dprocdef_id = self.Processclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)

        # Create an input instrument
        instrument_obj = IonObject(RT.InstrumentDevice, name='Inst1',description='an instrument that is creating the data product')
        instrument_id, rev = self.RRclient.create(instrument_obj)

        # Register the instrument so that the data producer and stream object are created
        data_producer_id = self.DAMSclient.register_instrument(instrument_id)
        log.debug("TestIntDataProcessManagementService  data_producer_id %s" % data_producer_id)



        #-------------------------------
        # Input Data Product
        #-------------------------------
        log.debug("TestIntDataProcessManagementService: create input data product")
        input_dp_obj = IonObject(RT.DataProduct, name='InputDataProduct', description='some new dp')
        try:
            input_dp_id = self.DPMSclient.create_data_product(input_dp_obj, instrument_id)
        except BadRequest as ex:
            self.fail("failed to create new input data product: %s" %ex)

        # Retrieve the stream via the DataProduct->Stream associations
        stream_ids, _ = self.RRclient.find_objects(input_dp_id, PRED.hasStream, None, True)

        log.debug("TestIntDataProcessManagementService: in stream_ids "   +  str(stream_ids))
        self.in_stream_id = stream_ids[0]
        log.debug("TestIntDataProcessManagementService: Input Stream: "   +  str( self.in_stream_id))

        #-------------------------------
        # Output Data Product
        #-------------------------------
        log.debug("TestIntDataProcessManagementService: create output data product")
        output_dp_obj = IonObject(RT.DataProduct, name='OutDataProduct',description='transform output')
        output_dp_id = self.DPMSclient.create_data_product(output_dp_obj)

        # this will NOT create a stream for the product becuase the data process (source) resource has not been created yet.

        #-------------------------------
        # Create the data process
        #-------------------------------
        log.debug("TestIntDataProcessManagementService: create_data_process start")
        try:
            dproc_id = self.Processclient.create_data_process(dprocdef_id, input_dp_id, output_dp_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        self.DAMSclient.assign_data_product(dproc_id, output_dp_id, False)

        log.debug("TestIntDataProcessManagementService: create_data_process return")

        #-------------------------------
        # Producer (Sample Input)
        #-------------------------------
        # Create a producing example process
        # cheat to make a publisher object to send messages in the test.
        # it is really hokey to pass process=self.cc but it works
        stream_route = self.PubSubClient.register_producer(exchange_name='producer_doesnt_have_a_name1', stream_id=self.in_stream_id)
        self.ctd_stream1_publisher = StreamPublisher(node=self.container.node, name=('science_data',stream_route.routing_key), process=self.container)

        num = 3
        msg = dict(num=str(num))
        self.ctd_stream1_publisher.publish(msg)

        time.sleep(1)

        num = 5
        msg = dict(num=str(num))
        self.ctd_stream1_publisher.publish(msg)

        time.sleep(1)

        num = 9
        msg = dict(num=str(num))
        self.ctd_stream1_publisher.publish(msg)

        # See /tmp/transform_output for results.....

        # clean up the data process
        try:
            self.Processclient.delete_data_process(dproc_id)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)

        try:
            self.Processclient.delete_data_process_definition(dprocdef_id)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)

