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
from pyon.public import Container, log, IonObject
from pyon.public import RT, LCS
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.util.context import LocalContextMixin

from pyon.util.int_test import IonIntegrationTestCase
import unittest

class FakeProcess(LocalContextMixin):
    name = ''

@attr('INT', group='sa')
@unittest.skip('coi/dm/sa services not working yet for integration tests to pass')
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
            dprocd_id = self.Processclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)

        # Create an input instrument
        instrument_obj = IonObject(RT.InstrumentDevice, name='Inst1',description='an instrument that is creating the data product')
        instrument_id, rev = self.RRclient.create(instrument_obj)

        # Register the instrument so that the data producer and stream object are created
        data_producer_id = self.DAMSclient.register_instrument(instrument_id)
        log.debug("TestIntDataProcessManagementService  data_producer_id %s" % data_producer_id)

        # Retrieve the stream via the Instrument->DataProducer->Stream associations
        stream_ids, _ = self.RRclient.find_objects(data_producer_id, PRED.hasStream, None, True)
        log.debug("TestIntDataProcessManagementService: stream_ids "   +  str(stream_ids))
        in_stream_id = stream_ids[0]
        log.debug("TestIntDataProcessManagementService: Input Stream: "   +  str(in_stream_id))

        #-------------------------------
        # Input Data Product
        #-------------------------------
        log.debug("TestIntDataProcessManagementService: create input data product")
        input_dp_obj = IonObject(RT.DataProduct, name='InputDataProduct', description='some new dp')
        try:
            input_dp_id = self.DPMSclient.create_data_product(input_dp_obj, instrument_id)
        except BadRequest as ex:
            self.fail("failed to create new input data product: %s" %ex)


        #-------------------------------
        # Output Data Product
        #-------------------------------
        log.debug("TestIntDataProcessManagementService: create output data product")
        output_dp_obj = IonObject(RT.DataProduct, name='OutDataProduct',description='transform output')
        output_dp_id = self.DPMSclient.create_data_product(output_dp_obj)

        #-------------------------------
        # Create the data process
        #-------------------------------
        log.debug("TestIntDataProcessManagementService: create_data_process start")
        try:
            dprocd_id = self.Processclient.create_data_process(dprocd_id, input_dp_id, output_dp_id)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)

        log.debug("TestIntDataProcessManagementService: create_data_process return")

        #-------------------------------
        # Producer (Sample Input)
        #-------------------------------
        # Create a producing example process
#        id_p = self.container.spawn_process('myproducer', 'ion.services.dm.transformation.example.transform_example', 'TransformExampleProducer', {'process':{'type':'stream_process','publish_streams':{'out_stream':in_stream_id}},'stream_producer':{'interval':4000}})
#        self.container.proc_manager.procs['%s.%s' %(self.container.id,id_p)].start()

        

