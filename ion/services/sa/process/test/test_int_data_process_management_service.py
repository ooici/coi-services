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
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.public import Container, log, IonObject
from pyon.util.containers import DotDict
from pyon.public import CFG, RT, LCS, PRED, StreamPublisher, StreamSubscriber, StreamPublisherRegistrar, StreamSubscriberRegistrar
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.util.context import LocalContextMixin
import time
from pyon.util.int_test import IonIntegrationTestCase
from prototype.sci_data.stream_defs import ctd_stream_definition, L0_pressure_stream_definition, L0_temperature_stream_definition, L0_conductivity_stream_definition
from interface.objects import StreamQuery, ExchangeQuery
import gevent
import unittest
from interface.objects import HdfStorage, CouchStorage
from prototype.sci_data.stream_parser import PointSupplementStreamParser


class FakeProcess(LocalContextMixin):
    name = ''

@attr('INT', group='sa')
#@unittest.skip('not working')
class TestIntDataProcessManagementService(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2sa.yml')


        # Now create client to DataProcessManagementService
        self.Processclient = DataProcessManagementServiceClient(node=self.container.node)
        self.RRclient = ResourceRegistryServiceClient(node=self.container.node)
        self.DAMSclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.DPMSclient = DataProductManagementServiceClient(node=self.container.node)
        self.PubSubClient = PubsubManagementServiceClient(node=self.container.node)
        self.ProcessDispatchclient = ProcessDispatcherServiceClient(node=self.container.node)

#    def test_createDataProcess(self):
#
#
#        #-------------------------------
#        # Data Process Definition
#        #-------------------------------
#        log.debug("TestIntDataProcessManagementService: create data process definition")
#        dpd_obj = IonObject(RT.DataProcessDefinition,
#                            name='data_process_definition',
#                            description='some new dpd',
#                            module='ion.processes.data.transforms.transform_example',
#                            class_name='TransformExample',
#                            process_source='some_source_reference')
#        try:
#            dprocdef_id = self.Processclient.create_data_process_definition(dpd_obj)
#        except BadRequest as ex:
#            self.fail("failed to create new data process definition: %s" %ex)
#
#
#        # test Data Process Definition creation in rr
#        dprocdef_obj = self.Processclient.read_data_process_definition(dprocdef_id)
#        self.assertEquals(dprocdef_obj.name,'data_process_definition')
#
#        # Create an input instrument
#        instrument_obj = IonObject(RT.InstrumentDevice, name='Inst1',description='an instrument that is creating the data product')
#        instrument_id, rev = self.RRclient.create(instrument_obj)
#
#        # Register the instrument so that the data producer and stream object are created
#        data_producer_id = self.DAMSclient.register_instrument(instrument_id)
#        log.debug("TestIntDataProcessManagementService  data_producer_id %s" % data_producer_id)
#
#        # create a stream definition for the data from the ctd simulator
#        ctd_stream_def = ctd_stream_definition()
#        ctd_stream_def_id = self.PubSubClient.create_stream_definition(container=ctd_stream_def, name='Simulated CTD data')
#        self.Processclient.assign_input_stream_definition_to_data_process_definition(ctd_stream_def_id, dprocdef_id )
#
#        #-------------------------------
#        # Input Data Product
#        #-------------------------------
#        log.debug("TestIntDataProcessManagementService: create input data product")
#        input_dp_obj = IonObject(RT.DataProduct, name='InputDataProduct', description='some new dp')
#        try:
#            input_dp_id = self.DPMSclient.create_data_product(input_dp_obj, ctd_stream_def_id)
#        except BadRequest as ex:
#            self.fail("failed to create new input data product: %s" %ex)
#
#        self.DAMSclient.assign_data_product(instrument_id, input_dp_id)
#
#        # Retrieve the stream via the DataProduct->Stream associations
#        stream_ids, _ = self.RRclient.find_objects(input_dp_id, PRED.hasStream, None, True)
#
#        log.debug("TestIntDataProcessManagementService: in stream_ids "   +  str(stream_ids))
#        self.in_stream_id = stream_ids[0]
#        log.debug("TestIntDataProcessManagementService: Input Stream: "   +  str( self.in_stream_id))
#
#        #-------------------------------
#        # Output Data Product
#        #-------------------------------
#        log.debug("TestIntDataProcessManagementService: create output data product")
#        output_dp_obj = IonObject(RT.DataProduct, name='OutDataProduct',description='transform output')
#        output_dp_id = self.DPMSclient.create_data_product(output_dp_obj, '')
#
#        # this will NOT create a stream for the product becuase the data process (source) resource has not been created yet.
#
#        #-------------------------------
#        # Create the data process
#        #-------------------------------
#        log.debug("TestIntDataProcessManagementService: create_data_process start")
#        try:
#            dproc_id = self.Processclient.create_data_process(dprocdef_id, input_dp_id, {"out":output_dp_id})
#        except BadRequest as ex:
#            self.fail("failed to create new data process: %s" %ex)
#
#        #self.DAMSclient.assign_data_product(dproc_id, output_dp_id, False)
#
#        log.debug("TestIntDataProcessManagementService: create_data_process return")
#
#        #-------------------------------
#        # Producer (Sample Input)
#        #-------------------------------
#        # Create a producing example process
#        # cheat to make a publisher object to send messages in the test.
#        # it is really hokey to pass process=self.cc but it works
#        #stream_route = self.PubSubClient.register_producer(exchange_name='producer_doesnt_have_a_name1', stream_id=self.in_stream_id)
#        #self.ctd_stream1_publisher = StreamPublisher(node=self.container.node, name=('science_data',stream_route.routing_key), process=self.container)
#
#
##        pid = self.container.spawn_process(name='dummy_process_for_test',
##            module='pyon.ion.process',
##            cls='SimpleProcess',
##            config={})
##        dummy_process = self.container.proc_manager.procs[pid]
##
##        publisher_registrar = StreamPublisherRegistrar(process=dummy_process, node=self.container.node)
##        self.ctd_stream1_publisher = publisher_registrar.create_publisher(stream_id=self.in_stream_id)
##
##        msg = {'num':'3'}
##        self.ctd_stream1_publisher.publish(msg)
##
##        time.sleep(1)
##
##        msg = {'num':'5'}
##        self.ctd_stream1_publisher.publish(msg)
##
##        time.sleep(1)
##
##        msg = {'num':'9'}
##        self.ctd_stream1_publisher.publish(msg)
#
#        # See /tmp/transform_output for results.....
#
#        # clean up the data process
#        self.Processclient.unassign_input_stream_definition_from_data_process_definition(ctd_stream_def_id, dprocdef_id )
#        log.debug('TestIntDataProcessMgmtServiceMultiOut: stream definition unassign  complete' )
#
#        try:
#            self.Processclient.delete_data_process(dproc_id)
#        except BadRequest as ex:
#            self.fail("failed to create new data process definition: %s" %ex)
#
#        with self.assertRaises(NotFound) as e:
#            self.Processclient.read_data_process(dproc_id)
#
#        try:
#            self.Processclient.delete_data_process_definition(dprocdef_id)
#        except BadRequest as ex:
#            self.fail("failed to create new data process definition: %s" %ex)
#
#        with self.assertRaises(NotFound) as e:
#            self.Processclient.read_data_process_definition(dprocdef_id)


@attr('INT', group='sa')
@unittest.skip('not working')
class TestIntDataProcessManagementServiceMultiOut(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2sa.yml')


        # Now create client to DataProcessManagementService
        self.Processclient = DataProcessManagementServiceClient(node=self.container.node)
        self.RRclient = ResourceRegistryServiceClient(node=self.container.node)
        self.DAMSclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.DPMSclient = DataProductManagementServiceClient(node=self.container.node)
        self.PubSubClient = PubsubManagementServiceClient(node=self.container.node)
        self.ProcessDispatchClient = ProcessDispatcherServiceClient(node=self.container.node)
        self.IngestClient = IngestionManagementServiceClient(node=self.container.node)

    def test_createDataProcess(self):


        # Set up the preconditions
        # ingestion configuration parameters
        self.exchange_point_id = 'science_data'
        self.number_of_workers = 2
        self.hdf_storage = HdfStorage(relative_path='ingest')
        self.couch_storage = CouchStorage(datastore_name='test_datastore')
        self.XP = 'science_data'
        self.exchange_name = 'ingestion_queue'

        # Create ingestion configuration and activate it
        ingestion_configuration_id =  self.IngestClient.create_ingestion_configuration(
            exchange_point_id=self.exchange_point_id,
            couch_storage=self.couch_storage,
            hdf_storage=self.hdf_storage,
            number_of_workers=self.number_of_workers
        )
        print 'test_activateInstrument: ingestion_configuration_id', ingestion_configuration_id

        # activate an ingestion configuration
        ret = self.IngestClient.activate_ingestion_configuration(ingestion_configuration_id)
        log.debug("test_activateInstrument: activate = %s"  % str(ret))



        #-------------------------------
        # Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L0_all',
                            description='transform ctd package into three separate L0 streams',
                            module='ion.processes.data.transforms.ctd.ctd_L0_all',
                            class_name='ctd_L0_all',
                            process_source='some_source_reference')
        try:
            dprocdef_id = self.Processclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)


        # test Data Process Definition creation in rr
        dprocdef_obj = self.Processclient.read_data_process_definition(dprocdef_id)
        self.assertEquals(dprocdef_obj.name,'ctd_L0_all')

        # Create an input instrument
        instrument_obj = IonObject(RT.InstrumentDevice, name='Inst1',description='an instrument that is creating the data product')
        instrument_id, rev = self.RRclient.create(instrument_obj)

        # Register the instrument so that the data producer and stream object are created
        data_producer_id = self.DAMSclient.register_instrument(instrument_id)
        log.debug("TestIntDataProcessMgmtServiceMultiOut  data_producer_id %s" % data_producer_id)

        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = ctd_stream_definition()
        ctd_stream_def_id = self.PubSubClient.create_stream_definition(container=ctd_stream_def, name='Simulated CTD data')

        self.Processclient.assign_input_stream_definition_to_data_process_definition(ctd_stream_def_id, dprocdef_id )


        #-------------------------------
        # Input Data Product
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create input data product")
        input_dp_obj = IonObject(RT.DataProduct, name='InputDataProduct', description='some new dp')
        try:
            input_dp_id = self.DPMSclient.create_data_product(input_dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new input data product: %s" %ex)

        self.DAMSclient.assign_data_product(instrument_id, input_dp_id)

        # Retrieve the stream via the DataProduct->Stream associations
        stream_ids, _ = self.RRclient.find_objects(input_dp_id, PRED.hasStream, None, True)

        log.debug("TestIntDataProcessMgmtServiceMultiOut: in stream_ids "   +  str(stream_ids))
        self.in_stream_id = stream_ids[0]
        log.debug("TestIntDataProcessMgmtServiceMultiOut: Input Stream: "   +  str( self.in_stream_id))

        #-------------------------------
        # Output Data Product
        #-------------------------------

        outgoing_stream_conductivity = L0_conductivity_stream_definition()
        outgoing_stream_conductivity_id = self.PubSubClient.create_stream_definition(container=outgoing_stream_conductivity, name='conductivity')
        self.Processclient.assign_stream_definition_to_data_process_definition(outgoing_stream_conductivity_id, dprocdef_id )

        outgoing_stream_pressure = L0_pressure_stream_definition()
        outgoing_stream_pressure_id = self.PubSubClient.create_stream_definition(container=outgoing_stream_pressure, name='pressure')
        self.Processclient.assign_stream_definition_to_data_process_definition(outgoing_stream_pressure_id, dprocdef_id )

        outgoing_stream_temperature = L0_temperature_stream_definition()
        outgoing_stream_temperature_id = self.PubSubClient.create_stream_definition(container=outgoing_stream_temperature, name='temperature')
        self.Processclient.assign_stream_definition_to_data_process_definition(outgoing_stream_temperature_id, dprocdef_id )


        self.output_products={}
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product conductivity")
        output_dp_obj = IonObject(RT.DataProduct, name='conductivity',description='transform output conductivity')
        output_dp_id_1 = self.DPMSclient.create_data_product(output_dp_obj, outgoing_stream_conductivity_id)
        self.output_products['conductivity'] = output_dp_id_1
        self.DPMSclient.activate_data_product_persistence(data_product_id=output_dp_id_1, persist_data=True, persist_metadata=True)


        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product pressure")
        output_dp_obj = IonObject(RT.DataProduct, name='pressure',description='transform output pressure')
        output_dp_id_2 = self.DPMSclient.create_data_product(output_dp_obj, outgoing_stream_pressure_id)
        self.output_products['pressure'] = output_dp_id_2
        self.DPMSclient.activate_data_product_persistence(data_product_id=output_dp_id_2, persist_data=True, persist_metadata=True)

        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product temperature")
        output_dp_obj = IonObject(RT.DataProduct, name='temperature',description='transform output ')
        output_dp_id_3 = self.DPMSclient.create_data_product(output_dp_obj, outgoing_stream_temperature_id)
        self.output_products['temperature'] = output_dp_id_3
        self.DPMSclient.activate_data_product_persistence(data_product_id=output_dp_id_3, persist_data=True, persist_metadata=True)


        #-------------------------------
        # Create the data process
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create_data_process start")
        try:
            dproc_id = self.Processclient.create_data_process(dprocdef_id, input_dp_id, self.output_products)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("TestIntDataProcessMgmtServiceMultiOut: create_data_process return")

        # these assigns happen inside create_data_process
        #self.DAMSclient.assign_data_product(input_resource_id=dproc_id, data_product_id=output_dp_id_1)
        #self.DAMSclient.assign_data_product(input_resource_id=dproc_id, data_product_id=output_dp_id_2)
        #self.DAMSclient.assign_data_product(input_resource_id=dproc_id, data_product_id=output_dp_id_3)


        #-------------------------------
        # ProcessDefinition for CTD Stream Publisher
        #-------------------------------
        producer_definition = ProcessDefinition()
        producer_definition.executable = {
            'module':'ion.processes.data.ctd_stream_publisher',
            'class':'SimpleCtdPublisher'
        }

        ctd_sim_procdef_id = self.ProcessDispatchClient.create_process_definition(process_definition=producer_definition)

        configuration = {
            'process':{
                'stream_id':self.in_stream_id,
            }
        }
        #begin sending ctd packets
        ctd_sim_pid = self.ProcessDispatchClient.schedule_process(process_definition_id=ctd_sim_procdef_id, configuration=configuration)


        #-------------------------------
        # Set up listeners to the output streams
        #-------------------------------
        #find stream for data products
        all_streams= {}
        stream_ids, _ = self.RRclient.find_objects(output_dp_id_1, PRED.hasStream, None, True)
        conductivity_stream = stream_ids[0]
        stream_ids, _ = self.RRclient.find_objects(output_dp_id_2, PRED.hasStream, None, True)
        pressure_stream = stream_ids[0]
        stream_ids, _ = self.RRclient.find_objects(output_dp_id_3, PRED.hasStream, None, True)
        temperature_stream = stream_ids[0]
        log.debug("TestIntDataProcessMgmtServiceMultiOut: all_streams %s", str(all_streams))


        #[conductivity_stream, pressure_stream, temperature_stream]

        subscription_id = self.PubSubClient.create_subscription(
            query=StreamQuery([conductivity_stream ]),
            exchange_name = 'conductivity_stream_test',
            name = "test conductivity_stream subscription",
            )

        pid = self.container.spawn_process(name='dummy_process_for_test',
            module='pyon.ion.process',
            cls='SimpleProcess',
            config={})
        dummy_process = self.container.proc_manager.procs[pid]

        subscriber_registrar = StreamSubscriberRegistrar(process=dummy_process, node=self.container.node)

        result = gevent.event.AsyncResult()
        results = []
        def message_received(message, headers):
            # Heads
            log.warn('conductivity_stream data received!  %s ', message)
            results.append(message)
            if len(results) >3:
                result.set(True)

        subscriber = subscriber_registrar.create_subscriber(exchange_name='conductivity_stream_test', callback=message_received)
        subscriber.start()

        # after the queue has been created it is safe to activate the subscription
        self.PubSubClient.activate_subscription(subscription_id=subscription_id)


        # Assert that we have received data
        self.assertTrue(result.get(timeout=10))

        # stop the flow parse the messages...
        self.ProcessDispatchClient.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data

        #todo: Need a fix to PointSupplementStreamParsern to make this work
#        for message in results:
#
#            psd = PointSupplementStreamParser(stream_definition=outgoing_stream_conductivity_id, stream_granule=message)
#
#            psd.list_field_names()
#
#            # Test the handy info method for the names of fields in the stream def
#            self.assertTrue('conductivity' in psd.list_field_names())




        log.debug('TestIntDataProcessMgmtServiceMultiOut: ProcessDispatchClient.cancel_process complete' )

        # See /tmp/transform_output for results.....

        # clean up the data process

        self.Processclient.unassign_input_stream_definition_from_data_process_definition(ctd_stream_def_id, dprocdef_id )
        self.Processclient.unassign_stream_definition_from_data_process_definition(outgoing_stream_conductivity_id, dprocdef_id )
        self.Processclient.unassign_stream_definition_from_data_process_definition(outgoing_stream_pressure_id, dprocdef_id )
        self.Processclient.unassign_stream_definition_from_data_process_definition(outgoing_stream_temperature_id, dprocdef_id )
        log.debug('TestIntDataProcessMgmtServiceMultiOut: stream definition unassign  complete' )

        try:
            self.Processclient.delete_data_process(dproc_id)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)

        with self.assertRaises(NotFound) as e:
            self.Processclient.read_data_process(dproc_id)

        try:
            self.Processclient.delete_data_process_definition(dprocdef_id)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)

        with self.assertRaises(NotFound) as e:
            self.Processclient.read_data_process_definition(dprocdef_id)

