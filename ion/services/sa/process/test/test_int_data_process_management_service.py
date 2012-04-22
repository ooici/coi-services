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
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
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
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition
from interface.objects import StreamQuery, ExchangeQuery
import gevent
import unittest
from interface.objects import HdfStorage, CouchStorage
from prototype.sci_data.stream_parser import PointSupplementStreamParser
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand



class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


#@attr('HARDWARE', group='sa')
#@unittest.skip('not working')
@attr('INT', group='mmm')
class TestIntDataProcessManagementServiceMultiOut(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2sa.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)


        # Set up the preconditions
        # ingestion configuration parameters
        self.exchange_point_id = 'science_data'
        self.number_of_workers = 2
        self.hdf_storage = HdfStorage(relative_path='ingest')
        self.couch_storage = CouchStorage(datastore_name='test_datastore')
        self.XP = 'science_data'
        self.exchange_name = 'ingestion_queue'

        # Create ingestion configuration and activate it
        ingestion_configuration_id =  self.ingestclient.create_ingestion_configuration(
            exchange_point_id=self.exchange_point_id,
            couch_storage=self.couch_storage,
            hdf_storage=self.hdf_storage,
            number_of_workers=self.number_of_workers
        )
        print 'test_activateInstrument: ingestion_configuration_id', ingestion_configuration_id

        # activate an ingestion configuration
        ret = self.ingestclient.activate_ingestion_configuration(ingestion_configuration_id)
        log.debug("test_activateInstrument: activate = %s"  % str(ret))

    #@unittest.skip('not working')
    def test_createDataProcess(self):

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
            dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new data process definition: %s" %ex)


        # test Data Process Definition creation in rr
        dprocdef_obj = self.dataprocessclient.read_data_process_definition(dprocdef_id)
        self.assertEquals(dprocdef_obj.name,'ctd_L0_all')

        # Create an input instrument
        instrument_obj = IonObject(RT.InstrumentDevice, name='Inst1',description='an instrument that is creating the data product')
        instrument_id, rev = self.rrclient.create(instrument_obj)

        # Register the instrument so that the data producer and stream object are created
        data_producer_id = self.damsclient.register_instrument(instrument_id)
        log.debug("TestIntDataProcessMgmtServiceMultiOut  data_producer_id %s" % data_producer_id)

        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = ctd_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def, name='Simulated CTD data')

        self.dataprocessclient.assign_input_stream_definition_to_data_process_definition(ctd_stream_def_id, dprocdef_id )


        #-------------------------------
        # Input Data Product
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create input data product")
        input_dp_obj = IonObject(RT.DataProduct, name='InputDataProduct', description='some new dp')
        try:
            input_dp_id = self.dataproductclient.create_data_product(input_dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new input data product: %s" %ex)

        self.damsclient.assign_data_product(instrument_id, input_dp_id)

        # Retrieve the stream via the DataProduct->Stream associations
        stream_ids, _ = self.rrclient.find_objects(input_dp_id, PRED.hasStream, None, True)

        log.debug("TestIntDataProcessMgmtServiceMultiOut: in stream_ids "   +  str(stream_ids))
        self.in_stream_id = stream_ids[0]
        log.debug("TestIntDataProcessMgmtServiceMultiOut: Input Stream: "   +  str( self.in_stream_id))

        #-------------------------------
        # Output Data Product
        #-------------------------------

        outgoing_stream_conductivity = L0_conductivity_stream_definition()
        outgoing_stream_conductivity_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_conductivity, name='conductivity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_conductivity_id, dprocdef_id )

        outgoing_stream_pressure = L0_pressure_stream_definition()
        outgoing_stream_pressure_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_pressure, name='pressure')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_pressure_id, dprocdef_id )

        outgoing_stream_temperature = L0_temperature_stream_definition()
        outgoing_stream_temperature_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_temperature, name='temperature')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_temperature_id, dprocdef_id )


        self.output_products={}
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product conductivity")
        output_dp_obj = IonObject(RT.DataProduct, name='conductivity',description='transform output conductivity')
        output_dp_id_1 = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_conductivity_id)
        self.output_products['conductivity'] = output_dp_id_1
        self.dataproductclient.activate_data_product_persistence(data_product_id=output_dp_id_1, persist_data=True, persist_metadata=True)


        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product pressure")
        output_dp_obj = IonObject(RT.DataProduct, name='pressure',description='transform output pressure')
        output_dp_id_2 = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_pressure_id)
        self.output_products['pressure'] = output_dp_id_2
        self.dataproductclient.activate_data_product_persistence(data_product_id=output_dp_id_2, persist_data=True, persist_metadata=True)

        log.debug("TestIntDataProcessMgmtServiceMultiOut: create output data product temperature")
        output_dp_obj = IonObject(RT.DataProduct, name='temperature',description='transform output ')
        output_dp_id_3 = self.dataproductclient.create_data_product(output_dp_obj, outgoing_stream_temperature_id)
        self.output_products['temperature'] = output_dp_id_3
        self.dataproductclient.activate_data_product_persistence(data_product_id=output_dp_id_3, persist_data=True, persist_metadata=True)

#
#        #-------------------------------
#        # Create the data process
#        #-------------------------------
#        log.debug("TestIntDataProcessMgmtServiceMultiOut: create_data_process start")
#        try:
#            dproc_id = self.dataprocessclient.create_data_process(dprocdef_id, input_dp_id, self.output_products)
#        except BadRequest as ex:
#            self.fail("failed to create new data process: %s" %ex)
#
#        log.debug("TestIntDataProcessMgmtServiceMultiOut: create_data_process return")
#
#        # these assigns happen inside create_data_process
#        #self.damsclient.assign_data_product(input_resource_id=dproc_id, data_product_id=output_dp_id_1)
#        #self.damsclient.assign_data_product(input_resource_id=dproc_id, data_product_id=output_dp_id_2)
#        #self.damsclient.assign_data_product(input_resource_id=dproc_id, data_product_id=output_dp_id_3)
#
#
#        #-------------------------------
#        # ProcessDefinition for CTD Stream Publisher
#        #-------------------------------
#        producer_definition = ProcessDefinition()
#        producer_definition.executable = {
#            'module':'ion.processes.data.ctd_stream_publisher',
#            'class':'SimpleCtdPublisher'
#        }
#
#        ctd_sim_procdef_id = self.ProcessDispatchClient.create_process_definition(process_definition=producer_definition)
#
#        configuration = {
#            'process':{
#                'stream_id':self.in_stream_id,
#            }
#        }
#        #begin sending ctd packets
#        ctd_sim_pid = self.ProcessDispatchClient.schedule_process(process_definition_id=ctd_sim_procdef_id, configuration=configuration)
#
#
#        #-------------------------------
#        # Set up listeners to the output streams
#        #-------------------------------
#        #find stream for data products
#        all_streams= {}
#        stream_ids, _ = self.rrclient.find_objects(output_dp_id_1, PRED.hasStream, None, True)
#        conductivity_stream = stream_ids[0]
#        stream_ids, _ = self.rrclient.find_objects(output_dp_id_2, PRED.hasStream, None, True)
#        pressure_stream = stream_ids[0]
#        stream_ids, _ = self.rrclient.find_objects(output_dp_id_3, PRED.hasStream, None, True)
#        temperature_stream = stream_ids[0]
#        log.debug("TestIntDataProcessMgmtServiceMultiOut: all_streams %s", str(all_streams))
#
#
#        #[conductivity_stream, pressure_stream, temperature_stream]
#
#        subscription_id = self.pubsubclient.create_subscription(
#            query=StreamQuery([conductivity_stream ]),
#            exchange_name = 'conductivity_stream_test',
#            name = "test conductivity_stream subscription",
#            )
#
#        pid = self.container.spawn_process(name='dummy_process_for_test',
#            module='pyon.ion.process',
#            cls='SimpleProcess',
#            config={})
#        dummy_process = self.container.proc_manager.procs[pid]
#
#        subscriber_registrar = StreamSubscriberRegistrar(process=dummy_process, node=self.container.node)
#
#        result = gevent.event.AsyncResult()
#        results = []
#        def message_received(message, headers):
#            # Heads
#            log.warn('conductivity_stream data received!  %s ', message)
#            results.append(message)
#            if len(results) >3:
#                result.set(True)
#
#        subscriber = subscriber_registrar.create_subscriber(exchange_name='conductivity_stream_test', callback=message_received)
#        subscriber.start()
#
#        # after the queue has been created it is safe to activate the subscription
#        self.pubsubclient.activate_subscription(subscription_id=subscription_id)
#
#
#        # Assert that we have received data
#        self.assertTrue(result.get(timeout=10))
#
#        # stop the flow parse the messages...
#        self.ProcessDispatchClient.cancel_process(ctd_sim_pid) # kill the ctd simulator process - that is enough data
#
#        #todo: Need a fix to PointSupplementStreamParsern to make this work
#        for message in results:
#
#            psd = PointSupplementStreamParser(stream_definition=outgoing_stream_conductivity_id, stream_granule=message)
#
#            namesList = psd.list_field_names()
#            log.debug('TestIntDataProcessMgmtServiceMultiOut: granule field names:  %s ', str(namesList))
#
#            # Test the handy info method for the names of fields in the stream def
#            self.assertTrue('conductivity' in psd.list_field_names())
#
#
#
#
#        log.debug('TestIntDataProcessMgmtServiceMultiOut: ProcessDispatchClient.cancel_process complete' )
#
#        # See /tmp/transform_output for results.....
#
#        # clean up the data process
#
#        self.dataprocessclient.unassign_input_stream_definition_from_data_process_definition(ctd_stream_def_id, dprocdef_id )
#        self.dataprocessclient.unassign_stream_definition_from_data_process_definition(outgoing_stream_conductivity_id, dprocdef_id )
#        self.dataprocessclient.unassign_stream_definition_from_data_process_definition(outgoing_stream_pressure_id, dprocdef_id )
#        self.dataprocessclient.unassign_stream_definition_from_data_process_definition(outgoing_stream_temperature_id, dprocdef_id )
#        log.debug('TestIntDataProcessMgmtServiceMultiOut: stream definition unassign  complete' )
#
#        try:
#            self.dataprocessclient.delete_data_process(dproc_id)
#        except BadRequest as ex:
#            self.fail("failed to create new data process definition: %s" %ex)
#
#        with self.assertRaises(NotFound) as e:
#            self.dataprocessclient.read_data_process(dproc_id)
#
#        try:
#            self.dataprocessclient.delete_data_process_definition(dprocdef_id)
#        except BadRequest as ex:
#            self.fail("failed to create new data process definition: %s" %ex)
#
#        with self.assertRaises(NotFound) as e:
#            self.dataprocessclient.read_data_process_definition(dprocdef_id)




    #@unittest.skip('not working')
    def test_createDataProcessUsingSim(self):
        #-------------------------------
        # Create InstrumentModel
        #-------------------------------
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel", model_label="SBE37IMModel" )
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        print 'test_createTransformsThenActivateInstrument: new InstrumentModel id = ', instModel_id


        #-------------------------------
        # Create InstrumentAgent
        #-------------------------------
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_module="ion.services.mi.instrument_agent", driver_class="InstrumentAgent" )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        print 'test_createTransformsThenActivateInstrument: new InstrumentAgent id = ', instAgent_id

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        #-------------------------------
        # Create InstrumentDevice
        #-------------------------------
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        try:
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        print 'test_createTransformsThenActivateInstrument: new InstrumentDevice id = ', instDevice_id

        #-------------------------------
        # Create InstrumentAgentInstance to hold configuration information
        #-------------------------------
        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance", svr_addr="localhost",
                                          driver_module="ion.services.mi.drivers.sbe37_driver", driver_class="SBE37Driver",
                                          cmd_port=5556, evt_port=5557, comms_method="ethernet", comms_device_address=CFG.device.sbe37.host, comms_device_port=CFG.device.sbe37.port,
                                          comms_server_address="localhost", comms_server_port=8888)
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)

        #-------------------------------
        # Create CTD Parsed as the first data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def)

        print 'test_createTransformsThenActivateInstrument: new Stream Definition id = ', instDevice_id

        print 'Creating new CDM data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,name='ctd_parsed',description='ctd stream test')
        try:
            ctd_parsed_data_product = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product, PRED.hasStream, None, True)
        print 'test_createTransformsThenActivateInstrument: Data product streams1 = ', stream_ids

        #-------------------------------
        # Create CTD Raw as the second data product
        #-------------------------------
        print 'test_createTransformsThenActivateInstrument: Creating new RAW data product with a stream definition'
        raw_stream_def = SBE37_RAW_stream_definition()
        raw_stream_def_id = self.pubsubclient.create_stream_definition(container=raw_stream_def)

        dp_obj = IonObject(RT.DataProduct,name='ctd_raw',description='raw stream test')
        try:
            ctd_raw_data_product = self.dataproductclient.create_data_product(dp_obj, raw_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new ctd_raw_data_product_id = ', ctd_raw_data_product

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_raw_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_raw_data_product, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_raw_data_product, PRED.hasStream, None, True)
        print 'Data product streams2 = ', stream_ids
        
        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition ctd_L0_all")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L0_all',
                            description='transform ctd package into three separate L0 streams',
                            module='ion.processes.data.transforms.ctd.ctd_L0_all',
                            class_name='ctd_L0_all',
                            process_source='some_source_reference')
        try:
            ctd_L0_all_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new ctd_L0_all data process definition: %s" %ex)
            
            
        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Output Data Products
        #-------------------------------

        outgoing_stream_l0_conductivity = L0_conductivity_stream_definition()
        outgoing_stream_l0_conductivity_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l0_conductivity, name='L0_Conductivity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_conductivity_id, ctd_L0_all_dprocdef_id )

        outgoing_stream_l0_pressure = L0_pressure_stream_definition()
        outgoing_stream_l0_pressure_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l0_pressure, name='L0_Pressure')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_pressure_id, ctd_L0_all_dprocdef_id )

        outgoing_stream_l0_temperature = L0_temperature_stream_definition()
        outgoing_stream_l0_temperature_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l0_temperature, name='L0_Temperature')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_temperature_id, ctd_L0_all_dprocdef_id )


        self.output_products={}
        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 conductivity")
        ctd_l0_conductivity_output_dp_obj = IonObject(RT.DataProduct, name='L0_Conductivity',description='transform output conductivity')
        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_conductivity_output_dp_obj, outgoing_stream_l0_conductivity_id)
        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_conductivity_output_dp_id, persist_data=True, persist_metadata=True)


        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 pressure")
        ctd_l0_pressure_output_dp_obj = IonObject(RT.DataProduct, name='L0_Pressure',description='transform output pressure')
        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_pressure_output_dp_obj, outgoing_stream_l0_pressure_id)
        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_pressure_output_dp_id, persist_data=True, persist_metadata=True)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 temperature")
        ctd_l0_temperature_output_dp_obj = IonObject(RT.DataProduct, name='L0_Temperature',description='transform output temperature')
        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj, outgoing_stream_l0_temperature_id)
        self.output_products['temperature'] = ctd_l0_temperature_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_temperature_output_dp_id, persist_data=True, persist_metadata=True)
        
        
        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L0 all data_process start")
        try:
            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L0_all_dprocdef_id, ctd_parsed_data_product, self.output_products)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L0 all data_process return")
        
        
#       #-------------------------------
#        # Set up listeners to the output streams
#        #-------------------------------
#        #find stream for data products
#        all_streams= {}
#        stream_ids, _ = self.rrclient.find_objects(ctd_l0_conductivity_output_dp_id, PRED.hasStream, None, True)
#        conductivity_stream = stream_ids[0]
#        stream_ids, _ = self.rrclient.find_objects(ctd_l0_pressure_output_dp_id, PRED.hasStream, None, True)
#        pressure_stream = stream_ids[0]
#        stream_ids, _ = self.rrclient.find_objects(ctd_l0_temperature_output_dp_id, PRED.hasStream, None, True)
#        temperature_stream = stream_ids[0]
#        log.debug("TestIntDataProcessMgmtServiceMultiOut: all_streams %s", str(all_streams))
#
#
#        #[conductivity_stream, pressure_stream, temperature_stream]
#
#        subscription_id = self.pubsubclient.create_subscription(
#            query=StreamQuery([conductivity_stream ]),
#            exchange_name = 'conductivity_stream_test',
#            name = "test conductivity_stream subscription",
#            )
#
#        pid = self.container.spawn_process(name='dummy_process_for_test',
#            module='pyon.ion.process',
#            cls='SimpleProcess',
#            config={})
#        dummy_process = self.container.proc_manager.procs[pid]
#
#        subscriber_registrar = StreamSubscriberRegistrar(process=dummy_process, node=self.container.node)
#
#        result = gevent.event.AsyncResult()
#        results = []
#        def message_received(message, headers):
#            # Heads
#            log.warn('conductivity_stream data received!  %s ', message)
#            results.append(message)
#            if len(results) >3:
#                result.set(True)
#
#        subscriber = subscriber_registrar.create_subscriber(exchange_name='conductivity_stream_test', callback=message_received)
#        subscriber.start()
#
#        # after the queue has been created it is safe to activate the subscription
#        self.pubsubclient.activate_subscription(subscription_id=subscription_id)
#
#        #-------------------------------
#        # Launch InstrumentAgentInstance, connect to the resource agent client
#        #-------------------------------
#        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)
#
#        inst_agent_instance_obj= self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
#        print 'test_createTransformsThenActivateInstrument: Instrument agent instance obj: = ', inst_agent_instance_obj
#
#        # Start a resource agent client to talk with the instrument agent.
#        self._ia_client = ResourceAgentClient('iaclient', name=inst_agent_instance_obj.agent_process_id,  process=FakeProcess())
#        print 'activate_instrument: got ia client %s', self._ia_client
#        log.debug(" test_createTransformsThenActivateInstrument:: got ia client %s", str(self._ia_client))
#
            
#        #-------------------------------
#        # Sampling
#        #-------------------------------
#        cmd = AgentCommand(command='initialize')
#        retval = self._ia_client.execute_agent(cmd)
#        print retval
#        log.debug("test_createTransformsThenActivateInstrument:: initialize %s", str(retval))
#        time.sleep(2)
#
#        cmd = AgentCommand(command='go_active')
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_activateInstrument: go_active %s", str(reply))
#        time.sleep(2)
#
#        cmd = AgentCommand(command='run')
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_activateInstrument: run %s", str(reply))
#        time.sleep(2)
#
#        log.debug("test_activateInstrument: calling acquire_sample ")
#        cmd = AgentCommand(command='acquire_sample')
#        reply = self._ia_client.execute(cmd)
#        log.debug("test_activateInstrument: return from acquire_sample %s", str(reply))
#        time.sleep(2)
#
#        log.debug("test_activateInstrument: calling acquire_sample 2")
#        cmd = AgentCommand(command='acquire_sample')
#        reply = self._ia_client.execute(cmd)
#        log.debug("test_activateInstrument: return from acquire_sample 2   %s", str(reply))
#        time.sleep(2)
#
#        log.debug("test_activateInstrument: calling acquire_sample 3")
#        cmd = AgentCommand(command='acquire_sample')
#        reply = self._ia_client.execute(cmd)
#        log.debug("test_activateInstrument: return from acquire_sample 3   %s", str(reply))
#        time.sleep(2)
#
#        log.debug("test_activateInstrument: calling go_inactive ")
#        cmd = AgentCommand(command='go_inactive')
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_activateInstrument: return from go_inactive %s", str(reply))
#        time.sleep(2)
#
#        log.debug("test_activateInstrument: calling reset ")
#        cmd = AgentCommand(command='reset')
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_activateInstrument: return from reset %s", str(reply))
#        time.sleep(2)
#