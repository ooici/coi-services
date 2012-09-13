#!/usr/bin/env python

'''
@file ion/services/sa/process/test/test_data_process_with_lookup_tables.py
@author Maurice Manning
@test ion.services.sa.process.DataProcessManagementService integration test
'''

from nose.plugins.attrib import attr
from interface.services.icontainer_agent import ContainerAgentClient
from interface.objects import AttachmentType
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
from ion.util.parameter_yaml_IO import get_param_dict

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.coverage import GridDomain, GridShape, CRS
from coverage_model.basic_types import MutabilityEnum, AxisTypeEnum

import base64

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


    

@attr('HARDWARE', group='foo')
class TestDataProcessWithLookupTable(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)


    def test_lookupTableProcessing(self):
        #-------------------------------
        # Create InstrumentModel
        #-------------------------------
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel", model="SBE37IMModel" )
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        print 'test_createTransformsThenActivateInstrument: new InstrumentModel id = ', instModel_id

        #-------------------------------
        # Create InstrumentAgent
        #-------------------------------
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_module="ion.agents.instrument.instrument_agent", driver_class="InstrumentAgent" )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        print 'test_createTransformsThenActivateInstrument: new InstrumentAgent id = ', instAgent_id

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        #-------------------------------
        # Create InstrumentDevice and attachment for lookup table
        #-------------------------------
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        try:
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        print 'test_createTransformsThenActivateInstrument: new InstrumentDevice id = ', instDevice_id

        contents = "this is the lookup table  contents, replace with a file..."
        att = IonObject(RT.Attachment, name='deviceLookupTable', content=base64.encodestring(contents), keywords=['DataProcessInput'], attachment_type=AttachmentType.ASCII)
        deviceAttachment = self.rrclient.create_attachment(instDevice_id, att)
        print 'test_createTransformsThenActivateInstrument: InstrumentDevice attachment id = ', deviceAttachment

        #-------------------------------
        # Create InstrumentAgentInstance to hold configuration information
        #-------------------------------

        driver_config = {
            'dvr_mod' : 'mi.instrument.seabird.sbe37smb.ooicore.driver',
            'dvr_cls' : 'SBE37Driver',
            'workdir' : '/tmp/',
        }

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance", driver_config = driver_config,
                                          comms_device_address='sbe37-simulator.oceanobservatories.org',   comms_device_port=4001,  port_agent_work_dir='/tmp/', port_agent_delimeter=['<<','>>'] )
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)



        #-------------------------------
        # Create CTD Parsed as the first data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def)

        print 'TestDataProcessWithLookupTable: new Stream Definition id = ', instDevice_id

        print 'Creating new CDM data product with a stream definition'

        # Construct temporal and spatial Coordinate Reference System objects
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT])

        # Construct temporal and spatial Domain objects
        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE) # 1d (timeline)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # 1d spatial topology (station/trajectory)

        sdom = sdom.dump()
        tdom = tdom.dump()

        parameter_dictionary = get_param_dict('ctd_parsed_param_dict')

        parameter_dictionary = parameter_dictionary.dump()

        dp_obj = IonObject(RT.DataProduct,
            name='ctd_parsed',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_parsed_data_product = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id, parameter_dictionary)

        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product, PRED.hasStream, None, True)
        print 'TestDataProcessWithLookupTable: Data product streams1 = ', stream_ids

        #-------------------------------
        # Create CTD Raw as the second data product
        #-------------------------------
        print 'TestDataProcessWithLookupTable: Creating new RAW data product with a stream definition'
        raw_stream_def = SBE37_RAW_stream_definition()
        raw_stream_def_id = self.pubsubclient.create_stream_definition(container=raw_stream_def)

        dp_obj = IonObject(RT.DataProduct,
            name='ctd_raw',
            description='raw stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_raw_data_product = self.dataproductclient.create_data_product(dp_obj, raw_stream_def_id, parameter_dictionary)

        print 'new ctd_raw_data_product_id = ', ctd_raw_data_product

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_raw_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_raw_data_product)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_raw_data_product, PRED.hasStream, None, True)
        print 'Data product streams2 = ', stream_ids

        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Data Process Definition
        #-------------------------------
        log.debug("TestDataProcessWithLookupTable: create data process definition ctd_L0_all")
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

        contents = "this is the lookup table  contents for L0 Conductivity - Temperature - Pressure: Data Process Definition, replace with a file..."
        att = IonObject(RT.Attachment, name='processDefinitionLookupTable',content=base64.encodestring(contents), keywords=['DataProcessInput'], attachment_type=AttachmentType.ASCII)
        processDefinitionAttachment = self.rrclient.create_attachment(ctd_L0_all_dprocdef_id, att)
        log.debug("TestDataProcessWithLookupTable:test_createTransformsThenActivateInstrument: InstrumentDevice attachment id %s", str(processDefinitionAttachment) )
        processDefinitionAttachment_obj = self.rrclient.read(processDefinitionAttachment)
        log.debug("TestDataProcessWithLookupTable:test_createTransformsThenActivateInstrument: InstrumentDevice attachment obj %s", str(processDefinitionAttachment_obj) )

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
        log.debug("TestDataProcessWithLookupTable: create output data product L0 conductivity")

        ctd_l0_conductivity_output_dp_obj = IonObject(  RT.DataProduct,
                                                        name='L0_Conductivity',
                                                        description='transform output conductivity',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)

        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_conductivity_output_dp_obj,
                                                                                    outgoing_stream_l0_conductivity_id,
                                                                                    parameter_dictionary)

        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_conductivity_output_dp_id)


        log.debug("TestDataProcessWithLookupTable: create output data product L0 pressure")

        ctd_l0_pressure_output_dp_obj = IonObject(RT.DataProduct,
            name='L0_Pressure',
            description='transform output pressure',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_pressure_output_dp_obj,
                                                                                outgoing_stream_l0_pressure_id,
                                                                                parameter_dictionary)
        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_pressure_output_dp_id)

        log.debug("TestDataProcessWithLookupTable: create output data product L0 temperature")

        ctd_l0_temperature_output_dp_obj = IonObject(   RT.DataProduct,
                                                        name='L0_Temperature',
                                                        description='transform output temperature',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)


        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj,
                                                                                    outgoing_stream_l0_temperature_id,
                                                                                    parameter_dictionary)

        self.output_products['temperature'] = ctd_l0_temperature_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_temperature_output_dp_id)


        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------
        log.debug("TestDataProcessWithLookupTable: create L0 all data_process start")
        try:
            in_prods = []
            in_prods.append(ctd_parsed_data_product)
            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L0_all_dprocdef_id, in_prods, self.output_products)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("TestDataProcessWithLookupTable: create L0 all data_process return")

        contents = "this is the lookup table  contents for L0 Conductivity - Temperature - Pressure: Data Process , replace with a file..."
        att = IonObject(RT.Attachment, name='processLookupTable',content=base64.encodestring(contents), keywords=['DataProcessInput'], attachment_type=AttachmentType.ASCII)
        processAttachment = self.rrclient.create_attachment(ctd_l0_all_data_process_id, att)
        print 'TestDataProcessWithLookupTable: InstrumentDevice attachment id = ', processAttachment




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
#
#        #-------------------------------
#        # Sampling
#        #-------------------------------
#        cmd = AgentCommand(command='initialize')
#        retval = self._ia_client.execute_agent(cmd)
#        print retval
#        log.debug("test_activateInstrument: initialize %s", str(retval))
#
#        time.sleep(2)
#
#        log.debug("test_activateInstrument: Sending go_active command (L4-CI-SA-RQ-334)")
#        cmd = AgentCommand(command='go_active')
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_activateInstrument: return value from go_active %s", str(reply))
#        time.sleep(2)
#        cmd = AgentCommand(command='get_current_state')
#        retval = self._ia_client.execute_agent(cmd)
#        state = retval.result
#        log.debug("test_activateInstrument: current state after sending go_active command %s    (L4-CI-SA-RQ-334)", str(state))
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
#        log.debug("test_activateInstrument: calling reset ")
#        cmd = AgentCommand(command='reset')
#        reply = self._ia_client.execute_agent(cmd)
#        log.debug("test_activateInstrument: return from reset %s", str(reply))
#        time.sleep(2)
#
#        #-------------------------------
#        # Deactivate InstrumentAgentInstance
#        #-------------------------------
#        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

