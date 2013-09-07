#!/usr/bin/env python

'''
@file ion/services/sa/process/test/test_data_process_with_lookup_tables.py
@author Maurice Manning
@test ion.services.sa.process.DataProcessManagementService integration test
'''

from nose.plugins.attrib import attr
from interface.objects import AttachmentType
from pyon.core.exception import BadRequest
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from pyon.public import log, IonObject
from pyon.public import RT, PRED, CFG
from pyon.util.context import LocalContextMixin
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.dm.utility.granule_utils import time_series_domain
DRV_URI_GOOD = CFG.device.sbe37.dvr_egg

import base64

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


    

@attr('INT', group='sa')
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
        self.dataset_management = self.datasetclient
        self.processdispatchclient = ProcessDispatcherServiceClient(node = self.container.node)


    def test_lookupTableProcessing(self):
        #-------------------------------
        # Create InstrumentModel
        #-------------------------------
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel" )
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        log.info('test_createTransformsThenActivateInstrument: new InstrumentModel id = %s', instModel_id)

        #-------------------------------
        # Create InstrumentAgent
        #-------------------------------
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_uri=DRV_URI_GOOD)
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        log.info( 'test_createTransformsThenActivateInstrument: new InstrumentAgent id = %s', instAgent_id)

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

        log.info( 'test_createTransformsThenActivateInstrument: new InstrumentDevice id = %s', instDevice_id)

        contents = "this is the lookup table  contents, replace with a file..."
        att = IonObject(RT.Attachment, name='deviceLookupTable', content=base64.encodestring(contents), keywords=['DataProcessInput'], attachment_type=AttachmentType.ASCII)
        deviceAttachment = self.rrclient.create_attachment(instDevice_id, att)
        log.info( 'test_createTransformsThenActivateInstrument: InstrumentDevice attachment id = %s', deviceAttachment)

        #-------------------------------
        # Create InstrumentAgentInstance to hold configuration information
        #-------------------------------

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance" )
        self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)



        #-------------------------------
        # Create CTD Parsed as the first data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(name='SBE37_CDM', parameter_dictionary_id=pdict_id)

        log.info( 'TestDataProcessWithLookupTable: new Stream Definition id = %s', instDevice_id)

        log.info( 'Creating new CDM data product with a stream definition')

        # Construct temporal and spatial Coordinate Reference System objects
        tdom, sdom = time_series_domain()

        sdom = sdom.dump()
        tdom = tdom.dump()


        dp_obj = IonObject(RT.DataProduct,
            name='ctd_parsed',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_parsed_data_product = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)

        log.info( 'new ctd_parsed_data_product_id = %s', ctd_parsed_data_product)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product, PRED.hasStream, None, True)
        log.info('TestDataProcessWithLookupTable: Data product streams1 = %s', stream_ids)

        #-------------------------------
        # Create CTD Raw as the second data product
        #-------------------------------
        log.info('TestDataProcessWithLookupTable: Creating new RAW data product with a stream definition')
        raw_stream_def_id = self.pubsubclient.create_stream_definition(name='SBE37_RAW', parameter_dictionary_id=pdict_id)

        dp_obj = IonObject(RT.DataProduct,
            name='ctd_raw',
            description='raw stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_raw_data_product = self.dataproductclient.create_data_product(dp_obj, raw_stream_def_id)

        log.info( 'new ctd_raw_data_product_id = %s', ctd_raw_data_product)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_raw_data_product)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_raw_data_product, PRED.hasStream, None, True)
        log.info( 'Data product streams2 = %s', stream_ids)

        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Data Process Definition
        #-------------------------------
        log.debug("TestDataProcessWithLookupTable: create data process definition ctd_L0_all")
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name='ctd_L0_all',
                            description='transform ctd package into three separate L0 streams',
                            module='ion.processes.data.transforms.ctd.ctd_L0_all',
                            class_name='ctd_L0_all')
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

        outgoing_stream_l0_conductivity_id = self.pubsubclient.create_stream_definition(name='L0_Conductivity', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_conductivity_id, ctd_L0_all_dprocdef_id, binding='conductivity' )

        outgoing_stream_l0_pressure_id = self.pubsubclient.create_stream_definition(name='L0_Pressure', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_pressure_id, ctd_L0_all_dprocdef_id, binding='pressure' )

        outgoing_stream_l0_temperature_id = self.pubsubclient.create_stream_definition(name='L0_Temperature', parameter_dictionary_id=pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_temperature_id, ctd_L0_all_dprocdef_id, binding='temperature' )


        log.debug("TestDataProcessWithLookupTable: create output data product L0 conductivity")

        ctd_l0_conductivity_output_dp_obj = IonObject(  RT.DataProduct,
                                                        name='L0_Conductivity',
                                                        description='transform output conductivity',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)

        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_conductivity_output_dp_obj,
                                                                                    outgoing_stream_l0_conductivity_id)

        log.debug("TestDataProcessWithLookupTable: create output data product L0 pressure")

        ctd_l0_pressure_output_dp_obj = IonObject(RT.DataProduct,
            name='L0_Pressure',
            description='transform output pressure',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_pressure_output_dp_obj,
                                                                                outgoing_stream_l0_pressure_id)

        log.debug("TestDataProcessWithLookupTable: create output data product L0 temperature")

        ctd_l0_temperature_output_dp_obj = IonObject(   RT.DataProduct,
                                                        name='L0_Temperature',
                                                        description='transform output temperature',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)


        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj,
                                                                                    outgoing_stream_l0_temperature_id)


        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------
        log.debug("TestDataProcessWithLookupTable: create L0 all data_process start")
        try:
            in_prods = []
            in_prods.append(ctd_parsed_data_product)
            out_prods = [ctd_l0_conductivity_output_dp_id, ctd_l0_pressure_output_dp_id,ctd_l0_temperature_output_dp_id]
            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(
                data_process_definition_id = ctd_L0_all_dprocdef_id,
                in_data_product_ids = in_prods,
                out_data_product_ids = out_prods)

        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("TestDataProcessWithLookupTable: create L0 all data_process return")

        data_process= self.rrclient.read(ctd_l0_all_data_process_id)

        process_ids, _ = self.rrclient.find_objects(subject= ctd_l0_all_data_process_id, predicate= PRED.hasProcess, object_type= RT.Process,id_only=True)
        self.addCleanup(self.processdispatchclient.cancel_process, process_ids[0])

        contents = "this is the lookup table  contents for L0 Conductivity - Temperature - Pressure: Data Process , replace with a file..."
        att = IonObject(RT.Attachment, name='processLookupTable',content=base64.encodestring(contents), keywords=['DataProcessInput'], attachment_type=AttachmentType.ASCII)
        processAttachment = self.rrclient.create_attachment(ctd_l0_all_data_process_id, att)
        log.info( 'TestDataProcessWithLookupTable: InstrumentDevice attachment id = %s', processAttachment)



