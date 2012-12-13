from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

from prototype.sci_data.stream_defs import ctd_stream_definition, L0_pressure_stream_definition, L0_temperature_stream_definition, L0_conductivity_stream_definition
from prototype.sci_data.stream_defs import L1_pressure_stream_definition, L1_temperature_stream_definition, L1_conductivity_stream_definition, L2_practical_salinity_stream_definition, L2_density_stream_definition
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter, SBE37ProtocolEvent

from pyon.public import log
from nose.plugins.attrib import attr
from coverage_model.coverage import CRS, GridDomain, GridShape
from coverage_model.basic_types import AxisTypeEnum, MutabilityEnum
from ion.util.parameter_yaml_IO import get_param_dict
from ion.services.dm.utility.granule_utils import time_series_domain

from ion.services.dm.utility.granule.taxonomy import TaxyTool

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG, RT, LCS, PRED
from pyon.core.exception import BadRequest, NotFound, Conflict

from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from pyon.agent.agent import ResourceAgentClient

from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from mock import patch
import time
import gevent

from pyon.util.context import LocalContextMixin

from interface.objects import ProcessStateEnum, StreamConfiguration, AgentCommand, ProcessDefinition
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType

# Used to validate param config retrieved from driver.
PARAMS = {
    SBE37Parameter.OUTPUTSAL : bool,
    SBE37Parameter.OUTPUTSV : bool,
    SBE37Parameter.NAVG : int,
    SBE37Parameter.SAMPLENUM : int,
    SBE37Parameter.INTERVAL : int,
    SBE37Parameter.STORETIME : bool,
    SBE37Parameter.TXREALTIME : bool,
    SBE37Parameter.SYNCMODE : bool,
    SBE37Parameter.SYNCWAIT : int,
    SBE37Parameter.TCALDATE : tuple,
    SBE37Parameter.TA0 : float,
    SBE37Parameter.TA1 : float,
    SBE37Parameter.TA2 : float,
    SBE37Parameter.TA3 : float,
    SBE37Parameter.CCALDATE : tuple,
    SBE37Parameter.CG : float,
    SBE37Parameter.CH : float,
    SBE37Parameter.CI : float,
    SBE37Parameter.CJ : float,
    SBE37Parameter.WBOTC : float,
    SBE37Parameter.CTCOR : float,
    SBE37Parameter.CPCOR : float,
    SBE37Parameter.PCALDATE : tuple,
    SBE37Parameter.PA0 : float,
    SBE37Parameter.PA1 : float,
    SBE37Parameter.PA2 : float,
    SBE37Parameter.PTCA0 : float,
    SBE37Parameter.PTCA1 : float,
    SBE37Parameter.PTCA2 : float,
    SBE37Parameter.PTCB0 : float,
    SBE37Parameter.PTCB1 : float,
    SBE37Parameter.PTCB2 : float,
    SBE37Parameter.POFFSET : float,
    SBE37Parameter.RCALDATE : tuple,
    SBE37Parameter.RTCA0 : float,
    SBE37Parameter.RTCA1 : float,
    SBE37Parameter.RTCA2 : float
}

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('HARDWARE', group='sa')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestCTDTransformsIntegration(IonIntegrationTestCase):
    pdict_id = None

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        print 'started services'

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.dataset_management = self.datasetclient

    def create_logger(self, name, stream_id=''):

        # logger process
        producer_definition = ProcessDefinition(name=name+'_logger')
        producer_definition.executable = {
            'module':'ion.processes.data.stream_granule_logger',
            'class':'StreamGranuleLogger'
        }

        logger_procdef_id = self.processdispatchclient.create_process_definition(process_definition=producer_definition)
        configuration = {
            'process':{
                'stream_id':stream_id,
                }
        }
        pid = self.processdispatchclient.schedule_process(process_definition_id= logger_procdef_id, configuration=configuration)

        return pid

    def _create_instrument_model(self):

        instModel_obj = IonObject(  RT.InstrumentModel,
            name='SBE37IMModel',
            description="SBE37IMModel"  )
        instModel_id = self.imsclient.create_instrument_model(instModel_obj)

        return instModel_id

    def _create_instrument_agent(self, instModel_id):

        raw_config = StreamConfiguration(stream_name='raw', parameter_dictionary_name='ctd_raw_param_dict', records_per_granule=2, granule_publish_rate=5 )
        parsed_config = StreamConfiguration(stream_name='parsed', parameter_dictionary_name='ctd_parsed_param_dict', records_per_granule=2, granule_publish_rate=5 )

        instAgent_obj = IonObject(RT.InstrumentAgent,
                                    name='agent007',
                                    description="SBE37IMAgent",
                                    driver_module="mi.instrument.seabird.sbe37smb.ooicore.driver",
                                    driver_class="SBE37Driver",
                                    stream_configurations = [raw_config, parsed_config] )
        instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        return instAgent_id

    def _create_instrument_device(self, instModel_id):

        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )

        instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)

        log.debug("test_activateInstrumentSample: new InstrumentDevice id = %s    (SA Req: L4-CI-SA-RQ-241) ", instDevice_id)

        return instDevice_id

    def _create_instrument_agent_instance(self, instAgent_id,instDevice_id):


        port_agent_config = {
            'device_addr':  CFG.device.sbe37.host,
            'device_port':  CFG.device.sbe37.port,
            'process_type': PortAgentProcessType.UNIX,
            'binary_path': "port_agent",
            'port_agent_addr': 'localhost',
            'command_port': CFG.device.sbe37.port_agent_cmd_port,
            'data_port': CFG.device.sbe37.port_agent_data_port,
            'log_level': 5,
            'type': PortAgentType.ETHERNET
        }

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance',
            description="SBE37IMAgentInstance",
            port_agent_config = port_agent_config)

        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj,
            instAgent_id,
            instDevice_id)

        return instAgentInstance_id

    def _create_param_dicts(self):
        tdom, sdom = time_series_domain()

        self.sdom = sdom.dump()
        self.tdom = tdom.dump()

        self.pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)

    def _create_input_data_products(self, ctd_stream_def_id, instDevice_id,  ):

        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)

        ctd_parsed_data_product = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product)

        #---------------------------------------------------------------------------
        # Retrieve the id of the OUTPUT stream from the out Data Product
        #---------------------------------------------------------------------------

        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product, PRED.hasStream, None, True)

        pid = self.create_logger('ctd_parsed', stream_ids[0] )
        self.loggerpids.append(pid)

        #---------------------------------------------------------------------------
        # Create CTD Raw as the second data product
        #---------------------------------------------------------------------------
        if not self.pdict_id:
            self._create_param_dicts()
        raw_stream_def_id = self.pubsubclient.create_stream_definition(name='SBE37_RAW', parameter_dictionary_id=self.pdict_id)

        dp_obj = IonObject(RT.DataProduct,
            name='the raw data',
            description='raw stream test',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)

        ctd_raw_data_product = self.dataproductclient.create_data_product(dp_obj, raw_stream_def_id)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_raw_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_raw_data_product)

        #---------------------------------------------------------------------------
        # Retrieve the id of the OUTPUT stream from the out Data Product
        #---------------------------------------------------------------------------

        stream_ids, _ = self.rrclient.find_objects(ctd_raw_data_product, PRED.hasStream, None, True)

        #---------------------------------------------------------------------------
        # Retrieve the id of the OUTPUT stream from the out Data Product
        #---------------------------------------------------------------------------

        stream_ids, _ = self.rrclient.find_objects(ctd_raw_data_product, PRED.hasStream, None, True)
        print 'Data product streams2 = ', stream_ids

        return ctd_parsed_data_product

    def _create_data_process_definitions(self):

        #-------------------------------------------------------------------------------------
        # L0 Conductivity - Temperature - Pressure: Data Process Definition
        #-------------------------------------------------------------------------------------

        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L0_all',
            description='transform ctd package into three separate L0 streams',
            module='ion.processes.data.transforms.ctd.ctd_L0_all',
            class_name='ctd_L0_all')
        self.ctd_L0_all_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)

        #-------------------------------------------------------------------------------------
        # L1 Conductivity: Data Process Definition
        #-------------------------------------------------------------------------------------
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L1_conductivity',
            description='create the L1 conductivity data product',
            module='ion.processes.data.transforms.ctd.ctd_L1_conductivity',
            class_name='CTDL1ConductivityTransform')
        self.ctd_L1_conductivity_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)

        #-------------------------------------------------------------------------------------
        # L1 Pressure: Data Process Definition
        #-------------------------------------------------------------------------------------
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L1_pressure',
            description='create the L1 pressure data product',
            module='ion.processes.data.transforms.ctd.ctd_L1_pressure',
            class_name='CTDL1PressureTransform')
        self.ctd_L1_pressure_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)

        #-------------------------------------------------------------------------------------
        # L1 Temperature: Data Process Definition
        #-------------------------------------------------------------------------------------
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L1_temperature',
            description='create the L1 temperature data product',
            module='ion.processes.data.transforms.ctd.ctd_L1_temperature',
            class_name='CTDL1TemperatureTransform')
        self.ctd_L1_temperature_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)

        #-------------------------------------------------------------------------------------
        # L2 Salinity: Data Process Definition
        #-------------------------------------------------------------------------------------
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L2_salinity',
            description='create the L1 temperature data product',
            module='ion.processes.data.transforms.ctd.ctd_L2_salinity',
            class_name='SalinityTransform')
        self.ctd_L2_salinity_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)

        #-------------------------------------------------------------------------------------
        # L2 Density: Data Process Definition
        #-------------------------------------------------------------------------------------
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L2_density',
            description='create the L1 temperature data product',
            module='ion.processes.data.transforms.ctd.ctd_L2_density',
            class_name='DensityTransform')
        self.ctd_L2_density_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)

        return self.ctd_L0_all_dprocdef_id, self.ctd_L1_conductivity_dprocdef_id,\
               self.ctd_L1_pressure_dprocdef_id,self.ctd_L1_temperature_dprocdef_id, \
                self.ctd_L2_salinity_dprocdef_id, self.ctd_L2_density_dprocdef_id

    def _create_stream_definitions(self):
        if not self.pdict_id:
            self._create_param_dicts()

        outgoing_stream_l0_conductivity_id = self.pubsubclient.create_stream_definition(name='L0_Conductivity', parameter_dictionary_id=self.pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_conductivity_id, self.ctd_L0_all_dprocdef_id, binding='conductivity' )

        outgoing_stream_l0_pressure_id = self.pubsubclient.create_stream_definition(name='L0_Pressure', parameter_dictionary_id=self.pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_pressure_id, self.ctd_L0_all_dprocdef_id, binding= 'pressure' )

        outgoing_stream_l0_temperature_id = self.pubsubclient.create_stream_definition(name='L0_Temperature', parameter_dictionary_id=self.pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_temperature_id, self.ctd_L0_all_dprocdef_id, binding='temperature' )

        return outgoing_stream_l0_conductivity_id, outgoing_stream_l0_pressure_id, outgoing_stream_l0_temperature_id


    def _create_l0_output_data_products(self, outgoing_stream_l0_conductivity_id, outgoing_stream_l0_pressure_id, outgoing_stream_l0_temperature_id):

        output_products = {}


        ctd_l0_conductivity_output_dp_obj = IonObject(  RT.DataProduct,
            name='L0_Conductivity',
            description='transform output conductivity',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)

        self.ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_conductivity_output_dp_obj,
            outgoing_stream_l0_conductivity_id)
        output_products['conductivity'] = self.ctd_l0_conductivity_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=self.ctd_l0_conductivity_output_dp_id)

        ctd_l0_pressure_output_dp_obj = IonObject(  RT.DataProduct,
            name='L0_Pressure',
            description='transform output pressure',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)

        self.ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_pressure_output_dp_obj,
            outgoing_stream_l0_pressure_id)
        output_products['pressure'] = self.ctd_l0_pressure_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=self.ctd_l0_pressure_output_dp_id)

        ctd_l0_temperature_output_dp_obj = IonObject(   RT.DataProduct,
            name='L0_Temperature',
            description='transform output temperature',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)

        self.ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj,
            outgoing_stream_l0_temperature_id)
        output_products['temperature'] = self.ctd_l0_temperature_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=self.ctd_l0_temperature_output_dp_id)

        return output_products

    def _create_l1_out_data_products(self):

        ctd_l1_conductivity_output_dp_obj = IonObject(  RT.DataProduct,
            name='L1_Conductivity',
            description='transform output L1 conductivity',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)

        self.ctd_l1_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l1_conductivity_output_dp_obj,
            self.outgoing_stream_l1_conductivity_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=self.ctd_l1_conductivity_output_dp_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product and add to granule logger
        stream_ids, _ = self.rrclient.find_objects(self.ctd_l1_conductivity_output_dp_id, PRED.hasStream, None, True)
        pid = self.create_logger('ctd_l1_conductivity', stream_ids[0] )
        self.loggerpids.append(pid)

        ctd_l1_pressure_output_dp_obj = IonObject(  RT.DataProduct,
            name='L1_Pressure',
            description='transform output L1 pressure',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)

        self.ctd_l1_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l1_pressure_output_dp_obj,
            self.outgoing_stream_l1_pressure_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=self.ctd_l1_pressure_output_dp_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product and add to granule logger
        stream_ids, _ = self.rrclient.find_objects(self.ctd_l1_pressure_output_dp_id, PRED.hasStream, None, True)
        pid = self.create_logger('ctd_l1_pressure', stream_ids[0] )
        self.loggerpids.append(pid)

        ctd_l1_temperature_output_dp_obj = IonObject(   RT.DataProduct,
            name='L1_Temperature',
            description='transform output L1 temperature',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)

        self.ctd_l1_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l1_temperature_output_dp_obj,
            self.outgoing_stream_l1_temperature_id)
        self.dataproductclient.activate_data_product_persistence(data_product_id=self.ctd_l1_temperature_output_dp_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product and add to granule logger
        stream_ids, _ = self.rrclient.find_objects(self.ctd_l1_temperature_output_dp_id, PRED.hasStream, None, True)
        pid = self.create_logger('ctd_l1_temperature', stream_ids[0] )
        self.loggerpids.append(pid)

    def _create_l2_out_data_products(self):

        #-------------------------------
        # L2 Salinity - Density: Output Data Products
        #-------------------------------

        if not self.pdict_id: self._create_param_dicts()
        outgoing_stream_l2_salinity_id = self.pubsubclient.create_stream_definition(name='L2_salinity', parameter_dictionary_id=self.pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l2_salinity_id, self.ctd_L2_salinity_dprocdef_id, binding='salinity' )

        outgoing_stream_l2_density_id = self.pubsubclient.create_stream_definition(name='L2_Density', parameter_dictionary_id=self.pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l2_density_id, self.ctd_L2_density_dprocdef_id, binding='density' )

        ctd_l2_salinity_output_dp_obj = IonObject(RT.DataProduct,
            name='L2_Salinity',
            description='transform output L2 salinity',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)

        self.ctd_l2_salinity_output_dp_id = self.dataproductclient.create_data_product(ctd_l2_salinity_output_dp_obj,
            outgoing_stream_l2_salinity_id)

        self.dataproductclient.activate_data_product_persistence(data_product_id=self.ctd_l2_salinity_output_dp_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product and add to granule logger
        stream_ids, _ = self.rrclient.find_objects(self.ctd_l2_salinity_output_dp_id, PRED.hasStream, None, True)
        pid = self.create_logger('ctd_l2_salinity', stream_ids[0] )
        self.loggerpids.append(pid)

        ctd_l2_density_output_dp_obj = IonObject(   RT.DataProduct,
            name='L2_Density',
            description='transform output pressure',
            temporal_domain = self.tdom,
            spatial_domain = self.sdom)

        self.ctd_l2_density_output_dp_id = self.dataproductclient.create_data_product(ctd_l2_density_output_dp_obj,
            outgoing_stream_l2_density_id)
            

        self.dataproductclient.activate_data_product_persistence(data_product_id=self.ctd_l2_density_output_dp_id)
        # Retrieve the id of the OUTPUT stream from the out Data Product and add to granule logger
        stream_ids, _ = self.rrclient.find_objects(self.ctd_l2_density_output_dp_id, PRED.hasStream, None, True)
        pid = self.create_logger('ctd_l2_density', stream_ids[0] )
        self.loggerpids.append(pid)

    def test_createTransformsThenActivateInstrument(self):

        self.loggerpids = []

        #-------------------------------------------------------------------------------------
        # Create InstrumentModel
        #-------------------------------------------------------------------------------------

        instModel_id = self._create_instrument_model()

        #-------------------------------------------------------------------------------------
        # Create InstrumentAgent
        #-------------------------------------------------------------------------------------

        instAgent_id = self._create_instrument_agent(instModel_id)

        #-------------------------------------------------------------------------------------
        # Create InstrumentDevice
        #-------------------------------------------------------------------------------------

        instDevice_id = self._create_instrument_device(instModel_id)

        #-------------------------------------------------------------------------------------
        # Create Instrument Agent Instance
        #-------------------------------------------------------------------------------------

        instAgentInstance_id = self._create_instrument_agent_instance(instAgent_id,instDevice_id )

        #-------------------------------------------------------------------------------------
        # create a stream definition for the data from the ctd simulator
        #-------------------------------------------------------------------------------------

        self._create_param_dicts()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(name='SBE37_CDM', 
                                                parameter_dictionary_id=self.pdict_id)



        #-------------------------------------------------------------------------------------
        # Create two data products
        #-------------------------------------------------------------------------------------

        ctd_parsed_data_product = self._create_input_data_products(ctd_stream_def_id,instDevice_id)

        #-------------------------------------------------------------------------------------
        # Create data process definitions
        #-------------------------------------------------------------------------------------
        self._create_data_process_definitions()

        #-------------------------------------------------------------------------------------
        # L0 Conductivity - Temperature - Pressure: Output Data Products
        #-------------------------------------------------------------------------------------

        outgoing_stream_l0_conductivity_id, \
        outgoing_stream_l0_pressure_id, \
        outgoing_stream_l0_temperature_id = self._create_stream_definitions()

        self.output_products={}
        self.output_products = self._create_l0_output_data_products(outgoing_stream_l0_conductivity_id,outgoing_stream_l0_pressure_id,outgoing_stream_l0_temperature_id)

        self.outgoing_stream_l1_conductivity_id = self.pubsubclient.create_stream_definition(name='L1_conductivity', parameter_dictionary_id=self.pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(self.outgoing_stream_l1_conductivity_id, self.ctd_L1_conductivity_dprocdef_id, binding='conductivity' )

        self.outgoing_stream_l1_pressure_id = self.pubsubclient.create_stream_definition(name='L1_Pressure', parameter_dictionary_id=self.pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(self.outgoing_stream_l1_pressure_id, self.ctd_L1_pressure_dprocdef_id, binding='pressure' )
        
        self.outgoing_stream_l1_temperature_id = self.pubsubclient.create_stream_definition(name='L1_Temperature', parameter_dictionary_id=self.pdict_id)
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(self.outgoing_stream_l1_temperature_id, self.ctd_L1_temperature_dprocdef_id, binding= 'temperature' )

        self._create_l1_out_data_products()

        self._create_l2_out_data_products()

        #-------------------------------------------------------------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------------------------------------------------------------
        ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(self.ctd_L0_all_dprocdef_id, [ctd_parsed_data_product], self.output_products)
        self.dataprocessclient.activate_data_process(ctd_l0_all_data_process_id)


        #-------------------------------------------------------------------------------------
        # L1 Conductivity: Create the data process
        #-------------------------------------------------------------------------------------
        l1_conductivity_data_process_id = self.dataprocessclient.create_data_process(self.ctd_L1_conductivity_dprocdef_id, [self.ctd_l0_conductivity_output_dp_id], {'conductivity':self.ctd_l1_conductivity_output_dp_id})
        self.dataprocessclient.activate_data_process(l1_conductivity_data_process_id)


        #-------------------------------------------------------------------------------------
        # L1 Pressure: Create the data process
        #-------------------------------------------------------------------------------------
        l1_pressure_data_process_id = self.dataprocessclient.create_data_process(self.ctd_L1_pressure_dprocdef_id, [self.ctd_l0_pressure_output_dp_id], {'pressure':self.ctd_l1_pressure_output_dp_id})
        self.dataprocessclient.activate_data_process(l1_pressure_data_process_id)

        #-------------------------------------------------------------------------------------
        # L1 Temperature: Create the data process
        #-------------------------------------------------------------------------------------
        l1_temperature_all_data_process_id = self.dataprocessclient.create_data_process(self.ctd_L1_temperature_dprocdef_id, [self.ctd_l0_temperature_output_dp_id], {'temperature':self.ctd_l1_temperature_output_dp_id})
        self.dataprocessclient.activate_data_process(l1_temperature_all_data_process_id)

        #-------------------------------------------------------------------------------------
        # L2 Salinity: Create the data process
        #-------------------------------------------------------------------------------------
        l2_salinity_all_data_process_id = self.dataprocessclient.create_data_process(self.ctd_L2_salinity_dprocdef_id, [ctd_parsed_data_product], {'salinity':self.ctd_l2_salinity_output_dp_id})
        self.dataprocessclient.activate_data_process(l2_salinity_all_data_process_id)

        #-------------------------------------------------------------------------------------
        # L2 Density: Create the data process
        #-------------------------------------------------------------------------------------
        l2_density_all_data_process_id = self.dataprocessclient.create_data_process(self.ctd_L2_density_dprocdef_id, [ctd_parsed_data_product], {'density':self.ctd_l2_density_output_dp_id})
        self.dataprocessclient.activate_data_process(l2_density_all_data_process_id)

        #-------------------------------------------------------------------------------------
        # Launch InstrumentAgentInstance, connect to the resource agent client
        #-------------------------------------------------------------------------------------
        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)
        self.addCleanup(self.imsclient.stop_instrument_agent_instance,
                        instrument_agent_instance_id=instAgentInstance_id)

        inst_agent_instance_obj= self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        
        # Wait for instrument agent to spawn
        gate = ProcessStateGate(self.processdispatchclient.read_process,
            inst_agent_instance_obj.agent_process_id, ProcessStateEnum.RUNNING)
        self.assertTrue(gate.await(15), "The instrument agent instance did not spawn in 15 seconds")

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient(instDevice_id,
            to_name=inst_agent_instance_obj.agent_process_id,
            process=FakeProcess())

        #-------------------------------------------------------------------------------------
        # Streaming
        #-------------------------------------------------------------------------------------


        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)

        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("(L4-CI-SA-RQ-334): current state after sending go_active command %s", str(state))
        self.assertTrue(state, 'DRIVER_STATE_COMMAND')

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)

        #todo ResourceAgentClient no longer has method set_param
#        # Make sure the sampling rate and transmission are sane.
#        params = {
#            SBE37Parameter.NAVG : 1,
#            SBE37Parameter.INTERVAL : 5,
#            SBE37Parameter.TXREALTIME : True
#        }
#        self._ia_client.set_param(params)


        #todo There is no ResourceAgentEvent attribute for go_streaming... so what should be the command for it?
        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)

        gevent.sleep(15)

        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client.execute_resource(cmd)



        #todo There is no ResourceAgentEvent attribute for go_observatory... so what should be the command for it?
#        log.debug("test_activateInstrumentStream: calling go_observatory")
#        cmd = AgentCommand(command='go_observatory')
#        reply = self._ia_client.execute_agent(cmd)
#        cmd = AgentCommand(command='get_current_state')
#        retval = self._ia_client.execute_agent(cmd)
#        state = retval.result
#        log.debug("test_activateInstrumentStream: return from go_observatory state  %s", str(state))

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client.execute_agent(cmd)
        self.assertTrue(reply.status == 0)


        #-------------------------------------------------------------------------------------------------
        # Cleanup processes
        #-------------------------------------------------------------------------------------------------
        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)
