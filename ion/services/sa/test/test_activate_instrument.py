from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient


from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import IDataProductManagementService
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient


from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter

from prototype.sci_data.stream_defs import ctd_stream_definition
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition
from prototype.sci_data.stream_defs import \
    ctd_stream_definition, \
    L0_pressure_stream_definition, L0_temperature_stream_definition, \
    L0_conductivity_stream_definition

from interface.objects import AgentCommand
from interface.objects import ProcessDefinition
from interface.objects import ProcessStateEnum

from gevent.event import AsyncResult
from ion.services.cei.process_dispatcher_service import ProcessStateGate

from pyon.public import CFG
from pyon.public import RT, LCS, PRED
from pyon.public import Container, log, IonObject

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase

from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent

from ion.util.parameter_yaml_IO import get_param_dict
from pyon.core.exception import BadRequest, NotFound, Conflict

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.coverage import GridDomain, GridShape, CRS
from coverage_model.basic_types import MutabilityEnum, AxisTypeEnum
from ion.util.parameter_yaml_IO import get_param_dict

from nose.plugins.attrib import attr

import unittest
import time



class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('HARDWARE', group='sa')
#@unittest.skip('run locally only')
class TestActivateInstrumentIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        
        #setup listerner vars
        self._data_greenlets = []
        self._no_samples = None
        self._samples_received = []

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
        pid = self.processdispatchclient.schedule_process(process_definition_id=logger_procdef_id,
                                                          configuration=configuration)

        return pid


    #@unittest.skip("TBD")
    def test_activateInstrumentSample(self):

        self.loggerpids = []

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='SBE37IMModel',
                                  description="SBE37IMModel",
                                  model="SBE37IMModel",
                                  custom_attributes= {'streams':{'raw': 'ctd_raw_param_dict' , 'parsed': 'ctd_parsed_param_dict' }})
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        log.debug( 'new InstrumentModel id = %s ', instModel_id)

        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_module="ion.agents.instrument.instrument_agent",
                                  driver_class="InstrumentAgent" )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        log.debug( 'new InstrumentAgent id = %s', instAgent_id)

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        log.debug('test_activateInstrumentSample: Create instrument resource to represent the SBE37 '
                + '(SA Req: L4-CI-SA-RQ-241) ')
        instDevice_obj = IonObject(RT.InstrumentDevice,
                                   name='SBE37IMDevice',
                                   description="SBE37IMDevice",
                                   serial_number="12345" )
        try:
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        log.debug("test_activateInstrumentSample: new InstrumentDevice id = %s    (SA Req: L4-CI-SA-RQ-241) ",
                  instDevice_id)

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance',
                                          description="SBE37IMAgentInstance",
                                          driver_module='mi.instrument.seabird.sbe37smb.ooicore.driver',
                                          driver_class='SBE37Driver',
                                          comms_device_address='sbe37-simulator.oceanobservatories.org',
                                          comms_device_port=4001,
                                          port_agent_work_dir='/tmp/',
                                          port_agent_delimeter=['<<','>>'] )

        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj,
                                                                               instAgent_id,
                                                                               instDevice_id)

        # create a stream definition for the data from the ctd simulator
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(name='SBE37')

        log.debug( 'new Stream Definition id = %s', instDevice_id)

        log.debug( 'Creating new CDM data product with a stream definition')

        # Construct temporal and spatial Coordinate Reference System objects
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT])

        # Construct temporal and spatial Domain objects
        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE) # 1d (timeline)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # 1d spatial topology (station/trajectory)

        sdom = sdom.dump()
        tdom = tdom.dump()

        parameter_dictionary = get_param_dict('sample_param_dict')
        parameter_dictionary = parameter_dictionary.dump()

        parsed_param_dict = get_param_dict('ctd_parsed_param_dict')
        raw_param_dict = get_param_dict('ctd_raw_param_dict')


        #-------------------------------
        # Create Raw and Parsed Data Products for the device
        #-------------------------------

        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id1 = self.dpclient.create_data_product(dp_obj, ctd_stream_def_id, parsed_param_dict.dump())

        log.debug( 'new dp_id = %s', data_product_id1)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id1)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'Data product streams1 = %s', stream_ids)

        pid = self.create_logger('ctd_parsed', stream_ids[0] )
        self.loggerpids.append(pid)


        log.debug( 'Creating new RAW data product with a stream definition')
        raw_stream_def_id = self.pubsubcli.create_stream_definition(name='SBE37_RAW')

        dp_obj = IonObject(RT.DataProduct,
            name='the raw data',
            description='raw stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id2 = self.dpclient.create_data_product(dp_obj, raw_stream_def_id, raw_param_dict.dump())
        log.debug( 'new dp_id = %s', str(data_product_id2))

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id2)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id2)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasStream, None, True)
        log.debug( 'Data product streams2 = %s', str(stream_ids))

        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Data Process Definition
        #-------------------------------
        log.debug("test_activateInstrumentSample: create data process definition ctd_L0_all")
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

        outgoing_stream_l0_conductivity_id = self.pubsubcli.create_stream_definition(
            name='L0_Conductivity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(
            outgoing_stream_l0_conductivity_id,
            ctd_L0_all_dprocdef_id )

        outgoing_stream_l0_pressure_id = self.pubsubcli.create_stream_definition(
            name='L0_Pressure')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(
            outgoing_stream_l0_pressure_id,
            ctd_L0_all_dprocdef_id )

        outgoing_stream_l0_temperature_id = self.pubsubcli.create_stream_definition(
            name='L0_Temperature')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(
            outgoing_stream_l0_temperature_id,
            ctd_L0_all_dprocdef_id )


        self.output_products={}
        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 conductivity")

        ctd_l0_conductivity_output_dp_obj = IonObject(  RT.DataProduct,
                                                        name='L0_Conductivity',
                                                        description='transform output conductivity',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)

        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(
            ctd_l0_conductivity_output_dp_obj,
            outgoing_stream_l0_conductivity_id,
            parameter_dictionary)
        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_conductivity_output_dp_id)

        stream_ids, _ = self.rrclient.find_objects(ctd_l0_conductivity_output_dp_id, PRED.hasStream, None, True)
        log.debug(" ctd_l0_conductivity stream id =  %s", str(stream_ids) )
        pid = self.create_logger(' ctd_l1_conductivity', stream_ids[0] )
        self.loggerpids.append(pid)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 pressure")

        ctd_l0_pressure_output_dp_obj = IonObject(  RT.DataProduct,
                                                    name='L0_Pressure',
                                                    description='transform output pressure',
                                                    temporal_domain = tdom,
                                                    spatial_domain = sdom)

        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(  ctd_l0_pressure_output_dp_obj,
                                                                                    outgoing_stream_l0_pressure_id,
                                                                                    parameter_dictionary)
        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_pressure_output_dp_id)

        stream_ids, _ = self.rrclient.find_objects(ctd_l0_pressure_output_dp_id, PRED.hasStream, None, True)
        log.debug(" ctd_l0_pressure stream id =  %s", str(stream_ids) )
        pid = self.create_logger(' ctd_l0_pressure', stream_ids[0] )
        self.loggerpids.append(pid)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 temperature")

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

        stream_ids, _ = self.rrclient.find_objects(ctd_l0_temperature_output_dp_id, PRED.hasStream, None, True)
        log.debug(" ctd_l0_temperature stream id =  %s", str(stream_ids) )
        pid = self.create_logger(' ctd_l0_temperature', stream_ids[0] )
        self.loggerpids.append(pid)

        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------
        log.debug("test_activateInstrumentSample: create L0 all data_process start")
        try:
            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(
                ctd_L0_all_dprocdef_id,
                [data_product_id1],
                self.output_products)
            self.dataprocessclient.activate_data_process(ctd_l0_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L0 all data_process return")

        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        #wait for start
        instance_obj = self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
                                instance_obj.agent_process_id,
                                ProcessStateEnum.SPAWN)
        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        instance_obj.agent_process_id)

        inst_agent_instance_obj = self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        log.debug( 'Instrument agent instance obj: = %s', str(inst_agent_instance_obj))

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient(instDevice_id,
                                              to_name=inst_agent_instance_obj.agent_process_id,
                                              process=FakeProcess())

        log.debug("test_activateInstrumentSample: got ia client %s", str(self._ia_client))

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: initialize %s", str(retval))

        time.sleep(1)

        log.debug("(L4-CI-SA-RQ-334): Sending go_active command ")
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: return value from go_active %s", str(reply))
        time.sleep(1)
        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("(L4-CI-SA-RQ-334): current state after sending go_active command %s", str(state))

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: run %s", str(reply))
        time.sleep(1)


        log.debug("test_activateInstrumentSample: calling reset ")
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: return from reset %s", str(reply))
        time.sleep(1)

        #-------------------------------
        # Deactivate InstrumentAgentInstance
        #-------------------------------
        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)


    @unittest.skip("TBD")
    def test_activateInstrumentStream(self):


        self.loggerpids = []

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel,
                                  name='SBE37IMModel',
                                  description="SBE37IMModel",
                                  model="SBE37IMModel" )
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        log.debug( 'new InstrumentModel id = %s ', instModel_id)

        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent,
                                  name='agent007',
                                  description="SBE37IMAgent",
                                  driver_module="ion.agents.instrument.instrument_agent",
                                  driver_class="InstrumentAgent" )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        log.debug( 'new InstrumentAgent id = %s', instAgent_id)

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        log.debug('test_activateInstrumentSample: Create instrument resource to represent the SBE37 '
            + '(SA Req: L4-CI-SA-RQ-241) ')
        instDevice_obj = IonObject(RT.InstrumentDevice,
                                   name='SBE37IMDevice',
                                   description="SBE37IMDevice",
                                   serial_number="12345" )
        try:
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        log.debug("test_activateInstrumentSample: new InstrumentDevice id = %s    (SA Req: L4-CI-SA-RQ-241) ", instDevice_id)

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance,
                                          name='SBE37IMAgentInstance',
                                          description="SBE37IMAgentInstance",
                                          driver_module='mi.instrument.seabird.sbe37smb.ooicore.driver',
                                          driver_class='SBE37Driver',
                                          comms_device_address='sbe37-simulator.oceanobservatories.org',
                                          comms_device_port=4001,
                                          port_agent_work_dir='/tmp/',
                                          port_agent_delimeter=['<<','>>'] )
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj,
                                                                               instAgent_id,
                                                                               instDevice_id)

        # create a stream definition for the data from the ctd simulator
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(name='SBE37_CDM')

        log.debug( 'new Stream Definition id = %s', instDevice_id)

        log.debug( 'Creating new CDM data product with a stream definition')

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
            name='the parsed data',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id1 = self.dpclient.create_data_product(dp_obj, ctd_stream_def_id, parameter_dictionary)
        log.debug( 'new dp_id = %s', data_product_id1)

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id1)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'Data product streams1 = %s', stream_ids)

        pid = self.create_logger('ctd_parsed', stream_ids[0] )
        self.loggerpids.append(pid)


        log.debug( 'Creating new RAW data product with a stream definition')
        raw_stream_def_id = self.pubsubcli.create_stream_definition(name='SBE37_RAW')

        dp_obj = IonObject(RT.DataProduct,
            name='the raw data',
            description='raw stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id2 = self.dpclient.create_data_product(dp_obj, raw_stream_def_id, parameter_dictionary)
        log.debug( 'new dp_id = %s', str(data_product_id2))

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id2)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id2)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasStream, None, True)
        log.debug( 'Data product streams2 = %s', str(stream_ids))

        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)


        inst_agent_instance_obj= self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        log.debug( 'test_activateInstrumentStream Instrument agent instance obj: = %s', str(inst_agent_instance_obj) )

        # Start a resource agent client to talk with the instrument agent.
        #self._ia_client = ResourceAgentClient('123xyz', name=inst_agent_instance_obj.agent_process_id,  process=FakeProcess())
        self._ia_client = ResourceAgentClient(instDevice_id,  process=FakeProcess())
        log.debug( 'test_activateInstrumentStream: got ia client %s', str(self._ia_client ))



        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentStream: initialize %s", str(retval))

        time.sleep(2)

        log.debug("test_activateInstrumentStream: Sending go_active command (L4-CI-SA-RQ-334)")
        cmd = AgentCommand(command='go_active')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentStream: return value from go_active %s", str(reply))
        time.sleep(2)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("test_activateInstrumentStream: current state after sending go_active command %s    "
            + "(L4-CI-SA-RQ-334)", str(state))

        cmd = AgentCommand(command='run')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("test_activateInstrumentStream: return from run state: %s", str(state))

        # Make sure the sampling rate and transmission are sane.
        params = {
            SBE37Parameter.NAVG : 1,
            SBE37Parameter.INTERVAL : 5,
            SBE37Parameter.TXREALTIME : True
        }
        self._ia_client.set_param(params)
        time.sleep(2)


        log.debug("test_activateInstrumentStream: calling go_streaming ")
        cmd = AgentCommand(command='go_streaming')
        reply = self._ia_client.execute_agent(cmd)
        time.sleep(2)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("test_activateInstrumentStream: return from go_streaming state: %s", str(state))


        time.sleep(5)

        log.debug("test_activateInstrumentStream: calling go_observatory")
        cmd = AgentCommand(command='go_observatory')
        reply = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("test_activateInstrumentStream: return from go_observatory state  %s", str(state))



        log.debug("test_activateInstrumentStream: calling reset ")
        cmd = AgentCommand(command='reset')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentStream: return from reset state:%s", str(reply.result))
        time.sleep(2)

        #-------------------------------
        # Deactivate InstrumentAgentInstance
        #-------------------------------
        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)
        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)

