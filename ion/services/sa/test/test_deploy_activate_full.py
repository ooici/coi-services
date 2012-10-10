from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.coverage import GridDomain, GridShape, CRS
from coverage_model.basic_types import MutabilityEnum, AxisTypeEnum
from ion.util.parameter_yaml_IO import get_param_dict

from ooi.logging import log
from nose.plugins.attrib import attr


from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG, RT, LCS, PRED, LCS, LCE
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.agent.agent import ResourceAgentClient, ResourceAgentState, ResourceAgentEvent
from pyon.util.unit_test import PyonTestCase

from interface.objects import AgentCommand
from interface.objects import ProcessStateEnum
from interface.objects import ProcessDefinition

from ion.services.cei.process_dispatcher_service import ProcessStateGate

from ion.agents.port.port_agent_process import PortAgentProcessType

from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent

import gevent
import time
import os
import signal
import unittest

from pyon.util.context import LocalContextMixin
from mock import patch

from ion.services.dm.utility.granule_utils import CoverageCraft

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('HARDWARE', group='sa')
#@unittest.skip("needs work")
#@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class TestIMSDeployAsPrimaryDevice(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()

        #self.container.start_rel_from_url('res/deploy/r2deploy.yml')
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
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)

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

    def cleanupprocs(self):
       stm = os.popen('ps -e | grep ion.agents.port.logger_process')
       procs = stm.read()
       if len(procs) > 0:
           procs = procs.split()
           if procs[0].isdigit():
               pid = int(procs[0])
               os.kill(pid,signal.SIGKILL)
       stm = os.popen('ps -e | grep ion.agents.instrument.zmq_driver_process')
       procs = stm.read()
       if len(procs) > 0:
           procs = procs.split()
           if procs[0].isdigit():
               pid = int(procs[0])
               os.kill(pid,signal.SIGKILL)
#       stm = os.popen('rm /tmp/*.pid.txt')


    @unittest.skip ("Deprecated by IngestionManagement refactor, timeout on start inst agent?")
    def test_deploy_activate_full(self):

        # ensure no processes or pids are left around by agents or Sims
        #self.cleanupprocs()

        self.loggerpids = []

        #-------------------------------
        # Create InstrumentModel
        #-------------------------------
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel", model="SBE37IMModel",
                                stream_configuration= {'raw': 'simple_data_particle_raw_param_dict' , 'parsed': 'simple_data_particle_parsed_param_dict' })
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)


        #-------------------------------
        # Create InstrumentAgent
        #-------------------------------
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


        #-------------------------------
        # Create Instrument Site
        #-------------------------------
        instrumentSite_obj = IonObject(RT.InstrumentSite, name='instrumentSite1', description="SBE37IMInstrumentSite" )
        try:
            instrumentSite_id = self.omsclient.create_instrument_site(instrument_site=instrumentSite_obj, parent_id='')
        except BadRequest as ex:
            self.fail("failed to create new InstrumentSite: %s" %ex)
        print 'test_deployAsPrimaryDevice: new instrumentSite id = ', instrumentSite_id

        self.omsclient.assign_instrument_model_to_instrument_site(instModel_id, instrumentSite_id)



        #-------------------------------
        # Logical Transform: Output Data Products
        #-------------------------------

        # Construct temporal and spatial Coordinate Reference System objects
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT])

        # Construct temporal and spatial Domain objects
        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE) # 1d (timeline)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # 1d spatial topology (station/trajectory)

        sdom = sdom.dump()
        tdom = tdom.dump()

        parsed_parameter_dictionary = get_param_dict('simple_data_particle_parsed_param_dict')
        parsed_stream_def_id = self.pubsubclient.create_stream_definition(name='parsed', parameter_dictionary=parsed_parameter_dictionary.dump())

        raw_parameter_dictionary = get_param_dict('simple_data_particle_raw_param_dict')
        raw_stream_def_id = self.pubsubclient.create_stream_definition(name='raw', parameter_dictionary=raw_parameter_dictionary.dump())

        #-------------------------------
        # Create Old InstrumentDevice
        #-------------------------------
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDeviceYear1',
            description="SBE37IMDevice for the FIRST year of deployment",
            serial_number="12345" )
        try:
            oldInstDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, oldInstDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        print 'test_deployAsPrimaryDevice: new Year 1 InstrumentDevice id = ', oldInstDevice_id

        self.rrclient.execute_lifecycle_transition(oldInstDevice_id, LCE.DEPLOY)
        self.rrclient.execute_lifecycle_transition(oldInstDevice_id, LCE.ENABLE)

        #-------------------------------
        # Create Raw and Parsed Data Products for the device
        #-------------------------------

        dp_obj = IonObject(RT.DataProduct,
            name='SiteDataProduct',
            description='SiteDataProduct',
            temporal_domain = tdom,
            spatial_domain = sdom)

        instrument_site_output_dp_id = self.dataproductclient.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id, parameter_dictionary=parsed_parameter_dictionary.dump())

        self.damsclient.assign_data_product(input_resource_id=oldInstDevice_id, data_product_id=instrument_site_output_dp_id)

        self.dataproductclient.activate_data_product_persistence(data_product_id=instrument_site_output_dp_id)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(instrument_site_output_dp_id, PRED.hasStream, None, True)
        log.debug( 'Data product streams1 = %s', stream_ids)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        dataset_ids, _ = self.rrclient.find_objects(instrument_site_output_dp_id, PRED.hasDataset, RT.Dataset, True)
        log.debug( 'Data set for data_product_id1 = %s', dataset_ids[0])
        self.parsed_dataset = dataset_ids[0]

        pid = self.create_logger('ctd_parsed', stream_ids[0] )
        self.loggerpids.append(pid)

        self.omsclient.create_site_data_product(instrumentSite_id, instrument_site_output_dp_id)


        #-------------------------------
        # Create Old Deployment
        #-------------------------------
        deployment_obj = IonObject(RT.Deployment, name='first deployment')

        oldDeployment_id = self.omsclient.create_deployment(deployment_obj)

        # deploy this device to the logical slot
        self.imsclient.deploy_instrument_device(oldInstDevice_id, oldDeployment_id)
        self.omsclient.deploy_instrument_site(instrumentSite_id, oldDeployment_id)



        #-------------------------------
        # Create InstrumentAgentInstance for OldInstrumentDevice to hold configuration information
        # cmd_port=5556, evt_port=5557, comms_method="ethernet", comms_device_address=CFG.device.sbe37.host, comms_device_port=CFG.device.sbe37.port,
        #-------------------------------

        port_agent_config = {
            'device_addr': 'sbe37-simulator.oceanobservatories.org',
            'device_port': 4001,
            'process_type': PortAgentProcessType.UNIX,
            'binary_path': "port_agent",
            'command_port': 4002,
            'data_port': 4003,
            'log_level': 5,
            }

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstanceYear1',
            description="SBE37IMAgentInstanceYear1",
            driver_module='mi.instrument.seabird.sbe37smb.ooicore.driver',
            driver_class='SBE37Driver',
            comms_device_address='sbe37-simulator.oceanobservatories.org',
            comms_device_port=4001,
            port_agent_config = port_agent_config)


        oldInstAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj,
            instAgent_id,
            oldInstDevice_id)


        sdom, tdom = CoverageCraft.create_domains()
        sdom = sdom.dump()
        tdom = tdom.dump()

        #-------------------------------
        # Create CTD Parsed as the Year 1 data product and attach to instrument
        #-------------------------------

        print 'Creating new CDM data product with a stream definition'

        dp_obj = IonObject(RT.DataProduct,
            name='ctd_parsed_year1',
            description='ctd stream test year 1',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_parsed_data_product_year1 = self.dataproductclient.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id, parameter_dictionary=parsed_parameter_dictionary.dump())


        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product_year1

        self.damsclient.assign_data_product(input_resource_id=oldInstDevice_id,
                                            data_product_id=ctd_parsed_data_product_year1)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product_year1, PRED.hasStream, None, True)
        print 'test_deployAsPrimaryDevice: Data product streams1 = ', stream_ids

        #-------------------------------
        # Create New InstrumentDevice
        #-------------------------------
        instDevice_obj_2 = IonObject(RT.InstrumentDevice,
                                     name='SBE37IMDeviceYear2',
                                     description="SBE37IMDevice for the SECOND year of deployment",
                                     serial_number="67890" )
        try:
            newInstDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj_2)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, newInstDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        print 'test_deployAsPrimaryDevice: new  Year 2 InstrumentDevice id = ', newInstDevice_id

        #set the LCSTATE
        self.rrclient.execute_lifecycle_transition(newInstDevice_id, LCE.DEPLOY)
        self.rrclient.execute_lifecycle_transition(newInstDevice_id, LCE.ENABLE)

        instDevice_obj_2 = self.rrclient.read(newInstDevice_id)
        log.debug("test_deployAsPrimaryDevice: Create New InstrumentDevice LCSTATE: %s ",
                  str(instDevice_obj_2.lcstate))




        #-------------------------------
        # Create Old Deployment
        #-------------------------------
        deployment_obj = IonObject(RT.Deployment, name='second deployment')

        newDeployment_id = self.omsclient.create_deployment(deployment_obj)

        # deploy this device to the logical slot
        self.imsclient.deploy_instrument_device(newInstDevice_id, newDeployment_id)
        self.omsclient.deploy_instrument_site(instrumentSite_id, newDeployment_id)


        #-------------------------------
        # Create InstrumentAgentInstance for NewInstrumentDevice to hold configuration information
        #-------------------------------

        port_agent_config = {
            'device_addr': 'sbe37-simulator.oceanobservatories.org',
            'device_port': 4004,
            'process_type': PortAgentProcessType.UNIX,
            'binary_path': "port_agent",
            'command_port': 4005,
            'data_port': 4006,
            'log_level': 5,
            }

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstanceYear2',
            description="SBE37IMAgentInstanceYear2",
            driver_module='mi.instrument.seabird.sbe37smb.ooicore.driver',
            driver_class='SBE37Driver',
            comms_device_address='sbe37-simulator.oceanobservatories.org',
            comms_device_port=4004,
            port_agent_config = port_agent_config)


        newInstAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj,
            instAgent_id,
            newInstDevice_id)



        #-------------------------------
        # Create CTD Parsed as the Year 2 data product
        #-------------------------------



        dp_obj = IonObject(RT.DataProduct,
            name='ctd_parsed_year2',
            description='ctd stream test year 2',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_parsed_data_product_year2 = self.dataproductclient.create_data_product(data_product=dp_obj, stream_definition_id=parsed_stream_def_id, parameter_dictionary=parsed_parameter_dictionary.dump())

        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product_year2

        self.damsclient.assign_data_product(input_resource_id=newInstDevice_id,
                                            data_product_id=ctd_parsed_data_product_year2)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product_year2, PRED.hasStream, None, True)
        print 'test_deployAsPrimaryDevice: Data product streams2 = ', stream_ids


        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Data Process Definition
        #-------------------------------
        log.debug("test_deployAsPrimaryDevice: create data process definition ctd_L0_all")
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

        outgoing_stream_l0_conductivity_id = self.pubsubclient.create_stream_definition(name='L0_Conductivity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_conductivity_id, ctd_L0_all_dprocdef_id, binding='conductivity' )

        outgoing_stream_l0_pressure_id = self.pubsubclient.create_stream_definition(name='L0_Pressure')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_pressure_id, ctd_L0_all_dprocdef_id, binding='pressure' )

        outgoing_stream_l0_temperature_id = self.pubsubclient.create_stream_definition(name='L0_Temperature')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l0_temperature_id, ctd_L0_all_dprocdef_id, binding='temperature' )


        self.output_products={}
        log.debug("test_deployAsPrimaryDevice: create output data product L0 conductivity")

        ctd_l0_conductivity_output_dp_obj = IonObject(RT.DataProduct,
            name='L0_Conductivity',
            description='transform output conductivity',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(data_product=ctd_l0_conductivity_output_dp_obj, stream_definition_id=parsed_stream_def_id, parameter_dictionary=parsed_parameter_dictionary.dump())
        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id
        #self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_conductivity_output_dp_id)


        log.debug("test_deployAsPrimaryDevice: create output data product L0 pressure")

        ctd_l0_pressure_output_dp_obj = IonObject(  RT.DataProduct,
                                                    name='L0_Pressure',
                                                    description='transform output pressure',
                                                    temporal_domain = tdom,
                                                    spatial_domain = sdom)

        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(data_product=ctd_l0_pressure_output_dp_obj, stream_definition_id=parsed_stream_def_id, parameter_dictionary=parsed_parameter_dictionary.dump())

        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id
        #self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_pressure_output_dp_id)

        log.debug("test_deployAsPrimaryDevice: create output data product L0 temperature")

        ctd_l0_temperature_output_dp_obj = IonObject(   RT.DataProduct,
                                                        name='L0_Temperature',
                                                        description='transform output temperature',
                                                        temporal_domain = tdom,
                                                        spatial_domain = sdom)

        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(data_product=ctd_l0_temperature_output_dp_obj, stream_definition_id=parsed_stream_def_id, parameter_dictionary=parsed_parameter_dictionary.dump())

        self.output_products['temperature'] = ctd_l0_temperature_output_dp_id
        #self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_temperature_output_dp_id)



        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process, listening to  Sim1   (later: logical instrument output product)
        #-------------------------------
        log.debug("test_deployAsPrimaryDevice: create L0 all data_process start")
        try:
            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L0_all_dprocdef_id, [ctd_parsed_data_product_year1], self.output_products)
            self.dataprocessclient.activate_data_process(ctd_l0_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)
        log.debug("test_deployAsPrimaryDevice: create L0 all data_process return")

        #--------------------------------
        # Activate the deployment
        #--------------------------------
        self.omsclient.activate_deployment(oldDeployment_id)


        #-------------------------------
        # Launch InstrumentAgentInstance Sim1, connect to the resource agent client
        #-------------------------------
        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=oldInstAgentInstance_id)


        #wait for start
        instance_obj = self.imsclient.read_instrument_agent_instance(oldInstAgentInstance_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
            instance_obj.agent_process_id,
            ProcessStateEnum.SPAWN)
        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        instance_obj.agent_process_id)

        inst_agent1_instance_obj= self.imsclient.read_instrument_agent_instance(oldInstAgentInstance_id)
        print 'test_deployAsPrimaryDevice: Instrument agent instance obj: = ', inst_agent1_instance_obj

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client_sim1 = ResourceAgentClient('iaclient Sim1', name=inst_agent1_instance_obj.agent_process_id,  process=FakeProcess())
        print 'activate_instrument: got _ia_client_sim1 %s', self._ia_client_sim1
        log.debug(" test_deployAsPrimaryDevice:: got _ia_client_sim1 %s", str(self._ia_client_sim1))


        #-------------------------------
        # Launch InstrumentAgentInstance Sim2, connect to the resource agent client
        #-------------------------------
        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=newInstAgentInstance_id)

        #wait for start
        instance_obj = self.imsclient.read_instrument_agent_instance(newInstAgentInstance_id)
        gate = ProcessStateGate(self.processdispatchclient.read_process,
            instance_obj.agent_process_id,
            ProcessStateEnum.SPAWN)
        self.assertTrue(gate.await(30), "The instrument agent instance (%s) did not spawn in 30 seconds" %
                                        instance_obj.agent_process_id)

        inst_agent2_instance_obj= self.imsclient.read_instrument_agent_instance(newInstAgentInstance_id)
        print 'test_deployAsPrimaryDevice: Instrument agent instance obj: = ', inst_agent2_instance_obj

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client_sim2 = ResourceAgentClient('iaclient Sim2', name=inst_agent2_instance_obj.agent_process_id,  process=FakeProcess())
        print 'activate_instrument: got _ia_client_sim2 %s', self._ia_client_sim2
        log.debug(" test_deployAsPrimaryDevice:: got _ia_client_sim2 %s", str(self._ia_client_sim2))


        #-------------------------------
        # Streaming Sim1 (old instrument)
        #-------------------------------

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client_sim1.execute_agent(cmd)
        log.debug("test_deployAsPrimaryDevice: initialize %s", str(retval))


        log.debug("(L4-CI-SA-RQ-334): Sending go_active command ")
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        reply = self._ia_client_sim1.execute_agent(cmd)
        log.debug("test_deployAsPrimaryDevice: return value from go_active %s", str(reply))

        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client_sim1.execute_agent(cmd)
        state = retval.result
        log.debug("(L4-CI-SA-RQ-334): current state after sending go_active command %s", str(state))

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        reply = self._ia_client_sim1.execute_agent(cmd)
        log.debug("test_deployAsPrimaryDevice: run %s", str(reply))

        gevent.sleep(2)

        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self._ia_client_sim1.execute_resource(cmd)
        log.debug("test_activateInstrumentSample: return from START_AUTOSAMPLE: %s", str(retval))


        #-------------------------------
        # Streaming Sim 2 (new instrument)
        #-------------------------------

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = self._ia_client_sim2.execute_agent(cmd)
        log.debug("test_deployAsPrimaryDevice: initialize_sim2 %s", str(retval))


        log.debug("(L4-CI-SA-RQ-334): Sending go_active command ")
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        reply = self._ia_client_sim2.execute_agent(cmd)
        log.debug("test_deployAsPrimaryDevice: return value from go_active_sim2 %s", str(reply))

        cmd = AgentCommand(command=ResourceAgentEvent.GET_RESOURCE_STATE)
        retval = self._ia_client_sim2.execute_agent(cmd)
        state = retval.result
        log.debug("(L4-CI-SA-RQ-334): current state after sending go_active_sim2 command %s", str(state))

        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        reply = self._ia_client_sim2.execute_agent(cmd)
        log.debug("test_deployAsPrimaryDevice: run %s", str(reply))

        gevent.sleep(2)

        cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
        retval = self._ia_client_sim2.execute_resource(cmd)
        log.debug("test_activateInstrumentSample: return from START_AUTOSAMPLE_sim2: %s", str(retval))

        gevent.sleep(10)


        #-------------------------------
        # Shutdown Sim1 (old instrument)
        #-------------------------------
        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client_sim1.execute_resource(cmd)
        log.debug("test_activateInstrumentSample: return from STOP_AUTOSAMPLE: %s", str(retval))

        log.debug("test_activateInstrumentSample: calling reset ")
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client_sim1.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: return from reset %s", str(reply))
        time.sleep(5)


        #-------------------------------
        # Shutdown Sim2 (old instrument)
        #-------------------------------
        cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
        retval = self._ia_client_sim2.execute_resource(cmd)
        log.debug("test_activateInstrumentSample: return from STOP_AUTOSAMPLE_sim2: %s", str(retval))

        log.debug("test_activateInstrumentSample: calling reset_sim2 ")
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        reply = self._ia_client_sim1.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: return from reset_sim2 %s", str(reply))
        time.sleep(5)



        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=oldInstAgentInstance_id)
        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=newInstAgentInstance_id)

