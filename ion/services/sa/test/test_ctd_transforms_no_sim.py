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
from ion.services.dm.utility.granule_utils import CoverageCraft

from prototype.sci_data.stream_defs import ctd_stream_definition, L0_pressure_stream_definition, L0_temperature_stream_definition, L0_conductivity_stream_definition
from prototype.sci_data.stream_defs import L1_pressure_stream_definition, L1_temperature_stream_definition, L1_conductivity_stream_definition, L2_practical_salinity_stream_definition, L2_density_stream_definition
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter

from ion.services.dm.utility.granule.taxonomy import TaxyTool

from pyon.public import log
from nose.plugins.attrib import attr

from pyon.public import StreamSubscriberRegistrar

from interface.objects import HdfStorage, CouchStorage
from interface.objects import ProcessDefinition

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG, RT, LCS, PRED
from pyon.core.exception import BadRequest, NotFound, Conflict

from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand

from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
import time


from pyon.util.context import LocalContextMixin

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


### Taxonomies are defined before hand out of band... somehow.
#tx = TaxyTool()
#tx.add_taxonomy_set('temp','long name for temp')
#tx.add_taxonomy_set('cond','long name for cond')
#tx.add_taxonomy_set('lat','long name for latitude')
#tx.add_taxonomy_set('lon','long name for longitude')
#tx.add_taxonomy_set('pres','long name for pres')
#tx.add_taxonomy_set('time','long name for time')
## This is an example of using groups it is not a normative statement about how to use groups
#tx.add_taxonomy_set('coordinates','This group contains coordinates...')
#tx.add_taxonomy_set('data','This group contains data...')


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
#@unittest.skip("not ready")
class TestCTDTransformsNoSim(IonIntegrationTestCase):

    def setUp(self):

        super(TestCTDTransformsNoSim, self).setUp()

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
        pid = self.processdispatchclient.schedule_process(process_definition_id= logger_procdef_id, configuration=configuration)

        return pid

    @staticmethod
    def create_process(name= '', module = '', class_name = '', configuration = None):
        '''
        A helper method to create a process
        '''

        producer_definition = ProcessDefinition(name=name)
        producer_definition.executable = {
            'module':module,
            'class': class_name
        }

        process_dispatcher = ProcessDispatcherServiceClient()

        procdef_id = process_dispatcher.create_process_definition(process_definition=producer_definition)
        pid = process_dispatcher.schedule_process(process_definition_id= procdef_id, configuration=configuration)

        return procdef_id

    #    @unittest.skip('test not working')
    def test_createTransformsThenPublishGranules(self):

        #-------------------------------
        # Create CTD Parsed as the first data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def)

        log.debug('test_createTransformsThenActivateInstrument: new Stream Definition id = %s' % ctd_stream_def_id)
        log.debug('Creating new CDM data product with a stream definition')

#        stream_id = self.pubsubclient.create_stream(stream_definition_id=ctd_stream_def_id)

        craft = CoverageCraft
        sdom, tdom = craft.create_domains()
        sdom = sdom.dump()
        tdom = tdom.dump()
        parameter_dictionary = craft.create_parameters()
        parameter_dictionary = parameter_dictionary.dump()

        dp_obj = IonObject(RT.DataProduct,
            name='ctd_parsed',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_parsed_data_product = self.dataproductclient.create_data_product(dp_obj, ctd_stream_def_id, parameter_dictionary)

        log.debug('new ctd_parsed_data_product_id = %s' % ctd_parsed_data_product)

#        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_parsed_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product)

#        Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product, PRED.hasStream, None, True)
        log.debug('test_createTransformsThenActivateInstrument: Data product streams1 = %s' % stream_ids)
        self.parsed_stream_id = stream_ids[0]

        log.debug("got the parsed stream id: %s" % self.parsed_stream_id)

        #-------------------------------------------------------------------------------------
        # The configuration for the Event Alert Transform... set up the event types to listen to
        #-------------------------------------------------------------------------------------
        configuration = {
            'process':{
                'exchange_point': 'science_data',
                'stream_id':self.parsed_stream_id,
                }
        }

        #todo simple_ctd_data_producer needs to be fixed

        #-------------------------------------------------------------------------------------
        # Create the process -  ctd simulator process
        #-------------------------------------------------------------------------------------
        producer_procdef_id = TestCTDTransformsNoSim.create_process(  name='Example Data Producer',
            module='ion.services.sa.test.simple_ctd_data_producer',
            class_name='SimpleCtdDataProducer',
            configuration= configuration)

        log.debug("Created an example data producer process with the following id: %s" % producer_procdef_id)

        self.assertIsNotNone(producer_procdef_id)

        #-------------------------------
        # Create CTD Raw as the second data product
        #-------------------------------
        log.debug('test_createTransformsThenActivateInstrument: Creating new RAW data product with a stream definition')

        raw_stream_def = SBE37_RAW_stream_definition()
        raw_stream_def_id = self.pubsubclient.create_stream_definition(container=raw_stream_def)

        dp_obj = IonObject(RT.DataProduct,
            name='ctd_raw',
            description='raw stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_raw_data_product = self.dataproductclient.create_data_product(dp_obj, raw_stream_def_id, parameter_dictionary)

        print 'new ctd_raw_data_product_id = ', ctd_raw_data_product

        #self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=ctd_raw_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_raw_data_product)

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


        #        ctd_L0_all_dprocdef_id = TestCTDTransformsNoSim.create_process(  name='ctd_L0_all',
        #            module='ion.processes.data.transforms.ctd.ctd_L0_all',
        #            class_name='ctd_L0_all',
        #            configuration= configuration)
        #
        #        self.assertIsNotNone(ctd_L0_all_dprocdef_id)


        #-------------------------------
        # L1 Conductivity: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition CTDL1ConductivityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L1_conductivity',
            description='create the L1 conductivity data product',
            module='ion.processes.data.transforms.ctd.ctd_L1_conductivity',
            class_name='CTDL1ConductivityTransform',
            process_source='CTDL1ConductivityTransform source code here...')
        try:
            ctd_L1_conductivity_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new CTDL1ConductivityTransform data process definition: %s" %ex)

        #
        #        ctd_L1_conductivity_dprocdef_id = TestCTDTransformsNoSim.create_process(  name='ctd_L1_conductivity',
        #            module='ion.processes.data.transforms.ctd.ctd_L1_conductivity',
        #            class_name='CTDL1ConductivityTransform',
        #            configuration= configuration)
        #
        #        self.assertIsNotNone(ctd_L1_conductivity_dprocdef_id)

        #-------------------------------
        # L1 Pressure: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition CTDL1PressureTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L1_pressure',
            description='create the L1 pressure data product',
            module='ion.processes.data.transforms.ctd.ctd_L1_pressure',
            class_name='CTDL1PressureTransform',
            process_source='CTDL1PressureTransform source code here...')
        try:
            ctd_L1_pressure_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new CTDL1PressureTransform data process definition: %s" %ex)

        #        ctd_L1_conductivity_dprocdef_id = TestCTDTransformsNoSim.create_process(  name='ctd_L1_conductivity',
        #            module='ion.processes.data.transforms.ctd.ctd_L1_conductivity',
        #            class_name='CTDL1ConductivityTransform',
        #            configuration= configuration)
        #
        #        self.assertIsNotNone(ctd_L1_conductivity_dprocdef_id)

        #-------------------------------
        # L1 Temperature: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition CTDL1TemperatureTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L1_temperature',
            description='create the L1 temperature data product',
            module='ion.processes.data.transforms.ctd.ctd_L1_temperature',
            class_name='CTDL1TemperatureTransform',
            process_source='CTDL1TemperatureTransform source code here...')
        try:
            ctd_L1_temperature_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new CTDL1TemperatureTransform data process definition: %s" %ex)

        #        ctd_L1_temperature_dprocdef_id = TestCTDTransformsNoSim.create_process(  name='ctd_L1_temperature',
        #            module='ion.processes.data.transforms.ctd.ctd_L1_temperature',
        #            class_name='CTDL1TemperatureTransform',
        #            configuration= configuration)
        #
        #        self.assertIsNotNone(ctd_L1_temperature_dprocdef_id)


        #-------------------------------
        # L2 Salinity: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition SalinityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L2_salinity',
            description='create the L1 temperature data product',
            module='ion.processes.data.transforms.ctd.ctd_L2_salinity',
            class_name='SalinityTransform',
            process_source='SalinityTransform source code here...')
        try:
            ctd_L2_salinity_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new SalinityTransform data process definition: %s" %ex)

        #        ctd_L2_salinity_dprocdef_id = TestCTDTransformsNoSim.create_process(  name='ctd_L2_salinity',
        #            module='ion.processes.data.transforms.ctd.ctd_L2_salinity',
        #            class_name='SalinityTransform',
        #            configuration= configuration)
        #
        #        self.assertIsNotNone(ctd_L2_salinity_dprocdef_id)

        #-------------------------------
        # L2 Density: Data Process Definition
        #-------------------------------
        log.debug("TestIntDataProcessMgmtServiceMultiOut: create data process definition DensityTransform")
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='ctd_L2_density',
            description='create the L1 temperature data product',
            module='ion.processes.data.transforms.ctd.ctd_L2_density',
            class_name='DensityTransform',
            process_source='DensityTransform source code here...')
        try:
            ctd_L2_density_dprocdef_id = self.dataprocessclient.create_data_process_definition(dpd_obj)
        except BadRequest as ex:
            self.fail("failed to create new DensityTransform data process definition: %s" %ex)

        #        ctd_L2_density_dprocdef_id = TestCTDTransformsNoSim.create_process(  name='ctd_L2_density',
        #            module='ion.processes.data.transforms.ctd.ctd_L2_density',
        #            class_name='DensityTransform',
        #            configuration= configuration)
        #
        #        self.assertIsNotNone(ctd_L2_density_dprocdef_id)

        self.loggerpids = []


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

        ctd_l0_conductivity_output_dp_obj = IonObject(RT.DataProduct,
            name='L0_Conductivity',
            description='transform output conductivity',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l0_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_conductivity_output_dp_obj,
            outgoing_stream_l0_conductivity_id,
            parameter_dictionary)
        self.output_products['conductivity'] = ctd_l0_conductivity_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_conductivity_output_dp_id)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 pressure")

        ctd_l0_pressure_output_dp_obj = IonObject(RT.DataProduct,
            name='L0_Pressure',
            description='transform output pressure',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l0_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_pressure_output_dp_obj, outgoing_stream_l0_pressure_id, parameter_dictionary)
        self.output_products['pressure'] = ctd_l0_pressure_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_pressure_output_dp_id)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L0 temperature")

        ctd_l0_temperature_output_dp_obj = IonObject(RT.DataProduct,
            name='L0_Temperature',
            description='transform output temperature',
            temporal_domain = tdom,
            spatial_domain = sdom)


        ctd_l0_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l0_temperature_output_dp_obj, outgoing_stream_l0_temperature_id, parameter_dictionary)
        self.output_products['temperature'] = ctd_l0_temperature_output_dp_id
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l0_temperature_output_dp_id)


        #-------------------------------
        # L1 Conductivity - Temperature - Pressure: Output Data Products
        #-------------------------------

        outgoing_stream_l1_conductivity = L1_conductivity_stream_definition()
        outgoing_stream_l1_conductivity_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l1_conductivity, name='L1_conductivity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_conductivity_id, ctd_L1_conductivity_dprocdef_id )

        outgoing_stream_l1_pressure = L1_pressure_stream_definition()
        outgoing_stream_l1_pressure_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l1_pressure, name='L1_Pressure')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_pressure_id, ctd_L1_pressure_dprocdef_id )

        outgoing_stream_l1_temperature = L1_temperature_stream_definition()
        outgoing_stream_l1_temperature_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l1_temperature, name='L1_Temperature')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l1_temperature_id, ctd_L1_temperature_dprocdef_id )

        log.debug("test_createTransformsThenActivateInstrument: create output data product L1 conductivity")

        ctd_l1_conductivity_output_dp_obj = IonObject(RT.DataProduct,
            name='L1_Conductivity',
            description='transform output L1 conductivity',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l1_conductivity_output_dp_id = self.dataproductclient.create_data_product(ctd_l1_conductivity_output_dp_obj, outgoing_stream_l1_conductivity_id, parameter_dictionary)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l1_conductivity_output_dp_id)

        stream_ids, _ = self.rrclient.find_objects(ctd_l1_conductivity_output_dp_id, PRED.hasStream, None, True)
        log.debug(" ctd_l1_conductivity stream id =  %s", str(stream_ids) )
        pid = self.create_logger(' ctd_l1_conductivity', stream_ids[0] )
        self.loggerpids.append(pid)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L1 pressure")

        ctd_l1_pressure_output_dp_obj = IonObject(RT.DataProduct,
            name='L1_Pressure',
            description='transform output L1 pressure',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l1_pressure_output_dp_id = self.dataproductclient.create_data_product(ctd_l1_pressure_output_dp_obj, outgoing_stream_l1_pressure_id, parameter_dictionary)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l1_pressure_output_dp_id)

        stream_ids, _ = self.rrclient.find_objects(ctd_l1_pressure_output_dp_id, PRED.hasStream, None, True)
        log.debug(" ctd_l1_pressure stream id =  %s", str(stream_ids) )
        pid = self.create_logger(' ctd_l1_pressure', stream_ids[0] )
        self.loggerpids.append(pid)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L1 temperature")

        ctd_l1_temperature_output_dp_obj = IonObject(RT.DataProduct,
            name='L1_Temperature',
            description='transform output L1 temperature',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l1_temperature_output_dp_id = self.dataproductclient.create_data_product(ctd_l1_temperature_output_dp_obj, outgoing_stream_l1_temperature_id, parameter_dictionary)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l1_temperature_output_dp_id)

        stream_ids, _ = self.rrclient.find_objects(ctd_l1_temperature_output_dp_id, PRED.hasStream, None, True)
        log.debug(" ctd_l1_temperature stream id =  %s", str(stream_ids) )
        pid = self.create_logger(' ctd_l1_temperature', stream_ids[0] )
        self.loggerpids.append(pid)



        #-------------------------------
        # L2 Salinity - Density: Output Data Products
        #-------------------------------

        outgoing_stream_l2_salinity = L2_practical_salinity_stream_definition()
        outgoing_stream_l2_salinity_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l2_salinity, name='L2_salinity')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l2_salinity_id, ctd_L2_salinity_dprocdef_id )

        outgoing_stream_l2_density = L2_density_stream_definition()
        outgoing_stream_l2_density_id = self.pubsubclient.create_stream_definition(container=outgoing_stream_l2_density, name='L2_Density')
        self.dataprocessclient.assign_stream_definition_to_data_process_definition(outgoing_stream_l2_density_id, ctd_L2_density_dprocdef_id )

        log.debug("test_createTransformsThenActivateInstrument: create output data product L2 Salinity")

        ctd_l2_salinity_output_dp_obj = IonObject(RT.DataProduct,
            name='L2_Salinity',
            description='transform output L2 salinity',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l2_salinity_output_dp_id = self.dataproductclient.create_data_product(ctd_l2_salinity_output_dp_obj, outgoing_stream_l2_salinity_id, parameter_dictionary)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l2_salinity_output_dp_id)

        log.debug("test_createTransformsThenActivateInstrument: create output data product L2 Density")

        ctd_l2_density_output_dp_obj = IonObject(RT.DataProduct,
            name='L2_Density',
            description='transform output pressure',
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_l2_density_output_dp_id = self.dataproductclient.create_data_product(ctd_l2_density_output_dp_obj, outgoing_stream_l2_density_id, parameter_dictionary)
        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_l2_density_output_dp_id)

        # Set up subscribers/loggers to these streams
        stream_ids, _ = self.rrclient.find_objects(ctd_l2_salinity_output_dp_id, PRED.hasStream, None, True)
        log.debug("L2 salinity stream id =  %s", str(stream_ids) )
        pid = self.create_logger('L2_salinity', stream_ids[0] )
        self.loggerpids.append(pid)

        stream_ids, _ = self.rrclient.find_objects(ctd_l2_density_output_dp_id, PRED.hasStream, None, True)
        log.debug("L2 density stream id =  %s", str(stream_ids) )
        pid = self.create_logger('L2_density', stream_ids[0] )
        self.loggerpids.append(pid)

        #-------------------------------
        # L0 Conductivity - Temperature - Pressure: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L0 all data_process start")
        try:
            ctd_l0_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L0_all_dprocdef_id, [ctd_parsed_data_product], self.output_products)
            self.dataprocessclient.activate_data_process(ctd_l0_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L0 all data_process return")


        #-------------------------------
        # L1 Conductivity: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L1 Conductivity data_process start")
        try:
            l1_conductivity_data_process_id = self.dataprocessclient.create_data_process(ctd_L1_conductivity_dprocdef_id, [ctd_l0_conductivity_output_dp_id], {'output':ctd_l1_conductivity_output_dp_id})
            self.dataprocessclient.activate_data_process(l1_conductivity_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L1 Conductivity data_process return")


        #-------------------------------
        # L1 Pressure: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L1_Pressure data_process start")
        try:
            l1_pressure_data_process_id = self.dataprocessclient.create_data_process(ctd_L1_pressure_dprocdef_id, [ctd_l0_pressure_output_dp_id], {'output':ctd_l1_pressure_output_dp_id})
            self.dataprocessclient.activate_data_process(l1_pressure_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L1_Pressure data_process return")



        #-------------------------------
        # L1 Temperature: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L1_Pressure data_process start")
        try:
            l1_temperature_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L1_temperature_dprocdef_id, [ctd_l0_temperature_output_dp_id], {'output':ctd_l1_temperature_output_dp_id})
            self.dataprocessclient.activate_data_process(l1_temperature_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L1_Pressure data_process return")



        #-------------------------------
        # L2 Salinity: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L2_salinity data_process start")
        try:
            l2_salinity_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L2_salinity_dprocdef_id, [ctd_parsed_data_product], {'output':ctd_l2_salinity_output_dp_id})
            self.dataprocessclient.activate_data_process(l2_salinity_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L2_salinity data_process return")

        #-------------------------------
        # L2 Density: Create the data process
        #-------------------------------
        log.debug("test_createTransformsThenActivateInstrument: create L2_Density data_process start")
        try:
            l2_density_all_data_process_id = self.dataprocessclient.create_data_process(ctd_L2_density_dprocdef_id, [ctd_parsed_data_product], {'output':ctd_l2_density_output_dp_id})
            self.dataprocessclient.activate_data_process(l2_density_all_data_process_id)
        except BadRequest as ex:
            self.fail("failed to create new data process: %s" %ex)

        log.debug("test_createTransformsThenActivateInstrument: create L2_Density data_process return")



#        producer_pid = self.processdispatchclient.schedule_process(process_definition_id= producer_procdef_id, configuration=configuration)

#        time.sleep(2.0)
#
#
#        # clean up the launched processes
#        self.processdispatchclient.cancel_process(producer_pid)
#        for pid in self.loggerpids:
#            self.processdispatchclient.cancel_process(pid)




