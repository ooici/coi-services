#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import  log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import  DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from prototype.sci_data.stream_defs import ctd_stream_definition, SBE37_CDM_stream_definition
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from coverage_model.parameter import ParameterDictionary
from ion.services.dm.utility.granule_utils import CoverageCraft

from pyon.util.context import LocalContextMixin
from pyon.util.containers import DotDict
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED
from mock import Mock
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from interface.objects import ProcessDefinition
import unittest
import time


class FakeProcess(LocalContextMixin):
    name = ''



@attr('INT', group='sa')
@unittest.skip('not working')
class TestDataProductVersions(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        print 'started services'

        # Now create client to DataProductManagementService
        self.client = DataProductManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.processdispatchclient   = ProcessDispatcherServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)


    @unittest.skip('not working')
    def test_createDataProductVersionSimple(self):

        ctd_stream_def_id = self.pubsubcli.create_stream_definition( name='test')

        # test creating a new data product which will also create the initial/default version
        log.debug('Creating new data product with a stream definition')

        craft = CoverageCraft
        sdom, tdom = craft.create_domains()
        sdom = sdom.dump()
        tdom = tdom.dump()
        parameter_dictionary = craft.create_parameters()
        parameter_dictionary = parameter_dictionary.dump()

        dp_obj = IonObject(RT.DataProduct,
            name='DP',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        dp_id = self.client.create_data_product(dp_obj, ctd_stream_def_id, parameter_dictionary)
        log.debug( 'new dp_id = %s', str(dp_id))

        #test that the links exist
        version_ids, _ = self.rrclient.find_objects(subject=dp_id, predicate=PRED.hasVersion, id_only=True)
        log.debug( 'version_ids = %s', str(version_ids))

        stream_ids, _ = self.rrclient.find_objects(subject=version_ids[0], predicate=PRED.hasStream, id_only=True)
        if not stream_ids:
            self.fail("failed to assoc new data product version with data product stream")

        # test creating a subsequent data product version which will update the data product pointers


        dpv_obj = IonObject(RT.DataProduct,
            name='DPV2',
            description='some new dp version',
            temporal_domain = tdom,
            spatial_domain = sdom)

        dpv2_id = self.client.create_data_product_version(dp_id, dpv_obj)
        log.debug( 'new dpv_id = %s', str(dpv2_id))


        #test that the links exist
        version_ids, _ = self.rrclient.find_objects(subject=dp_id, predicate=PRED.hasVersion, id_only=True)
        if len(version_ids) != 2:
            self.fail("data product should have two versions")

        stream_ids = self.rrclient.find_objects(subject=dpv2_id, predicate=PRED.hasStream, id_only=True)
        if not stream_ids:
            self.fail("failed to assoc second data product version with a stream")

        dp_stream_ids, _ = self.rrclient.find_objects(subject=dp_id, predicate=PRED.hasStream, id_only=True)
        if not dp_stream_ids:
            self.fail("the data product is not assoc with a stream")

#        if not str(dp_stream_ids[0]) == str(stream_ids[0]):
#            self.fail("the data product is not assoc with the stream of the most recent version")

        # test creating a subsequent data product version which will update the data product pointers

        dpv_obj = IonObject(RT.DataProduct,
            name='DPV2',
            description='some new dp version',
            temporal_domain = tdom,
            spatial_domain = sdom)

        dpv_obj = IonObject(RT.DataProductVersion, name='DPV2',description='some new dp version')
        dpv3_id = self.client.create_data_product_version(dp_id, dpv_obj)
        log.debug( 'new dpv_id = %s', str(dpv3_id))


    @unittest.skip('not working')
    def test_createDataProductVersionFromSim(self):

        # ctd simulator process
        producer_definition = ProcessDefinition(name='Example Data Producer')
        producer_definition.executable = {
            'module':'ion.services.sa.test.simple_ctd_data_producer',
            'class':'SimpleCtdDataProducer'
        }

        producer_procdef_id = self.processdispatchclient.create_process_definition(process_definition=producer_definition)


        #-------------------------------
        # Create InstrumentDevice
        #-------------------------------
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        try:
            instDevice_id1 = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.damsclient.register_instrument(instDevice_id1)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        #-------------------------------
        # Create CTD Parsed as the first data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(container=ctd_stream_def)

        print 'test_createTransformsThenActivateInstrument: new Stream Definition id = ', ctd_stream_def_id

        print 'Creating new CDM data product with a stream definition'

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
        print 'new ctd_parsed_data_product_id = ', ctd_parsed_data_product

        self.damsclient.assign_data_product(input_resource_id=instDevice_id1, data_product_id=ctd_parsed_data_product)

        self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product, PRED.hasStream, None, True)
        print 'test_createTransformsThenActivateInstrument: Data product streams1 = ', stream_ids
        self.parsed_stream_id = stream_ids[0]

        #-------------------------------
        # Streaming
        #-------------------------------

        # Start the ctd simulator to produce some data
        configuration = {
            'process':{
                'stream_id':self.parsed_stream_id,
                }
        }
        producer_pid = self.processdispatchclient.schedule_process(process_definition_id= producer_procdef_id, configuration=configuration)

        time.sleep(2.0)

        # clean up the launched processes
        self.processdispatchclient.cancel_process(producer_pid)



        #-------------------------------
        # Create InstrumentDevice 2
        #-------------------------------
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice2', description="SBE37IMDevice", serial_number="6789" )
        try:
            instDevice_id2 = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.damsclient.register_instrument(instDevice_id2)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice2: %s" %ex)

        #-------------------------------
        # Create CTD Parsed as the new version of the original data product
        #-------------------------------
        # create a stream definition for the data from the ctd simulator

        dataproductversion_obj = IonObject(RT.DataProduct,
            name='CTDParsedV2',
            description="new version" ,
            temporal_domain = tdom,
            spatial_domain = sdom)

        ctd_parsed_data_product_new_version = self.dataproductclient.create_data_product_version(ctd_parsed_data_product, dataproductversion_obj)

        print 'new ctd_parsed_data_product_version_id = ', ctd_parsed_data_product_new_version

        self.damsclient.assign_data_product(input_resource_id=instDevice_id1, data_product_id=ctd_parsed_data_product, data_product_version_id=ctd_parsed_data_product_new_version)
        #-------------------------------
        # ACTIVATE PERSISTANCE FOR DATA PRODUCT VERSIONS NOT IMPL YET!!!!!!!!
        #-------------------------------
        #self.dataproductclient.activate_data_product_persistence(data_product_id=ctd_parsed_data_product_new_version, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(ctd_parsed_data_product_new_version, PRED.hasStream, None, True)
        print 'test_createTransformsThenActivateInstrument: Data product streams2 = ', stream_ids
        self.parsed_stream_id2 = stream_ids[0]

        #-------------------------------
        # Streaming
        #-------------------------------

        # Start the ctd simulator to produce some data
        configuration = {
            'process':{
                'stream_id':self.parsed_stream_id2,
                }
        }
        producer_pid = self.processdispatchclient.schedule_process(process_definition_id= producer_procdef_id, configuration=configuration)

        time.sleep(2.0)

        # clean up the launched processes
        self.processdispatchclient.cancel_process(producer_pid)

