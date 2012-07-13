#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import  log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.sa.product.data_product_management_service import DataProductManagementService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import  DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from prototype.sci_data.stream_defs import ctd_stream_definition, SBE37_CDM_stream_definition
from interface.objects import HdfStorage, CouchStorage, DataProduct, LastUpdate

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
#@unittest.skip('not working')
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
        self.process_dispatcher   = ProcessDispatcherServiceClient()


    def test_createDataProductVersionSimple(self):

        ctd_stream_def_id = self.pubsubcli.create_stream_definition( name='test')

        # test creating a new data product which will also create the initial/default version
        log.debug('Creating new data product with a stream definition')
        dp_obj = IonObject(RT.DataProduct, name='DP',description='some new dp')
        try:
            dp_id = self.client.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        log.debug( 'new dp_id = %s', str(dp_id))

        #test that the links exist
        version_ids, _ = self.rrclient.find_objects(subject=dp_id, predicate=PRED.hasVersion, id_only=True)
        log.debug( 'version_ids = %s', str(version_ids))
        if not version_ids:
            self.fail("failed to create new data product version as part of data product create processing")

        stream_ids, _ = self.rrclient.find_objects(subject=version_ids[0], predicate=PRED.hasStream, id_only=True)
        if not stream_ids:
            self.fail("failed to assoc new data product version with data product stream")

        # test creating a subsequent data product version which will update the data product pointers
        try:
            dpv_obj = IonObject(RT.DataProductVersion, name='DPV2',description='some new dp version')
            dpv2_id = self.client.create_data_product_version(dp_id, dpv_obj)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
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
        try:
            dpv_obj = IonObject(RT.DataProductVersion, name='DPV2',description='some new dp version')
            dpv3_id = self.client.create_data_product_version(dp_id, dpv_obj)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        log.debug( 'new dpv_id = %s', str(dpv3_id))

