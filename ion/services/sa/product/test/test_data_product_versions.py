#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import  log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import  DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from ion.services.dm.utility.granule_utils import time_series_domain
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from pyon.core.exception import BadRequest, NotFound, Conflict

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest
from pyon.public import RT, PRED
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
        self.processdispatchclient   = ProcessDispatcherServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()


    #@unittest.skip('not working')
    def test_createDataProductVersionSimple(self):

        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.pubsubcli.create_stream_definition( name='test', parameter_dictionary_id=pdict_id)

        # test creating a new data product which will also create the initial/default version
        log.debug('Creating new data product with a stream definition')

        # Construct temporal and spatial Coordinate Reference System objects
        tdom, sdom = time_series_domain()

        sdom = sdom.dump()
        tdom = tdom.dump()


        dp_obj = IonObject(RT.DataProduct,
            name='DP',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        dp_id = self.client.create_data_product(dp_obj, ctd_stream_def_id)
        log.debug( 'new dp_id = %s', str(dp_id))

        dpc_id = self.client.create_data_product_collection( data_product_id=dp_id, collection_name='firstCollection', collection_description='collection desc')

        #test that the links exist
        version_ids, _ = self.rrclient.find_objects(subject=dpc_id, predicate=PRED.hasVersion, id_only=True)
        log.debug( 'version_ids = %s', str(version_ids))
        self.assertTrue(version_ids, 'Failed to connect the data product to the version collection.')
        self.assertTrue(version_ids[0] ==  dp_id, 'Failed to connect the data product to the version collection.')


        # test creating a subsequent data product version which will update the data product pointers

        dp2_obj = IonObject(RT.DataProduct,
            name='DP2',
            description='a second dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        dp2_id = self.client.create_data_product(dp2_obj, ctd_stream_def_id)
        log.debug( 'second dp_id = %s', str(dp2_id))


        self.client.add_data_product_version_to_collection(data_product_id=dp2_id, data_product_collection_id=dpc_id, version_name="second version", version_description="a second version created" )

        #test that the links exist
        version_ids, _ = self.rrclient.find_objects(subject=dpc_id, predicate=PRED.hasVersion, id_only=True)
        if len(version_ids) != 2:
            self.fail("data product should have two versions")

        recent_version_id = self.client.get_current_version(dpc_id)
        self.assertEquals(recent_version_id, dp2_id )
        base_version_id = self.client.get_base_version(dpc_id)
        self.assertEquals(base_version_id, dp_id )

        #---------------------------------------------------------------------------------------------
        # Now check that we can subscribe to the stream for the data product version
        #---------------------------------------------------------------------------------------------
        # Activating the data products contained in the versions held by the data product collection
        data_product_collection_obj = self.rrclient.read(dpc_id)
        version_list = data_product_collection_obj.version_list

        self.assertEquals(len(version_list), 2)

        for version in version_list:
            data_product_id = version.data_product_id
            self.client.activate_data_product_persistence(data_product_id)

            streams, _ = self.rrclient.find_objects(subject=data_product_id,
                                                 predicate=PRED.hasStream,
                                                object_type=RT.Stream)
            self.assertTrue(len(streams) > 0)

            for stream in streams:
                self.assertTrue(stream.persisted)

        log.debug("This satisfies L4-CI-DM-RQ-053: 'The dynamic data distribution services shall support multiple versions of a given data topic.' "
                  "This is true because we have shown above that we can persist the streams associated to the data product versions, and therefore we "
                  "can subscribe to them. In test_oms_launch2.py, we have tested that activated data products like the ones we have here can be used by data processes "
                  "to gather streaming data and it all works together correctly. This therefore completes the demonstration of req L4-CI-DM-RQ-053.")


        #---------------------------------------------------------------------------------------------
        # Now delete all the created stuff and look for possible problems in doing so
        #---------------------------------------------------------------------------------------------

        self.client.delete_data_product(dp_id)
        self.client.delete_data_product(dp2_id)
        self.client.delete_data_product_collection(dpc_id)
        self.client.force_delete_data_product_collection(dpc_id)
        # now try to get the deleted dp object
        try:
            dp_obj = self.client.read_data_product_collection(dpc_id)
        except NotFound as ex:
            pass
        else:
            self.fail("deleted data product collection was found during read")

