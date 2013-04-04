#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from ion.services.sa.observatory.observatory_management_service import LOGICAL_TRANSFORM_DEFINITION_NAME
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.sa.test.helpers import any_old
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.public import log, IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient


from pyon.util.context import LocalContextMixin
from pyon.core.exception import NotFound, BadRequest
from pyon.public import RT, PRED
#from mock import Mock, patch
from pyon.util.ion_time import IonTime
from nose.plugins.attrib import attr
from pyon.public import OT


from coverage_model.coverage import GridDomain, GridShape, CRS
from coverage_model.basic_types import MutabilityEnum, AxisTypeEnum

import datetime

class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
class TestDeployment(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dmpsclient = DataProductManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.psmsclient = PubsubManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()

        self.c = DotDict()
        self.c.resource_registry = self.rrclient
        self.RR2 = EnhancedResourceRegistryClient(self.rrclient)

        # create missing data process definition
        self.dsmsclient = DataProcessManagementServiceClient(node=self.container.node)
        dpd_obj = IonObject(RT.DataProcessDefinition,
                            name=LOGICAL_TRANSFORM_DEFINITION_NAME,
                            description="normally in preload",
                            module='ion.processes.data.transforms.logical_transform',
                            class_name='logical_transform')
        self.dsmsclient.create_data_process_definition(dpd_obj)

        # deactivate all data processes when tests are complete
        def killAllDataProcesses():
            for proc_id in self.rrclient.find_resources(RT.DataProcess, None, None, True)[0]:
                self.dsmsclient.deactivate_data_process(proc_id)
                self.dsmsclient.delete_data_process(proc_id)
        self.addCleanup(killAllDataProcesses)


    #@unittest.skip("targeting")
    def test_create_deployment(self):

        #create a deployment with metadata and an initial site and device
        platform_site__obj = IonObject(RT.PlatformSite,
                                        name='PlatformSite1',
                                        description='test platform site')
        site_id = self.omsclient.create_platform_site(platform_site__obj)

        platform_device__obj = IonObject(RT.PlatformDevice,
                                        name='PlatformDevice1',
                                        description='test platform device')
        device_id = self.imsclient.create_platform_device(platform_device__obj)

        start = IonTime(datetime.datetime(2013,1,1))
        end = IonTime(datetime.datetime(2014,1,1))
        temporal_bounds = IonObject(OT.TemporalBounds, name='planned', start_datetime=start.to_string(), end_datetime=end.to_string())
        deployment_obj = IonObject(RT.Deployment,
                                        name='TestDeployment',
                                        description='some new deployment',
                                        constraint_list=[temporal_bounds])
        deployment_id = self.omsclient.create_deployment(deployment_obj)
        self.omsclient.deploy_platform_site(site_id, deployment_id)
        self.imsclient.deploy_platform_device(device_id, deployment_id)

        log.debug("test_create_deployment: created deployment id: %s ", str(deployment_id) )

        #retrieve the deployment objects and check that the assoc site and device are attached
        read_deployment_obj = self.omsclient.read_deployment(deployment_id)
        log.debug("test_create_deployment: created deployment obj: %s ", str(read_deployment_obj) )

        site_ids, _ = self.rrclient.find_subjects(RT.PlatformSite, PRED.hasDeployment, deployment_id, True)
        self.assertEqual(len(site_ids), 1)

        device_ids, _ = self.rrclient.find_subjects(RT.PlatformDevice, PRED.hasDeployment, deployment_id, True)
        self.assertEqual(len(device_ids), 1)

        #delete the deployment
        self.RR2.pluck(deployment_id)
        self.omsclient.force_delete_deployment(deployment_id)
        # now try to get the deleted dp object
        try:
            self.omsclient.read_deployment(deployment_id)
        except NotFound:
            pass
        else:
            self.fail("deleted deployment was found during read")




    #@unittest.skip("targeting")
    def base_activate_deployment(self):

        #-------------------------------------------------------------------------------------
        # Create platform site, platform device, platform model
        #-------------------------------------------------------------------------------------

        platform_site__obj = IonObject(RT.PlatformSite,
                                        name='PlatformSite1',
                                        description='test platform site')
        platform_site_id = self.omsclient.create_platform_site(platform_site__obj)

        platform_device_obj = IonObject(RT.PlatformDevice,
                                        name='PlatformDevice1',
                                        description='test platform device')
        platform_device_id = self.imsclient.create_platform_device(platform_device_obj)

        platform_model__obj = IonObject(RT.PlatformModel,
                                        name='PlatformModel1',
                                        description='test platform model')
        platform_model_id = self.imsclient.create_platform_model(platform_model__obj)



        #-------------------------------------------------------------------------------------
        # Create instrument site
        #-------------------------------------------------------------------------------------

        instrument_site_obj = IonObject(RT.InstrumentSite,
                                        name='InstrumentSite1',
                                        description='test instrument site')
        instrument_site_id = self.omsclient.create_instrument_site(instrument_site_obj, platform_site_id)

        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        ctd_stream_def_id = self.psmsclient.create_stream_definition(name='SBE37_CDM', parameter_dictionary_id=pdict_id)


        # Construct temporal and spatial Coordinate Reference System objects
        tdom, sdom = time_series_domain()

        sdom = sdom.dump()
        tdom = tdom.dump()



        dp_obj = IonObject(RT.DataProduct,
            name='Log Data Product',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        out_log_data_product_id = self.dmpsclient.create_data_product(dp_obj, ctd_stream_def_id)

        #----------------------------------------------------------------------------------------------------
        # Start the transform (a logical transform) that acts as an instrument site
        #----------------------------------------------------------------------------------------------------

        self.omsclient.create_site_data_product(    site_id= instrument_site_id,
                                                    data_product_id =  out_log_data_product_id)


        #----------------------------------------------------------------------------------------------------
        # Create an instrument device
        #----------------------------------------------------------------------------------------------------

        instrument_device_obj = IonObject(RT.InstrumentDevice,
                                        name='InstrumentDevice1',
                                        description='test instrument device')
        instrument_device_id = self.imsclient.create_instrument_device(instrument_device_obj)
        self.rrclient.create_association(platform_device_id, PRED.hasDevice, instrument_device_id)


        dp_obj = IonObject(RT.DataProduct,
            name='Instrument Data Product',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        inst_data_product_id = self.dmpsclient.create_data_product(dp_obj, ctd_stream_def_id)

        #assign data products appropriately
        self.damsclient.assign_data_product(input_resource_id=instrument_device_id,
                                            data_product_id=inst_data_product_id)
        #----------------------------------------------------------------------------------------------------
        # Create an instrument model
        #----------------------------------------------------------------------------------------------------

        instrument_model_obj = IonObject(RT.InstrumentModel,
                                        name='InstrumentModel1',
                                        description='test instrument model')
        instrument_model_id = self.imsclient.create_instrument_model(instrument_model_obj)


        #----------------------------------------------------------------------------------------------------
        # Create a deployment object
        #----------------------------------------------------------------------------------------------------

        start = IonTime(datetime.datetime(2013,1,1))
        end = IonTime(datetime.datetime(2014,1,1))
        temporal_bounds = IonObject(OT.TemporalBounds, name='planned', start_datetime=start.to_string(), end_datetime=end.to_string())
        deployment_obj = IonObject(RT.Deployment,
                                        name='TestDeployment',
                                        description='some new deployment',
                                        constraint_list=[temporal_bounds])
        deployment_id = self.omsclient.create_deployment(deployment_obj)

        log.debug("test_create_deployment: created deployment id: %s ", str(deployment_id) )


        ret = DotDict(instrument_site_id=instrument_site_id,
                      instrument_device_id=instrument_device_id,
                      instrument_model_id=instrument_model_id,
                      platform_site_id=platform_site_id,
                      platform_device_id=platform_device_id,
                      platform_model_id=platform_model_id,
                      deployment_id=deployment_id)


        return ret

    #@unittest.skip("targeting")
    def test_activate_deployment_normal(self):

        res = self.base_activate_deployment()

        log.debug("assigning platform and instrument models")
        self.imsclient.assign_platform_model_to_platform_device(res.platform_model_id, res.platform_device_id)
        self.imsclient.assign_instrument_model_to_instrument_device(res.instrument_model_id, res.instrument_device_id)
        self.omsclient.assign_platform_model_to_platform_site(res.platform_model_id, res.platform_site_id)
        self.omsclient.assign_instrument_model_to_instrument_site(res.instrument_model_id, res.instrument_site_id)

        log.debug("adding instrument site and device to deployment")
        self.omsclient.deploy_instrument_site(res.instrument_site_id, res.deployment_id)
        self.imsclient.deploy_instrument_device(res.instrument_device_id, res.deployment_id)

        log.debug("adding platform site and device to deployment")
        self.omsclient.deploy_platform_site(res.platform_site_id, res.deployment_id)
        self.imsclient.deploy_platform_device(res.platform_device_id, res.deployment_id)

        log.debug("activating deployment, expecting success")
        self.omsclient.activate_deployment(res.deployment_id)

        log.debug("deactivatin deployment, expecting success")
        self.omsclient.deactivate_deployment(res.deployment_id)

    #@unittest.skip("targeting")
    def test_activate_deployment_nomodels(self):

        res = self.base_activate_deployment()

        self.omsclient.deploy_instrument_site(res.instrument_site_id, res.deployment_id)
        self.imsclient.deploy_instrument_device(res.instrument_device_id, res.deployment_id)

        log.debug("activating deployment without site+device models, expecting fail")
        self.assert_deploy_fail(res.deployment_id, "Expected at least 1 model for InstrumentSite")

        log.debug("assigning instrument site model")
        self.omsclient.assign_instrument_model_to_instrument_site(res.instrument_model_id, res.instrument_site_id)

        log.debug("activating deployment without device models, expecting fail")
        self.assert_deploy_fail(res.deployment_id, "Expected 1 model for InstrumentDevice")

    #@unittest.skip("targeting")
    def test_activate_deployment_nosite(self):

        res = self.base_activate_deployment()

        log.debug("assigning instrument models")
        self.imsclient.assign_instrument_model_to_instrument_device(res.instrument_model_id, res.instrument_device_id)
        self.omsclient.assign_instrument_model_to_instrument_site(res.instrument_model_id, res.instrument_site_id)

        log.debug("deploying instrument device only")
        self.imsclient.deploy_instrument_device(res.instrument_device_id, res.deployment_id)

        log.debug("activating deployment without device models, expecting fail")
        self.assert_deploy_fail(res.deployment_id, "No sites were found in the deployment")

    #@unittest.skip("targeting")
    def test_activate_deployment_nodevice(self):

        res = self.base_activate_deployment()

        log.debug("assigning platform and instrument models")
        self.imsclient.assign_instrument_model_to_instrument_device(res.instrument_model_id, res.instrument_device_id)
        self.omsclient.assign_instrument_model_to_instrument_site(res.instrument_model_id, res.instrument_site_id)

        log.debug("deploying instrument site only")
        self.omsclient.deploy_instrument_site(res.instrument_site_id, res.deployment_id)

        log.debug("activating deployment without device models, expecting fail")
        self.assert_deploy_fail(res.deployment_id, "The set of devices could not be mapped to the set of sites")


    def assert_deploy_fail(self, deployment_id, fail_message="did not specify fail_message"):
        with self.assertRaises(BadRequest) as cm:
            self.omsclient.activate_deployment(deployment_id)
        self.assertIn(fail_message, cm.exception.message)


    def test_3x3_matchups(self):
        """
        This will be 1 root platform, 3 sub platforms (2 of one model, 1 of another) and 3 sub instruments each (2-to-1)
        """

        instrument_model_id  = [self.RR2.create(any_old(RT.InstrumentModel)) for _ in range(6)]
        platform_model_id    = [self.RR2.create(any_old(RT.PlatformModel)) for _ in range(3)]

        instrument_device_id = [self.RR2.create(any_old(RT.InstrumentDevice)) for _ in range(9)]
        platform_device_id   = [self.RR2.create(any_old(RT.PlatformDevice)) for _ in range(4)]

        instrument_site_id   = [self.RR2.create(any_old(RT.InstrumentSite,
                                                {"planned_uplink_port":
                                                     IonObject(OT.PlatformPort,
                                                               reference_designator="instport_%d" % (i+1))}))
                                for i in range(9)]

        platform_site_id     = [self.RR2.create(any_old(RT.PlatformSite,
                                                {"planned_uplink_port":
                                                    IonObject(OT.PlatformPort,
                                                              reference_designator="platport_%d" % (i+1))}))
                                for i in range(4)]

        deployment_id = self.RR2.create(any_old(RT.Deployment,
                                        {"context": IonObject(OT.RemotePlatformDeploymentContext)}))

        def instrument_model_at(platform_idx, instrument_idx):
            m = platform_idx * 2
            if instrument_idx > 0:
                m += 1
            return m

        def platform_model_at(platform_idx):
            if platform_idx > 0:
                return 1
            return 0

        def instrument_at(platform_idx, instrument_idx):
            return platform_idx * 3 + instrument_idx

        # set up the structure
        for p in range(3):
            m = platform_model_at(p)
            self.RR2.assign_platform_model_to_platform_site_with_has_model(platform_model_id[m], platform_site_id[p])
            self.RR2.assign_platform_model_to_platform_device_with_has_model(platform_model_id[m], platform_device_id[p])
            self.RR2.assign_platform_device_to_platform_device_with_has_device(platform_device_id[p], platform_device_id[3])
            self.RR2.assign_platform_site_to_platform_site_with_has_site(platform_site_id[p], platform_site_id[3])
            self.RR2.assign_deployment_to_platform_device_with_has_deployment(deployment_id, platform_device_id[p])
            self.RR2.assign_deployment_to_platform_site_with_has_deployment(deployment_id, platform_site_id[p])

            for i in range(3):
                m = instrument_model_at(p, i)
                idx = instrument_at(p, i)
                self.RR2.assign_instrument_model_to_instrument_site_with_has_model(instrument_model_id[m], instrument_site_id[idx])
                self.RR2.assign_instrument_model_to_instrument_device_with_has_model(instrument_model_id[m], instrument_device_id[idx])
                self.RR2.assign_instrument_device_to_platform_device_with_has_device(instrument_device_id[idx], platform_device_id[p])
                self.RR2.assign_instrument_site_to_platform_site_with_has_site(instrument_site_id[idx], platform_site_id[p])
                self.RR2.assign_deployment_to_instrument_device_with_has_deployment(deployment_id, instrument_device_id[idx])
                self.RR2.assign_deployment_to_instrument_site_with_has_deployment(deployment_id, instrument_site_id[idx])

        # top level models
        self.RR2.assign_platform_model_to_platform_device_with_has_model(platform_model_id[2], platform_device_id[3])
        self.RR2.assign_platform_model_to_platform_site_with_has_model(platform_model_id[2], platform_site_id[3])
        self.RR2.assign_deployment_to_platform_device_with_has_deployment(deployment_id, platform_device_id[3])
        self.RR2.assign_deployment_to_platform_site_with_has_deployment(deployment_id, platform_site_id[3])




        # verify structure
        for p in range(3):
            parent_id = self.RR2.find_platform_device_id_by_platform_device_using_has_device(platform_device_id[p])
            self.assertEqual(platform_device_id[3], parent_id)

            parent_id = self.RR2.find_platform_site_id_by_platform_site_using_has_site(platform_site_id[p])
            self.assertEqual(platform_site_id[3], parent_id)
        for p in platform_device_id:
            self.assertEqual(deployment_id, self.RR2.find_deployment_id_of_platform_device_using_has_deployment(p))
        for p in platform_site_id:
            self.assertEqual(deployment_id, self.RR2.find_deployment_id_of_platform_site_using_has_deployment(p))
        for i in instrument_device_id:
            self.assertEqual(deployment_id, self.RR2.find_deployment_id_of_instrument_device_using_has_deployment(i))
        for i in instrument_site_id:
            self.assertEqual(deployment_id, self.RR2.find_deployment_id_of_instrument_site_using_has_deployment(i))

        for i in range(len(platform_site_id)):
            self.assertEqual(self.RR2.find_platform_model_of_platform_device_using_has_model(platform_device_id[i]),
                             self.RR2.find_platform_model_of_platform_site_using_has_model(platform_site_id[i]))

        for i in range(len(instrument_site_id)):
            self.assertEqual(self.RR2.find_instrument_model_of_instrument_device_using_has_model(instrument_device_id[i]),
                             self.RR2.find_instrument_model_of_instrument_site_using_has_model(instrument_site_id[i]))


        self.omsclient.activate_deployment(deployment_id)

        # verify proper associations
        for i, d in enumerate(platform_device_id):
            self.assertEqual(d, self.RR2.find_platform_device_id_of_platform_site_using_has_device(platform_site_id[i]))

        for i, d in enumerate(instrument_device_id):
            self.assertEqual(d, self.RR2.find_instrument_device_id_of_instrument_site_using_has_device(instrument_site_id[i]))

        pass

