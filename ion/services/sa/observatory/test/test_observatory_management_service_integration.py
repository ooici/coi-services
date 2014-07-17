#!/usr/bin/env python

import unittest
from nose.plugins.attrib import attr
import os

from pyon.core.exception import NotFound, BadRequest, Inconsistent
import binascii
import uuid

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.containers import DotDict, get_ion_ts
from pyon.util.context import LocalContextMixin
from pyon.public import RT, PRED, OT, log, CFG, IonObject
from pyon.event.event import EventPublisher
from pyon.agent.agent import ResourceAgentState
from pyon.core.governance import get_actor_header

from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.sa.test.helpers import any_old
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from interface.objects import ComputedValueAvailability
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.coi.iorg_management_service import OrgManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.processes.bootstrap.ion_loader import TESTED_DOC




class FakeProcess(LocalContextMixin):
    name = ''

TEST_PATH = TESTED_DOC
TEST_XLS_FOLDER = './ion/services/sa/observatory/test_xls/'
 
@attr('INT', group='sa')
class TestObservatoryManagementServiceIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.RR = ResourceRegistryServiceClient(node=self.container.node)
        self.RR2 = EnhancedResourceRegistryClient(self.RR)
        self.OMS = ObservatoryManagementServiceClient(node=self.container.node)
        self.org_management_service = OrgManagementServiceClient(node=self.container.node)
        self.IMS =  InstrumentManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()
        #print 'TestObservatoryManagementServiceIntegration: started services'

        self.event_publisher = EventPublisher()

    def _perform_preload(self, load_cfg):
        #load_cfg["ui_path"] = "res/preload/r2_ioc/ui_assets"
        #load_cfg["path"] = "R2PreloadedResources.xlsx"
        #load_cfg["assetmappings"] = "OOIPreload.xlsx"
        self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=load_cfg)


    def _preload_scenario(self, scenario, path=TEST_PATH, idmap=False, **kwargs):
        load_cfg = dict(op="load",
                    scenario=scenario,
                    attachments="res/preload/r2_ioc/attachments",
                    path=path,
                    idmap=idmap)
        load_cfg.update(kwargs)
        self._perform_preload(load_cfg)

    def _preload_cfg(self, cfg, path=''):
        load_cfg = dict(cfg=cfg,
                    path=path)
        self._perform_preload(load_cfg)

#    @unittest.skip('this exists only for debugging the launch process')
#    def test_just_the_setup(self):
#        return

    def destroy(self, resource_ids):
        self.OMS.force_delete_observatory(resource_ids.observatory_id)
        self.OMS.force_delete_subsite(resource_ids.subsite_id)
        self.OMS.force_delete_subsite(resource_ids.subsite2_id)
        self.OMS.force_delete_subsite(resource_ids.subsiteb_id)
        self.OMS.force_delete_subsite(resource_ids.subsitez_id)
        self.OMS.force_delete_platform_site(resource_ids.platform_site_id)
        self.OMS.force_delete_platform_site(resource_ids.platform_siteb_id)
        self.OMS.force_delete_platform_site(resource_ids.platform_siteb2_id)
        self.OMS.force_delete_platform_site(resource_ids.platform_site3_id)
        self.OMS.force_delete_instrument_site(resource_ids.instrument_site_id)
        self.OMS.force_delete_instrument_site(resource_ids.instrument_site2_id)
        self.OMS.force_delete_instrument_site(resource_ids.instrument_siteb3_id)
        self.OMS.force_delete_instrument_site(resource_ids.instrument_site4_id)

    #@unittest.skip('targeting')
    def test_observatory_management(self):
        resources = self._make_associations()

        self._do_test_find_related_sites(resources)

        self._do_test_get_sites_devices_status(resources)

        self._do_test_find_site_data_products(resources)

        self._do_test_find_related_frames_of_reference(resources)

        self._do_test_create_geospatial_point_center(resources)

        self._do_test_find_observatory_org(resources)

        self.destroy(resources)

    def _do_test_find_related_sites(self, resources):

        site_resources, site_children, _, _ = self.OMS.find_related_sites(resources.org_id)

        #import sys, pprint
        #print >> sys.stderr, pprint.pformat(site_resources)
        #print >> sys.stderr, pprint.pformat(site_children)

        #self.assertIn(resources.org_id, site_resources)
        self.assertIn(resources.observatory_id, site_resources)
        self.assertIn(resources.subsite_id, site_resources)
        self.assertIn(resources.subsite_id, site_resources)
        self.assertIn(resources.subsite2_id, site_resources)
        self.assertIn(resources.platform_site_id, site_resources)
        self.assertIn(resources.instrument_site_id, site_resources)
        self.assertEquals(len(site_resources), 13)

        self.assertEquals(site_resources[resources.observatory_id].type_, RT.Observatory)

        self.assertIn(resources.org_id, site_children)
        self.assertIn(resources.observatory_id, site_children)
        self.assertIn(resources.subsite_id, site_children)
        self.assertIn(resources.subsite_id, site_children)
        self.assertIn(resources.subsite2_id, site_children)
        self.assertIn(resources.platform_site_id, site_children)
        self.assertNotIn(resources.instrument_site_id, site_children)
        self.assertEquals(len(site_children), 9)

        self.assertIsInstance(site_children[resources.subsite_id], list)
        self.assertEquals(len(site_children[resources.subsite_id]), 2)

    def _do_test_get_sites_devices_status(self, resources):
        #bin/nosetests -s -v --nologcapture ion/services/sa/observatory/test/test_observatory_management_service_integration.py:TestObservatoryManagementServiceIntegration.test_observatory_management

        full_result_dict = self.OMS.get_sites_devices_status(parent_resource_ids=[resources.org_id], include_sites=True)

        result_dict = full_result_dict[resources.org_id]

        site_resources = result_dict.get("site_resources", None)
        site_children = result_dict.get("site_children", None)

        self.assertEquals(len(site_resources), 14)
        self.assertEquals(len(site_children), 9)

        full_result_dict = self.OMS.get_sites_devices_status(parent_resource_ids=[resources.org_id], include_sites=True, include_devices=True, include_status=True)

        result_dict = full_result_dict[resources.org_id]

        log.debug("RESULT DICT: %s", result_dict.keys())
        site_resources = result_dict.get("site_resources", None)
        site_children = result_dict.get("site_children", None)
        site_status = result_dict.get("site_status", None)

        self.assertEquals(len(site_resources), 14)
        self.assertEquals(len(site_children), 9)


        full_result_dict = self.OMS.get_sites_devices_status(parent_resource_ids=[resources.observatory_id], include_sites=True, include_devices=True, include_status=True)

        result_dict = full_result_dict[resources.observatory_id]

        site_resources = result_dict.get("site_resources")
        site_children = result_dict.get("site_children")
        site_status = result_dict.get("site_status")

        self.assertEquals(len(site_resources), 13)
        self.assertEquals(len(site_children), 8)


    def _do_test_find_site_data_products(self, resources):
        res_dict = self.OMS.find_site_data_products(resources.org_id)

        #import sys, pprint
        #print >> sys.stderr, pprint.pformat(res_dict)

        self.assertIsNone(res_dict['data_product_resources'])
        self.assertIn(resources.platform_device_id, res_dict['device_data_products'])
        self.assertIn(resources.instrument_device_id, res_dict['device_data_products'])

    #@unittest.skip('targeting')
    def _do_test_find_related_frames_of_reference(self, stuff):
        # finding subordinates gives a dict of obj lists, convert objs to ids
        def idify(adict):
            ids = {}
            for k, v in adict.iteritems():
                ids[k] = []
                for obj in v:
                    ids[k].append(obj._id)

            return ids

        # a short version of the function we're testing, with id-ify
        def short(resource_id, output_types):
            ret = self.OMS.find_related_frames_of_reference(resource_id,
                                                            output_types)
            return idify(ret)
            
            
        #set up associations first
        stuff = self._make_associations()
        #basic traversal of tree from instrument to platform
        ids = short(stuff.instrument_site_id, [RT.PlatformSite])
        self.assertIn(RT.PlatformSite, ids)
        self.assertIn(stuff.platform_site_id, ids[RT.PlatformSite])
        self.assertIn(stuff.platform_siteb_id, ids[RT.PlatformSite])
        self.assertNotIn(stuff.platform_siteb2_id, ids[RT.PlatformSite])

        #since this is the first search, just make sure the input inst_id got stripped
        if RT.InstrumentSite in ids:
            self.assertNotIn(stuff.instrument_site_id, ids[RT.InstrumentSite])

        #basic traversal of tree from platform to instrument
        ids = short(stuff.platform_siteb_id, [RT.InstrumentSite])
        self.assertIn(RT.InstrumentSite, ids)
        self.assertIn(stuff.instrument_site_id, ids[RT.InstrumentSite])
        self.assertNotIn(stuff.instrument_site2_id, ids[RT.InstrumentSite])


        #full traversal of tree from observatory down to instrument
        ids = short(stuff.observatory_id, [RT.InstrumentSite])
        self.assertIn(RT.InstrumentSite, ids)
        self.assertIn(stuff.instrument_site_id, ids[RT.InstrumentSite])


        #full traversal of tree from instrument to observatory
        ids = short(stuff.instrument_site_id, [RT.Observatory])
        self.assertIn(RT.Observatory, ids)
        self.assertIn(stuff.observatory_id, ids[RT.Observatory])


        #partial traversal, only down to platform
        ids = short(stuff.observatory_id, [RT.Subsite, RT.PlatformSite])
        self.assertIn(RT.PlatformSite, ids)
        self.assertIn(RT.Subsite, ids)
        self.assertIn(stuff.platform_site_id, ids[RT.PlatformSite])
        self.assertIn(stuff.platform_siteb_id, ids[RT.PlatformSite])
        self.assertIn(stuff.platform_siteb2_id, ids[RT.PlatformSite])
        self.assertIn(stuff.platform_site3_id, ids[RT.PlatformSite])
        self.assertIn(stuff.subsite_id, ids[RT.Subsite])
        self.assertIn(stuff.subsite2_id, ids[RT.Subsite])
        self.assertIn(stuff.subsitez_id, ids[RT.Subsite])
        self.assertIn(stuff.subsiteb_id, ids[RT.Subsite])
        self.assertNotIn(RT.InstrumentSite, ids)


        #partial traversal, only down to platform
        ids = short(stuff.instrument_site_id, [RT.Subsite, RT.PlatformSite])
        self.assertIn(RT.PlatformSite, ids)
        self.assertIn(RT.Subsite, ids)
        self.assertIn(stuff.platform_siteb_id, ids[RT.PlatformSite])
        self.assertIn(stuff.platform_site_id, ids[RT.PlatformSite])
        self.assertIn(stuff.subsite_id, ids[RT.Subsite])
        self.assertIn(stuff.subsiteb_id, ids[RT.Subsite])
        self.assertNotIn(stuff.subsite2_id, ids[RT.Subsite])
        self.assertNotIn(stuff.subsitez_id, ids[RT.Subsite])
        self.assertNotIn(stuff.platform_siteb2_id, ids[RT.PlatformSite])
        self.assertNotIn(RT.Observatory, ids)

        self.destroy(stuff)

    def _make_associations(self):
        """
        create one of each resource and association used by OMS
        to guard against problems in ion-definitions
        """

        #raise unittest.SkipTest("https://jira.oceanobservatories.org/tasks/browse/CISWCORE-41")
        

        """
        the tree we're creating (observatory, sites, platforms, instruments)

        rows are lettered, colums numbered.  
         - first row is implied a
         - first column is implied 1
         - site Z, just because 

        O--Sz
        |
        S--S2--P3--I4
        |
        Sb-Pb2-Ib3
        |
        P--I2 <- PlatformDevice, InstrumentDevice2
        |
        Pb <- PlatformDevice b
        |
        I <- InstrumentDevice

        """

        org_id = self.OMS.create_marine_facility(any_old(RT.Org))

        def create_under_org(resource_type, extra_fields=None):
            obj = any_old(resource_type, extra_fields)

            if RT.InstrumentDevice == resource_type:
                resource_id = self.IMS.create_instrument_device(obj)
            else:
                resource_id, _ = self.RR.create(obj)

            self.OMS.assign_resource_to_observatory_org(resource_id=resource_id, org_id=org_id)
            return resource_id

        #stuff we control
        observatory_id          = create_under_org(RT.Observatory)
        subsite_id              = create_under_org(RT.Subsite)
        subsite2_id             = create_under_org(RT.Subsite)
        subsiteb_id             = create_under_org(RT.Subsite)
        subsitez_id             = create_under_org(RT.Subsite)
        platform_site_id        = create_under_org(RT.PlatformSite)
        platform_siteb_id       = create_under_org(RT.PlatformSite)
        platform_siteb2_id      = create_under_org(RT.PlatformSite)
        platform_site3_id       = create_under_org(RT.PlatformSite)
        instrument_site_id      = create_under_org(RT.InstrumentSite)
        instrument_site2_id     = create_under_org(RT.InstrumentSite)
        instrument_siteb3_id    = create_under_org(RT.InstrumentSite)
        instrument_site4_id     = create_under_org(RT.InstrumentSite)

        #stuff we associate to
        instrument_device_id    = create_under_org(RT.InstrumentDevice)
        instrument_device2_id   = create_under_org(RT.InstrumentDevice)
        platform_device_id      = create_under_org(RT.PlatformDevice)
        platform_deviceb_id     = create_under_org(RT.PlatformDevice)
        instrument_model_id, _  = self.RR.create(any_old(RT.InstrumentModel))
        platform_model_id, _    = self.RR.create(any_old(RT.PlatformModel))
        deployment_id, _        = self.RR.create(any_old(RT.Deployment))

        # marine tracking resources
        asset_id        = create_under_org(RT.Asset)
        asset_type_id   = create_under_org(RT.AssetType)
        event_duration_id        = create_under_org(RT.EventDuration)
        event_duration_type_id   = create_under_org(RT.EventDurationType)

        #observatory
        self.RR.create_association(observatory_id, PRED.hasSite, subsite_id)
        self.RR.create_association(observatory_id, PRED.hasSite, subsitez_id)

        #site
        self.RR.create_association(subsite_id, PRED.hasSite, subsite2_id)
        self.RR.create_association(subsite_id, PRED.hasSite, subsiteb_id)
        self.RR.create_association(subsite2_id, PRED.hasSite, platform_site3_id)
        self.RR.create_association(subsiteb_id, PRED.hasSite, platform_siteb2_id)
        self.RR.create_association(subsiteb_id, PRED.hasSite, platform_site_id)
        
        #platform_site(s)
        self.RR.create_association(platform_site3_id, PRED.hasSite, instrument_site4_id)
        self.RR.create_association(platform_siteb2_id, PRED.hasSite, instrument_siteb3_id)
        self.RR.create_association(platform_site_id, PRED.hasSite, instrument_site2_id)
        self.RR.create_association(platform_site_id, PRED.hasSite, platform_siteb_id)
        self.RR.create_association(platform_siteb_id, PRED.hasSite, instrument_site_id)

        self.RR.create_association(platform_siteb_id, PRED.hasDevice, platform_deviceb_id)
        #test network parent link
        self.OMS.assign_device_to_network_parent(platform_device_id, platform_deviceb_id)

        self.RR.create_association(platform_site_id, PRED.hasModel, platform_model_id)
        self.RR.create_association(platform_site_id, PRED.hasDevice, platform_device_id)
        self.RR.create_association(platform_site_id, PRED.hasDeployment, deployment_id)

        #instrument_site(s)
        self.RR.create_association(instrument_site_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_site_id, PRED.hasDevice, instrument_device_id)
        self.RR.create_association(instrument_site_id, PRED.hasDeployment, deployment_id)

        self.RR.create_association(instrument_site2_id, PRED.hasDevice, instrument_device2_id)

        #platform_device
        self.RR.create_association(platform_device_id, PRED.hasModel, platform_model_id)

        #instrument_device
        self.RR.create_association(instrument_device_id, PRED.hasModel, instrument_model_id)
        self.RR.create_association(instrument_device2_id, PRED.hasModel, instrument_model_id)

        ret = DotDict()
        ret.org_id                = org_id
        ret.observatory_id        = observatory_id
        ret.subsite_id            = subsite_id
        ret.subsite2_id           = subsite2_id
        ret.subsiteb_id           = subsiteb_id
        ret.subsitez_id           = subsitez_id
        ret.platform_site_id      = platform_site_id
        ret.platform_siteb_id     = platform_siteb_id
        ret.platform_siteb2_id    = platform_siteb2_id
        ret.platform_site3_id     = platform_site3_id
        ret.instrument_site_id    = instrument_site_id
        ret.instrument_site2_id   = instrument_site2_id
        ret.instrument_siteb3_id  = instrument_siteb3_id
        ret.instrument_site4_id   = instrument_site4_id

        ret.instrument_device_id  = instrument_device_id
        ret.instrument_device2_id = instrument_device2_id
        ret.platform_device_id    = platform_device_id
        ret.platform_deviceb_id    = platform_deviceb_id
        ret.instrument_model_id   = instrument_model_id
        ret.platform_model_id     = platform_model_id
        ret.deployment_id         = deployment_id

        ret.asset_id              = asset_id
        ret.asset_type_id         = asset_type_id
        ret.event_duration_id     = event_duration_id
        ret.event_duration_type_id = event_duration_type_id

        return ret

    #@unittest.skip("targeting")
    def test_create_observatory(self):
        observatory_obj = IonObject(RT.Observatory,
                                        name='TestFacility',
                                        description='some new mf')
        observatory_id = self.OMS.create_observatory(observatory_obj)
        self.OMS.force_delete_observatory(observatory_id)

    #@unittest.skip("targeting")
    def _do_test_create_geospatial_point_center(self, resources):
        platformsite_obj = IonObject(RT.PlatformSite,
                                        name='TestPlatformSite',
                                        description='some new TestPlatformSite')
        geo_index_obj = IonObject(OT.GeospatialBounds)
        geo_index_obj.geospatial_latitude_limit_north = 20.0
        geo_index_obj.geospatial_latitude_limit_south = 10.0
        geo_index_obj.geospatial_longitude_limit_east = 15.0
        geo_index_obj.geospatial_longitude_limit_west = 20.0
        platformsite_obj.constraint_list = [geo_index_obj]

        platformsite_id = self.OMS.create_platform_site(platformsite_obj)

        # now get the dp back to see if it was updated
        platformsite_obj = self.OMS.read_platform_site(platformsite_id)
        self.assertEquals('some new TestPlatformSite', platformsite_obj.description)
        self.assertAlmostEqual(15.0, platformsite_obj.geospatial_point_center.lat, places=1)


        #now adjust a few params
        platformsite_obj.description = 'some old TestPlatformSite'
        geo_index_obj = IonObject(OT.GeospatialBounds)
        geo_index_obj.geospatial_latitude_limit_north = 30.0
        geo_index_obj.geospatial_latitude_limit_south = 20.0
        platformsite_obj.constraint_list = [geo_index_obj]
        update_result = self.OMS.update_platform_site(platformsite_obj)

        # now get the dp back to see if it was updated
        platformsite_obj = self.OMS.read_platform_site(platformsite_id)
        self.assertEquals('some old TestPlatformSite', platformsite_obj.description)
        self.assertAlmostEqual(25.0, platformsite_obj.geospatial_point_center.lat, places=1)

        self.OMS.force_delete_platform_site(platformsite_id)


    #@unittest.skip("targeting")
    def _do_test_find_observatory_org(self, resources):
        log.debug("Make TestOrg")
        org_obj = IonObject(RT.Org,
                            name='TestOrg',
                            description='some new mf org')

        org_id =  self.OMS.create_marine_facility(org_obj)

        log.debug("Make Observatory")
        observatory_obj = IonObject(RT.Observatory,
                                        name='TestObservatory',
                                        description='some new obs')
        observatory_id = self.OMS.create_observatory(observatory_obj)

        log.debug("assign observatory to org")
        self.OMS.assign_resource_to_observatory_org(observatory_id, org_id)


        log.debug("verify assigment")
        org_objs = self.OMS.find_org_by_observatory(observatory_id)
        self.assertEqual(1, len(org_objs))
        self.assertEqual(org_id, org_objs[0]._id)
        log.debug("org_id=<" + org_id + ">")

        log.debug("create a subsite with parent Observatory")
        subsite_obj =  IonObject(RT.Subsite,
                                name= 'TestSubsite',
                                description = 'sample subsite')
        subsite_id = self.OMS.create_subsite(subsite_obj, observatory_id)
        self.assertIsNotNone(subsite_id, "Subsite not created.")

        log.debug("verify that Subsite is linked to Observatory")
        mf_subsite_assoc = self.RR.get_association(observatory_id, PRED.hasSite, subsite_id)
        self.assertIsNotNone(mf_subsite_assoc, "Subsite not connected to Observatory.")


        log.debug("add the Subsite as a resource of this Observatory")
        self.OMS.assign_resource_to_observatory_org(resource_id=subsite_id, org_id=org_id)
        log.debug("verify that Subsite is linked to Org")
        org_subsite_assoc = self.RR.get_association(org_id, PRED.hasResource, subsite_id)
        self.assertIsNotNone(org_subsite_assoc, "Subsite not connected as resource to Org.")


        log.debug("create a logical platform with parent Subsite")
        platform_site_obj =  IonObject(RT.PlatformSite,
                                name= 'TestPlatformSite',
                                description = 'sample logical platform')
        platform_site_id = self.OMS.create_platform_site(platform_site_obj, subsite_id)
        self.assertIsNotNone(platform_site_id, "PlatformSite not created.")

        log.debug("verify that PlatformSite is linked to Site")
        site_lp_assoc = self.RR.get_association(subsite_id, PRED.hasSite, platform_site_id)
        self.assertIsNotNone(site_lp_assoc, "PlatformSite not connected to Site.")


        log.debug("add the PlatformSite as a resource of this Observatory")
        self.OMS.assign_resource_to_observatory_org(resource_id=platform_site_id, org_id=org_id)
        log.debug("verify that PlatformSite is linked to Org")
        org_lp_assoc = self.RR.get_association(org_id, PRED.hasResource, platform_site_id)
        self.assertIsNotNone(org_lp_assoc, "PlatformSite not connected as resource to Org.")



        log.debug("create a logical instrument with parent logical platform")
        instrument_site_obj =  IonObject(RT.InstrumentSite,
                                name= 'TestInstrumentSite',
                                description = 'sample logical instrument')
        instrument_site_id = self.OMS.create_instrument_site(instrument_site_obj, platform_site_id)
        self.assertIsNotNone(instrument_site_id, "InstrumentSite not created.")


        log.debug("verify that InstrumentSite is linked to PlatformSite")
        li_lp_assoc = self.RR.get_association(platform_site_id, PRED.hasSite, instrument_site_id)
        self.assertIsNotNone(li_lp_assoc, "InstrumentSite not connected to PlatformSite.")


        log.debug("add the InstrumentSite as a resource of this Observatory")
        self.OMS.assign_resource_to_observatory_org(resource_id=instrument_site_id, org_id=org_id)
        log.debug("verify that InstrumentSite is linked to Org")
        org_li_assoc = self.RR.get_association(org_id, PRED.hasResource, instrument_site_id)
        self.assertIsNotNone(org_li_assoc, "InstrumentSite not connected as resource to Org.")


        log.debug("remove the InstrumentSite as a resource of this Observatory")
        self.OMS.unassign_resource_from_observatory_org(instrument_site_id, org_id)
        log.debug("verify that InstrumentSite is linked to Org")
        assocs,_ = self.RR.find_objects(org_id, PRED.hasResource, RT.InstrumentSite, id_only=True )
        self.assertEqual(0, len(assocs))

        log.debug("remove the InstrumentSite, association should drop automatically")
        self.OMS.delete_instrument_site(instrument_site_id)
        assocs, _ = self.RR.find_objects(platform_site_id, PRED.hasSite, RT.InstrumentSite, id_only=True )
        self.assertEqual(0, len(assocs))


        log.debug("remove the PlatformSite as a resource of this Observatory")
        self.OMS.unassign_resource_from_observatory_org(platform_site_id, org_id)
        log.debug("verify that PlatformSite is linked to Org")
        assocs,_ = self.RR.find_objects(org_id, PRED.hasResource, RT.PlatformSite, id_only=True )
        self.assertEqual(0, len(assocs))


        log.debug("remove the Site as a resource of this Observatory")
        self.OMS.unassign_resource_from_observatory_org(subsite_id, org_id)
        log.debug("verify that Site is linked to Org")
        assocs,_ = self.RR.find_objects(org_id, PRED.hasResource, RT.Subsite, id_only=True )
        self.assertEqual(0, len(assocs))

        self.RR.delete(org_id)
        self.OMS.force_delete_observatory(observatory_id)
        self.OMS.force_delete_subsite(subsite_id)
        self.OMS.force_delete_platform_site(platform_site_id)
        self.OMS.force_delete_instrument_site(instrument_site_id)


    @attr('EXT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode as it depends on modifying CFG on service side')
    def test_observatory_extensions(self):
        self.patch_cfg(CFG["container"], {"extended_resources": {"strip_results": False}})

        obs_id = self.RR2.create(any_old(RT.Observatory))
        pss_id = self.RR2.create(any_old(RT.PlatformSite, dict(alt_resource_type="StationSite")))
        pas_id = self.RR2.create(any_old(RT.PlatformSite, dict(alt_resource_type="PlatformAssemblySite")))
        pcs_id = self.RR2.create(any_old(RT.PlatformSite, dict(alt_resource_type="PlatformComponentSite")))
        ins_id = self.RR2.create(any_old(RT.InstrumentSite))

        obs_obj = self.RR2.read(obs_id)
        pss_obj = self.RR2.read(pss_id)
        pas_obj = self.RR2.read(pas_id)
        pcs_obj = self.RR2.read(pcs_id)
        ins_obj = self.RR2.read(ins_id)

        self.RR2.create_association(obs_id, PRED.hasSite, pss_id)
        self.RR2.create_association(pss_id, PRED.hasSite, pas_id)
        self.RR2.create_association(pas_id, PRED.hasSite, pcs_id)
        self.RR2.create_association(pcs_id, PRED.hasSite, ins_id)

        extended_obs = self.OMS.get_observatory_site_extension(obs_id, user_id=12345)
        self.assertEqual([pss_obj], extended_obs.platform_station_sites)
        self.assertEqual([pas_obj], extended_obs.platform_assembly_sites)
        self.assertEqual([pcs_obj], extended_obs.platform_component_sites)
        self.assertEqual([ins_obj], extended_obs.instrument_sites)

        extended_pss = self.OMS.get_observatory_site_extension(obs_id, user_id=12345)
        self.assertEqual([pas_obj], extended_pss.platform_assembly_sites)
        self.assertEqual([pcs_obj], extended_pss.platform_component_sites)
        self.assertEqual([ins_obj], extended_pss.instrument_sites)

        extended_pas = self.OMS.get_observatory_site_extension(pas_id, user_id=12345)
        self.assertEqual([pcs_obj], extended_pas.platform_component_sites)
        self.assertEqual([ins_obj], extended_pas.instrument_sites)

        extended_pcs = self.OMS.get_platform_component_site_extension(pcs_id, user_id=12345)
        self.assertEqual([ins_obj], extended_pcs.instrument_sites)


    #@unittest.skip("in development...")
    @attr('EXT')
    @attr('EXT1')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode as it depends on modifying CFG on service side')
    def test_observatory_org_extended(self):
        self.patch_cfg(CFG["container"], {"extended_resources": {"strip_results": False}})

        stuff = self._make_associations()

        parsed_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',
                                                                                    id_only=True)

        parsed_stream_def_id = self.pubsubcli.create_stream_definition(name='parsed',
                                                                       parameter_dictionary_id=parsed_pdict_id)
        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test')


        data_product_id1 = self.dpclient.create_data_product(data_product=dp_obj,
                                                             stream_definition_id=parsed_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=stuff.instrument_device_id,
                                            data_product_id=data_product_id1)


        #Create a  user to be used as regular member
        member_actor_obj = IonObject(RT.ActorIdentity, name='org member actor')
        member_actor_id,_ = self.RR.create(member_actor_obj)
        assert(member_actor_id)
        member_actor_header = get_actor_header(member_actor_id)


        member_user_obj = IonObject(RT.UserInfo, name='org member user')
        member_user_id,_ = self.RR.create(member_user_obj)
        assert(member_user_id)

        self.RR.create_association(subject=member_actor_id, predicate=PRED.hasInfo, object=member_user_id)


        #Build the Service Agreement Proposal to enroll a user actor
        sap = IonObject(OT.EnrollmentProposal,consumer=member_actor_id, provider=stuff.org_id )

        sap_response = self.org_management_service.negotiate(sap, headers=member_actor_header )

        #enroll the member without using negotiation
        self.org_management_service.enroll_member(org_id=stuff.org_id, actor_id=member_actor_id)

        #--------------------------------------------------------------------------------
        # Get the extended Site (platformSite)
        #--------------------------------------------------------------------------------

        try:
            extended_site = self.OMS.get_site_extension(stuff.platform_site_id)
        except:
            log.error('failed to get extended site', exc_info=True)
            raise
        log.debug("extended_site:  %r ", extended_site)
        self.assertEquals(stuff.subsiteb_id, extended_site.parent_site._id)
        self.assertEqual(2, len(extended_site.sites))
        self.assertEqual(2, len(extended_site.platform_devices))
        self.assertEqual(2, len(extended_site.platform_models))
        self.assertIn(stuff.platform_device_id, [o._id for o in extended_site.platform_devices])
        self.assertIn(stuff.platform_model_id, [o._id for o in extended_site.platform_models if o is not None])

        log.debug("verify that PlatformDeviceb is linked to PlatformDevice with hasNetworkParent link")
        associations = self.RR.find_associations(subject=stuff.platform_deviceb_id, predicate=PRED.hasNetworkParent, object=stuff.platform_device_id, id_only=True)
        self.assertIsNotNone(associations, "PlatformDevice child not connected to PlatformDevice parent.")


        #--------------------------------------------------------------------------------
        # Get the extended Org
        #--------------------------------------------------------------------------------
        #test the extended resource
        extended_org = self.OMS.get_marine_facility_extension(stuff.org_id)
        log.debug("test_observatory_org_extended: extended_org:  %s ", str(extended_org))
        #self.assertEqual(2, len(extended_org.instruments_deployed) )
        #self.assertEqual(1, len(extended_org.platforms_not_deployed) )
        self.assertEqual(2, extended_org.number_of_platforms)
        self.assertEqual(2, len(extended_org.platform_models) )

        self.assertEqual(2, extended_org.number_of_instruments)
        self.assertEqual(2, len(extended_org.instrument_models) )

        self.assertEqual(1, len(extended_org.members))
        self.assertNotEqual(extended_org.members[0]._id, member_actor_id)
        self.assertEqual(extended_org.members[0]._id, member_user_id)

        self.assertEqual(1, len(extended_org.open_requests))

        self.assertTrue(len(extended_site.deployments)>0)
        self.assertEqual(len(extended_site.deployments), len(extended_site.deployment_info))

        self.assertEqual(1, extended_org.number_of_assets)
        self.assertEqual(1, extended_org.number_of_asset_types)
        self.assertEqual(1, extended_org.number_of_event_durations)
        self.assertEqual(1, extended_org.number_of_event_duration_types)

        #test the extended resource of the ION org
        ion_org_id = self.org_management_service.find_org()
        extended_org = self.OMS.get_marine_facility_extension(ion_org_id._id, user_id=12345)
        log.debug("test_observatory_org_extended: extended_ION_org:  %s ", str(extended_org))
        self.assertEqual(1, len(extended_org.members))
        self.assertEqual(0, extended_org.number_of_platforms)
        #self.assertEqual(1, len(extended_org.sites))


        #--------------------------------------------------------------------------------
        # Get the extended Site
        #--------------------------------------------------------------------------------

        #create device state events to use for op /non-op filtering in extended
        t = get_ion_ts()
        self.event_publisher.publish_event(  ts_created= t,  event_type = 'ResourceAgentStateEvent',
            origin = stuff.instrument_device_id, state=ResourceAgentState.STREAMING  )

        self.event_publisher.publish_event( ts_created= t,   event_type = 'ResourceAgentStateEvent',
            origin = stuff.instrument_device2_id, state=ResourceAgentState.INACTIVE )
        extended_site =  self.OMS.get_site_extension(stuff.instrument_site2_id)


        log.debug("test_observatory_org_extended: extended_site:  %s ", str(extended_site))

        self.dpclient.delete_data_product(data_product_id1)


    """
    #--------------------------------------------------------------------
    Marine Asset Management             [hook]

    Unit tests for Marine Asset Management

    Base CRUD functions
        test_create_asset_type
        test_create_asset
        test_create_asset_bad_altid             - exercise create_asset, test altid existence, uniqueness and format
      * test_create_asset_value_types           - add attributes value_type RealValue, CodeValue, StringValue, etc
        test_create_event_duration_type
        test_create_event_duration
        test_create_event_bad_altid             - exercise create_event_duration and altid existence, uniqueness and format
        test_update_attribute_specifications    - see details below
        test_delete_attribute_specification     - see details below
        test_create_codespace                   - exercise many codespace services (see details below)

    Prepare and extensions
        test_create_asset_extension
        test_create_asset_extension_with_prepare
        test_create_event_duration_extension
        test_create_event_duration_extension_with_prepare


    Spread sheet upload and download:
        test_upload_xls                         - single upload, all sheets
        test_download_xls                       - single dump of system instances of marine tracking resources, types and code info
        test_upload_xls_twice                   - multi pass test add and update
        test_upload_codes                       - requires update and testing with CodeSpaces sheet included
        test_download_codes                     - requires update and testing with CodeSpaces sheet included
        test_upload_xls_with_codes              - loading only code related then loading everything but code related
        test_upload_xls_triple_codes            - multi load exercises 'add', update and 'remove' (remove code 'pink')
        test_upload_xls_triple_codes_only       - multi load, no CodeSpaces sheet, only Codes (remove code 'pink')
        test_upload_without_codespace_instance  - multi load, utilize code space instance, if available

        test_upload_remove_codeset
        test_upload_xls_triple_assets           - load system, add resources, remove and/or modify resources (assets)
        test_upload_xls_triple_events           - load system, add resources, remove and/or modify resources (events)
        test_upload_all_sheets_twice            - load xlsx (all sheets), reload same
        test_attribute_value_encoding
        test_get_picklist
      * test_asset_update_and_altid             - update[asset|event_duration] ensure unique altid in namespace (res.name)
        test_upload_new_attribute_specification - add new AttributeSpecification to existing type resource instance


    Data input testing:

      General:
        test_empty_workbook                         - general

        test_add_new_asset_type                     - add new asset type and include base type in spread sheet
        test_add_new_asset_type_extend_wo_base      - add asset type without base in spread sheets
        test_add_new_asset_type_extend_from_device  - add asset type which extends device
        test_add_new_asset_type_extend_from_platform- add asset type which extends (leaf) platform

        test_add_new_event_type                     - add event duration type which extends base (base in spread sheets)
        test_add_new_event_type_wo_base             - add event duration type which extends base (base not in spread sheets)

      Asset, AssetType and Attribute tests:
        test_new_asset_base                         - add new asset, extends Base AssetType (4 sheets)
        test_new_asset_base_attributes              - add new asset, extends Base
        test_new_asset_base_attributes_short        -
        test_new_asset_base_attributes_short_update -
        test_new_asset_base_one_attribute_update    -
        test_new_asset_base_one_attribute_no_types  - attribute specification sheet; provide single 'descr' attribute value for 'NewAsset'; expect defaults for all attributes other than 'descr'
        test_new_asset_base_one_attribute_only      - no attribute specification sheet; single 'descr' attribute value, expect defaults for remaing values

        test_add_new_asset_device
        test_add_new_asset_platform
        test_add_new_asset_NewType
      * test_upload_just_attributes                 - two sheets AssetAttributes and EventAttributes
      * test_alpha_preload                          - used to verify alpha preload works (for UI support)

      EventDuration to Asset Mapping Tests:
        test_deployment_to_multiple_assets

       test_update_attribute_specifications   (Exercise RT.AttributeSpecification, RT.AssetType, and service
                                                   update_attribute_specifications)
       test_delete_attribute_specification    (Exercise RT.AttributeSpecification, RT.AssetType, and service
                                                   delete_attribute_specifications)
       test_create_codespace                  (Exercise RT.CodeSpace, OT.Code, OT.CodeSet as well as services (6):
                                                   read_codes_by_name, read_codesets_by_name, update_ codes,
                                                   update_codesets, delete_ codes, delete_codesets

       * indicates spreadsheet update required or test targeted (skip for now)

       Helper functions used by unit tests:
           load_marine_assets_from_xlsx     - load system: code space, codes, codesets, assets, asset types, event durations and event durations types
           create_value                     - used to create IntegerValue, RealValue, StringValue, BooleanValue
           create_complex_value             - used to create complex types, such as CodeValue, etc.
           _create_attribute                - create 'any old' attribute of specific value type
           _create_attribute_specification  - create 'any old' attribute specification of specific value type
           _get_type_resource_by_name

    #--------------------------------------------------------------------
    """

    # -----
    # ----- UNIT TEST: test_create_asset_type
    # -----
    @attr('UNIT', group='sa')
    def test_create_asset_type(self):

        log.debug('\n\n***** Start : *test_create_asset_type')

        # Create test AssetType object
        ion_asset_type = IonObject(RT.AssetType, name='TestAssetType')
        asset_type_id = self.OMS.create_asset_type(ion_asset_type)

        # Create attribute specification
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)
        attribute_specification = self._create_attribute_specification('StringValue', 's_name', asset_type_obj.name,None,None,None)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)

        asset_type_obj = self.OMS.read_asset_type(asset_type_id)
        attribute_specification = self._create_attribute_specification('StringValue', 'descr', asset_type_obj.name,None,None,None)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)
        asset_type = self.OMS.read_asset_type(asset_type_id)

        log.debug('\n\n *** Read asset_type with two attribute_specifications: %s', asset_type)

        # ---- cleanup
        self.OMS.force_delete_asset_type(asset_type_id)
        log.debug('\n\n***** Completed: test_create_asset_type')

    # -----
    # ----- UNIT TEST: test_create_asset
    # -----
    @attr('UNIT', group='sa')
    def test_create_asset(self):

        log.debug('\n\n***** Start : test_create_asset')

        # ----- Create AssetType object with attribute specification
        ion_asset_spec = IonObject(RT.AssetType, name='TestAssetType')
        asset_type_id = self.OMS.create_asset_type(ion_asset_spec)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)

        asset_type_obj = self.OMS.read_asset_type(asset_type_id)
        attribute_specification = self._create_attribute_specification('StringValue', 's_name', asset_type_obj.name,None,None,None)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)

        # ----- Create Asset object
        asset_obj = IonObject(RT.Asset, name='Test Asset')
        asset_id = self.OMS.create_asset(asset_obj, asset_type_id)

        # ----- Read, create attribute and update Asset object
        asset_obj = self.OMS.read_asset(asset_id)

        # Create Attribute for Asset
        attribute = self._create_attribute(value_type='StringValue', name='s_name', value='hello')
        asset_obj.asset_attrs[attribute['name']] = attribute
        self.OMS.update_asset(asset_obj)

        # ----- unassign association test
        self.OMS.unassign_asset_type_from_asset(asset_type_id, asset_id)

        # ----- cleanup
        self.OMS.force_delete_asset_type(asset_type_id)
        self.OMS.force_delete_asset(asset_id)

        log.debug('\n\n***** Completed: test_create_asset')

    # -----
    # ----- UNIT TEST: test_create_asset_bad_altid
    # -----
    @attr('UNIT', group='sa')
    def test_create_asset_bad_altid(self):

        log.debug('\n\n***** Start : test_create_asset_bad_altid')

        # ----- Create Asset object - negative tests re: alt_ids
        asset_obj = IonObject(RT.Asset, name='Test Asset')

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Overview: If alt_ids are provided during asset creation, verify
        # they are well formed and unique
        #   Step 1. alt_ids with invalid namespace
        #   Step 2. alt_ids with empty namespace
        #   Step 3. alt_ids with empty name
        #   Step 4. alt_ids with multiple alt_ids provided (len != 1)
        #   Step 5. create Asset and then try to create another with same alt_ids
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Step 1. alt_ids with invalid namespace
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        log.debug('\n\n[unit] Step 1. alt_ids with empty name')
        altid = RT.EventDuration + ':' + asset_obj.name
        asset_obj.alt_ids.append(altid)
        try:
            asset_id = self.OMS.create_asset(asset_obj)
        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest: %s', Arguments.get_error_message())
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound: %s', Arguments.get_error_message())
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] Inconsistent: %s', Arguments.get_error_message())
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Step 2. alt_ids with empty namespace
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        log.debug('\n\n[unit] Step 2. alt_ids with empty name')
        asset_obj.alt_ids = []
        altid = ':' + asset_obj.name
        asset_obj.alt_ids.append(altid)
        try:
            asset_id = self.OMS.create_asset(asset_obj)
        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest: %s', Arguments.get_error_message())
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound: %s', Arguments.get_error_message())
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] Inconsistent: %s', Arguments.get_error_message())
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Step 3. alt_ids with empty name
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        log.debug('\n\n[unit] Step 3. alt_ids with empty name')
        asset_obj.alt_ids = []
        altid = RT.Asset + ':'
        asset_obj.alt_ids.append(altid)
        try:
            asset_id = self.OMS.create_asset(asset_obj)

        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest: %s', Arguments.get_error_message())
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound: %s', Arguments.get_error_message())
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] Inconsistent: %s', Arguments.get_error_message())
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Step 4. alt_ids with multiple alt_ids provided (len != 1)
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        log.debug('\n\n[unit] Step 4. alt_ids with multiple alt_ids provided (len != 1)')
        asset_obj.alt_ids = []
        altid = RT.Asset + ':' + 'fred'
        asset_obj.alt_ids.append(altid)
        asset_obj.alt_ids.append(altid)
        try:
            asset_id = self.OMS.create_asset(asset_obj)

        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest: %s', Arguments.get_error_message())
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound: %s', Arguments.get_error_message())
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] Inconsistent: %s', Arguments.get_error_message())
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Step 5. create Asset and then try to create another with same alt_ids
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        log.debug('\n\n[unit] Step 5. create Asset and then try to create another with same alt_ids')
        asset_obj.alt_ids = []
        altid = RT.Asset + ':' + asset_obj.name
        asset_obj.alt_ids.append(altid)
        try:
            asset_id = self.OMS.create_asset(asset_obj)
            asset_id = self.OMS.create_asset(asset_obj)

        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest: %s', Arguments.get_error_message())
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound: %s', Arguments.get_error_message())
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] Inconsistent: %s', Arguments.get_error_message())
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)


        log.debug('\n\n***** Completed: test_create_asset_bad_altid')

        # -----
    # ----- UNIT TEST: test_create_asset_bad_altid
    # -----
    @attr('UNIT', group='sa')
    def test_create_event_bad_altid(self):

        log.debug('\n\n***** Start : test_create_event_bad_altid')

        # ----- Create Asset object - negative tests re: alt_ids
        asset_obj = IonObject(RT.EventDuration, name='Test Event')

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Overview: If alt_ids are provided during event duration creation, verify
        # they are well formed and unique
        #   Step 1. alt_ids with invalid namespace
        #   Step 2. alt_ids with empty namespace
        #   Step 3. alt_ids with empty name
        #   Step 4. alt_ids with multiple alt_ids provided (len != 1)
        #   Step 5. create EventDuration and then try to create another with same alt_ids
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Step 1. alt_ids with invalid namespace
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        log.debug('\n\n[unit] Step 1. alt_ids with empty name')
        altid = RT.Asset + ':' + asset_obj.name
        asset_obj.alt_ids.append(altid)
        try:
            asset_id = self.OMS.create_event_duration(asset_obj)
        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest: %s', Arguments.get_error_message())
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound: %s', Arguments.get_error_message())
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] Inconsistent: %s', Arguments.get_error_message())
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Step 2. alt_ids with empty namespace
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        log.debug('\n\n[unit] Step 2. alt_ids with empty name')
        asset_obj.alt_ids = []
        altid = ':' + asset_obj.name
        asset_obj.alt_ids.append(altid)
        try:
            asset_id = self.OMS.create_event_duration(asset_obj)
        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest: %s', Arguments.get_error_message())
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound: %s', Arguments.get_error_message())
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] Inconsistent: %s', Arguments.get_error_message())
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Step 3. alt_ids with empty name
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        log.debug('\n\n[unit] Step 3. alt_ids with empty name')
        asset_obj.alt_ids = []
        altid = RT.EventDuration + ':'
        asset_obj.alt_ids.append(altid)
        try:
            asset_id = self.OMS.create_event_duration(asset_obj)

        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest: %s', Arguments.get_error_message())
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound: %s', Arguments.get_error_message())
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] Inconsistent: %s', Arguments.get_error_message())
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Step 4. alt_ids with multiple alt_ids provided (len != 1)
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        log.debug('\n\n[unit] Step 4. alt_ids with multiple alt_ids provided (len != 1)')
        asset_obj.alt_ids = []
        altid = RT.EventDuration + ':' + 'fred'
        asset_obj.alt_ids.append(altid)
        asset_obj.alt_ids.append(altid)
        try:
            asset_id = self.OMS.create_event_duration(asset_obj)

        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest: %s', Arguments.get_error_message())
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound: %s', Arguments.get_error_message())
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] Inconsistent: %s', Arguments.get_error_message())
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)

        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        # Step 5. create Asset and then try to create another with same alt_ids
        # - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - -
        log.debug('\n\n[unit] Step 5. create Event and then try to create another with same alt_ids')
        asset_obj.alt_ids = []
        altid = RT.EventDuration + ':' + asset_obj.name
        asset_obj.alt_ids.append(altid)
        try:
            asset_id = self.OMS.create_event_duration(asset_obj)
            asset_id = self.OMS.create_event_duration(asset_obj)

        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest: %s', Arguments.get_error_message())
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound: %s', Arguments.get_error_message())
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] Inconsistent: %s', Arguments.get_error_message())
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)

        log.debug('\n\n***** Completed: test_create_event_bad_altid')

    # -----
    # ----- UNIT TEST: test_create_asset_value_types
    # -----
    @unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_create_asset_value_types(self):

        log.debug('\n\n***** Start : test_create_asset_value_types')
        verbose = True

        # Create AssetType with 2 AttributeSpecifications
        # Create an Asset with 2 attributes
        # AssetType [assign] Asset
        # Create AssetExtension(using Asset id)
        # Show associations for AssetExtension (verify one is displayed)
        # Cleanup
        fid = TEST_XLS_FOLDER + 'CodeSpaces150.xlsx'       # CodeSpaces, Codes and CodeSets

        code_space_ids = []
        interactive = False
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Load marine assets into system from xslx file
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        response = self.load_marine_assets_from_xlsx(fid)

        if response:

            if verbose: log.debug('\n\n[unit] response: %s', response)

            if response['status'] == 'ok' and not response['err_msg']:
                if response['res_modified']:
                    if 'code_spaces' in response['res_modified']:
                        code_space_ids = response['res_modified']['code_spaces'][:]
            else:
                raise BadRequest('failed to process codespace related items...')

        # set breakpoint for testing...
        if interactive:
            from pyon.util.breakpoint import breakpoint
            breakpoint(locals(), globals())

        # ----- Create AssetType object
        ion_asset_spec = IonObject(RT.AssetType, name='Test AssetType')
        asset_type_id = self.OMS.create_asset_type(ion_asset_spec)


        if verbose: log.debug('\n\n***** Creating first Attribute for Asset...')

        # Create AttributeSpecification 1
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)
        attribute_specification = self._create_attribute_specification('RealValue', 'height', asset_type_obj.name,None,None,None)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)

        # Create AttributeSpecification 2
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)
        attribute_specification = self._create_attribute_specification('StringValue', 's_name', asset_type_obj.name,None,None,None)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)

        # Create AttributeSpecification 3
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)
        attribute_specification = self._create_attribute_specification('CodeValue', 'Instrument',source=asset_type_obj.name,
                                                                       constraints='asset type',pattern='asset type',
                                                                       codeset_name='asset type')
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)

        # ----- Create Asset object
        asset_obj = IonObject(RT.Asset, name='Test Asset')
        asset_id  = self.OMS.create_asset(asset_obj, asset_type_id)
        asset_obj = self.OMS.read_asset(asset_id)

        # ----- assign AssetType to Asset
        #self.OMS.assign_asset_type_to_asset(asset_type_id, asset_id)
        #if verbose: log.debug('\n\n***** Create Association: Asset (predicate=PRED.implementsAssetType) AssetType')

        # set breakpoint for testing...
        if interactive:
            from pyon.util.breakpoint import breakpoint
            breakpoint(locals(), globals())

        # Create Attribute 1
        attribute = self._create_attribute('RealValue', 'height', value=10.1)
        log.debug('\n\n[unit] attribute: %s', attribute)
        asset_obj = self.OMS.read_asset(asset_id)
        asset_obj.asset_attrs[attribute['name']] = attribute
        self.OMS.update_asset(asset_obj)

        # Create Attribute 2
        log.debug('\n\n[unit] Create Attribute 2')
        attribute = self._create_attribute('StringValue', 's_name', value='some unique name')
        asset_obj = self.OMS.read_asset(asset_id)
        asset_obj.asset_attrs[attribute['name']] = attribute
        self.OMS.update_asset(asset_obj)

        # Create Attribute 3
        log.debug('\n\n[unit] Create Attribute 3')
        asset_obj = self.OMS.read_asset(asset_id)
        log.debug('\n\n[unit] Create Attribute 3A')
        attribute = self._create_attribute('CodeValue', 'asset type', value='Mooring riser component')

        log.debug('\n\n[unit] attribute: %s', attribute)


        log.debug('\n\n[unit] Create Attribute 3B')
        asset_obj = self.OMS.read_asset(asset_id)
        log.debug('\n\n[unit] Create Attribute 3C')
        asset_obj.asset_attrs[attribute['name']] = attribute
        self.OMS.update_asset(asset_obj)

        if verbose: log.debug('\n\n***** set attribute values....')
        self.OMS.read_asset(asset_id)
        if interactive:
            from pyon.util.breakpoint import breakpoint
            breakpoint(locals(), globals())

        asset_obj = self.OMS.read_asset(asset_id)

        attributes = asset_obj.asset_attrs
        #log.debug('\n[unit] after set attributes: %s', attributes)

        # update RealValue attribute
        for name, attr in attributes.iteritems():
            if name == 'Attribute real value':
                # let's update value
                attribute = IonObject(OT.Attribute)
                attribute['name'] = 'Attribute real value'
                value = self.create_value(20.8)
                attribute['value'] = [value]
                asset_obj.asset_attrs[name] = attribute
                break
        self.OMS.update_asset(asset_obj)
        asset_obj = self.OMS.read_asset(asset_id)

        if verbose: log.debug('\n\n***** updated \'real value\' attribute values....')
        if interactive:
            from pyon.util.breakpoint import breakpoint
            breakpoint(locals(), globals())

        # Add CodeValue
        asset_obj = self.OMS.read_asset(asset_id)
        attribute = IonObject(OT.Attribute)
        attribute['name'] = 'asset type'
        value = self.create_complex_value('CodeValue', 'asset type', 'RSN Primary cable')
        attribute['value'] = [value]
        asset_obj.asset_attrs[attribute['name']] = attribute
        self.OMS.update_asset(asset_obj)
        if verbose: log.debug('\n\n***** added \'code value\' attribute values....')
        if interactive:
            from pyon.util.breakpoint import breakpoint
            breakpoint(locals(), globals())

        # Update CodeValue
        asset_obj = self.OMS.read_asset(asset_id)
        attribute = IonObject(OT.Attribute)
        attribute['name'] = 'asset type'
        value = self.create_complex_value('CodeValue', 'asset type', 'Platform')
        attribute['value'] = [value]
        asset_obj.asset_attrs[attribute['name']] = attribute
        self.OMS.update_asset(asset_obj)
        if verbose: log.debug('\n\n***** update \'code value\' attribute values....')
        if interactive:
            from pyon.util.breakpoint import breakpoint
            breakpoint(locals(), globals())

        # ----- Clean up
        if verbose: log.debug('\n\n***** Cleanup........')
        self.OMS.unassign_asset_type_from_asset(asset_type_id, asset_id)
        self.OMS.force_delete_asset_type(asset_type_id)
        self.OMS.force_delete_asset(asset_id)

        log.debug('\n\n***** Completed: test_create_asset_value_types')

    # -----
    # ----- UNIT TEST: test_create_asset_extension
    # -----
    @attr('UNIT', group='sa')
    def test_create_asset_extension(self):

        log.debug('\n\n***** Start : test_create_asset_extension')

        # Create AssetType with 2 AttributeSpecifications
        # Create an Asset with 2 attributes
        # AssetType [assign] Asset
        # Create AssetExtension(using Asset id)
        # Show associations for AssetExtension (verify one is displayed)
        # Cleanup

        verbose = True

        # ----- Create AssetType object
        ion_asset_spec = IonObject(RT.AssetType, name='Test AssetType')
        asset_type_id = self.OMS.create_asset_type(ion_asset_spec)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)

        if verbose: log.debug('\n\n***** Creating first AttributeSpecification for Asset...')

        # Create AttributeSpecification 1
        attribute_specification = self._create_attribute_specification('StringValue', 'operator name', asset_type_obj.name,None,None,None)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)

        # Create AttributeSpecification 2
        attribute_specification = self._create_attribute_specification('RealValue', 'operator height', asset_type_obj.name,None,None,None)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)

        # ----- Create Asset object
        asset_obj = IonObject(RT.Asset, name='Test Asset')
        asset_id = self.OMS.create_asset(asset_obj, asset_type_id)
        asset_obj = self.OMS.read_asset(asset_id)

        # Create Attribute for Asset; update Asset
        attribute = self._create_attribute('StringValue', 'operator name', 'nina recorder')
        asset_obj.asset_attrs[attribute['name']] = attribute
        self.OMS.update_asset(asset_obj)

        #attribute = IonObject(OT.Attribute)
        attribute = self._create_attribute('RealValue', 'operator height', 1.0)
        asset_obj = self.OMS.read_asset(asset_id)
        asset_obj.asset_attrs[attribute['name']] = attribute
        self.OMS.update_asset(asset_obj)
        asset_obj = self.OMS.read_asset(asset_id)

        # ----- assign AssetType to Asset
        #self.OMS.assign_asset_type_to_asset(asset_type_id, asset_id)
        #if verbose: log.debug('\n\n***** Create Association: Asset (predicate=PRED.implementsAssetType) AssetType')

        # - - - - - - - - - - - - - - - - - - - -
        # Create an AssetExtension (using Asset id)
        ae = self.OMS.get_asset_extension(asset_id)

        if verbose: log.debug('\n\n***** Create and Display AssetExtension: %s', ae)
        if verbose: log.debug('\n\n*****\n***** Note: AssetExtensionID: %s,  AssetID: %s', ae._id, asset_id)

        # ----- Review Associations (shows an association between Asset and AssetType)
        if verbose: log.debug('\n\n***** Review Associations')
        assetExtension_associations = self.container.resource_registry.find_associations(anyside=ae._id, id_only=False)
        if verbose: log.debug('\n\n***** AssetExtension Associations(%d): %s ',
                              len(assetExtension_associations),assetExtension_associations)

        # ----- Clean up
        if verbose: log.debug('\n\n***** Cleanup........')
        self.OMS.unassign_asset_type_from_asset(asset_type_id, asset_id)
        self.OMS.force_delete_asset_type(asset_type_id)
        self.OMS.force_delete_asset(asset_id)

        log.debug('\n\n***** Completed: test_create_asset_extension')

    # -----
    # ----- UNIT TEST: test_create_asset_extension_with_prepare
    # -----
    @attr('UNIT', group='sa')
    def test_create_asset_extension_with_prepare(self):

        log.debug('\n\n***** Start : test_create_asset_extension_with_prepare')
        verbose = True

        # Create AssetType with 2 AttributeSpecifications
        # Create an Asset with 2 attributes
        # AssetType [assign] Asset
        # Create AssetExtension(using Asset id)
        # Show associations for AssetExtension (verify one is displayed)
        # Cleanup

        # ----- Create AssetType object
        ion_asset_spec = IonObject(RT.AssetType, name='Test AssetType')
        asset_type_id = self.OMS.create_asset_type(ion_asset_spec)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)

        if verbose: log.debug('\n\n***** Creating first Attribute for Asset...')

        # Create AttributeSpecification 1
        attribute_specification = self._create_attribute_specification('StringValue', 'operator name', asset_type_obj.name,None,None,None)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)

        # Create AttributeSpecification 2
        attribute_specification = self._create_attribute_specification('RealValue', 'operator height', asset_type_obj.name,None,None,None)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)

        # ----- Create Asset object
        asset_obj = IonObject(RT.Asset, name='Test Asset')
        asset_id = self.OMS.create_asset(asset_obj, asset_type_id)  # test association
        asset_obj = self.OMS.read_asset(asset_id)

        if verbose: log.debug('\n\n***** Review Associations (on create)')
        asset_associations = self.container.resource_registry.find_associations(anyside=asset_id, id_only=False)
        if verbose: log.debug('\n\n***** Asset Associations(%d): %s ', len(asset_associations),asset_associations)

        # Create Attribute for Asset; update Asset
        attribute = self._create_attribute('StringValue', 'operator name', 'nina recorder')
        asset_obj.asset_attrs[attribute['name']] = attribute
        self.OMS.update_asset(asset_obj)

        #attribute = IonObject(OT.Attribute)
        attribute = self._create_attribute('RealValue', 'operator height', 2.0)
        asset_obj = self.OMS.read_asset(asset_id)
        asset_obj.asset_attrs[attribute['name']] = attribute
        self.OMS.update_asset(asset_obj)

        # ----- assign AssetType to Asset
        #self.OMS.assign_asset_type_to_asset(asset_type_id, asset_id)
        if verbose: log.debug('\n\n***** Create Association: Asset (predicate=PRED.implementsAssetType) AssetType')

        # - - - - - - - - - - - - - - - - - - - -
        # Create an AssetExtension (using Asset id)
        ae = self.OMS.get_asset_extension(asset_id)

        if verbose: log.debug('\n\n***** Create and Display AssetExtension: %s', ae)
        if verbose: log.debug('\n\n*****\n***** Note: AssetExtensionID: %s,  AssetID: %s', ae._id, asset_id)

        # - - - - - - - - - - - - - - - - - - - -
        # Create an AssetPrepareSupport (using Asset id)
        aps = self.OMS.prepare_asset_support(asset_id)

        if verbose: log.debug('\n\n OMS.prepare_asset_support returned with %s', str(aps))

        # ----- Review Associations (shows an association between Asset and AssetType)
        if verbose: log.debug('\n\n***** Review Associations')
        assetExtension_associations = self.container.resource_registry.find_associations(anyside=ae._id, id_only=False)
        if verbose: log.debug('\n\n***** AssetExtension Associations(%d): %s ',
                              len(assetExtension_associations),assetExtension_associations)

        # ----- Clean up
        if verbose: log.debug('\n\n***** Cleanup........')
        self.OMS.unassign_asset_type_from_asset(asset_type_id, asset_id)
        self.OMS.force_delete_asset_type(asset_type_id)
        self.OMS.force_delete_asset(asset_id)

        log.debug('\n\n***** Completed: test_create_asset_extension_with_prepare')

    # -----
    # ----- UNIT TEST: test_get_assets_picklist
    # -----
    @attr('UNIT', group='sa')
    def test_get_picklist(self):

        try:
            # picklist[ (res.name, res.id), ... ]
            log.debug('\n\n***** Start : test_get_picklist')
            interactive = False
            fid = TEST_XLS_FOLDER +   'test500.xlsx'  #'test505.xlsx'  #
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Load marine assets into system from xslx file
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            response = self.load_marine_assets_from_xlsx(fid)
            if response:
                if response['status'] != 'ok' or response['err_msg']:
                    raise BadRequest(response['err_msg'])


            response = self.load_marine_assets_from_xlsx(fid)
            if response:
                if response['status'] != 'ok' or response['err_msg']:
                    raise BadRequest(response['err_msg'])

            # set breakpoint for testing...
            if interactive:
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # get assets picklist
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            log.debug('\n\n[unit] Get Assets picklist.....')
            picklist = []
            picklist = self.OMS.get_assets_picklist(id_only='False')
            self.assertEquals(4, len(picklist), msg='asset picklist failed')
            if picklist:
                log.debug('\n\n[unit] assets picklist(%d): %s', len(picklist),picklist)
            else:
                log.debug('\n\n[unit] assets picklist empty!')

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # get events picklist
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            log.debug('\n\n[unit] Get Events picklist.....')
            picklist = []
            picklist = self.OMS.get_events_picklist(id_only='False')
            self.assertEquals(8, len(picklist), msg='events picklist failed')
            if picklist:
                log.debug('\n\n[unit] events picklist (%d): %s', len(picklist), picklist)
            else:
                log.debug('\n\n[unit] events picklist empty!')

        except BadRequest, Argument:
            log.debug('\n\n[unit] BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n[unit] NotFound: %s', Argument)
        except:
            log.debug('\n\n[unit] failed ', exc_info=True)


        log.debug('\n\n***** Completed: test_get_picklist')


    # -----
    # ----- UNIT TEST: test_asset_update_and_altid - requires changes for altid uniqueness (new update_asset,etc.)
    # -----
    @unittest.skip("targeting")
    @attr('UNIT', group='sa')
    def test_asset_update_and_altid(self):

        # Step 1.   create asset_type (name=Base)
        #           create asset (AssetUpdateTest) and asset_type association (no alt_id)
        #           update asset description ((rev 3)
        # Result:   One new asset with res.name='AssetUpdateTest'
        #           altid=resname, association to AssetType named 'Base'
        #
        # Step 2.   create asset_type
        #           create asset (AssetUpdateTest) and asset_type association (no alt_id)
        #           update asset
        # Result:   One new asset with res.name='AssetUpdateTest'
        #           altid=(resname+'-" + id[:5]), association to AssetType named 'Base'
        #
        #           Total: 2 Assets, 1 AssetType
        #
        # Step 3.   create asset_type
        #           create asset (AssetUpdateTest) and asset_type association (no alt_id)
        #           update asset
        # Result:   One new asset with res.name='AssetUpdateTest'
        #           altid=(resname+'-" + id[:5]), association to AssetType named 'Base'
        #
        #           Total: 3 Assets, 1 AssetType
        #
        # Step 4.   negative test - expect failure (alter altid with invalid namespace
        #
        # Step 5.   negative test - expect failure (add additional altid)
        #
        log.debug('\n\n***** Start : test_asset_update_and_altid')
        verbose = True
        step_number = 0

        fid = TEST_XLS_FOLDER + 'test500-load-asset-types.xlsx'
        self.load_marine_assets_from_xlsx(fid)
        asset_type = self._get_type_resource_by_name('Base', RT.AssetType)

        value_string = 'hello world'
        value_real = '1.45'
        value_date = '12/25/2014'
        value_time = '23:17'
        value_datetime = '12/25/2014 23:17'
        value_integer = '5'

        spec_attributes = asset_type.attribute_specifications
        attributes = {}
        attribute = {}
        for name, spec in spec_attributes.iteritems():
            value_type = spec['value_type']
            if value_type == 'CodeValue':
                label = spec['attr_label']
                attribute = self._create_attribute(value_type, name, label)
            else:
                attribute = self._create_attribute(value_type, name, None)

            if attribute:
                attributes[name] = attribute

        #log.debug('\n\n[unit] attributes(%d): %s', len(attributes), attributes)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # create asset_type
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if verbose: log.debug('\n\n[unit] create asset_type with single attribute specification.....')

        # ----- Create AssetType object (use helper)
        asset_type_id =  asset_type._id   #self.create_asset_type('Base')

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Step 1. Create first Asset with res.name == 'AssetUpdateTest'
        # create asset (which doesn't have alt_id)
        # expect asset to be created and on update have altid of 'Asset:AssetUpdateTest'
        # Issue another update to modify description and verify altid processing fine.
        # note: create asset (provide asset_type_id for association)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        try:

            log.debug('\n\n[unit] Step 1. Create first Asset with res.name == \'AssetUpdateTest\'')
            step_number += 1
            asset_obj = IonObject(RT.Asset, name='AssetUpdateTest')
            asset_id = self.OMS.create_asset(asset_obj, asset_type_id)
            asset_obj = self.OMS.read_asset(asset_id)

            asset_associations = self.container.resource_registry.find_associations(anyside=asset_id, id_only=True)
            self.assertEqual(1, len(asset_associations))

            if verbose: log.debug('\n\n[unit] show asset_obj.alt_ids: %s', asset_obj.alt_ids)

            test_description = 'step ' + str(step_number)
            asset_obj.asset_attrs = attributes
            asset_obj.description = test_description

            self.OMS.update_asset(asset_obj)
            asset_obj = self.OMS.read_asset(asset_id)

            test_description = 'update asset description successfully!'
            asset_obj.description = test_description
            self.OMS.update_asset(asset_obj)
            asset_obj = self.OMS.read_asset(asset_id)
            msg = 'step ' + str(step_number) + ' description update failed'
            self.assertEqual(asset_obj.description, test_description, msg=msg)


            picklist = []
            picklist = self.OMS.get_assets_picklist(id_only='False')
            self.assertEqual(1, len(picklist), msg='should have 1 item(s) in pick list; assert failed')

            if verbose: log.debug('\n\n[unit] step %d UPDATED asset_obj.alt_ids: %s', step_number, asset_obj.alt_ids)
            self.assertEqual(1, len(asset_obj.alt_ids), msg='one and only one altid permitted for Asset resources')
            value = asset_obj.alt_ids[0]
            self.assertEqual('Asset:AssetUpdateTest', value, msg='alt_id assigned not what was expected (Asset:AssetUpdateTest)')

        except BadRequest, Argument:
            log.debug('\n\n[unit] BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n[unit] NotFound: %s', Argument)
        except:

            log.debug('\n\n[unit] failed Step %d ', step_number, exc_info=True)
            raise

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Step 2. Create second Asset with res.name == 'AssetUpdateTest'
        # create asset
        # Expect asset to be created and altid of 'Asset:(res.name)-12345' where 12345 are asset_obj._id[:5]
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        try:
            log.debug('\n\n[unit] Step 2. Create second Asset with res.name == \'AssetUpdateTest\'')
            step_number += 1
            asset_obj = IonObject(RT.Asset, name='AssetUpdateTest', description='second AssetUpdateTest, push same attr_key_name...')
            asset_id = self.OMS.create_asset(asset_obj, asset_type_id)  # test association
            asset_obj = self.OMS.read_asset(asset_id)

            asset_associations = self.container.resource_registry.find_associations(anyside=asset_id, id_only=True)
            self.assertEquals(1, len(asset_associations))

            if verbose: log.debug('\n\n[unit] show asset_obj.alt_ids: %s', asset_obj.alt_ids)

            test_description = 'step ' + str(step_number)
            asset_obj.description = test_description
            asset_obj.asset_attrs = attributes

            self.OMS.update_asset(asset_obj)
            asset_obj = self.OMS.read_asset(asset_id)
            msg = 'step ' + str(step_number) + ' description update failed'
            alt_id_name = RT.Asset + ":" + asset_obj.name + '-' + asset_obj._id[:5]
            self.assertEquals(asset_obj.description, test_description, msg=msg)

            picklist = []
            picklist = self.OMS.get_assets_picklist(id_only='False')
            self.assertEqual(2, len(picklist), msg='should have 2 item(s) in pick list; assert failed')
            self.assertEqual(1, len(asset_obj.alt_ids), msg='should have 1 and only one item in alt_ids')
            self.assertEqual(asset_obj.alt_ids[0], alt_id_name, msg='alt_id assigned not equal to expected')

            #if verbose: log.debug('\n\n[unit] step %d UPDATED asset_obj.alt_ids: %s', step_number, asset_obj.alt_ids)
            #if verbose: log.debug('\n\n[unit] step %d UPDATED asset_obj: %s', step_number, asset_obj)

        except BadRequest, Argument:
            log.debug('\n\n[unit] BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n[unit] NotFound: %s', Argument)
        except:
            log.debug('\n\n[unit] failed Step %d ', step_number, exc_info=True)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Step 3. Create second Asset with res.name == 'AssetUpdateTest'
        # create asset
        # Expect asset to be created and altid of 'Asset:(res.name)-12345' where 12345 are asset_obj._id[:5]
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        try:
            step_number += 1
            log.debug('\n\n[unit] Step %d. Create third Asset with res.name == \'AssetUpdateTest\'', step_number)

            asset_obj = IonObject(RT.Asset, name='AssetUpdateTest', description='description...')
            asset_id = self.OMS.create_asset(asset_obj, asset_type_id)  # test association
            asset_obj = self.OMS.read_asset(asset_id)

            test_description = 'step ' + str(step_number)
            asset_obj.description = test_description
            asset_obj.asset_attrs = attributes
            self.OMS.update_asset(asset_obj)
            asset_obj = self.OMS.read_asset(asset_id)
            msg = 'step ' + str(step_number) + ' description update failed'
            alt_id_name = RT.Asset + ":" + asset_obj.name + '-' + asset_obj._id[:5]

            self.assertEqual(asset_obj.description, test_description, msg=msg)

            picklist = []
            picklist = self.OMS.get_assets_picklist(id_only='False')
            self.assertEqual(3, len(picklist), msg='should have 3 item(s) in pick list; assert failed')
            self.assertEqual(1, len(asset_obj.alt_ids), msg='should have 1 and only one item in alt_ids')
            self.assertEqual(asset_obj.alt_ids[0], alt_id_name, msg='should have 1 and only one item in alt_ids')

            if verbose: log.debug('\n\n[unit] step %d UPDATED asset_obj.alt_ids: %s', step_number, asset_obj.alt_ids)
            if verbose: log.debug('\n\n[unit] step %d picklist: %s', step_number, picklist)

            unique = self.unique_altids(RT.Asset)
            if unique != True:
                log.debug('\n\n[unit] duplicate altids found')
                raise
            else:
                log.debug('\n\n[unit] all altids unique')

        except BadRequest, Argument:
            log.debug('\n\n[unit] BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n[unit] NotFound: %s', Argument)
        except:
            log.debug('\n\n[unit] failed Step %d ', step_number, exc_info=True)
            raise       # raise here to fail test case

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Step 4. Create Asset with res.name == 'AssetUpdateTest' (negative test)
        # create asset
        # Expect asset to be created and altid of 'Asset:(res.name)-12345' where 12345 are asset_obj._id[:5]
        # Clear alt_ids, set fake altid with inconsistent namespace and issue update_asset - expect failure
        # Error message:
        #   'BadRequest: 400 - alt_id provided has invalid namespace (EventDuration); expected Asset'
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        try:
            step_number += 1
            log.debug('\n\n[unit] Step %d. Create Asset with res.name == \'AssetUpdateTest\' (expect failure)', step_number)

            asset_obj = IonObject(RT.Asset, name='AssetUpdateTest', description='description...')
            asset_id = self.OMS.create_asset(asset_obj, asset_type_id)  # test association
            asset_obj = self.OMS.read_asset(asset_id)

            test_description = 'step ' + str(step_number)
            asset_obj.description = test_description
            asset_obj.alt_ids = []
            fake_altid = RT.EventDuration + ':AssetUpdateTest' + asset_obj._id[:5]
            asset_obj.alt_ids.append(fake_altid)
            asset_obj.asset_attrs = attributes
            self.OMS.update_asset(asset_obj)
            asset_obj = self.OMS.read_asset(asset_id)
            msg = 'step ' + str(step_number) + ' description update failed'
            alt_id_name = RT.Asset + ":" + asset_obj.name + '-' + asset_obj._id[:5]

            self.assertEqual(asset_obj.description, test_description, msg=msg)

            picklist = []
            picklist = self.OMS.get_assets_picklist(id_only='False')
            if verbose: log.debug('\n\n[unit] asset picklist: %s', picklist)

            self.assertEqual(3, len(picklist), msg='should have 3 item(s) in pick list; assert failed')
            self.assertEqual(1, len(asset_obj.alt_ids), msg='should have 1 and only one item in alt_ids')
            self.assertEqual(asset_obj.alt_ids[0], alt_id_name, msg='should have 1 and only one item in alt_ids')

            if verbose: log.debug('\n\n[unit] step %d UPDATED asset_obj.alt_ids: %s', step_number, asset_obj.alt_ids)
            if verbose: log.debug('\n\n[unit] step %d picklist: %s', step_number, picklist)

            unique = self.unique_altids(RT.Asset)
            if unique != True:
                log.debug('\n\n[unit] duplicate altids found')
            else:
                log.debug('\n\n[unit] all altids unique')

        except BadRequest, Argument:
            log.debug('\n\n[unit] BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n[unit] NotFound: %s', Argument)
        except:
            log.debug('\n\n[unit] failed Step %d ', step_number, exc_info=True)
            #raise       # raise here to fail test case

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Step 5. Create Asset with res.name == 'AssetUpdateTest' (negative test)
        # create asset
        # Expect asset to be created and altid of 'Asset:(res.name)-12345' where 12345 are asset_obj._id[:5]
        # Leave alt_ids, add another altid (with consistent namespace) and issue update_asset - expect failure
        # Error message:
        #   'BadRequest: 400 - marine tracking resources require one and only one unique alt_id value'
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        try:
            step_number += 1
            log.debug('\n\n[unit] Step %d. Create Asset with res.name == \'AssetUpdateTest\' (expect failure)', step_number)

            asset_obj = IonObject(RT.Asset, name='AssetUpdateTest', description='description...')
            fake_altid = RT.Asset + ':AssetUpdateTest-1'
            asset_obj.alt_ids.append(fake_altid)
            asset_id = self.OMS.create_asset(asset_obj, asset_type_id)  # test association
            asset_obj = self.OMS.read_asset(asset_id)

            test_description = 'step ' + str(step_number)
            asset_obj.description = test_description
            if verbose: log.debug('\n\n[unit] existing altids: %s', asset_obj.alt_ids)
            fake_altid = RT.Asset + ':AssetUpdateTest-2' #+ asset_obj._id[:5]
            asset_obj.alt_ids.append(fake_altid)
            asset_obj.asset_attrs = attributes
            self.OMS.update_asset(asset_obj)
            asset_obj = self.OMS.read_asset(asset_id)
            msg = 'step ' + str(step_number) + ' description update failed'
            alt_id_name = RT.Asset + ":" + asset_obj.name + '-' + asset_obj._id[:5]

            self.assertEqual(asset_obj.description, test_description, msg=msg)

            picklist = []
            picklist = self.OMS.get_assets_picklist(id_only='False')
            if verbose: log.debug('\n\n[unit] asset picklist: %s', picklist)

            self.assertEqual(3, len(picklist), msg='should have 3 item(s) in pick list; assert failed')
            self.assertEqual(1, len(asset_obj.alt_ids), msg='should have 1 and only one item in alt_ids')
            self.assertEqual(asset_obj.alt_ids[0], alt_id_name, msg='should have 1 and only one item in alt_ids')

            if verbose: log.debug('\n\n[unit] step %d UPDATED asset_obj.alt_ids: %s', step_number, asset_obj.alt_ids)
            if verbose: log.debug('\n\n[unit] step %d picklist: %s', step_number, picklist)

            unique = self.unique_altids(RT.Asset)
            if unique != True:
                log.debug('\n\n[unit] duplicate altids found')
            else:
                log.debug('\n\n[unit] all altids unique')

        except BadRequest, Argument:
            log.debug('\n\n[unit] BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n[unit] NotFound: %s', Argument)
        except:
            log.debug('\n\n[unit] failed Step %d ', step_number, exc_info=True)
            #raise       # raise here to fail test case

        log.debug('\n\n***** Completed: test_asset_update_and_altid')

    # -----
    # ----- UNIT TEST: test_create_event_duration_extension
    # -----
    @attr('UNIT', group='sa')
    def test_create_event_duration_extension(self):

        log.debug('\n\n***** Start : test_create_event_duration_extension')

        # Create EventDurationType with 2 AttributeSpecifications
        # Create an EventDuration with 2 attributes
        # EventDurationType [assign] EventDuration
        # Create EventDurationExtension(using EventDuration id)
        # Show associations for EventDurationExtension (verify one is displayed)
        # Cleanup
        verbose = False

        # ----- Create EventDurationType object
        ion_ed_type = IonObject(RT.EventDurationType, name='Test EventDurationType')
        ed_type_id = self.OMS.create_event_duration_type(ion_ed_type)
        ed_type_obj = self.OMS.read_event_duration_type(ed_type_id)

        # Create AttributeSpecification 1
        attribute_specification = self._create_attribute_specification('StringValue', 'operator name', ed_type_obj.name,None,None,None)
        ed_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_event_duration_type(ed_type_obj)

        # Create AttributeSpecification 2
        attribute_specification = self._create_attribute_specification('RealValue', 'operator height', ed_type_obj.name,None,None,None)
        ed_type_obj = self.OMS.read_event_duration_type(ed_type_id)
        ed_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_event_duration_type(ed_type_obj)
        ed_type_obj = self.OMS.read_event_duration_type(ed_type_id)

        # ----- Create EventDuration
        ed_obj = IonObject(RT.EventDuration, name='Test EventDuration')
        ed_id = self.OMS.create_event_duration(ed_obj, ed_type_id)
        ed_obj = self.OMS.read_event_duration(ed_id)

        # Create Attribute for EventDuration; update EventDuration
        if verbose: log.debug('\n\n***** creating attributes...')
        attribute = self._create_attribute( 'StringValue', 'operator name', 'unique sysid')
        ed_obj.event_duration_attrs[attribute['name']] = attribute

        attribute = self._create_attribute( 'RealValue', 'operator height', 3.0)
        ed_obj.event_duration_attrs[attribute['name']] = attribute
        self.OMS.update_event_duration(ed_obj)
        ed_obj = self.OMS.read_event_duration(ed_id)

        # ----- assign EventDurationType to EventDuration
        #self.OMS.assign_event_duration_type_to_event_duration(ed_type_id, ed_id)
        #if verbose: log.debug('\n\n***** Create Association: EventDuration (predicate=PRED.implementsEventDurationType) EventDurationType')

        # - - - - - - - - - - - - - - - - - - - -
        # Create an EventDurationExtension (using EventDuration id)
        ee = self.OMS.get_event_duration_extension(ed_id)

        if verbose: log.debug('\n\n***** Create and Display EventDurationExtension: %s', ee)
        if verbose: log.debug('\n\n*****\n***** Note: EventDurationExtensionID: %s,  EventDurationID: %s', ee._id, ed_id)

        # ----- Review Associations (shows an association between EventDuration and EventDurationType)
        if verbose: log.debug('\n\n***** Review Associations')
        extension_associations = self.container.resource_registry.find_associations(anyside=ee._id, id_only=False)
        if verbose: log.debug('\n\n***** Extension Associations(%d): %s ',
                              len(extension_associations),extension_associations)

        # ----- Clean up
        if verbose: log.debug('\n\n***** Cleanup........')
        self.OMS.unassign_event_duration_type_from_event_duration(ed_type_id, ed_id)
        self.OMS.force_delete_event_duration_type(ed_type_id)
        self.OMS.force_delete_event_duration(ed_id)

        log.debug('\n\n***** Completed: test_create_event_duration_extension')

    # -----
    # ----- UNIT TEST: test_create_event_duration_extension_with_prepare
    # -----
    @attr('UNIT', group='sa')
    def test_create_event_duration_extension_with_prepare(self):

        log.debug('\n\n***** Start : test_create_event_duration_extension_with_prepare')

        # Create EventDurationType with 2 AttributeSpecifications
        # Create an EventDuration with 2 attributes
        # EventDurationType [assign] EventDuration
        # Create EventDurationExtension(using EventDuration id)
        # Show associations for EventDurationExtension (verify one is displayed)
        # Cleanup

        verbose = True

        # ----- Create EventDurationType object
        ion_ed_type = IonObject(RT.EventDurationType, name='Test EventDurationType')
        ed_type_id = self.OMS.create_event_duration_type(ion_ed_type)
        ed_type_obj = self.OMS.read_event_duration_type(ed_type_id)

        # Create AttributeSpecification 1
        attribute_specification = self._create_attribute_specification('StringValue', 'operator name', ed_type_obj.name,None,None,None)
        ed_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_event_duration_type(ed_type_obj)

        # Create AttributeSpecification 2
        attribute_specification = self._create_attribute_specification('RealValue', 'operator height', ed_type_obj.name,None,None,None)
        ed_type_obj = self.OMS.read_event_duration_type(ed_type_id)
        ed_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_event_duration_type(ed_type_obj)
        ed_type_obj = self.OMS.read_event_duration_type(ed_type_id)

        # ----- Create EventDuration
        ed_obj = IonObject(RT.EventDuration, name='Test EventDuration')
        ed_id = self.OMS.create_event_duration(ed_obj, ed_type_id)      # add association on create
        ed_obj = self.OMS.read_event_duration(ed_id)

        if verbose: log.debug('\n\n***** Review Associations from Create')
        associations = self.container.resource_registry.find_associations(anyside=ed_id, id_only=False)
        if verbose: log.debug('\n\n***** Associations(%d): %s ',
                              len(associations),associations)

        # hook
        if verbose: log.debug('\n\n***** creating attributes...')
        # Create Attribute for EventDuration; update EventDuration
        attribute = self._create_attribute( 'StringValue', 'operator name', 'unique sysid')
        ed_obj.event_duration_attrs[attribute['name']] = attribute

        attribute = self._create_attribute( 'RealValue', 'operator height', 51.5)
        ed_obj.event_duration_attrs[attribute['name']] = attribute
        self.OMS.update_event_duration(ed_obj)
        ed_obj = self.OMS.read_event_duration(ed_id)


        # ----- assign EventDurationType to EventDuration
        #self.OMS.assign_event_duration_type_to_event_duration(ed_type_id, ed_id)
        #if verbose: log.debug('\n\n***** Create Association: EventDuration (predicate=PRED.implementsEventDurationType) EventDurationType')

        # - - - - - - - - - - - - - - - - - - - -
        # Create an EventDurationExtension (using EventDuration id)
        ee = self.OMS.get_event_duration_extension(ed_id)

        if verbose: log.debug('\n\n***** Create and Display EventDurationExtension: %s', ee)
        if verbose: log.debug('\n\n*****\n***** Note: EventDurationExtensionID: %s,  EventDurationID: %s', ee._id, ed_id)

        # - - - - - - - - - - - - - - - - - - - -
        # Create an AssetPrepareSupport (using Asset id)
        edps = self.OMS.prepare_event_duration_support(ed_id)

        if verbose: log.debug('\n\n OMS.prepare_event_duration_support returned with %s', str(edps))

        # ----- Review Associations (shows an association between EventDuration and EventDurationType)
        if verbose: log.debug('\n\n***** Review Associations')
        extension_associations = self.container.resource_registry.find_associations(anyside=ee._id, id_only=False)
        if verbose: log.debug('\n\n***** Extension Associations(%d): %s ',
                              len(extension_associations),extension_associations)

        # ----- Clean up
        if verbose: log.debug('\n\n***** Cleanup........')
        self.OMS.unassign_event_duration_type_from_event_duration(ed_type_id, ed_id)
        self.OMS.force_delete_event_duration_type(ed_type_id)
        self.OMS.force_delete_event_duration(ed_id)

        log.debug('\n\n***** Completed: test_create_event_duration_extension_with_prepare')

    # -----
    # ----- UNIT TEST: test_create_event_duration_type
    # -----
    @attr('UNIT', group='sa')
    def test_create_event_duration_type(self):

        log.debug("\n\n***** Start : test_create_event_duration_type")

        # ----- create EventDurationType
        event_duration_type_obj = IonObject(RT.EventDurationType,
                                 name='TestEventDurationType',
                                 description='a new EventDurationType')

        event_duration_type_id = self.OMS.create_event_duration_type(event_duration_type_obj)
        event_duration_type = self.OMS.read_event_duration_type(event_duration_type_id)

        # Create AttributeSpecification and update EventDurationType
        attr_spec_obj = IonObject(OT.AttributeSpecification)
        attr_spec_obj['id'] = 's_name'
        attr_spec_obj['description'] = 'some description'
        attr_spec_obj['value_type'] = 'StringValue'
        attr_spec_obj['group_label'] = 'a group_label'
        attr_spec_obj['attr_label'] = 'a attr_label'
        attr_spec_obj['rank'] = 'a rank'
        attr_spec_obj['visibility'] = 'a visibility'
        attr_spec_obj['value_constraints'] = 'some valueSelectionSet'
        attr_spec_obj['default_value'] = 'some defaultValue'
        attr_spec_obj['uom'] = 'some unitOfMeasure'
        attr_spec_obj['value_pattern'] = 'some valueValidationRules'
        attr_spec_obj['cardinality'] = 'cardinality'
        attr_spec_obj['editable'] = 'True'
        attr_spec_obj['journal'] = 'False'
        attr_spec_obj['_source_id'] = ''
        event_duration_type.attribute_specifications[attr_spec_obj['id']] = attr_spec_obj
        self.OMS.update_event_duration_type(event_duration_type)

        # ---- cleanup
        self.OMS.force_delete_event_duration_type(event_duration_type_id)

        log.debug("\n\n***** Completed: test_create_event_duration_type")

    # -----
    # ----- UNIT TEST: CreateEventDuration (note: make AttributeSpecification)
    # -----
    @attr('UNIT', group='sa')
    def test_create_event_duration(self):

        try:
            log.debug('\n\n ***** Start : test_create_event_duration')
            verbose = False

            # ----- create EventDurationType object and read
            event_duration_type_obj = IonObject(RT.EventDurationType, name='TestEventDurationType',
                                                        description='new EventDurationType')

            event_duration_type_id = self.OMS.create_event_duration_type(event_duration_type_obj)
            event_duration_type_obj = self.OMS.read_event_duration_type(event_duration_type_id)
            if verbose: log.debug('\n\n***** \n***** EventDurationType: %s ',event_duration_type_obj)

            # ----- create EventDuration object
            event_duration_obj = IonObject(RT.EventDuration,name='EventDuration',description='new EventDuration')
            try:
                event_duration_id = self.OMS.create_event_duration(event_duration=event_duration_obj,
                                                                   event_duration_type_id=event_duration_type_id)

            except BadRequest, Argument:
                log.debug('\n\n *** BadRequest: %s', Argument)
            except NotFound, Argument:
                log.debug('\n\n *** NotFound: %s', Argument)
            except:
                log.debug('\n\nfailed to create EventDuration obj with association')

            if event_duration_id:
                event_duration_obj = self.OMS.read_event_duration(event_duration_id)
            else:
                raise BadRequest('create_event_duration failed to provide event_duration_id')

            # Populate the attribute(s)
            attr_obj = IonObject(OT.Attribute, name='s_name')
            attr_obj['name'] = 's_name'
            values = []
            value = self.create_value('unique sys id')
            values.append(value)
            value = self.create_value('a super secret high interest value?')
            values.append(value)
            attr_obj['value'] = values
            event_duration_obj.event_duration_attrs[attr_obj['name']] = attr_obj

            attr_obj = IonObject(OT.Attribute, name='Attribute real value')
            attr_obj['name'] = 'real value'

            values = []
            value = self.create_value(2.078925)
            values.append(value)
            value = self.create_value(3.14)
            values.append(value)
            value = self.create_value(2114.94738)
            values.append(value)
            attr_obj['value'] = values
            event_duration_obj.event_duration_attrs[attr_obj['name']] = attr_obj


            self.OMS.update_event_duration(event_duration_obj)
            if verbose: log.debug('\n\n***** EventDuration with attribute: %s', event_duration_obj)




            # ----- assign EventDurationType to EventDuration
            self.OMS.assign_event_duration_type_to_event_duration(event_duration_type_id, event_duration_id)
            if verbose: log.debug('\n\n***** \n***** EventDuration Relationship: %s -> ', event_duration_id)

            # ----- determine associations
            id = event_duration_id
            asset_event_associations = self.container.resource_registry.find_associations(anyside=id, id_only=False)
            if verbose: log.debug("\n\n***** \n***** EventDuration Relationships: %s ", asset_event_associations)

            # ----- unassign asset associations
            self.OMS.unassign_event_duration_type_from_event_duration(event_duration_type_id, event_duration_id)

            # ----- cleanup
            self.OMS.force_delete_event_duration_type(event_duration_type_id)
            self.OMS.force_delete_event_duration(event_duration_id)

            log.debug('\n\n***** Completed: test_create_event_duration')

        except BadRequest, Argument:
            log.debug('\n\n *** BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n *** NotFound: %s', Argument)
        except:
            log.debug('\n\nfailed to create EventDuration obj with association')


    # -------------------------------------------------------------------------
    # ----- UNIT TEST: test_update_attribute_specifications
    # ----- todo if reserver attribute (like s_name, then review mods to spec for that attribute
    @attr('UNIT', group='sa')
    def test_update_attribute_specifications(self):

        log.debug('\n\n***** Start : * test_update_attribute_specifications')

        verbose = True

        #---------------------------------------------------------------------------------------
        # Update the AssetType AttributeSpecification for a given attribute
        #---------------------------------------------------------------------------------------
        # ----- Create AssetType object
        ion_asset_spec = IonObject(RT.AssetType, name='Test AssetType')
        asset_type_id = self.OMS.create_asset_type(ion_asset_spec)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)

        # Create AttributeSpecification 1
        attribute_specification = self._create_attribute_specification('StringValue', 'operator name', asset_type_obj.name,None,None,None)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)

        # Create AttributeSpecification 2
        attribute_specification = self._create_attribute_specification('StringValue', 'operator height', asset_type_obj.name,None,None,None)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)

        # Read attribute specifications, modify and update
        attribute_specifications = asset_type_obj.attribute_specifications

        # Modify those attribute_specification - change description
        spec_dict = {}
        attribute_specification = attribute_specifications['operator name']
        attribute_specification['description'] = 'a new description!'
        spec_dict['operator name'] = attribute_specification

        attribute_specification = attribute_specifications['operator height']
        attribute_specification['description'] = 'operator height - a new description!'
        spec_dict['operator height'] = attribute_specification

        attr_spec = IonObject(OT.AttributeSpecification)
        attr_spec['id'] = 'Attribute New'
        attr_spec['description'] = '- - - an AttributeSpecification not currently available to this AssetSpecification'
        attr_spec['value_type'] = 'StringValue'
        attr_spec['group_label'] = 'a group_label'
        attr_spec['attr_label'] = 'a attr_label'
        attr_spec['rank'] = 'a rank'
        attr_spec['visibility'] = 'a visibility'
        attr_spec['value_constraints'] = 'a valueSelectionSet'
        attr_spec['value_pattern'] = 'a valueValidationRules'
        attr_spec['default_value'] = 'a defaultValue'
        attr_spec['uom'] = 'a unitOfMeasure'
        attr_spec['cardinality'] = 'a cardinality'
        attr_spec['editable'] = 'True'
        attr_spec['journal'] = 'False'
        attr_spec['_source_id'] = ''
        spec_dict[attr_spec['id']] = attr_spec

        self.OMS.update_attribute_specifications(resource_id=asset_type_id, spec_dict=spec_dict)

        # read updated AssetType attribute_specifications, verify (here by inspection)
        # that each attribute has been updated. (todo use assert)
        xobj = self.OMS.read_asset_type(asset_type_id)
        if xobj:
            if verbose: log.debug('\n\n ***** AFTER  attribute_specifications: %s',xobj.attribute_specifications)

        # Cleanup
        self.OMS.delete_asset_type(asset_type_id)

        log.debug('\n\n***** Test Completed: test_update_attribute_specifications')

    # -------------------------------------------------------------------------
    # ----- UNIT TEST: test_delete_attribute_specification
    # -----
    #@unittest.skip("targeting")
    @attr('UNIT', group='sa')
    def test_delete_attribute_specification(self):

        log.debug('\n\n***** Start : * test_delete_attribute_specification')

        #---------------------------------------------------------------------------------------
        # Test service delete_attribute_specification
        # 1. Create TypeResource object (AssetType)
        # 2. Create and populate with two AttributeSpecifications
        # 3. Exercise delete_attribute_specification
        #       1. Send in empty list of AttributeSpecification names
        #       2. Send in one valid AttributeSpecification name
        #       3. Send in one invalid AttributeSpecification name ('junk name')
        #       4. Send in last valid AttributeSpecification name
        #       5. Send in resource_id for AssetType (with empty attribute_specifications - we deleted them above)
        #               and request an attribute be deleted.
        #---------------------------------------------------------------------------------------
        verbose = True

        #---------------------------------------------------------------------------------------
        # ----- Create TypeResource object (AssetType) and populate
        # ----- with 2 AttributeSpecifications
        ion_asset_spec = IonObject(RT.AssetType, name='Test AssetType')
        asset_type_id = self.OMS.create_asset_type(ion_asset_spec)
        asset_type_obj = self.OMS.read_asset_type(asset_type_id)

        # Create AttributeSpecification 1
        #attribute_specification = _create_attribute_specification(value_type, id,source, constraints, pattern, codeset_name)
        attribute_specification = self._create_attribute_specification('StringValue', 'operator name', asset_type_obj.name,None, None, None)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)

        # Create AttributeSpecification 2
        attribute_specification = self._create_attribute_specification('RealValue', 'operator height', asset_type_obj.name,None, None, None)

        asset_type_obj = self.OMS.read_asset_type(asset_type_id)
        asset_type_obj.attribute_specifications[attribute_specification['id']] = attribute_specification
        self.OMS.update_asset_type(asset_type_obj)

        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - -- - - - - - ')
        #---------------------------------------------------------------------------------------
        # Exercise delete_attribute_specification
        # 1. Send in empty list of AttributeSpecification names (receive BadRequest)
        try:
            attr_name_list = []
            self.OMS.delete_attribute_specification(resource_id=asset_type_id, attr_spec_names=attr_name_list)

        except BadRequest, Argument:
            if verbose: log.debug('\n\n BadRequest: %s', Argument.get_error_message())
        except NotFound, Argument:
            if verbose: log.debug('\n\n NotFound: %s', Argument.get_error_message())
            raise
        except:
            if verbose: log.debug('\n\n Exception!')
            raise

        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - -- - - - - - ')
        # 2. Send in one valid AttributeSpecification name ('AttributeSpecification TWO')
        try:
            attr_name_list = ['operator name']
            self.OMS.delete_attribute_specification(resource_id=asset_type_id, attr_spec_names=attr_name_list)

        except BadRequest, Argument:
            if verbose: log.debug('\n\n BadRequest: %s', Argument.get_error_message())
        except NotFound, Argument:
            if verbose: log.debug('\n\n NotFound: %s', Argument.get_error_message())
        except:
            if verbose: log.debug('\n\n Exception!')

        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - -- - - - - - ')
        # 3. Send in one invalid AttributeSpecification name ('junk name') silent
        try:
            attr_name_list = ['junk name']        # valid AttributeSpecification to delete
            self.OMS.delete_attribute_specification(resource_id=asset_type_id, attr_spec_names=attr_name_list)

        except BadRequest, Argument:
            if verbose: log.debug('\n\n BadRequest: %s', Argument.get_error_message())
        except NotFound, Argument:
            if verbose: log.debug('\n\n NotFound: %s', Argument.get_error_message())
        except:
            if verbose: log.debug('\n\n Exception!')

        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - -- - - - - - ')
        # 4. Send in last valid AttributeSpecification name
        try:
            attr_name_list = ['operator height']        # valid AttributeSpecification to delete
            self.OMS.delete_attribute_specification(resource_id=asset_type_id, attr_spec_names=attr_name_list)

        except BadRequest, Argument:
            if verbose: log.debug('\n\n BadRequest: %s', Argument.get_error_message())
        except NotFound, Argument:
            if verbose: log.debug('\n\n NotFound: %s', Argument.get_error_message())
        except:
            if verbose: log.debug('\n\n Exception!')

        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - -- - - - - - ')
        # 5. Send in resource_id for AssetSpecification (with empty attribute_specifications)
        #    and request an attribute be deleted. (receive NotFound)
        try:
            attr_name_list = ['operator name']        # No AttributeSpecification to delete
            self.OMS.delete_attribute_specification(resource_id=asset_type_id, attr_spec_names=attr_name_list)

        except BadRequest, Argument:
            if verbose: log.debug('\n\n BadRequest: %s', Argument.get_error_message())
            raise
        except NotFound, Argument:
            if verbose: log.debug('\n\n NotFound: %s', Argument.get_error_message())
        except:
            if verbose: log.debug('\n\n Exception!')
            raise

        #---------------------------------------------------------------------------------------
        # Cleanup
        self.OMS.force_delete_asset_type(asset_type_id)
        log.debug('\n\n***** Test Completed: test_delete_attribute_specification')

    #-------------------------------------------------------
    # CodeSpaces, Codes and CodeSets unit tests start...
    #-------------------------------------------------------

    # -------------------------------------------------------------------------
    #  UNIT TEST: test_create_codespace
    #  Exercises following:
    #   OMS.create_code_space
    #   OMS.update_code_space
    #   OMS.read_code_space
    #   OMS.force_delete_code_space
    #   OMS.delete_code_space
    #   OMS.read_codes_by_name          returns list of Codes
    #   OMS.read_codesets_by_name       returns list of CodeSets
    #   OMS.update_codes
    #   OMS.update_codesets
    #   OMS.delete_codes                returns list of Codes (?)  todo: mods per discussion
    #   OMS.delete_codesets             returns list of CodeSets (?)
    #
    #@unittest.skip("targeting")
    @attr('UNIT', group='sa')
    def test_create_codespace(self):

        log.debug('\n\n***** Start : * test_create_codespace')

        #---------------------------------------------------------------------------------------
        # Process:
        # 1. Create create CodeSpace, Code(s), create two CodeSets using codes
        # 2. Request codesets by list of name(s) - one codeset name valid, one codeset name not
        # 3. Request codes by list of name(s) - two code names valid, one code name invalid
        # 4. Update codes (change description field for 'Repair Event')
        # 5. Update codeset - add new code to CodeSpace, then CodeSet
        # 6. Delete codes - delete code(s) in CodeSpace (uses list of code names to identify what to delete)
        # 7. Delete codesets - delete codeset(s) in CodeSpace (uses list of codeset name(s) to identify what to delete)
        # 8. Delete all codesets in CodeSpace
        # 9. Cleanup
        #---------------------------------------------------------------------------------------
        verbose = False

        #---------------------------------------------------------------------------------------
        # 1. Create create CodeSpace, Code(s), create two CodeSets using codes
        #---------------------------------------------------------------------------------------
        code_space = IonObject(RT.CodeSpace, name='MAM', description='Marine Asset Management')
        id = self.OMS.create_code_space(code_space)
        code_space = self.OMS.read_code_space(id)

        # - - - Create codes, create CodeSet with name 'DemoCodeSet', add codes to CodeSet
        description = ''
        name = 'demo code1'
        code_space.codes[name] = IonObject(OT.Code,id=str(uuid.uuid4()), name=name, description=description)

        name = 'cheese'
        code_space.codes[name] = IonObject(OT.Code,id=str(uuid.uuid4()), name=name, description=description)
        # Create a code in CodeSpace not utilized w/i any codeset
        name = 'unused code'
        code_space.codes[name] = IonObject(OT.Code,id=str(uuid.uuid4()), name=name, description=description)

        self.OMS.update_code_space(code_space)
        code_space = self.OMS.read_code_space(id)

        # - - - Create codes and add to CodeSpace (used in CodeSet named 'event_type')
        EventTypeCode = ['Return to Manufacturer Event', 'Deployment Event', 'Repair Event',
                         'Inoperability Event', 'Retirement Event', 'Integration Event', 'Test Event',
                         'Calibration Event','cheese'] # note 'cheese' is used in TWO codesets

        # for all items in EventTypeCode list - if not already a Code, create code in CodeSpace
        for name in EventTypeCode:
            if name not in code_space.codes.keys():
                code_space.codes[name] = IonObject(OT.Code,id=str(uuid.uuid4()), name=name, description=description)
            else:
                if verbose: log.debug('\n\n code name %s already in code_space', name)
        self.OMS.update_code_space(code_space)
        code_space = self.OMS.read_code_space(id)
        number_of_codesets = len(code_space.codesets)
        #log.debug('\n\n cs.codes(%d): %s', len(code_space.codes), code_space.codes)

        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - - - -')
        # Create CodeSet 'event type', add list of names to enumeration; add CodeSet to CodeSpace
        # CodeSet.enumeration is a list of Code names
        try:
            codeset1 = IonObject(OT.CodeSet,name='event type',description='Valid codes for EventDuration Attribute: event type.')
            #log.debug('\n\n after creating codeset1...')
            #log.debug('\n\n code_space.codes.keys()...%s', code_space.codes.keys())
            for name in EventTypeCode:
                #log.debug('\n\n name %s in EventTypeCode', name)
                if name in code_space.codes.keys():                     # name for a valid code
                    #codeset1.enumeration.append(code_space.codes[name])
                    if name not in codeset1.enumeration:                # if name not already in enumeration
                        codeset1.enumeration.append(name)
                        #log.debug('\n\n[unit] add name %s to enumeration..', name)
                    else:
                        if verbose: log.debug('\n\n[unit] name %s already in enumeration..')
                else:
                    if verbose: log.debug('\n\n name %s not in CodeSpace codes, do not add to enumeration', name)

            # add 'cheese' to enumeration for test removal of 'cheese' from two code_sets
            #codeset1.enumeration.append('cheese')

            code_space.codesets[codeset1.name] = codeset1

            self.OMS.update_code_space(code_space)
            code_space = self.OMS.read_code_space(id)

            # Create CodeSet 'DemoCodeSet', add codes; add CodeSet to CodeSpace
            democodeset = IonObject(OT.CodeSet,name='DemoCodeSet',description='second [demo] code set')
            #democodeset.enumeration = [code_space.codes['demo code1'], code_space.codes['cheese'] ]
            democodeset.enumeration = ['demo code1', 'cheese' ]
            code_space.codesets[democodeset.name] = democodeset
            self.OMS.update_code_space(code_space)
            code_space = self.OMS.read_code_space(id)

            self.assertTrue(code_space.codesets[democodeset.name], msg='democodeset.name assert True')
            self.assertEqual(2,len(code_space.codesets[democodeset.name].enumeration), msg='len of democodeset.enumeration' )
            self.assertEqual(number_of_codesets+2, len(code_space.codesets),msg='number of codesets')
            self.assertEqual(9, len(code_space.codesets['event type'].enumeration), msg='len of enumeration')


            if verbose: log.debug('\n\n[unit] code_space.codesets: %s', code_space.codesets)
        except:
            log.debug('\n\n[unit] Failure: to create CodeSpace, Codes or CodeSets')
            raise

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # 2. Request codesets by list of name(s) - request one codeset name valid, one codeset name not
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - - - -')
        try:
            request = []
            request.append('event type')
            request.append('non existent codeset name')
            codesets = self.OMS.read_codesets_by_name(resource_id=id, names=request)
            self.assertEqual(1, len(codesets))

        except:
            log.debug('\n\n[unit] Failure: read_codesets_by_name')
            raise

        #---------------------------------------------------------------------------------------
        # 3. Request codes by list of name(s) - two code names valid, one code name invalid
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - - - -')
        try:
            codes = []
            request = []
            request.append('Repair Event')
            request.append('invalid code name')
            request.append('Test Event')
            codes = self.OMS.read_codes_by_name(resource_id=id, names=request, id_only=False)
            self.assertEqual(2, len(codes))

        except:
            log.debug('\n\n[unit] Failure: read_codes_by_name')
            raise

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # 4. Update codes
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - - - -')
        try:
            cs = self.OMS.read_code_space(id)
            updated_description = '*** UPDATED DESCRIPTION ***'
            name = 'Repair Event'
            code = cs.codes[name]
            code.description = updated_description

            # Dictionary of codes
            codes = {}
            codes[code.name] = code
            self.OMS.update_codes(id,codes)
            cs = self.OMS.read_code_space(id)
            self.assertEqual(cs.codes[name].description, updated_description)

        except:
            log.debug('\n\n[unit] Failure: update_codes')
            raise

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # 5. Update codeset - add new code to CodeSpace then CodeSet
        #    (using enumeration as list of Codes, NOT Code names) todo correct this
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - - - -')
        try:
            # Make a new code called 'Demo Event' and add to CodeSpace
            cs = self.OMS.read_code_space(id)
            name = 'Demo Event'
            description = ''
            cs.codes[name] = IonObject(OT.Code,id=str(uuid.uuid4()), name=name, description=description)
            self.OMS.update_code_space(cs)

            # Add this code to enumeration for democodeset (add str not code) todo
            #democodeset.enumeration.append(cs.codes[name])
            democodeset.enumeration.append(name)

            # Dictionary of codesets
            codesets = {}
            codesets[democodeset.name] = democodeset
            self.OMS.update_codesets(id,codesets)
            cs = self.OMS.read_code_space(id)
            self.assertEqual(cs.codes[name].name, name)

        except BadRequest, Argument:
            log.debug('\n\n[unit] BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n[unit] NotFound: %s', Argument)
        except:
            log.debug('\n\n[unit] Failure: update_codesets')
            raise

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # 6. Delete codes - delete code(s) in CodeSpace
        #
        #  Otherwise, if deleting Code from CodeSpace...
        #  Determine if code is in use before delete; if in use in a codeset delete from codeset
        #  update codeset (todo correction per discussion with Matt)
        #
        #  Discuss: code name values and use of codes
        #  id versus name. Codes are uniquely identified by the id value, not the name value.
        #  Discuss: do codes require a description field? If not recommend removing (todo?)
        #  Discuss: CodeSpace revisions - who's got what (delta between CodeSpaces on revision)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - - - -')

        try:
            # Modify CodeSet name and description value for code with name 'cheese'
            cs = self.OMS.read_code_space(id)
            number_of_codes = len(cs.codes)
            updated_description = 'french cheese'
            updated_name = 'fromage'

            # get the 'cheese' code from CodeSpace and modify name and descripition of the
            # cheese code used in democodeset.enumeration. (todo make new code with same id, change name)
            name = 'cheese'
            code = cs.codes[name]
            code.name = updated_name
            code.description = updated_description

            # Dictionary of codesets
            codesets = {}
            codesets[democodeset.name] = democodeset
            self.OMS.update_codesets(id,codesets)
            cs = self.OMS.read_code_space(id)
            #log.debug('\n\n democodeset.enumeration: %s', cs.codesets[democodeset.name].enumeration)

            # Request deletion of one or more codes; some existent, some not and some which
            # exist are in use in CodeSets
            tcodes = []
            tcodes = cs.codes.keys()

            if verbose: log.debug('\n\n[unit] codes available before delete(%d): %s', len(cs.codes.keys()),cs.codes.keys())

            codes = []                          # Names of Codes to be deleted
            codes.append('unused code')         # should be deleted
            codes.append('non existent code')   # doesn't exist to delete, but shouldn't fail service (ignore)
            codes.append('cheese')              # delete: from 2 CodeSet enumerations; Code from CodeSpace
            codes.append('unused code')         # should already be deleted, service should not fail
            codes.append('Demo Event')          # delete from DemoCodeSet, leaving one code names in enumeration
            codes.append('demo code1')          # delete from DemoCodeSet, leaving zero code names in enumeration
            # Delete one code from CodeSpace; delete_codes returns (per spec, a list of Codes)
            rcodes = []
            rcodes = self.OMS.delete_codes(id, codes)   # Should return dictionary of codes, not list of codes

            names_of_codes = []
            if rcodes:
                for c in rcodes:
                    names_of_codes.append(c.name)
                #if verbose: log.debug('\n\n***** returned list of codes: %s', rcodes)
                if verbose: log.debug('\n\n[unit] names of codes returned(%d): %s', len(names_of_codes),names_of_codes)

            cs = self.OMS.read_code_space(id)
            self.assertTrue(cs.codes)
            if verbose: log.debug('\n\n[unit] a. check number of codes(%d)', len(cs.codes))

            # Try to delete a code name from codeset when there is an empty codeset enumeration
            codes = []                          # Names of Codes to be deleted;
            codes.append('Repair Event')        # is code in codespace, if so try to delete when
                                                # empty code set enumeration in CodeSpace
            rcodes = []
            rcodes = self.OMS.delete_codes(id, codes)
            cs = self.OMS.read_code_space(id)
            if verbose: log.debug('\n\n[unit] b. check number of codes(%d)', len(cs.codes))


            if verbose: log.debug('\n\n[unit] AFTER: cs.codes(%d): %s', len(cs.codes),cs.codes)

            if verbose: log.debug('\n\n[unit] sample dictionary codesets(%d): %s', len(cs.codesets), cs.codesets)

        except BadRequest, Argument:
            log.debug('\n\n[unit] BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n[unit] NotFound: %s', Argument)
        except:
            log.debug('\n\n[unit] Failure: delete codes')
            raise

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # 7. Delete codesets - delete codesets(s) in CodeSpace based on list of CodeSet name(s)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - - - -')

        try:
            cs = self.OMS.read_code_space(id)
            number_of_codesets = len(cs.codesets)
            # Delete one existing codeset, if codeset does not exist then verify pass ok
            codesets = []                                   # list CodeSet names to be returned
            codesets.append('DemoCodeSet')                  # exists, should be deleted
            codesets.append('non existent codeset')         # doesn't exist to delete
            rcodesets = []                                  # list of CodeSets returned
            rcodesets = self.OMS.delete_codesets(id, codesets)

            if verbose:
                if rcodesets:
                    if verbose: log.debug('\n\n[unit] (after deleting one CodeSet) returned codesets (%d): %s', len(rcodesets), rcodesets)

            cs = self.OMS.read_code_space(id)
            self.assertEqual((number_of_codesets-1), len(cs.codesets))

        except BadRequest, Argument:
            log.debug('\n\n[unit] BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n[unit] NotFound: %s', Argument)
        except:
            log.debug('\n\n[unit] Failure: delete codesets')
            raise

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # 8. Delete all codesets in CodeSpace
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - - - -')
        try:
            cs = self.OMS.read_code_space(id)
            if cs.codesets:
                if verbose: log.debug('\n\n***** before delete codesets (%d): %s', len(cs.codesets), cs.codesets)
                codeset_name_list = cs.codesets.keys()
                rcodesets = []
                rcodesets = self.OMS.delete_codesets(id, codeset_name_list)
                self.assertEqual(0, len(rcodesets))
            else:
                if verbose: log.debug('\n\n***** No codesets in CodeSpace to delete!')

        except BadRequest, Argument:
            log.debug('\n\n *** BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n *** NotFound: %s', Argument)
        except Argument:
            log.debug('\n\n *** Failure: delete all codesets, Argument: %s', Argument)
            raise

        #---------------------------------------------------------------------------------------
        # 9. Cleanup
        #---------------------------------------------------------------------------------------
        if verbose: log.debug('\n\n - - - - - - - - - - - - - - - - - - -')
        try:
            if verbose: log.debug('\n\n[unit] force delete CodeSpace...')
            self.OMS.force_delete_code_space(id)
            #self.OMS.delete_code_space(id)

        except BadRequest, Argument:
            log.debug('\n\n *** BadRequest: %s', Argument)
        except NotFound, Argument:
            log.debug('\n\n *** NotFound: %s', Argument)
        except Argument:
            log.debug('\n\n *** Failure: force delete CodeSpace, Argument: %s', Argument)
            raise

        log.debug('\n\n***** Test Completed: test_create_codespace')

    # -----
    # ----- UNIT TEST: test_upload_codes
    # -----
    @attr('UNIT', group='sa')
    def test_upload_codes(self):

        # test service declare_asset_tracking_codes
        # Load CodeSpace, Codes and CodeSets from xlsx, view resources using localhost:8080 at breakpoints
        # Continue to delete resources objects created, use localhost:8080 to observe all have been
        # deleted at cleanup.
        #
        #  sample response:
        #   response:
        #           {
        #           'status': 'ok',
        #           'res_modified':
        #                           {
        #                           'code_spaces': ['3c9b3df056c040b3aa3179aef7d19dd0'],
        #                           'codes': [],
        #                           'code_sets': []
        #                           },
        #           'err_msg': '',
        #           'res_removed': {
        #                           'code_spaces': [],
        #                           'codes': [],
        #                           'code_sets': []
        #                           }
        #           }
        #

        log.debug('\n\n***** Start : test_upload_codes')
        verbose = False
        breakpoint1A = False
        breakpoint1B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint1B = True


        # Input and folder(s) and files for driving test
        fid = TEST_XLS_FOLDER + 'CodeSpaces150.xlsx'       # CodeSpaces, Codes and CodeSets

        code_space_ids = []
        try:

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Load marine assets into system from xslx file
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            response = self.load_marine_assets_from_xlsx(fid)

            if response:

                if verbose: log.debug('\n\n[unit] response: %s', response)

                if response['status'] == 'ok' and not response['err_msg']:

                    if response['res_modified']:
                        if 'code_spaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['code_spaces'][:]
                        if 'codes' in response['res_modified']:
                            code_names = response['res_modified']['codes'][:]
                        if 'code_sets' in response['res_modified']:
                            code_set_names = response['res_modified']['code_sets'][:]

                    if response['res_removed']:
                        if 'code_spaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['code_spaces'][:]
                        if 'codes' in response['res_modified']:
                            code_names = response['res_modified']['codes'][:]
                        if 'code_sets' in response['res_modified']:
                            code_set_names = response['res_modified']['code_sets'][:]

                if code_space_ids:

                    if len(code_space_ids) == 1:
                        code_space_obj = self.OMS.read_code_space(code_space_ids[0])
                        if code_space_obj:
                            if code_space_obj.codes:
                                if verbose: log.debug('\n\n code_space_obj.codes: %s\n\n', code_space_obj.codes.keys())
                            if code_space_obj.codesets:
                                if verbose: log.debug('\n\n code_space_obj.codes: %s\n\n', code_space_obj.codesets.keys())
                    else:
                        if verbose: log.debug('\n\n[service] more than one CodeSpace id returned, issue.')
                        raise BadRequest('[service] more than one CodeSpace id returned, issue.')

            # set breakpoint for testing...
            if breakpoint1A:
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            if code_space_ids:
                if code_space_ids[0]:
                    self.OMS.force_delete_code_space(code_space_ids[0])

            # set breakpoint for testing...code_space should be deleted
            if breakpoint1B:
                log.debug('\n\n[unit] verify all code_space(s) which have been created are removed.')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', fid, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_codes')

    # -----
    # ----- UNIT TEST: test_download_codes
    # -----
    @attr('UNIT', group='sa')
    def test_download_codes(self):

        # test service(s) - use declare_asset_tracking_codes to declare CodeSpace, Codes and CodeSets
        # in the system. Once resources are loaded, call real_download_xls to generate asset tracking report (xls)
        # (Note: modifications required in service declare_asset_tracking_codes since addition of CodeSpaces sheet)

        log.debug('\n\n***** Start : test_download_codes')

        verbose = False
        breakpointLoaded = False            # after loading marine tracking resources
        breakpoint1B = False                # after delete
        breakpointCleanup = False           # after update pass
        breakpointVerifyCleanup = False     # after cleanup

        # Input and folder(s) and files for driving test
        fid = TEST_XLS_FOLDER + 'CodeSpaces150.xlsx'
        output_file =  TEST_XLS_FOLDER + 'CodeSpaces150_report.xls'
        const_code_space_name = "MAM"
        code_space_ids = code_ids = code_set_ids = []
        try:

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Load marine assets into system from xslx file
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            response = self.load_marine_assets_from_xlsx(fid)
            if response:

                if verbose: log.debug('\n\n[unit] response: %s', response)
                if response['status'] == 'ok' and not response['err_msg']:
                    if 'code_spaces' in response['res_modified']:
                        code_space_ids = response['res_modified']['code_spaces'][:]
                        if code_space_ids:
                            if len(code_space_ids) != 1:
                                raise BadRequest('[unit] more than one CodeSpace id returned, issue.')

                            res_objs, res_keys = self.container.resource_registry.find_resources_ext(alt_id_ns=RT.CodeSpace,
                                                        alt_id=const_code_space_name, id_only=False)
                            if res_keys:
                                self.assertEqual(1,len(res_keys), msg='more than one codespace key returned')
                        else:
                            raise BadRequest('failed to receive codespace_id in response')

            # Breakpoint - Marine Asset code related resources loaded into system
            if breakpointLoaded:
                log.debug('\n\n[unit] Breakpoint - Marine Asset code related resources loaded into system')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # call asset_tracking_report service, report on marine tracking code space related resources in System
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            if verbose: log.debug('\n\n[unit] request Marine Asset tracking codes report ...\n')
            response = self.OMS.asset_tracking_report()

            if not response:
                log.debug('\n\n[unit] Failed to generate marine asset tracking codes report.')
                raise BadRequest('Failed to generate asset tracking codes report')
            else:

                # receive content from download_xls service, write to file
                try:
                    f = open(output_file, 'wb')
                except:
                    log.error('failed to open xls file for write: ', exc_info=True)
                    raise
                try:
                    rcontent = binascii.a2b_hex(response)
                    f.write(rcontent)
                    f.close()
                except:
                    log.error('[unit] failed to write xls content to output file (%s)', output_file)

                log.debug('\n\n[unit] marine asset codes tracking report saved to file: %s\n\n', output_file)

                if breakpointCleanup:
                    log.debug('\n\n[unit] Breakpoint - preparing to delete resources which were created')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Cleanup
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            if verbose: log.debug('\n\n[unit] cleanup...')

            if code_space_ids:
                for id in code_space_ids:
                    self.OMS.force_delete_code_space(id)

            # set breakpoint for testing...assets and asset_type should be deleted
            if breakpointVerifyCleanup:
                log.debug('\n\n[unit] Breakpoint - verify all Marine Asset resources have been removed')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file %s)', fid, exc_info=True)
            raise           # raise here to fail test case

        finally:
            log.debug('\n\n***** Completed : test_download_codes')

    # -----
    # ----- UNIT TEST: test_upload_xls_with_codes
    # -----
    @attr('UNIT', group='sa')
    def test_upload_xls_with_codes(self):

        # test service declare_asset_tracking_resources with CodeSpace(s), Codes, or CodeSets loaded
        # (Currently only testing single CodeSpace.) This test resembles a typical system engineering workflow,
        # where changes are not CodeSpace, Code or CodeSet related but focused on introducing Assets, AssetTypes
        # EventDuration and EventDurationTypes into the OOI system.
        # Step 1. Load CodeSpaces, Codes and CodeSets only
        # Step 2. Load everything except CodeSpaces, Codes and CodeSets

        log.debug('\n\n***** Start : test_upload_xls_with_codes')

        #self._preload_scenario("BETA")                  # Enable for actual testing

        verbose = False
        breakpoint1A = False                            # after create pass, but before delete
        breakpoint_cleanup = False                      # after update pass
        breakpoint_after_cleanup = False                # after deleting resources created during test

        # Input and folder(s) and files for driving test
        fid_codes = TEST_XLS_FOLDER + 'test500-code-related-only.xlsx'       # CodeSpaces, Codes and CodeSets
        fid       = TEST_XLS_FOLDER + 'test500-no-code-related.xlsx'         # no CodeSpaces, Codes or CodeSets
        current_file = fid_codes
        try:
            code_space_ids = []

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Load marine asset code related information into system (CodeSpace(s), Code(s), CodeSet(s))
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            current_file = fid_codes
            response = self.load_marine_assets_from_xlsx(fid_codes)

            if response:

                #if verbose: log.debug('\n\n[unit] response: %s', response)
                if response['status'] != 'ok' or response['err_msg']:
                    raise BadRequest('[unit] Error: %s' % response['err_msg'])

                if response['res_modified']:
                    if 'code_spaces' in response['res_modified']:
                        code_space_ids = response['res_modified']['code_spaces'][:]

                if code_space_ids:
                    if len(code_space_ids) == 1:
                        code_space_obj = self.OMS.read_code_space(code_space_ids[0])
                        if verbose: log.debug('\n\n[unit] code_space_obj.codes: %s\n\n', code_space_obj.codes.keys())
                        if verbose: log.debug('\n\n[unit] code_space_obj.codes: %s\n\n', code_space_obj.codesets.keys())
                        if verbose: log.debug('\n\n[unit] codeset[event type].enumeration: %s\n\n',
                                              code_space_obj.codesets['event type']['enumeration'])
                    elif len(code_space_ids) > 1:
                        if verbose: log.debug('\n\n[unit] more than one CodeSpace id returned, issue.')
                        raise BadRequest('[unit] more than one CodeSpace id returned, issue.')
                    else:
                        raise BadRequest('[unit] CodeSpace failed to load.')

                _, res_keys = self.container.resource_registry.find_resources_ext(alt_id_ns=RT.CodeSpace,
                                                                                 id_only=False)
                if res_keys:
                    if verbose: log.debug('\n\n[unit] res_keys: %s', res_keys)

            # set breakpoint for testing...
            if breakpoint1A:
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Load marine assets into system from xslx file
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            current_file = fid
            result = self.load_marine_assets_from_xlsx(fid)

            if verbose: log.debug('\n\n (pass 2) response: %s', result)

            asset_type_ids = result['res_modified']['asset_types']     # ids of resources created
            if asset_type_ids:
                if verbose: log.debug('\n\n[unit] have %d asset_type_ids: %s', len(asset_type_ids), asset_type_ids)
            else:
                log.debug('\n\n[unit] Error no asset_types returned!')
                raise BadRequest('Error no asset_types returned!')

            # set breakpoint for testing...
            if breakpoint_cleanup:
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Cleanup marine asset resources and CodeSpace
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            total_resources_deleted = 0
            asset_type_ids = result['res_modified']['asset_types'][:]
            if asset_type_ids:
                if verbose: log.debug('\n\n[unit] cleanup...asset_types...')
                total_resources_deleted += len(asset_type_ids)
                for id in asset_type_ids:
                    self.OMS.force_delete_asset_type(id)

            event_type_ids = result['res_modified']['event_types'][:]
            if event_type_ids:
                if verbose: log.debug('\n\n[unit] cleanup...event_duration_types...')
                total_resources_deleted += len(event_type_ids)
                for id in event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)

            asset_ids = result['res_modified']['assets'][:]
            if asset_ids:
                if verbose: log.debug('\n\n[unit] cleanup...assets...')
                total_resources_deleted += len(asset_ids)
                for id in asset_ids:
                    self.OMS.force_delete_asset(id)

            event_ids = result['res_modified']['events'][:]
            if event_ids:
                if verbose: log.debug('\n\n[unit] cleanup...asset events...')
                total_resources_deleted += len(event_ids)
                for id in event_ids:
                    self.OMS.force_delete_event_duration(id)

            if code_space_ids:
                if verbose: log.debug('\n\n[unit] cleanup...code_space_ids...')
                inx = 0
                total_resources_deleted += len(code_space_ids)
                for code_space_id in code_space_ids:
                    id = code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            # set breakpoint for testing...assets and asset_type should be deleted
            if breakpoint_after_cleanup:
                log.debug('\n\n[unit] verify all resources (%d) which have been created are removed.', total_resources_to_delete)
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise               # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise               # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_xls_with_codes')

    #-------------------------------------------------------
    # CodeSpaces, Codes and CodeSets unit tests end...
    #-------------------------------------------------------

    #------------------------------------------------------------------
    # Section: Handle declarations from xlsx spreadsheets (start...)
    #------------------------------------------------------------------
    # -----
    # ----- UNIT TEST: test_empty_workbook
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_empty_workbook(self):

        # test OMS service declare_asset_tracking_resources
        log.debug('\n\n***** Start : test_empty_workbook')

        # Input and folder(s) and files for driving test
        fid = TEST_XLS_FOLDER +  'EmptyWorkbook.xlsx'            #  negative test
        try:

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Load marine assets into system from xslx file
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            response = self.load_marine_assets_from_xlsx(fid)
            if response:
                if response['status'] != 'ok' or response['err_msg']:
                    log.debug('\n\n[unit] Error: %s' % response['err_msg'])
                else:
                    log.debug('\n\n[unit]Failed test - should have received an err_msg')
                    raise
        except:
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_empty_workbook')

    # -----
    # ----- UNIT TEST: test_upload_xls
    # -----
    @attr('UNIT', group='sa')
    def test_upload_xls(self):

        # test OMS service declare_asset_tracking_resources
        log.debug('\n\n***** Start : test_upload_xls')

        self._preload_scenario("BETA")  # required

        verbose = False
        breakpoint1A = False
        breakpoint1B = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint1B = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        fid = TEST_XLS_FOLDER +  'test500.xlsx'
        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []

        try:

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Load marine assets into system from xslx file
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            response = self.load_marine_assets_from_xlsx(fid)
            if response:

                if verbose: log.debug('\n\n[unit] response: %s', response)
                if response['status'] != 'ok' or response['err_msg']:
                    if response['err_msg']:
                        raise BadRequest('[unit] Error: %s' % response['err_msg'])
                    elif response['status']:
                        raise BadRequest('[unit] Error: %s' % response['status'])
                    else:
                        raise BadRequest('[unit] Error: err_msg and status not populated')

                if response['res_modified']:
                    code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                    if 'codespaces' in response['res_modified']:
                        code_space_ids = response['res_modified']['codespaces'][:]  # ids of resources created
                    if 'asset_types' in response['res_modified']:
                        asset_type_ids = response['res_modified']['asset_types']    # ids of resources created
                    if 'assets' in response['res_modified']:
                        asset_ids = response['res_modified']['assets']              # ids of resources created
                    if 'event_types' in response['res_modified']:
                        event_type_ids = response['res_modified']['event_types']    # ids of resources created
                    if 'events' in response['res_modified']:
                        event_ids = response['res_modified']['events']              # ids of resources created

            # set breakpoint for testing...
            if breakpoint1A:
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            # cleanup
            total_resources_deleted = 0
            if asset_type_ids:
                total_resources_deleted += len(asset_type_ids)
                for id in asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if event_type_ids:
                total_resources_deleted += len(event_type_ids)
                for id in event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if asset_ids:
                total_resources_deleted += len(asset_ids)
                for id in asset_ids:
                    self.OMS.force_delete_asset(id)
            if event_ids:
                total_resources_deleted += len(event_ids)
                for id in event_ids:
                    self.OMS.force_delete_event_duration(id)
            if code_space_ids:
                inx = 0
                total_resources_deleted += len(code_space_ids)
                for code_space_id in code_space_ids:
                    id = code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            #log.debug('\n\n[unit] total number of resources deleted: %d', total_resources_deleted)
            #self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            # set breakpoint for testing...assets and asset_type should be deleted
            if breakpoint1B:
                log.debug('\n\n[unit] verify all resources (%d) which have been created are removed.', total_resources_deleted)
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', fid, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_xls')

    # -----
    # ----- UNIT TEST: test_upload_xls_master
    # -----
    @attr('UNIT', group='sa')
    def test_upload_xls_master(self):

        # test OMS service declare_asset_tracking_resources
        log.debug('\n\n***** Start : test_upload_xls_master')

        self._preload_scenario("BETA")  # required

        verbose = False
        breakpoint1A = False
        breakpoint1B = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint1B = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        fid = TEST_XLS_FOLDER +  'test500_master.xlsx'
        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []

        try:

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Load marine assets into system from xslx file
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            response = self.load_marine_assets_from_xlsx(fid)
            if response:

                if verbose: log.debug('\n\n[unit] response: %s', response)
                if response['status'] != 'ok' or response['err_msg']:
                    if response['err_msg']:
                        raise BadRequest('[unit] Error: %s' % response['err_msg'])
                    elif response['status']:
                        raise BadRequest('[unit] Error: %s' % response['status'])
                    else:
                        raise BadRequest('[unit] Error: err_msg and status not populated')

                if response['res_modified']:
                    code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                    if 'codespaces' in response['res_modified']:
                        code_space_ids = response['res_modified']['codespaces'][:]  # ids of resources created
                    if 'asset_types' in response['res_modified']:
                        asset_type_ids = response['res_modified']['asset_types']    # ids of resources created
                    if 'assets' in response['res_modified']:
                        asset_ids = response['res_modified']['assets']              # ids of resources created
                    if 'event_types' in response['res_modified']:
                        event_type_ids = response['res_modified']['event_types']    # ids of resources created
                    if 'events' in response['res_modified']:
                        event_ids = response['res_modified']['events']              # ids of resources created

            # set breakpoint for testing...
            if breakpoint1A:
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            # cleanup
            total_resources_deleted = 0
            if asset_type_ids:
                total_resources_deleted += len(asset_type_ids)
                for id in asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if event_type_ids:
                total_resources_deleted += len(event_type_ids)
                for id in event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if asset_ids:
                total_resources_deleted += len(asset_ids)
                for id in asset_ids:
                    self.OMS.force_delete_asset(id)
            if event_ids:
                total_resources_deleted += len(event_ids)
                for id in event_ids:
                    self.OMS.force_delete_event_duration(id)
            if code_space_ids:
                inx = 0
                total_resources_deleted += len(code_space_ids)
                for code_space_id in code_space_ids:
                    id = code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            #log.debug('\n\n[unit] total number of resources deleted: %d', total_resources_deleted)
            #self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            # set breakpoint for testing...assets and asset_type should be deleted
            if breakpoint1B:
                log.debug('\n\n[unit] verify all resources (%d) which have been created are removed.', total_resources_deleted)
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', fid, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_xls_master')


    # -----
    # ----- UNIT TEST: test_download_xls
    # -----
    @attr('UNIT', group='sa')
    def test_download_xls(self):

        # test service(s) - use service declare_asset_tracking_resources to declare marine tracking
        # resources (instances) in the system. Once resources are loaded, call asset_tracking_report to
        # report of all instances in system, including CodeSpace, Codes, and CodeSets (xls)
        # Also reported are the event and asset associations
        #
        # Notes: Verification requirements indicate we must prove every marine tracking resource
        # has unique system id. Suggest optional parameter with_ids={False | True} where default False.
        # When called with parameter with_ids=True, then an additional column is added to output report for
        # AssetTypes, EventDurationTypes, Assets and EventDurations with column name 'Unique ID' populated with
        # actual OOI system id (altid) assigned for that resource instance.
        #
        # Notes: Integration. declare_asset_tracking_resources currently reports all marine tracking
        # resources, namely everything in namespaces 'CodeSpaces' (includes Codes and CodeSets), 'AssetType',
        # 'EventDurationType', 'Asset' and 'EventDuration'. One can see it to be a reasonable request to ask for
        #  'just give me the Assets' or 'just give me the CodeSpaces,etc'- meaning a partial report.
        # In addition, it is easy to envision users wanting a report constrained to asset tracking resources
        # for their Org.  (Partial reports would be most useful when dealing with CodeSpaces, Codes and Codesets)

        log.debug('\n\n***** Start : test_download_xls')

        self._preload_scenario("BETA")          # required

        verbose = True
        breakpointLoaded = False
        breakpoint1B = False
        breakpointCleanup = False
        breakpointVerifyCleanup = False

        interactive = False
        if interactive:
            verbose = True
            breakpointLoaded = True
            breakpoint1B = True
            breakpointCleanup = True
            breakpointVerifyCleanup = True

        # Input and folder(s) and files for driving test
        fid     = TEST_XLS_FOLDER + 'test500.xlsx'
        outfile = TEST_XLS_FOLDER + 'test500_download_report.xls'
        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []

        try:

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Load marine assets into system from xslx file
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            response = self.load_marine_assets_from_xlsx(fid)
            if response:
                if verbose: log.debug('\n\n[unit] response: %s', response)

            if response['status'] != 'ok' or response['err_msg']:
                raise BadRequest('[unit] Error: %s' % response['err_msg'])

            if response['res_modified']:
                asset_type_ids  = response['res_modified']['asset_types'][:]
                asset_ids       = response['res_modified']['assets'][:]
                event_type_ids  = response['res_modified']['event_types'][:]
                event_ids       = response['res_modified']['events'][:]


            # Breakpoint - Marine Asset resources loaded into system
            if breakpointLoaded:
                log.debug('\n\n[unit] Breakpoint - Marine Asset resources loaded into system')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # call asset_tracking_report service, report on marine tracking resources in System
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            if verbose: log.debug('\n\n[unit] request Marine Asset tracking resources ...\n')
            response = self.OMS.asset_tracking_report()

            if not response:
                log.debug('\n\n[unit] Failed to generate marine asset tracking report.')
                raise BadRequest('Failed to generate asset tracking report')
            else:

                # receive content from download_xls service, write to file
                try:
                    f = open(outfile, 'wb')
                except:
                    log.error('failed to open xlsx file for write: ', exc_info=True)
                    raise
                try:
                    rcontent = binascii.a2b_hex(response)
                    f.write(rcontent)
                    f.close()
                except:
                    log.error('[unit] failed to write xls content to output file (%s)', outfile)

                log.debug('\n\n[unit] marine asset tracking report saved to file: %s\n\n', outfile)

                # load what was just created
                response = self.load_marine_assets_from_xlsx(outfile)
                if response:
                    if verbose: log.debug('\n\n[unit] response: %s', response)

                if response['status'] != 'ok' or response['err_msg']:
                    raise BadRequest('[unit] Error: %s' % response['err_msg'])



                if breakpointCleanup:
                    log.debug('\n\n[unit] Breakpoint - preparing to delete resources which were created')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())


                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Cleanup
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                if asset_type_ids:
                    for id in asset_type_ids:
                        self.OMS.force_delete_asset_type(id)
                if asset_ids:
                    for id in asset_ids:
                        self.OMS.force_delete_asset(id)
                if event_type_ids:
                    for id in event_type_ids:
                        self.OMS.force_delete_event_duration_type(id)
                if event_ids:
                    for id in event_ids:
                        self.OMS.force_delete_event_duration(id)

                # set breakpoint for testing...assets and asset_type should be deleted
                if breakpointVerifyCleanup:
                    log.debug('\n\n[unit] Breakpoint - verify all Marine Asset resources have been removed')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise      # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', fid, exc_info=True)
            raise      # raise here to fail test case

        finally:
            log.debug('\n\n***** Completed : test_download_xls')


    # -----
    # ----- unit test: test_upload_all_sheets_twice
    # -----
    @attr('UNIT', group='sa')
    def test_upload_all_sheets_twice(self):

        # ('charlton' test) It is important UI developers can reload xlsx container without having to
        # reload container. And of course, is a possible (but unlikely) scenario for system engineers.
        # Step 1. load a single spreadsheet with all sheets (test505.xlsx) when there is no CodeSpace instance available
        # Step 2. load (again) same spread sheet

        log.debug('\n\n***** Start : test_upload_all_sheets_twice')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []

        try:
            sum_code_space_ids = []
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)
                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)
                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:

                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:

                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEquals(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEquals(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEquals(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEquals(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEquals(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEquals(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two 'add' again (causing update) of all resources - full load
                # asserts specifically for this unit test
                if pass_count == 2:
                    # Check uniqueness of alt_ids
                    unique = self.unique_altids(RT.Asset)
                    if unique != True:
                        if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                        raise BadRequest('duplicate Asset altids found!')
                    else:
                        if verbose: log.debug('\n\n[unit] all Asset altids unique')

                    picklist = self.OMS.get_assets_picklist(id_only='False')
                    log.debug('\n\n[unit] Assets picklist(%d): %s', len(picklist), picklist)

                    altids = self.OMS.get_altids(RT.Asset)
                    log.debug('\n\n[unit] Asset altids: %s', altids)
                    len_altids = 0
                    squish_len = 0
                    len_altids = len(altids)
                    squish_list = []
                    for item in picklist:
                        squish_list.append(item[2][0])

                    log.debug('\n\n[unit] Asset squish_list: %s', squish_list)
                    len_squish = len(list(set(squish_list)))
                    log.debug('\n\n[unit] Asset squish_list: %s', squish_list)
                    log.debug('\n\n[unit] Asset len squish_list: %d', len_squish)
                    if len_squish != len_altids:
                        raise BadRequest('squish test failed')

                    self.assertEquals(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEquals(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEquals(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEquals(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEquals(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0

            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)

            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids), msg='cleanup rm_code_space_ids')
            self.assertEquals(4, len(rm_asset_ids),     msg='cleanup rm_asset_ids')
            self.assertEquals(4, len(rm_asset_type_ids),msg='cleanup rm_asset_type_ids')
            self.assertEquals(8, len(rm_event_ids),     msg='cleanup rm_event_ids')
            self.assertEquals(9, len(rm_event_type_ids),msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted +=len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1
            log.debug('\n\n[unit] total number of resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_all_sheets_twice')

    # -----
    # ----- unit test: test_upload_xls_twice
    # -----
    @attr('UNIT', group='sa')
    def test_upload_xls_twice(self):

        # test service declare_asset_tracking_resources by calling twice to exercise create and update
        # functionality in service.
        #
        # Scenario: OOI loaded with AssetTypes available to all Orgs, then Org A
        # loads xlsx for their Assets using AssetTypes available in system; Org B uses xlsx to load their
        # AssetTypes and Assets, maybe reusing existing AssetTypes etc.
        #
        # Pass 1. Load all resources, including code related.
        # Pass 2. Only load some event and event types of xlsx on second load (per scenario outlined above)

        log.debug('\n\n***** Start : test_upload_xls_twice')

        self._preload_scenario("BETA")

        verbose = False
        breakpoint1A = False
        breakpoint1B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint1B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505-a.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
        pass_count = 0

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            for fid in input_files:
                pass_count += 1

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:
                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = []
                        asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces']
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']    # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']              # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_type_ids = response['res_modified']['event_types']    # ids of resources created
                            if event_type_ids:
                                sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_modified']:
                            event_ids = response['res_modified']['events']              # ids of resources created
                            if event_ids:
                                sum_event_ids.extend(event_ids)

                    if response['res_removed']:
                        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
                        if 'codespaces' in response['res_removed']:
                            rem_code_space_ids = response['res_removed']['codespaces'][:]
                            if rem_code_space_ids:
                                del_sum_code_space_ids.extend(rem_code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            rem_asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if rem_asset_type_ids:
                                del_sum_asset_type_ids.extend(rem_asset_type_ids)
                        if 'assets' in response['res_removed']:
                            rem_asset_ids = response['res_removed']['assets']               # ids of resources created
                            if rem_asset_ids:
                                del_sum_asset_ids.extend(rem_asset_ids)
                        if 'event_types' in response['res_removed']:
                            rem_event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if rem_event_type_ids:
                                del_sum_event_type_ids.extend(rem_event_type_ids)
                        if 'events' in response['res_removed']:
                            rem_event_ids = response['res_removed']['events']               # ids of resources created
                            if rem_event_ids:
                                del_sum_event_ids.extend(rem_event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    log.debug('\n\n len(sum_code_space_ids): %d', len(sum_code_space_ids))
                    log.debug('\n\n sum_code_space_ids: %s', sum_code_space_ids)

                    self.assertEquals(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEquals(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEquals(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEquals(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEquals(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEquals(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two 'add' again (causing update) of 1 event resource and two (2) event types
                # asserts specifically for this unit test
                if pass_count == 2:
                    # response results...
                    self.assertEquals(1, len(code_space_ids),        msg='pass 2: res_modified code_space_ids')
                    self.assertEquals(1, len(event_ids),             msg='pass 2: res_modified event_ids')
                    self.assertEquals(2, len(event_type_ids),        msg='pass 2: res_modified event_type_ids')

                    # totals summary (duplicates simply indicate 'touched' more than once
                    self.assertEquals(2, len(sum_code_space_ids),    msg='pass 2: sum_code_space_ids')
                    self.assertEquals(4, len(sum_asset_ids),         msg='pass 2: sum_asset_ids')
                    self.assertEquals(4, len(sum_asset_type_ids),    msg='pass 2: sum_asset_type_ids')
                    self.assertEquals(9, len(sum_event_ids),         msg='pass 2: sum_event_ids')
                    self.assertEquals(11,len(sum_event_type_ids),    msg='pass 2: sum_event_type_ids')

                    # resources removed...
                    self.assertEquals(0, len(del_sum_code_space_ids),msg='pass 2: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),     msg='pass 2: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),msg='pass 2: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),     msg='pass 2: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify asset tracking instances in system...')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEquals(1, len(rm_code_space_ids),    msg='cleanup rm_code_space_ids')
            self.assertEquals(4, len(rm_asset_ids),         msg='cleanup rm_asset_ids')
            self.assertEquals(4, len(rm_asset_type_ids),    msg='cleanup rm_asset_type_ids')
            self.assertEquals(8, len(rm_event_ids),         msg='cleanup rm_event_ids')
            self.assertEquals(9, len(rm_event_type_ids),    msg='cleanup rm_event_type_ids')
            self.assertEquals(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                #if verbose: log.debug('\n\n[unit] cleanup...asset_types...')
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                #if verbose: log.debug('\n\n[unit] cleanup...event_duration_types...')
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                #if verbose: log.debug('\n\n[unit] cleanup...assets...')
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                #if verbose: log.debug('\n\n[unit] cleanup...event durations...')
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                #if verbose: log.debug('\n\n[unit] cleanup...code_space_ids...%d', len(rm_code_space_ids))
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            log.debug('\n\n[unit] total number of resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint1B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            picklist = []
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                log.debug('\n\n[unit] duplicate altids found')
                raise BadRequest('duplicate altids found!')
            else:
                log.debug('\n\n[unit] all altids unique')

            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate altids')

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise       # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise       # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_xls_twice')

    # -----
    # ----- unit test: test_upload_new_attribute_specification
    # -----
    @attr('UNIT', group='sa')
    def test_upload_new_attribute_specification(self):

        # test service declare_asset_tracking_resources by calling twice to exercise create and update
        # functionality in service.
        #
        # Scenario: OOI loaded with AssetTypes available to all Orgs, then Org A
        # loads xlsx for their Assets using AssetTypes available in system; Org B uses xlsx to load their
        # AssetTypes and Assets, maybe reusing existing AssetTypes etc.
        #
        # Pass 1. Load all resources, including code related.
        # Pass 2. Load platform type, asset and attr spec with NEW (additional) attribute specification
        #

        log.debug('\n\n***** Start : test_upload_new_attribute_specification')

        #self._preload_scenario("BETA")

        verbose = False
        summary = False
        breakpoint1A = False
        breakpoint1B = False

        interactive = False
        if interactive:
            verbose = True
            summary = True
            breakpoint1A = True
            breakpoint1B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test500-new-attribute-specification.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
        pass_count = 0

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            for fid in input_files:
                pass_count += 1

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:
                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = []
                        asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces']
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']    # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']              # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_type_ids = response['res_modified']['event_types']    # ids of resources created
                            if event_type_ids:
                                sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_modified']:
                            event_ids = response['res_modified']['events']              # ids of resources created
                            if event_ids:
                                sum_event_ids.extend(event_ids)

                    if response['res_removed']:
                        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
                        if 'codespaces' in response['res_removed']:
                            rem_code_space_ids = response['res_removed']['codespaces'][:]
                            if rem_code_space_ids:
                                del_sum_code_space_ids.extend(rem_code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            rem_asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if rem_asset_type_ids:
                                del_sum_asset_type_ids.extend(rem_asset_type_ids)
                        if 'assets' in response['res_removed']:
                            rem_asset_ids = response['res_removed']['assets']               # ids of resources created
                            if rem_asset_ids:
                                del_sum_asset_ids.extend(rem_asset_ids)
                        if 'event_types' in response['res_removed']:
                            rem_event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if rem_event_type_ids:
                                del_sum_event_type_ids.extend(rem_event_type_ids)
                        if 'events' in response['res_removed']:
                            rem_event_ids = response['res_removed']['events']               # ids of resources created
                            if rem_event_ids:
                                del_sum_event_ids.extend(rem_event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEquals(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEquals(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEquals(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEquals(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEquals(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEquals(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                    # verify unique AttributeSpecification names in attribute_specifications {}
                    for id in asset_type_ids:
                        names = ''
                        unique_names = ''
                        type = self.OMS.read_asset_type(id)
                        if type.attribute_specifications:
                            if summary: log.debug('\n\n[unit] asset type: %s has %d specs', type.name, len(type.attribute_specifications))
                            names = type.attribute_specifications.keys()
                            if names:
                                unique_names = list(set(names))
                                if summary:
                                    log.debug('\n\n[unit] len(names): %d   len(unique_names): %d', len(names), len(unique_names))
                                    outline = '\n\n '+ type.name + ' unique names...\n'
                                    for uname in unique_names:
                                        outline += uname + '\n'
                                    log.debug('\n\n[unit] %s', outline)

                            self.assertEqual(len(names),len(unique_names), msg='duplicate names in attribute specification')

                # pass two 'add' new attribute specification to platform type resource
                # asserts specifically for this unit test
                if pass_count == 2:

                    # response results...
                    self.assertEquals(1, len(code_space_ids),        msg='pass 2: res_modified code_space_ids')
                    self.assertEquals(0, len(event_ids),             msg='pass 2: res_modified event_ids')
                    self.assertEquals(0, len(event_type_ids),        msg='pass 2: res_modified event_type_ids')

                    # totals summary (duplicates simply indicate 'touched' more than once
                    self.assertEquals(2, len(sum_code_space_ids),    msg='pass 2: sum_code_space_ids')
                    self.assertEquals(5, len(sum_asset_ids),         msg='pass 2: sum_asset_ids')
                    self.assertEquals(7, len(sum_asset_type_ids),    msg='pass 2: sum_asset_type_ids')
                    self.assertEquals(8, len(sum_event_ids),         msg='pass 2: sum_event_ids')
                    self.assertEquals(9, len(sum_event_type_ids),    msg='pass 2: sum_event_type_ids')

                    # resources removed...
                    self.assertEquals(0, len(del_sum_code_space_ids),msg='pass 2: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),     msg='pass 2: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),msg='pass 2: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),     msg='pass 2: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),msg='pass 2: del_sum_event_type_ids')

                    # verify unique AttributeSpecification names in attribute_specifications {}
                    for id in asset_type_ids:
                        names = ''
                        unique_names = ''
                        type = self.OMS.read_asset_type(id)
                        if type.attribute_specifications:
                            if summary: log.debug('\n\n[unit] asset type: %s has %d specs', type.name, len(type.attribute_specifications))
                            names = type.attribute_specifications.keys()
                            if names:
                                unique_names = list(set(names))
                                if summary:
                                    log.debug('\n\n[unit] len(names): %d   len(unique_names): %d', len(names), len(unique_names))
                                    outline = '\n\n '+ type.name + ' unique names...\n'
                                    for uname in unique_names:
                                        outline += uname + '\n'
                                    log.debug('\n\n[unit] %s', outline)

                            self.assertEqual(len(names),len(unique_names), msg='duplicate names in attribute specification')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify asset tracking instances in system...')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            if summary:
                log.debug('\n\n[unit] Summary of \'add\' items processed:\nNumber of passes: %d\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    pass_count, len(sum_asset_ids), len(sum_asset_type_ids),
                    len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] Summary of \'remove\' items processed:\nNumber of passes: %d\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    pass_count, len(del_sum_asset_ids), len(del_sum_asset_type_ids),
                    len(del_sum_event_ids), len(del_sum_event_type_ids),
                    len(del_sum_code_space_ids))

            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEquals(1, len(rm_code_space_ids),    msg='cleanup rm_code_space_ids')
            self.assertEquals(4, len(rm_asset_ids),         msg='cleanup rm_asset_ids')
            self.assertEquals(4, len(rm_asset_type_ids),    msg='cleanup rm_asset_type_ids')
            self.assertEquals(8, len(rm_event_ids),         msg='cleanup rm_event_ids')
            self.assertEquals(9, len(rm_event_type_ids),    msg='cleanup rm_event_type_ids')
            self.assertEquals(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total number of resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint1B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

            picklist = []
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                log.debug('\n\n[unit] duplicate altids found')
                raise BadRequest('duplicate altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all altids unique')

            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate altids')

        except BadRequest, Arguments:
            log.debug('\n\n[unit] bad request exception')
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise       # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] not found exception')
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except Inconsistent, Arguments:
            log.debug('\n\n[unit] inconsistent exception')
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.debug('\n\n[unit] general exception')
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise       # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_new_attribute_specification')


    # -----
    # ----- unit test: test_upload_xls_triple_codes
    # -----
    @attr('UNIT', group='sa')
    def test_upload_xls_triple_codes(self):

        # test service declare_asset_tracking_resources by calling three times to exercise create and update
        # functionality in service - specifically for CodeSpaces, Codes, CodeSets, Events, EventTypes,
        # Event attribute specs and event attributes. 'remove' functionality tested for Codes.
        #
        # Scenario: OOI loaded with AssetTypes available to all Orgs, then Org A
        # loads xlsx for their Assets using AssetTypes available in system; Org B uses xlsx to load their
        # AssetTypes and Assets, maybe reusing existing AssetTypes etc. Consider delete also, since an AssetType
        # available to all Orgs and used by more than zero Orgs when deleted will affect Orgs which use it.
        # Thnk about how to handle deletion of types and instances.
        #
        # This unit test loads three different xlsx spreadsheets to accomplish the following:
        # load 1 - load all sheets, including (test505.xslx)
        #   CodeSpaces, Codes, CodeSets, Assets, Events, AssetTypes EventTypes, Attribute Specs and Attributes
        #
        # load 2 'add'
        #   'add' new codes and codeset for colors (test505-a.xslx);
        #   modify EventDuration instance ReturnToManufacturer instance attributes:
        #       'event description' == 'device damaged by trawler'
        #       'recording operator name' == 'Nina Recorder'
        #       'RTM return authorization number' == 'RTM-RAN-43'
        #   (loads sheets CodeSpaces, Codes, CodeSets, Events, EventTypes, Event Attribute Specs and Event Attributes)
        #
        # load 3 'remove' Code 'pink' from CodeSpace; verify CodeSet 'colors' is updated. (test505-b.xslx; sheets CodeSpaces and Codes)
        #

        log.debug('\n\n***** Start : test_upload_xls_triple_codes')

        #self._preload_scenario("BETA")     # required

        verbose = False
        breakpoint1A = False
        breakpoint1B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint1B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505-a.xlsx', 'test505-b.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
        pass_count = 0

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            for fid in input_files:
                pass_count += 1

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces']
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_type_ids:
                                sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_modified']:
                            event_ids = response['res_modified']['events']             # ids of resources created
                            if event_ids:
                                sum_event_ids.extend(event_ids)

                    if response['res_removed']:
                        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
                        if 'codespaces' in response['res_removed']:
                            rem_code_space_ids = response['res_removed']['codespaces'][:]
                            if rem_code_space_ids:
                                del_sum_code_space_ids.extend(rem_code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            rem_asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if rem_asset_type_ids:
                                del_sum_asset_type_ids.extend(rem_asset_type_ids)
                        if 'assets' in response['res_removed']:
                            rem_asset_ids = response['res_removed']['assets']               # ids of resources created
                            if rem_asset_ids:
                                del_sum_asset_ids.extend(rem_asset_ids)
                        if 'event_types' in response['res_removed']:
                            rem_event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if rem_event_type_ids:
                                del_sum_event_type_ids.extend(rem_event_type_ids)
                        if 'events' in response['res_removed']:
                            rem_event_ids = response['res_removed']['events']               # ids of resources created
                            if rem_event_ids:
                                del_sum_event_ids.extend(rem_event_ids)
                #---------------------------------------------------------------------------------
                # Pass 1.
                # pass one 'add' all resources - full load
                # asserts specifically for pass 1 of this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')

                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                #---------------------------------------------------------------------------------
                # Pass 2.
                # pass two 'add' again (causing update) of 1 event resource and two (2) event types
                # asserts specifically for pass 2 of this unit test
                if pass_count == 2:
                    # What changed.........
                    # what changed through action=='add' this pass...
                    self.assertEqual(1, len(code_space_ids),       msg='pass 2: res_modified code_space_ids')
                    self.assertEqual(1, len(event_ids),            msg='pass 2: res_modified event_ids')
                    self.assertEqual(2, len(event_type_ids),       msg='pass 2: res_modified event_type_ids')
                    self.assertEqual(0, len(asset_ids),            msg='pass 2: res_modified asset_ids')
                    self.assertEqual(0, len(asset_type_ids),       msg='pass 2: res_modified asset_type_ids')
                    # what changed through action=='remove' this pass...
                    self.assertEqual(0, len(rem_code_space_ids),   msg='pass 3: res_removed code_space_ids')
                    self.assertEqual(0, len(rem_event_ids),        msg='pass 3: res_removed event_ids')
                    self.assertEqual(0, len(rem_event_type_ids),   msg='pass 3: res_removed event_type_ids')
                    self.assertEqual(0, len(rem_asset_ids),        msg='pass 2: res_removed asset_ids')
                    self.assertEqual(0, len(rem_asset_type_ids),   msg='pass 2: res_removed asset_type_ids')

                    #--------------------------------------------------------------
                    # Verify detailed field changes/updated and removals
                    #--------------------------------------------------------------
                    # Verify EventDuration instance has expected modifications:
                    # Verify modifications to EventDuration instance ReturnToManufacturer instance attributes:
                    #       'event description' == 'device damaged by trawler'
                    #       'recording operator name' == 'Nina Recorder'
                    #       'RTM return authorization number' == 'RTM-RAN-43'
                    event_obj2 = self.RR2.read(event_ids[0],specific_type=RT.EventDuration)
                    #log.debug('\n\n[unit] event_obj2: %s', event_obj2)
                    if event_obj2:
                        if event_obj2.event_duration_attrs:
                            if 'event description' in event_obj2.event_duration_attrs:
                                attr = event_obj2.event_duration_attrs['event description']['value']
                                #log.debug('\n\n[unit] event description attr: %s', attr)
                                attr_value = attr[0]['value']
                                #log.debug('\n\n[unit] event description attr_value: %s', attr_value)
                                self.assertEqual('device damaged by trawler',
                                                 attr_value,
                                                 msg='failed to update "event description" ')
                            if 'recording operator name' in event_obj2.event_duration_attrs:
                                attr = event_obj2.event_duration_attrs['recording operator name']['value']
                                attr_value = attr[0]['value']
                                self.assertEqual('Nina Recorder',
                                                 attr_value,
                                                 msg='failed to update "recording operator name" ')
                            if 'RTM return authorization number' in event_obj2.event_duration_attrs:
                                attr = event_obj2.event_duration_attrs['RTM return authorization number']['value']
                                attr_value = attr[0]['value']
                                self.assertEqual('RTM-RAN-43',
                                                 attr_value,
                                                 msg='failed to update "RTM return authorization number" ')
                    cs = self.OMS.read_code_space(code_space_ids[0])
                    if cs:
                        if cs.codesets:
                            if 'colors' in cs.codesets:
                                log.debug('\n\n[unit]codeset \'colors\' has been created')
                                codeset_colors = cs.codesets['colors']
                                if codeset_colors:
                                    if codeset_colors['enumeration']:
                                        log.debug('\n\n[unit] codespace.codeset[colors] enumeration: %s',
                                                  codeset_colors['enumeration'])

                    # Running totals.....
                    # totals summary res_modified (duplicates simply indicate 'touched' more than once during multiple passes)
                    self.assertEqual(2, len(sum_code_space_ids),   msg='pass 2: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),        msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),   msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(9, len(sum_event_ids),        msg='pass 2: sum_event_ids')
                    self.assertEqual(11,len(sum_event_type_ids),   msg='pass 2: sum_event_type_ids')
                    # totals summary of res_removed - summary of resources removed during multiple passes
                    self.assertEqual(0, len(del_sum_code_space_ids),   msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),        msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),   msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),        msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),   msg='pass 2: del_sum_event_type_ids')

                #---------------------------------------------------------------------------------
                # Pass 3.
                # pass three 'remove' codeset  (causing update) of 1 CodeSpace resource
                # asserts specifically for pass 3 of this unit test
                if pass_count == 3:
                    # what changed through action=='add' this pass...
                    self.assertEqual(1, len(code_space_ids),       msg='pass 3: res_modified code_space_ids')
                    self.assertEqual(0, len(event_ids),            msg='pass 3: res_modified event_ids')
                    self.assertEqual(0, len(event_type_ids),       msg='pass 3: res_modified event_type_ids')
                    self.assertEqual(0, len(asset_ids),            msg='pass 3: res_modified asset_ids')
                    self.assertEqual(0, len(asset_type_ids),       msg='pass 3: res_modified asset_type_ids')

                    # what changed through action=='remove' this pass...
                    self.assertEqual(0, len(rem_code_space_ids),   msg='pass 3: res_removed code_space_ids')
                    self.assertEqual(0, len(rem_event_ids),        msg='pass 3: res_removed event_ids')
                    self.assertEqual(0, len(rem_event_type_ids),   msg='pass 3: res_removed event_type_ids')
                    self.assertEqual(0, len(rem_asset_ids),        msg='pass 3: res_removed asset_ids')
                    self.assertEqual(0, len(rem_asset_type_ids),   msg='pass 3: res_removed asset_type_ids')

                    #--------------------------------------------------------------
                    # Verify detailed field changes/updated and removals
                    #--------------------------------------------------------------
                    # Verify codeset 'colors' had 'pink' removed from codeset:
                    cs = self.OMS.read_code_space(code_space_ids[0])
                    if cs:
                        if cs.codesets:
                            if 'colors' in cs.codesets:
                                log.debug('\n\n[unit]codeset \'colors\' present')
                                codeset_colors = cs.codesets['colors']
                                if codeset_colors:
                                    if codeset_colors['enumeration']:
                                        log.debug('\n\n[unit] codespace.codeset[\'colors\'] enumeration: %s',
                                                  codeset_colors['enumeration'])
                                        if 'pink' not in codeset_colors['enumeration']:
                                            log.debug('\n\n[unit] \'pink\' successfully removed from codeset \'colors\' enumeration')

                    # totals summary resources 'add'ed during multiple passes
                    self.assertEqual(3, len(sum_code_space_ids),   msg='pass 3: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),        msg='pass 3: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),   msg='pass 3: sum_asset_type_ids')
                    self.assertEqual(9, len(sum_event_ids),        msg='pass 3: sum_event_ids')
                    self.assertEqual(11,len(sum_event_type_ids),   msg='pass 3: sum_event_type_ids')

                    # totals of resources 'removed'ed during multiple passes
                    self.assertEqual(0, len(del_sum_code_space_ids),   msg='pass 3: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),        msg='pass 3: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),   msg='pass 3: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),        msg='pass 3: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),   msg='pass 3: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify asset tracking instances in system...')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0


            if verbose:
                log.debug('\n\n[unit] Summary of \'add\' items processed:\nNumber of passes: %d\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    pass_count, len(sum_asset_ids), len(sum_asset_type_ids),
                    len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

            if verbose:
                log.debug('\n\n[unit] Summary of \'remove\' items processed:\nNumber of passes: %d\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    pass_count, len(del_sum_asset_ids), len(del_sum_asset_type_ids),
                    len(del_sum_event_ids), len(del_sum_event_type_ids),
                    len(del_sum_code_space_ids))

            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids), msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),     msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),     msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1
            log.debug('\n\n[unit] total number of resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint1B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise       # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise       # raise here to fail test case
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise       # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_xls_triple_codes')

    # -----
    # ----- unit test: test_upload_xls_triple_codes_only
    # -----
    @attr('UNIT', group='sa')
    def test_upload_xls_triple_codes_only(self):

        # test service declare_asset_tracking_resources by calling three times to exercise create and update
        # functionality in service - specifically for CodeSpaces, Codes, CodeSets, Events, EventTypes,
        # Event attribute specs and event attributes. 'remove functionality tested for Codes.
        #
        # Scenario: OOI loaded with AssetTypes available to all Orgs, then Org A
        # loads xlsx for their Assets using AssetTypes available in system; Org B uses xlsx to load their
        # AssetTypes and Assets, maybe reusing existing AssetTypes etc. Consider delete also, since an AssetType
        # available to all Orgs and used by more than zero Orgs when deleted will affect Orgs which use it.
        # Thnk about how to handle deletion of types and instances.
        #
        # This unit test three different xlsx spreadsheets to accomplish the following:
        # load 1 - load all sheets, including (test505.xslx)
        #   CodeSpaces, Codes, CodeSets, Assets, Events, AssetTypes EventTypes, Attribute Specs and Attributes
        #
        # load 2 'add'
        #   new codes and codeset for colors (test505-a.xslx);
        #   modify EventDuration instance ReturnToManufacturer instance attributes:
        #       'event description' == 'device damaged by trawler'
        #       'recording operator name' == 'Nina Recorder'
        #       'RTM return authorization number' == 'RTM-RAN-43'
        #   (loads sheets CodeSpaces, Codes, CodeSets, Events, EventTypes, Event Attribute Specs and Event Attributes)
        #
        # load 3 'remove' Code 'pink' from Codespace; verify CodeSet colors is updated. (test505-b.xslx; sheets Codes (only))
        # Test the removal of pink from CodeSet colors by removal of code 'pink' fom codes - resulting in
        # CodeSet reflecting the removal of enumeration value 'pink'
        #
        #

        log.debug('\n\n***** Start : test_upload_xls_triple_codes_only')

        #self._preload_scenario("BETA")

        verbose = False
        breakpoint1A = False
        breakpoint1B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint1B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505-a.xlsx', 'test505-c.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
        pass_count = 0

        try:
            sum_code_space_ids = []
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            for fid in input_files:
                pass_count += 1

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:

                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces']
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_type_ids:
                                sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_modified']:
                            event_ids = response['res_modified']['events']             # ids of resources created
                            if event_ids:
                                sum_event_ids.extend(event_ids)

                    if response['res_removed']:

                        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
                        if 'codespaces' in response['res_removed']:
                            rem_code_space_ids = response['res_removed']['codespaces'][:]
                            if rem_code_space_ids:
                                del_sum_code_space_ids.extend(rem_code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            rem_asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if rem_asset_type_ids:
                                del_sum_asset_type_ids.extend(rem_asset_type_ids)
                        if 'assets' in response['res_removed']:
                            rem_asset_ids = response['res_removed']['assets']               # ids of resources created
                            if rem_asset_ids:
                                del_sum_asset_ids.extend(rem_asset_ids)
                        if 'event_types' in response['res_removed']:
                            rem_event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if rem_event_type_ids:
                                del_sum_event_type_ids.extend(rem_event_type_ids)
                        if 'events' in response['res_removed']:
                            rem_event_ids = response['res_removed']['events']               # ids of resources created
                            if rem_event_ids:
                                del_sum_event_ids.extend(rem_event_ids)

                #---------------------------------------------------------------------------------
                # Pass 1.
                # pass one 'add' all resources - full load
                # asserts specifically for pass 1 of this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')

                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                #---------------------------------------------------------------------------------
                # Pass 2.
                # pass two 'add' again (causing update) of 1 event resource and two (2) event types
                # asserts specifically for pass 2 of this unit test
                if pass_count == 2:

                    # What changed.........
                    # what changed through action=='add' this pass...
                    self.assertEqual(1, len(code_space_ids),       msg='pass 2: res_modified code_space_ids')
                    self.assertEqual(1, len(event_ids),            msg='pass 2: res_modified event_ids')
                    self.assertEqual(2, len(event_type_ids),       msg='pass 2: res_modified event_type_ids')
                    self.assertEqual(0, len(asset_ids),            msg='pass 2: res_modified asset_ids')
                    self.assertEqual(0, len(asset_type_ids),       msg='pass 2: res_modified asset_type_ids')
                    # what changed through action=='remove' this pass...
                    self.assertEqual(0, len(rem_code_space_ids),   msg='pass 2: res_removed code_space_ids')
                    self.assertEqual(0, len(rem_event_ids),        msg='pass 2: res_removed event_ids')
                    self.assertEqual(0, len(rem_event_type_ids),   msg='pass 2: res_removed event_type_ids')
                    self.assertEqual(0, len(rem_asset_ids),        msg='pass 2: res_removed asset_ids')
                    self.assertEqual(0, len(rem_asset_type_ids),   msg='pass 2: res_removed asset_type_ids')

                    #--------------------------------------------------------------
                    # Verify detailed field changes/updated and removals
                    #--------------------------------------------------------------
                    # Verify EventDuration instance has expected modifications:
                    # Verify modifications to EventDuration instance ReturnToManufacturer instance attributes:
                    #       'event description' == 'device damaged by trawler'
                    #       'recording operator name' == 'Nina Recorder'
                    #       'RTM return authorization number' == 'RTM-RAN-43'
                    event_obj2 = self.RR2.read(event_ids[0],specific_type=RT.EventDuration)
                    #log.debug('\n\n[unit] event_obj2: %s', event_obj2)
                    if event_obj2:
                        if event_obj2.event_duration_attrs:
                            if 'event description' in event_obj2.event_duration_attrs:
                                attr = event_obj2.event_duration_attrs['event description']['value']
                                attr_value = attr[0]['value']
                                self.assertEqual('device damaged by trawler',
                                                 attr_value,
                                                 msg='failed to update "event description" ')
                            if 'recording operator name' in event_obj2.event_duration_attrs:
                                attr = event_obj2.event_duration_attrs['recording operator name']['value']
                                attr_value = attr[0]['value']
                                self.assertEqual('Nina Recorder',
                                                 attr_value,
                                                 msg='failed to update "recording operator name" ')
                            if 'RTM return authorization number' in event_obj2.event_duration_attrs:
                                attr = event_obj2.event_duration_attrs['RTM return authorization number']['value']
                                attr_value = attr[0]['value']
                                self.assertEqual('RTM-RAN-43',
                                                 attr_value,
                                                 msg='failed to update "RTM return authorization number" ')
                    cs = self.OMS.read_code_space(code_space_ids[0])
                    if cs:
                        if cs.codesets:
                            if 'colors' in cs.codesets:
                                log.debug('\n\n[unit]codeset \'colors\' has been created')
                                codeset_colors = cs.codesets['colors']
                                if codeset_colors:
                                    if codeset_colors['enumeration']:
                                        log.debug('\n\n[unit] codespace.codeset[solors] enumeration: %s',
                                                  codeset_colors['enumeration'])

                    # Running totals.....
                    # totals summary res_modified (duplicates simply indicate 'touched' more than once during multiple passes)
                    self.assertEqual(2, len(sum_code_space_ids),   msg='pass 2: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),        msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),   msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(9, len(sum_event_ids),        msg='pass 2: sum_event_ids')
                    self.assertEqual(11,len(sum_event_type_ids),   msg='pass 2: sum_event_type_ids')
                    # totals summary of res_removed - summary of resources removed during multiple passes
                    self.assertEqual(0, len(del_sum_code_space_ids),   msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),        msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),   msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),        msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),   msg='pass 2: del_sum_event_type_ids')

                #---------------------------------------------------------------------------------
                # Pass 3.
                # pass three 'remove' codeset  (causing update) of 1 CodeSpace resource
                # asserts specifically for pass 3 of this unit test
                if pass_count == 3:
                    # what changed through action=='add' this pass...
                    self.assertEqual(1, len(code_space_ids),       msg='pass 3: res_modified code_space_ids')
                    self.assertEqual(0, len(event_ids),            msg='pass 3: res_modified event_ids')
                    self.assertEqual(0, len(event_type_ids),       msg='pass 3: res_modified event_type_ids')
                    self.assertEqual(0, len(asset_ids),            msg='pass 3: res_modified asset_ids')
                    self.assertEqual(0, len(asset_type_ids),       msg='pass 3: res_modified asset_type_ids')

                    # what changed through action=='remove' this pass...
                    self.assertEqual(0, len(rem_code_space_ids),   msg='pass 3: res_removed code_space_ids')
                    self.assertEqual(0, len(rem_event_ids),        msg='pass 3: res_removed event_ids')
                    self.assertEqual(0, len(rem_event_type_ids),   msg='pass 3: res_removed event_type_ids')
                    self.assertEqual(0, len(rem_asset_ids),        msg='pass 3: res_removed asset_ids')
                    self.assertEqual(0, len(rem_asset_type_ids),   msg='pass 3: res_removed asset_type_ids')

                    #--------------------------------------------------------------
                    # Verify detailed field changes/updated and removals
                    #--------------------------------------------------------------
                    # Verify codeset 'colors' had 'pink' removed from codeset:
                    cs = self.OMS.read_code_space(code_space_ids[0])
                    if cs:
                        if cs.codesets:
                            if 'colors' in cs.codesets:
                                log.debug('\n\n[unit]codeset \'colors\' present')
                                codeset_colors = cs.codesets['colors']
                                if codeset_colors:
                                    if codeset_colors['enumeration']:
                                        log.debug('\n\n[unit] codespace.codeset[\'colors\'] enumeration: %s',
                                                  codeset_colors['enumeration'])
                                        if 'pink' not in codeset_colors['enumeration']:
                                            log.debug('\n\n[unit] \'pink\' successfully removed from codeset \'colors\' enumeration')

                    # totals summary resources 'add'ed during multiple passes
                    self.assertEqual(3, len(sum_code_space_ids),   msg='pass 3: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),        msg='pass 3: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),   msg='pass 3: sum_asset_type_ids')
                    self.assertEqual(9, len(sum_event_ids),        msg='pass 3: sum_event_ids')
                    self.assertEqual(11,len(sum_event_type_ids),   msg='pass 3: sum_event_type_ids')

                    # totals of resources 'removed'ed during multiple passes
                    self.assertEqual(0, len(del_sum_code_space_ids),   msg='pass 3: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),        msg='pass 3: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),   msg='pass 3: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),        msg='pass 3: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),   msg='pass 3: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify asset tracking instances in system...')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            if verbose:
                log.debug('\n\n[unit] Summary of \'add\' items processed:\nNumber of passes: %d\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    pass_count, len(sum_asset_ids), len(sum_asset_type_ids),
                    len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

            if verbose:
                log.debug('\n\n[unit] Summary of \'remove\' items processed:\nNumber of passes: %d\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    pass_count, len(del_sum_asset_ids), len(del_sum_asset_type_ids),
                    len(del_sum_event_ids), len(del_sum_event_type_ids),
                    len(del_sum_code_space_ids))

            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids), msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),     msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),     msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1
            log.debug('\n\n[unit] total number of resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint1B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise       # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise       # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_xls_triple_codes_only')

    # -----
    # ----- unit test: test_upload_remove_codeset
    # -----
    @attr('UNIT', group='sa')
    def test_upload_remove_codeset(self):

        # test service declare_asset_tracking_resources by calling four times to exercise create and update
        # functionality in service. Also exercise 'remove' action to perform delete. (load 4)
        #
        # Scenario: OOI loaded with AssetTypes available to all Orgs, then Org A
        # loads xlsx for their Assets using AssetTypes available in system; Org B uses xlsx to load their
        # AssetTypes and Assets, maybe reusing existing AssetTypes etc.
        #
        # This unit test three different xlsx spreadsheets to accomplish the following:
        # load 1 - load all sheets, including (test505.xslx)
        #   CodeSpaces, Codes, CodeSets, Assets, Events, AssetTypes EventTypes, Attribute Specs and Attributes
        #
        # load 2 'add' new codes and codeset for colors (test505-add-codeset.xslx); also modify Event RTM attributes
        #   (loads sheets CodeSpaces, Codes, CodeSets, Events, EventTypes, Event Attribute Specs and Event Attributes)
        #
        # load 3 - change CodeSet 'color' change enumeration to not have 'yellow' and 'green' (test505-change-codeset.xlsx)
        #
        # load 4 'remove' CodeSet 'colors'. (test505-rm-codeset.xslx; sheet CodeSet)
        #

        log.debug('\n\n***** Start : test_upload_remove_codeset')

        #self._preload_scenario("BETA")     # not required for test unless Orgs for code related resources

        verbose = False
        breakpoint1A = False
        breakpoint1B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint1B = True

        # Input: folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505-add-codeset.xlsx', 'test505-change-codeset.xlsx', 'test505-rm-codeset.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
        pass_count = 0
        try:
            sum_code_space_ids = []
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            for fid in input_files:
                pass_count += 1

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces']
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']    # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']              # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_type_ids:
                                sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_modified']:
                            event_ids = response['res_modified']['events']             # ids of resources created
                            if event_ids:
                                sum_event_ids.extend(event_ids)

                    if response['res_removed']:
                        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
                        if 'codespaces' in response['res_removed']:
                            rem_code_space_ids = response['res_removed']['codespaces'][:]
                            if rem_code_space_ids:
                                del_sum_code_space_ids.extend(rem_code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            rem_asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if rem_asset_type_ids:
                                del_sum_asset_type_ids.extend(rem_asset_type_ids)
                        if 'assets' in response['res_removed']:
                            rem_asset_ids = response['res_removed']['assets']               # ids of resources created
                            if rem_asset_ids:
                                del_sum_asset_ids.extend(rem_asset_ids)
                        if 'event_types' in response['res_removed']:
                            rem_event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if rem_event_type_ids:
                                del_sum_event_type_ids.extend(rem_event_type_ids)
                        if 'events' in response['res_removed']:
                            rem_event_ids = response['res_removed']['events']               # ids of resources created
                            if rem_event_ids:
                                del_sum_event_ids.extend(rem_event_ids)
                #---------------------------------------------------------------------------------
                # Pass 1.
                # pass one 'add' all resources - full load
                # asserts specifically for pass 1 of this unit test
                if pass_count == 1:
                    log.debug('\n\n len(sum_code_space_ids): %d', len(sum_code_space_ids))
                    log.debug('\n\n sum_code_space_ids: %s', sum_code_space_ids)

                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')

                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                #---------------------------------------------------------------------------------
                # Pass 2.
                # pass two 'add' again (causing update) of 1 event resource and two (2) event types
                # asserts specifically for pass 2 of this unit test
                if pass_count == 2:

                    # What changed.........
                    # what changed through action=='add' this pass...
                    self.assertEqual(1, len(code_space_ids),       msg='pass 2: res_modified code_space_ids')
                    self.assertEqual(0, len(event_ids),            msg='pass 2: res_modified event_ids')
                    self.assertEqual(0, len(event_type_ids),       msg='pass 2: res_modified event_type_ids')
                    self.assertEqual(0, len(asset_ids),            msg='pass 2: res_modified asset_ids')
                    self.assertEqual(0, len(asset_type_ids),       msg='pass 2: res_modified asset_type_ids')
                    # what changed through action=='remove' this pass...
                    self.assertEqual(0, len(rem_code_space_ids),   msg='pass 2: res_removed code_space_ids')
                    self.assertEqual(0, len(rem_event_ids),        msg='pass 2: res_removed event_ids')
                    self.assertEqual(0, len(rem_event_type_ids),   msg='pass 2: res_removed event_type_ids')
                    self.assertEqual(0, len(rem_asset_ids),        msg='pass 2: res_removed asset_ids')
                    self.assertEqual(0, len(rem_asset_type_ids),   msg='pass 2: res_removed asset_type_ids')

                    #--------------------------------------------------------------
                    # Verify detailed field changes/updated and removals
                    #--------------------------------------------------------------
                    cs = self.OMS.read_code_space(code_space_ids[0])
                    if cs:
                        if cs.codesets:
                            if 'colors' in cs.codesets:
                                log.debug('\n\n[unit]codeset \'colors\' has been created')
                                codeset_colors = cs.codesets['colors']
                                if codeset_colors:
                                    if codeset_colors['enumeration']:
                                        log.debug('\n\n[unit] codespace.codeset[colors] enumeration: %s',
                                                  codeset_colors['enumeration'])

                    # Running totals.....
                    # totals summary res_modified (duplicates simply indicate 'touched' more than once during multiple passes)
                    self.assertEqual(2, len(sum_code_space_ids),   msg='pass 2: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),        msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),   msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),        msg='pass 2: sum_event_ids')
                    self.assertEqual(9,len(sum_event_type_ids),   msg='pass 2: sum_event_type_ids')
                    # totals summary of res_removed - summary of resources removed during multiple passes
                    self.assertEqual(0, len(del_sum_code_space_ids),   msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),        msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),   msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),        msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),   msg='pass 2: del_sum_event_type_ids')

                #---------------------------------------------------------------------------------
                # Pass 3.
                # asserts specifically for pass 3 of this unit test
                if pass_count == 3:
                    # what changed through action=='add' this pass...
                    self.assertEqual(1, len(code_space_ids),       msg='pass 3: res_modified code_space_ids')
                    self.assertEqual(0, len(event_ids),            msg='pass 3: res_modified event_ids')
                    self.assertEqual(0, len(event_type_ids),       msg='pass 3: res_modified event_type_ids')
                    self.assertEqual(0, len(asset_ids),            msg='pass 3: res_modified asset_ids')
                    self.assertEqual(0, len(asset_type_ids),       msg='pass 3: res_modified asset_type_ids')

                    # what changed through action=='remove' this pass...
                    self.assertEqual(0, len(rem_code_space_ids),   msg='pass 3: res_removed code_space_ids')
                    self.assertEqual(0, len(rem_event_ids),        msg='pass 3: res_removed event_ids')
                    self.assertEqual(0, len(rem_event_type_ids),   msg='pass 3: res_removed event_type_ids')
                    self.assertEqual(0, len(rem_asset_ids),        msg='pass 3: res_removed asset_ids')
                    self.assertEqual(0, len(rem_asset_type_ids),   msg='pass 3: res_removed asset_type_ids')

                    #--------------------------------------------------------------
                    # Verify detailed field changes/updated and removals
                    #--------------------------------------------------------------
                    # Verify codeset 'colors' had 'pink' removed from codeset:
                    cs = self.OMS.read_code_space(code_space_ids[0])
                    if cs:
                        if cs.codesets:
                            if 'colors' in cs.codesets:
                                log.debug('\n\n[unit]codeset \'colors\' present')
                                codeset_colors = cs.codesets['colors']
                                if codeset_colors:
                                    if codeset_colors['enumeration']:
                                        log.debug('\n\n[unit] codespace.codeset[\'colors\'] enumeration: %s',
                                                  codeset_colors['enumeration'])
                                        if 'yellow' not in codeset_colors['enumeration']:
                                            log.debug('\n\n[unit] \'yellow\' successfully removed from codeset \'colors\' enumeration')
                                        if 'green' not in codeset_colors['enumeration']:
                                            log.debug('\n\n[unit] \'green\' successfully removed from codeset \'colors\' enumeration')

                    # totals summary resources 'add'ed during multiple passes
                    self.assertEqual(3, len(sum_code_space_ids),   msg='pass 3: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),        msg='pass 3: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),   msg='pass 3: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),        msg='pass 3: sum_event_ids')
                    self.assertEqual(9,len(sum_event_type_ids),    msg='pass 3: sum_event_type_ids')

                    # totals of resources 'removed'ed during multiple passes
                    self.assertEqual(0, len(del_sum_code_space_ids),   msg='pass 3: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),        msg='pass 3: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),   msg='pass 3: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),        msg='pass 3: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),   msg='pass 3: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify asset tracking instances in system...')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            if verbose:
                log.debug('\n\n[unit] Summary of \'add\' items processed:\nNumber of passes: %d\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    pass_count, len(sum_asset_ids), len(sum_asset_type_ids),
                    len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

            if verbose:
                log.debug('\n\n[unit] Summary of \'remove\' items processed:\nNumber of passes: %d\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    pass_count, len(del_sum_asset_ids), len(del_sum_asset_type_ids),
                    len(del_sum_event_ids), len(del_sum_event_type_ids),
                    len(del_sum_code_space_ids))

            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids), msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),     msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),     msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)

            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint1B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise       # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise       # raise here to fail test case


        log.debug('\n\n***** Completed : test_upload_remove_codeset')

    # -----
    # ----- unit test: test_upload_without_codespace_instance
    # -----
    @attr('UNIT', group='sa')
    def test_upload_without_codespace_instance(self):

        # Step 1. load a single spreadsheet (test505-no-codespace.xlsx) when there is no CodeSpace instance available
        # (and no CodeSpace sheet in upload xlsx). Expect to receive this err_msg in response:
        #
        #     'err_msg': "Unable to locate CodeSpace instance named 'Marine Asset Management'"
        #
        # Sample response:
        #   {'res_removed': {'asset_types': [], 'assets': [], 'events': [], 'event_types': []},
        #   'status': 'error', 'res_modified': {'assets': [], 'asset_types': [], 'codespaces': [], 'events': [],
        #   'event_types': []}, 'err_msg': "Unable to locate CodeSpace instance named 'Marine Asset Management'"}
        #
        # Step 2. Then load CodeSpaces (only sheet in xlsx; filename: test505-codespace.xlsx).
        # Step 3. Load the xlsx which previously failed in step 1 (filename: test505-no-codespace.xlsx).
        #

        log.debug('\n\n***** Start : test_upload_without_codespace_instance')

        #self._preload_scenario("BETA")             # Used

        verbose = False
        breakpoint1A = False
        breakpoint1B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint1B = True

        # Folder(s) and files for driving test
        input_files= ['test505-no-codespace.xlsx', 'test505-codespace.xlsx', 'test505-no-codespace.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
        pass_count = 0
        try:
            sum_code_space_ids = []
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            for fid in input_files:
                pass_count += 1

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        if pass_count != 1:
                            raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:

                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces']
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']    # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']              # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_type_ids = response['res_modified']['event_types']    # ids of resources created
                            if event_type_ids:
                                sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_modified']:
                            event_ids = response['res_modified']['events']              # ids of resources created
                            if event_ids:
                                sum_event_ids.extend(event_ids)

                    if response['res_removed']:
                        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
                        if 'codespaces' in response['res_removed']:
                            rem_code_space_ids = response['res_removed']['codespaces'][:]
                            if rem_code_space_ids:
                                del_sum_code_space_ids.extend(rem_code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            rem_asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if rem_asset_type_ids:
                                del_sum_asset_type_ids.extend(rem_asset_type_ids)
                        if 'assets' in response['res_removed']:
                            rem_asset_ids = response['res_removed']['assets']               # ids of resources created
                            if rem_asset_ids:
                                del_sum_asset_ids.extend(rem_asset_ids)
                        if 'event_types' in response['res_removed']:
                            rem_event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if rem_event_type_ids:
                                del_sum_event_type_ids.extend(rem_event_type_ids)
                        if 'events' in response['res_removed']:
                            rem_event_ids = response['res_removed']['events']               # ids of resources created
                            if rem_event_ids:
                                del_sum_event_ids.extend(rem_event_ids)
                #---------------------------------------------------------------------------------
                # Pass 1.
                # pass one 'add' all resources - full load
                # asserts specifically for pass 1 of this unit test
                if pass_count == 1:
                    self.assertEqual(0, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(0, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(0, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(0, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(0, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')

                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                #---------------------------------------------------------------------------------
                # Pass 2.
                # pass two 'add' again (causing update) of 1 event resource and two (2) event types
                # asserts specifically for pass 2 of this unit test
                if pass_count == 2:

                    # What changed.........
                    # what changed through action=='add' this pass...
                    self.assertEqual(1, len(code_space_ids),       msg='pass 2: res_modified code_space_ids')
                    self.assertEqual(0, len(event_ids),            msg='pass 2: res_modified event_ids')
                    self.assertEqual(0, len(event_type_ids),       msg='pass 2: res_modified event_type_ids')
                    self.assertEqual(0, len(asset_ids),            msg='pass 2: res_modified asset_ids')
                    self.assertEqual(0, len(asset_type_ids),       msg='pass 2: res_modified asset_type_ids')
                    # what changed through action=='remove' this pass...
                    self.assertEqual(0, len(rem_code_space_ids),   msg='pass 2: res_removed code_space_ids')
                    self.assertEqual(0, len(rem_event_ids),        msg='pass 2: res_removed event_ids')
                    self.assertEqual(0, len(rem_event_type_ids),   msg='pass 2: res_removed event_type_ids')
                    self.assertEqual(0, len(rem_asset_ids),        msg='pass 2: res_removed asset_ids')
                    self.assertEqual(0, len(rem_asset_type_ids),   msg='pass 2: res_removed asset_type_ids')

                    #--------------------------------------------------------------
                    # Verify detailed field changes/updated and removals
                    #--------------------------------------------------------------
                    cs = self.OMS.read_code_space(code_space_ids[0])
                    if cs:
                        log.debug('\n\n[unit] CodeSpace loaded')

                    # Running totals.....
                    # totals summary res_modified (duplicates simply indicate 'touched' more than once during multiple passes)
                    self.assertEqual(1, len(sum_code_space_ids),   msg='pass 2: sum_code_space_ids')
                    self.assertEqual(0, len(sum_asset_ids),        msg='pass 2: sum_asset_ids')
                    self.assertEqual(0, len(sum_asset_type_ids),   msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(0, len(sum_event_ids),        msg='pass 2: sum_event_ids')
                    self.assertEqual(0, len(sum_event_type_ids),   msg='pass 2: sum_event_type_ids')
                    # totals summary of res_removed - summary of resources removed during multiple passes
                    self.assertEqual(0, len(del_sum_code_space_ids),   msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),        msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),   msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),        msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),   msg='pass 2: del_sum_event_type_ids')

                #---------------------------------------------------------------------------------
                # Pass 3.
                # pass three 'remove' codeset  (causing update) of 1 CodeSpace resource
                # asserts specifically for pass 3 of this unit test
                if pass_count == 3:
                    # what changed through action=='add' this pass...
                    self.assertEqual(1, len(code_space_ids),       msg='pass 3: res_modified code_space_ids')
                    self.assertEqual(8, len(event_ids),            msg='pass 3: res_modified event_ids')
                    self.assertEqual(9, len(event_type_ids),       msg='pass 3: res_modified event_type_ids')
                    self.assertEqual(4, len(asset_ids),            msg='pass 3: res_modified asset_ids')
                    self.assertEqual(4, len(asset_type_ids),       msg='pass 3: res_modified asset_type_ids')

                    # what changed through action=='remove' this pass...
                    self.assertEqual(0, len(rem_code_space_ids),   msg='pass 3: res_removed code_space_ids')
                    self.assertEqual(0, len(rem_event_ids),        msg='pass 3: res_removed event_ids')
                    self.assertEqual(0, len(rem_event_type_ids),   msg='pass 3: res_removed event_type_ids')
                    self.assertEqual(0, len(rem_asset_ids),        msg='pass 3: res_removed asset_ids')
                    self.assertEqual(0, len(rem_asset_type_ids),   msg='pass 3: res_removed asset_type_ids')

                    #--------------------------------------------------------------
                    # Verify detailed field changes/updated and removals
                    #--------------------------------------------------------------
                    # Verify codeset 'colors' had 'pink' removed from codeset:
                    cs = self.OMS.read_code_space(code_space_ids[0])
                    if cs:
                        if cs.codesets:
                            if 'colors' in cs.codesets:
                                log.debug('\n\n[unit]codeset \'colors\' present')
                                codeset_colors = cs.codesets['colors']
                                if codeset_colors:
                                    if codeset_colors['enumeration']:
                                        log.debug('\n\n[unit] codespace.codeset[\'colors\'] enumeration: %s',
                                                  codeset_colors['enumeration'])
                                        if 'pink' not in codeset_colors['enumeration']:
                                            log.debug('\n\n[unit] \'pink\' successfully removed from codeset \'colors\' enumeration')

                    # totals summary resources 'add'ed during multiple passes
                    self.assertEqual(2, len(sum_code_space_ids),   msg='pass 3: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),        msg='pass 3: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),   msg='pass 3: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),        msg='pass 3: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),   msg='pass 3: sum_event_type_ids')

                    # totals of resources 'removed'ed during multiple passes
                    self.assertEqual(0, len(del_sum_code_space_ids),   msg='pass 3: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),        msg='pass 3: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),   msg='pass 3: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),        msg='pass 3: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),   msg='pass 3: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify asset tracking instances in system...')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            if verbose:
                log.debug('\n\n[unit] Summary of \'add\' items processed:\nNumber of passes: %d\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    pass_count, len(sum_asset_ids), len(sum_asset_type_ids),
                    len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

            if verbose:
                log.debug('\n\n[unit] Summary of \'remove\' items processed:\nNumber of passes: %d\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    pass_count, len(del_sum_asset_ids), len(del_sum_asset_type_ids),
                    len(del_sum_event_ids), len(del_sum_event_type_ids),
                    len(del_sum_code_space_ids))

            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids), msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),     msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),     msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1
            log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint1B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise       # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise       # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_without_codespace_instance')

    # -----
    # ----- unit test: test_upload_xls_triple_assets
    # -----
    @attr('UNIT', group='sa')
    def test_upload_xls_triple_assets(self):

        # test service declare_asset_tracking_resources by calling multiple (3) times to exercise create, update
        # and remove functionality in service - specifically for CodeSpaces, Codes, CodeSets, Events, EventTypes,
        # Event attribute specs and event attributes. 'remove' functionality tested for Codes.
        #
        # Scenario: OOI loaded with AssetTypes available to all Orgs, then Org A
        # loads xlsx for their Assets using AssetTypes available in system; Org B uses xlsx to load their
        # AssetTypes and Assets, maybe reusing existing AssetTypes etc. Consider delete also, since an AssetType
        # available to all Orgs and used by more than zero Orgs when deleted will affect Orgs which use it.
        # Thnk about how to handle deletion of types and instances.
        #
        # This unit test three different xlsx spreadsheets to accomplish the following:
        # load 1 - load all sheets, including (test505.xslx)
        #   CodeSpaces, Codes, CodeSets, Assets, Events, AssetTypes EventTypes, Attribute Specs and Attributes
        #
        # load 2 'add' assets, asset types etc (test505-assets.xslx); also modify xxxx
        #   (loads sheets CodeSpaces, Codes, CodeSets, AssetTypes, Assets, AssetAttributeSpecs, AssetAttributes)
        #
        # load 3 'remove' AssetType 'Device', Asset 'Iridium Sim card'; (test-505-rm-assets.xlsx)
        # modify Platform ('Pioneer 1 Platform') attributes s_name, op_stat (to 'not functioning'), exp_date for update
        # (test505-rm-assets.xslx; sheets AssetTypes, Assets, AssetAttributeSpecs, AssetAttributes)
        #
        # Removing TypeResources - (rule) a request to remove a type resource will only be honored if the TypeResource
        # is not engaged in an association with another TypeResource as the object of the extends.  (TODO DISCUSS)
        #
        # sample response - third pass:
        # {
        #  'status': 'ok',
        #  'err_msg': '',
        #  'res_modified': {
        #       'assets': ['f217b77115194a88bb73b13128def639', '8c770046dd1046478b8630875d63be2a', '203a82050943455b96feca6c96c14239'],
        #       'asset_types': ['8018654cecf54dadbc786d5b77974088', 'b845ed83a5bd42ad977ac10d849f2d7e', '276830c9267b4ec0a4a14121845cd746'],
        #       'codespaces': [],
        #       'events': [],
        #       'event_types': []
        #                   },
        #  'res_removed': {
        #       'asset_types': ['4d82d3ccf0c84c11bad9c6d2c028b378'],
        #       'assets': ['2b0b8781e45c4d8ea449de790c6290d0'],
        #       'events': [],
        #       'event_types': []
        #                   }
        # }
        #
        #
        # TODO detailed asserts etc.

        log.debug('\n\n***** Start : test_upload_xls_triple_assets')

        self._preload_scenario("BETA")             # Used

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint3A = False
        breakpoint3B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint3A = True
            breakpoint3B = True

        # Input and folder(s) and files for driving test
        input_files = ['test505.xlsx', 'test505-assets.xlsx', 'test505-rm-assets.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
        pass_count = 0
        try:
            sum_code_space_ids = []
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            for fid in input_files:
                pass_count += 1

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - first pass: %s', response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('[unit] Error: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']    # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']              # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_type_ids = response['res_modified']['event_types']    # ids of resources created
                            if event_type_ids:
                                sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_modified']:
                            event_ids = response['res_modified']['events']              # ids of resources created
                            if event_ids:
                                sum_event_ids.extend(event_ids)

                    if response['res_removed']:
                        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
                        if 'codespaces' in response['res_removed']:
                            rem_code_space_ids = response['res_removed']['codespaces'][:]
                            if rem_code_space_ids:
                                del_sum_code_space_ids.extend(rem_code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            rem_asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if rem_asset_type_ids:
                                del_sum_asset_type_ids.extend(rem_asset_type_ids)
                        if 'assets' in response['res_removed']:
                            rem_asset_ids = response['res_removed']['assets']               # ids of resources created
                            if rem_asset_ids:
                                del_sum_asset_ids.extend(rem_asset_ids)
                        if 'event_types' in response['res_removed']:
                            rem_event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if rem_event_type_ids:
                                del_sum_event_type_ids.extend(rem_event_type_ids)
                        if 'events' in response['res_removed']:
                            rem_event_ids = response['res_removed']['events']               # ids of resources created
                            if rem_event_ids:
                                del_sum_event_ids.extend(rem_event_ids)
                #---------------------------------------------------------------------------------
                # Pass 1.
                # pass one 'add' all resources - full load
                # asserts specifically for pass 1 of this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')

                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify asset tracking instances in system...')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            if verbose:
                log.debug('\n\n rm_code_space_ids: %s', rm_code_space_ids)
                log.debug('\n\n rm_asset_ids: %s',      rm_asset_ids)
                log.debug('\n\n rm_asset_type_ids: %s', rm_asset_type_ids)
                log.debug('\n\n rm_event_ids: %s',      rm_event_ids)
                log.debug('\n\n rm_event_type_ids: %s', rm_event_type_ids)

            if rm_asset_type_ids:
                if verbose: log.debug('\n\n[unit] cleanup...asset_types...')
                total_resources_to_delete += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                if verbose: log.debug('\n\n[unit] cleanup...event_duration_types...')
                total_resources_to_delete += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                if verbose: log.debug('\n\n[unit] cleanup...assets...')
                total_resources_to_delete += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                if verbose: log.debug('\n\n[unit] cleanup...event durations...')
                total_resources_to_delete += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                if verbose: log.debug('\n\n[unit] cleanup...code_space_ids...%d', len(rm_code_space_ids))
                inx = 0
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if breakpoint3B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception file(%s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_xls_triple_assets')

    # -----
    # ----- unit test: test_upload_xls_triple_assets
    # -----
    @attr('UNIT', group='sa')
    def test_upload_xls_triple_events(self):

        # test service declare_asset_tracking_resources by calling three times to exercise create and update
        # functionality in service - specifically for CodeSpaces, Codes, CodeSets, Events, EventTypes,
        # Event attribute specs and event attributes. 'remove functionality tested for Codes.
        #
        # Scenario: OOI loaded with AssetTypes available to all Orgs, then Org A
        # loads xlsx for their Assets using AssetTypes available in system; Org B uses xlsx to load their
        # AssetTypes and Assets, maybe reusing existing AssetTypes etc. Consider delete also, since an AssetType
        # available to all Orgs and used by more than zero Orgs when deleted will affect Orgs which use it.
        # Think about how to handle deletion of types and instances.
        #
        # This unit test three different xlsx spreadsheets to accomplish the following:
        # load 1 - load all sheets, including (test505.xlsx)
        #   CodeSpaces, Codes, CodeSets, Assets, Events, AssetTypes EventTypes, Attribute Specs and Attributes
        #
        # load 2 - 'add' events, event types etc (test505-events.xlsx); also modify Base and RTM type description values;
        # also ReturnToManufacturer event description.
        #   (loads sheets CodeSpaces, Codes, CodeSets, AssetTypes, Assets, AssetAttributeSpecs, AssetAttributes)
        #
        # load 3 - 'remove' EventDurationType 'Calibration', EventDuration 'Calibration';
        # modify 'ReturnToManufacturer' attributes 'event description', 'recording operator name' for update
        # (test505-rm-events.xlsx; sheets EventTypes, Events, EventAttributeSpecs, EventAttributes)
        #
        # Removing TypeResources - (rule) a request to remove a type resource will only be honored if the TypeResource
        # is not engaged in an association with another TypeResource as the object of the extends.  (TODO DISCUSS)
        #
        # sample response - third pass:
        #
        #   {
        #       'status': 'ok',
        #       'err_msg': '',
        #       'res_modified': {
        #                       'assets': [],
        #                       'asset_types': [],
        #                       'codespaces': [],
        #                       'events': ['cc55432061804356973179bb3ebb9042'],
        #                       'event_types': ['cb17142f5fdc4a1a998e1a3ad63f112e', '5e19093651b048e1ac386a7cf451737f']
        #                       },
        #       'res_removed': {
        #                       'asset_types': [],
        #                       'assets': [],
        #                       'events': ['d036d8b63474405e9e043439e4b9b817'],
        #                       'event_types': ['fbcd45361e0447498fabc0ecb20d5486']
        #                       }
        #   }
        #
        #
        # TODO asserts for each pass, etc.

        log.debug('\n\n***** Start : test_upload_xls_triple_events')

        #self._preload_scenario("BETA")

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint3A = False
        breakpoint3B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint3A = True
            breakpoint3B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505-events.xlsx', 'test505-rm-events.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
        rem_code_space_ids = rem_asset_type_ids = rem_asset_ids = rem_event_type_ids = rem_event_ids = []
        pass_count = 0
        try:
            sum_code_space_ids = []
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            for fid in input_files:

                pass_count += 1

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - first pass: %s', response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('[unit] Error: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = []
                            asset_type_ids = response['res_modified']['asset_types']    # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']              # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_type_ids = response['res_modified']['event_types']    # ids of resources created
                            if event_type_ids:
                                sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_modified']:
                            event_ids = response['res_modified']['events']              # ids of resources created
                            if event_ids:
                                sum_event_ids.extend(event_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                #---------------------------------------------------------------------------------
                # Pass 1.
                # pass one 'add' all resources - full load
                # asserts specifically for pass 1 of this unit test
                if pass_count == 1:
                    log.debug('\n\n[unit] sum_code_space_ids: %d', len(sum_code_space_ids))
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')

                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')
                #---------------------------------------------------------------------------------
                # Pass 2.
                # pass one 'add' all resources - full load
                # asserts specifically for pass 1 of this unit test
                # set breakpoint for testing...
                # todo detailed asserts

                #---------------------------------------------------------------------------------
                # Pass 3.
                # pass one 'add' all resources - full load
                # asserts specifically for pass 1 of this unit test
                # todo detailed asserts

                if breakpoint1A:
                    log.debug('\n\n[unit] verify asset tracking instances in system...')
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # cleanup
            total_resources_to_delete = 0
            sum_code_space_ids = list(set(sum_code_space_ids))
            sum_asset_ids = list(set(sum_asset_ids))
            sum_asset_type_ids = list(set(sum_asset_type_ids))
            sum_event_ids = list(set(sum_event_ids))
            sum_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(sum_code_space_ids) + len(sum_asset_ids) + len(sum_asset_type_ids) + \
                                        len(sum_event_ids) + len(sum_event_type_ids)

            #log.debug('\n\n[unit] total number of resources (\'add\') to delete: %d', total_resources_to_delete)

            if verbose:
                log.debug('\n\n[unit] Summary of items processed (\'remove\'):\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(del_sum_asset_ids), len(del_sum_asset_type_ids), len(del_sum_event_ids), len(del_sum_event_type_ids),
                    len(del_sum_code_space_ids))

            del_sum_code_space_ids = list(set(del_sum_code_space_ids))
            del_sum_asset_ids = list(set(del_sum_asset_ids))
            del_sum_asset_type_ids = list(set(del_sum_asset_type_ids))
            del_sum_event_ids = list(set(del_sum_event_ids))
            del_sum_event_type_ids = list(set(del_sum_event_type_ids))

            if verbose:
                log.debug('\n\n[unit] Summary of items removed (\'remove\'):\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(del_sum_asset_ids), len(del_sum_asset_type_ids), len(del_sum_event_ids), len(del_sum_event_type_ids),
                    len(del_sum_code_space_ids))


            rm_code_space_ids = list(set(sum_code_space_ids)     - set(del_sum_code_space_ids))
            rm_asset_ids =      list(set(sum_asset_ids)          - set(del_sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids)     - set(del_sum_asset_type_ids))
            rm_event_ids =      list(set(sum_event_ids)          - set(del_sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids)     - set(del_sum_event_type_ids))

            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total resources to delete: %d', total_resources_to_delete)

            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1
            cnt = 1
            if rm_event_type_ids:
                if verbose: log.debug('\n\n[unit] cleanup...event_duration_types...')
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
                    cnt += 1

            log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint3B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_xls_triple_events')

    # -----
    # ----- unit test: test_upload_twice
    # -----
    @attr('UNIT', group='sa')
    def test_upload_twice(self):

        # ('charlton' test) It is important UI developers can reload same xlsx without having to
        # reload container. And of course, is a possible (but unlikely) scenario for system engineers.
        # Step 1. load a single spreadsheet with all sheets (test505.xlsx) when there is no CodeSpace instance available
        # Step 2. load (again) same spread sheet

        log.debug('\n\n***** Start : test_upload_twice')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two
                # asserts specifically for this unit test
                if pass_count == 2:

                    picklist = self.OMS.get_assets_picklist(id_only='False')
                    log.debug('\n\n[unit] assets - picklist(%d): %s', len(picklist), picklist)

                    self.assertEqual(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_twice')

    # -----
    # ----- unit test: test_upload_just_attributes
    # -----
    @unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_upload_just_attributes(self):

        # ('charlton' test) It is important UI developers can reload same xlsx without having to
        # reload container. And of course, is a possible (but unlikely) scenario for system engineers.
        # Step 1. load a single spreadsheet with all sheets (test505.xlsx) when there is no CodeSpace instance available
        # Step 2. load (again) same spread sheet

        log.debug('\n\n***** Start : test_upload_just_attributes')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test500-just-attributes.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two
                # asserts specifically for this unit test
                if pass_count == 2:

                    picklist = self.OMS.get_assets_picklist(id_only='False')
                    log.debug('\n\n[unit] assets - picklist(%d): %s', len(picklist), picklist)

                    self.assertEqual(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_just_attributes')

    # -----
    # ----- unit test: test_add_new_asset_type
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_add_new_asset_type(self):

        # Create a new asset type instance by providing two (2) sheets: AssetTypes and AssetAttributeSpecs
        # Step 1. load a single spreadsheet with all sheets (test505.xlsx)
        # Step 2. load spread sheet with single asset type and corresponding attribute specification - with base type
        # defined in the spread sheet (test505-add-new-asset-type-1.xlsx)

        log.debug('\n\n***** Start : test_add_new_asset_type')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505-add-new-asset-type-1.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xlsx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(5, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(5, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_add_new_asset_type')

    # -----
    # ----- unit test: test_add_new_asset_type
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_add_new_asset_type_wo_base(self):

        # Create a new asset type instance by providing two (2) sheets: AssetTypes and AssetAttributeSpecs
        # Step 1. load a single spreadsheet with all sheets (test505.xlsx)
        # Step 2. load spread sheet with single asset type and corresponding attribute specification - without base type
        # being defined in the spread sheet (which the new asset type extends) (test505-add-new-asset-type-2.xlsx)
        # # (create new asset type through extend of root type (Base); concrete == False for 'Base'
        log.debug('\n\n***** Start : test_add_new_asset_type_wo_base')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505-add-new-asset-type-2.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(5, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(5, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_add_new_asset_type_wo_base')

    # -----
    # ----- unit test: test_add_new_asset_type_extend_from_device
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_add_new_asset_type_extend_from_device(self):

        # Create a new asset type instance by providing two (2) sheets: AssetTypes and AssetAttributeSpecs
        # Step 1. load a single spreadsheet with all sheets (test505.xlsx)
        # Step 2. load spread sheet with single asset type and corresponding attribute specification - without base type
        # being defined in the spread sheet (which the new asset type extends) (test505-add-new-asset-type-3.xlsx)
        # (create new asset type through extend of type resource which is not the root type resource; concrete == True (Device)
        log.debug('\n\n***** Start : test_add_new_asset_type_extend_from_device')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505-asset-only.xlsx', 'test505-add-new-asset-type-3.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(0, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(0, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    #self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    #self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(5, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(0, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(0, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    #self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    #self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0

            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(5, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(0, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(0, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(10, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_add_new_asset_type_extend_from_device')

    # -----
    # ----- unit test: test_add_new_asset_type_extend_from_platform
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_add_new_asset_type_extend_from_platform(self):

        # Create a new asset type instance by providing two (2) sheets: AssetTypes and AssetAttributeSpecs
        # Step 1. load a single spreadsheet with all sheets (test505.xlsx)
        # Step 2. load spread sheet with single asset type and corresponding attribute specification - without base type
        # being defined in the spread sheet (which the new asset type extends) (test505-add-new-asset-type-4.xlsx)
        # (create new asset type through extend of out leaf type resource and not the root type resource; concrete == True (Platform)
        log.debug('\n\n***** Start : test_add_new_asset_type_extend_from_platform')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505-asset-only.xlsx', 'test505-add-new-asset-type-4.xlsx']
        current_file = ''

        sum_code_space_ids = sum_asset_type_ids = sum_asset_ids = sum_event_ids = sum_event_type_ids = []
        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(0, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(0, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    #self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    #self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(5, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(0, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(0, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    #self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    #self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(5, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(0, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(0, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(10, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_add_new_asset_type_extend_from_platform')

    # -----
    # ----- unit test: test_add_new_asset_type
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_add_new_event_type(self):

        # Create a new event duration type instance by providing two (2) sheets: EventTypes and EventAttributeSpecs
        # Step 1. load a single spreadsheet with all sheets (test505.xlsx)
        # Step 2. load spread sheet with single asset type and base type  (which the new asset type extends)
        # corresponding attribute specification (extends Base) (test505-add-new-event-type-1.xlsx)

        log.debug('\n\n***** Start : test_add_new_event_type')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505-add-new-event-type-1.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(10, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0

            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)

            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(10, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_add_new_event_type')

    # -----
    # ----- unit test: test_add_new_event_type_wo_base
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_add_new_event_type_wo_base(self):

        # Create a new event duration type instance by providing two (2) sheets: EventTypes and EventAttributeSpecs
        # Step 1. load a single spreadsheet with all sheets (test505.xlsx)
        # Step 2. load spread sheet with single asset type and base type  (which the new asset type extends)
        # corresponding attribute specification (extends Base) (test505-add-new-event-type-2.xlsx)

        log.debug('\n\n***** Start : test_add_new_event_type_wo_base')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505-add-new-event-type-2.xlsx']
        current_file = ''

        sum_code_space_ids = sum_asset_type_ids = sum_asset_ids = sum_event_ids = sum_event_type_ids = []
        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(10, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(10, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_add_new_event_type_wo_base')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Asset, AssetType and Attribute Tests (START)
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    # -----
    # ----- unit test: test_new_asset_base
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_new_asset_base(self):

        # Create a new asset ('NewAsset') - no attributes and extends base
        # Step 1. load a single spreadsheet with all sheets (test500.xlsx)
        # Step 2. load spread sheet with single asset type and base type  (which the new asset type extends)
        # corresponding attribute specification (extends Base) (test500-add-new-asset-base.xlsx)

        log.debug('\n\n***** Start : test_new_asset_base')

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test500-add-new-asset-base.xlsx']
        current_file = ''

        sum_code_space_ids = sum_asset_type_ids = sum_asset_ids = sum_event_ids = sum_event_type_ids = []
        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_one_asset_ids = response['res_modified']['assets'][:]

                    log.debug('\n\n[unit] Pass %d - pass_one_asset_ids: %s', pass_count, pass_one_asset_ids)

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(5, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_two_asset_ids = response['res_modified']['assets'][:]

                    new_asset_ids = set(pass_two_asset_ids) - set(pass_one_asset_ids)
                    list_new_asset_ids = list(new_asset_ids)
                    self.assertEqual(1, len(list_new_asset_ids), msg='one new asset added in pass two (NewAsset)')
                    asset_id = list_new_asset_ids[0]
                    asset_obj = self.OMS.read_asset(asset_id)
                    attributes = asset_obj.asset_attrs
                    associations = self.container.resource_registry.find_associations(subject=asset_id,
                                                                predicate=PRED.implementsAssetType,id_only=False)
                    self.assertEqual(1, len(associations), msg='one and only one associated type resource')
                    asset_type_id = ''
                    asset_type_id = associations[0].o
                    asset_type_obj = self.OMS.read_asset_type(asset_type_id)
                    base_names = asset_type_obj.attribute_specifications.keys()
                    attribute_keys = attributes.keys()
                    self.assertEqual(len(base_names), len(attributes), msg='number of attributes should equal len base attributes')
                    # verify base attribute specification names are each in newly created NewAsset attributes
                    for name in base_names:
                        if name not in attribute_keys:
                            raise BadRequest('all attribute names in NewAsset must match Base type resource names')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(5, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_new_asset_base')

    # ----- unit test: test_new_asset_base_attributes
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_new_asset_base_attributes(self):

        # Create a new asset ('NewAsset') - provide attributes and extends base
        # Step 1. load a single spreadsheet with all sheets (test500.xlsx)
        # Step 2. load spread sheet with two assets - do not provide attributes for 'NewAsset'; expect defaults
        # corresponding attribute specification (extends Base) (test500-add-new-asset-base-attributes.xlsx)

        log.debug('\n\n***** Start : test_new_asset_base_attributes')

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test500-add-new-asset-base-attributes.xlsx']
        current_file = ''

        sum_code_space_ids = sum_asset_type_ids = sum_asset_ids = sum_event_ids = sum_event_type_ids = []
        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_one_asset_ids = response['res_modified']['assets'][:]

                    log.debug('\n\n[unit] Pass %d - pass_one_asset_ids: %s', pass_count, pass_one_asset_ids)

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(5, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_two_asset_ids = response['res_modified']['assets'][:]

                    new_asset_ids = set(pass_two_asset_ids) - set(pass_one_asset_ids)
                    list_new_asset_ids = list(new_asset_ids)
                    self.assertEqual(1, len(list_new_asset_ids), msg='one new asset added in pass two (NewAsset)')
                    asset_id = list_new_asset_ids[0]
                    asset_obj = self.OMS.read_asset(asset_id)
                    attributes = asset_obj.asset_attrs
                    associations = self.container.resource_registry.find_associations(subject=asset_id,
                                                                predicate=PRED.implementsAssetType,id_only=False)
                    self.assertEqual(1, len(associations), msg='one and only one associated type resource')
                    asset_type_id = ''
                    asset_type_id = associations[0].o
                    asset_type_obj = self.OMS.read_asset_type(asset_type_id)
                    base_names = asset_type_obj.attribute_specifications.keys()
                    attribute_keys = attributes.keys()
                    self.assertEqual(len(base_names), len(attributes), msg='number of attributes should equal len base attributes')
                    # verify base attribute specification names are each in newly created NewAsset attributes
                    for name in base_names:
                        if name not in attribute_keys:
                            raise BadRequest('all attribute names in NewAsset must match Base type resource names')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(5, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_new_asset_base_attributes')

    # ----- unit test: test_new_asset_base_attributes_short
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_new_asset_base_attributes_short(self):

        # Create a new asset ('NewAsset') - provide attributes and extends base
        # Step 1. load a single spreadsheet with all sheets (test500.xlsx)
        # Step 2. load spread sheet with two assets - provide attributes for 'NewAsset' except do not provide 'description' value;
        # expect defaults
        # corresponding attribute specification (extends Base) (test500-add-new-asset-base.xlsx)

        log.debug('\n\n***** Start : test_new_asset_base_attributes_short')

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test500-add-new-asset-base-attributes-short.xlsx']
        current_file = ''

        sum_code_space_ids = sum_asset_type_ids = sum_asset_ids = sum_event_ids = sum_event_type_ids = []
        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_one_asset_ids = response['res_modified']['assets'][:]

                    log.debug('\n\n[unit] Pass %d - pass_one_asset_ids: %s', pass_count, pass_one_asset_ids)

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(5, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_two_asset_ids = response['res_modified']['assets'][:]

                    new_asset_ids = set(pass_two_asset_ids) - set(pass_one_asset_ids)
                    list_new_asset_ids = list(new_asset_ids)
                    self.assertEqual(1, len(list_new_asset_ids), msg='one new asset added in pass two (NewAsset)')
                    asset_id = list_new_asset_ids[0]
                    asset_obj = self.OMS.read_asset(asset_id)
                    attributes = asset_obj.asset_attrs

                    associations = self.container.resource_registry.find_associations(subject=asset_id,
                                                                predicate=PRED.implementsAssetType,id_only=False)
                    self.assertEqual(1, len(associations), msg='one and only one associated type resource')
                    asset_type_id = ''
                    asset_type_id = associations[0].o
                    asset_type_obj = self.OMS.read_asset_type(asset_type_id)
                    base_names = asset_type_obj.attribute_specifications.keys()
                    attribute_keys = attributes.keys()
                    self.assertEqual(len(base_names), len(attributes), msg='number of attributes should equal len base attributes')

                    # verify base attribute specification names are each in newly created NewAsset attributes
                    for name in base_names:
                        if name not in attribute_keys:
                            raise BadRequest('all attribute names in NewAsset must match Base type resource names')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(5, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_new_asset_base_attributes_short')

    # ----- unit test: test_new_asset_base_attributes_short_update
    # -----
    unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_new_asset_base_attributes_short_update(self):

        # Create a new asset ('NewAsset') - provide attributes and extends base
        # Step 1. load a single spreadsheet with all sheets (test500.xlsx)
        # Step 2. load spread sheet with two assets - provide attributes for 'NewAsset' provide 'descr' value;
        # Step 3. load spread sheet with one asset  - provide attributes for 'NewAsset' except do not provide 'descr' value;
        # Verify the default value is NOT used but the current descr value (provided in Step 2) is retained.
        #

        log.debug('\n\n***** Start : test_new_asset_base_attributes_short_update')

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test500-add-new-asset-base-attributes.xlsx', 'test500-add-new-asset-base-attributes-short.xlsx']
        current_file = ''

        sum_code_space_ids = sum_asset_type_ids = sum_asset_ids = sum_event_ids = sum_event_type_ids = []
        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []
        save_asset_id = ''
        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')
                    pass_one_asset_ids = ''
                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_one_asset_ids = response['res_modified']['assets'][:]

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(5, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                    pass_two_asset_ids = ''
                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_two_asset_ids = response['res_modified']['assets'][:]

                    new_asset_ids = set(pass_two_asset_ids) - set(pass_one_asset_ids)
                    list_new_asset_ids = list(new_asset_ids)
                    self.assertEqual(1, len(list_new_asset_ids), msg='one new asset added in pass two (NewAsset)')
                    asset_id = list_new_asset_ids[0]
                    save_asset_id = asset_id
                    if verbose: log.debug('\n\n[unit] Pass %d: asset_id: %s', pass_count, asset_id)
                    asset_obj = self.OMS.read_asset(asset_id)
                    attributes = asset_obj.asset_attrs
                    if 'descr' in attributes:
                        value = attributes['descr']
                        if verbose: log.debug('\n\n[unit] Pass %d: value of \'descr\' attribute: %s', pass_count, value)

                    associations = self.container.resource_registry.find_associations(subject=asset_id,
                                                                predicate=PRED.implementsAssetType,id_only=False)
                    self.assertEqual(1, len(associations), msg='one and only one associated type resource')
                    asset_type_id = ''
                    asset_type_id = associations[0].o
                    asset_type_obj = self.OMS.read_asset_type(asset_type_id)
                    base_names = asset_type_obj.attribute_specifications.keys()
                    attribute_keys = attributes.keys()
                    self.assertEqual(len(base_names), len(attributes), msg='number of attributes should equal len base attributes')
                    # verify base attribute specification names are each in newly created NewAsset attributes
                    for name in base_names:
                        if name not in attribute_keys:
                            raise BadRequest('all attribute names in NewAsset must match Base type resource names')

                # pass three - asserts specifically for this unit test
                if pass_count == 3:
                    if verbose: log.debug('\n\n[unit] Pass %d Description: Review contents of asset \'NewAsset\' attribute \'descr\' ' +
                              'and determine it matches value from previous pass', pass_count)

                    if verbose: log.debug('\n\n[unit] Pass %d: asset_id: %s', pass_count, save_asset_id)
                    asset_obj2 = self.OMS.read_asset(save_asset_id)
                    attributes2 = asset_obj2.asset_attrs
                    self.assertEqual(len(base_names), len(attributes2), msg='number of attributes should equal len base attributes')

                    if 'descr' in attributes2:
                        value = attributes2['descr']
                        if verbose: log.debug('\n\n[unit] Pass %d: value of \'descr\' attribute: %s', pass_count, value)

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(5, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_new_asset_base_attributes_short_update')

    # ----- unit test: test_add_new_asset_base_one_attribute_update
    # -----
    #unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_new_asset_base_one_attribute_update(self):

        # Create a new asset ('NewAsset') - provide attributes and extends base
        # Step 1. load a single spreadsheet with all sheets (test500.xlsx)
        # Step 2. load spread sheet with two assets - provide attributes for 'NewAsset' provide 'descr' value
        # Step 3. load spread sheet with one asset  - provide one attribute for 'NewAsset', the 'descr' value;
        # Verify the values provided in Step 2 are used for everything except 'descr' which will have a
        # new value (provided in Step 2) is retained.

        log.debug('\n\n***** Start : test_new_asset_base_one_attribute_update')

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test500-add-new-asset-base-attributes.xlsx', 'test500-add-new-asset-base-one_attribute.xlsx']
        current_file = ''

        sum_code_space_ids, sum_asset_type_ids, sum_asset_ids, sum_event_ids,sum_event_type_ids = [], [], [], [], []
        del_sum_code_space_ids, del_sum_asset_type_ids, del_sum_asset_ids, del_sum_event_ids, del_sum_event_type_ids = [], [], [], [], []
        rm_code_space_ids, rm_asset_type_ids, rm_asset_ids, rm_event_ids, rm_event_type_ids = [], [], [], [], []
        save_asset_id = ''
        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []

            code_space_ids = []
            pass_count = 1

            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids, asset_type_ids, asset_ids, event_type_ids, event_ids = [], [], [], [], []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids, asset_type_ids, asset_ids, event_type_ids, event_ids = [], [], [], [], []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_one_asset_ids = response['res_modified']['assets'][:]

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(5, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                    pass_two_asset_ids = ''
                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_two_asset_ids = response['res_modified']['assets'][:]

                    # difference between passes should provide single asset id for 'NewAsset'
                    new_asset_ids = set(pass_two_asset_ids) - set(pass_one_asset_ids)
                    list_new_asset_ids = list(new_asset_ids)
                    self.assertEqual(1, len(list_new_asset_ids), msg='one new asset added in pass two (NewAsset)')
                    asset_id = list_new_asset_ids[0]
                    save_asset_id = asset_id
                    if verbose:  log.debug('\n\n[unit] Pass %d: asset_id: %s', pass_count, asset_id)
                    asset_obj = self.OMS.read_asset(asset_id)
                    attributes = asset_obj.asset_attrs
                    if 'descr' in attributes:
                        value = attributes['descr']
                        if verbose:  log.debug('\n\n[unit] Pass %d: value of \'descr\' attribute: %s', pass_count, value)

                    associations = self.container.resource_registry.find_associations(subject=asset_id,
                                                                predicate=PRED.implementsAssetType,id_only=False)
                    self.assertEqual(1, len(associations), msg='one and only one associated type resource')

                    asset_type_id = ''
                    asset_type_id = associations[0].o
                    asset_type_obj = self.OMS.read_asset_type(asset_type_id)
                    base_names = asset_type_obj.attribute_specifications.keys()
                    attribute_keys = attributes.keys()
                    self.assertEqual(len(base_names), len(attributes), msg='number of attributes should equal len base attributes')
                    #log.debug('\n\n[unit] base_names(%d): %s', len(base_names), base_names)
                    # verify base attribute specification names are each in newly created NewAsset attributes
                    for name in base_names:
                        if name not in attribute_keys:
                            raise BadRequest('all attribute names in NewAsset must match Base type resource names')

                # pass three
                # asserts specifically for this unit test
                if pass_count == 3:
                    if verbose: log.debug('\n\n[unit] Pass %d Description: Review contents of asset \'NewAsset\' (other than attribute \'descr\') ' +
                              'and determine each matches value from previous pass', pass_count)

                    if verbose: log.debug('\n\n[unit] Pass %d: asset_id: %s', pass_count, save_asset_id)
                    asset_obj2 = self.OMS.read_asset(save_asset_id)
                    attributes2 = asset_obj2.asset_attrs
                    self.assertEqual(len(attribute_keys), len(attributes2), msg='number of attributes should equal len base attributes')

                    if 'descr' in attributes2:
                        value1 = attributes['descr']
                        value2 = attributes2['descr']
                        self.assertNotEqual(value1, value2, msg='descr changed, values should differ')
                        if verbose: log.debug('\n\n[unit] Pass %d: value of \'descr\' attribute: %s', (pass_count-1), value1)
                        if verbose: log.debug('\n\n[unit] Pass %d: value of \'descr\' attribute: %s', pass_count, value2)

                    for name in attributes:
                        value1 = attributes[name]
                        value2 = attributes2[name]
                        if name != 'descr':
                            self.assertEqual(value1, value2, msg='update should not change existing values unless value explicitly provided')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0

            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(5, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_new_asset_base_one_attribute_update')

    # ----- unit test: test_new_asset_base_one_attribute_no_types
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_new_asset_base_one_attribute_no_types(self):

        # Create a new asset ('NewAsset') - provide attributes and extends base
        # Step 1. load a single spreadsheet with all sheets (test500.xlsx)
        # Step 2. load spread sheet with one asset - provide single 'descr' attribute value for 'NewAsset';
        #           expect defaults for all attributes other than 'descr'
        # NOTE: still have attributespecifications sheet!!

        log.debug('\n\n***** Start : test_new_asset_base_one_attribute_no_types')

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test500-new-asset-base-one-attribute-no-types.xlsx']
        current_file = ''

        sum_code_space_ids = sum_asset_type_ids = sum_asset_ids = sum_event_ids = sum_event_type_ids = []
        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_one_asset_ids = response['res_modified']['assets'][:]

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(5, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_two_asset_ids = response['res_modified']['assets'][:]

                    new_asset_ids = set(pass_two_asset_ids) - set(pass_one_asset_ids)
                    list_new_asset_ids = list(new_asset_ids)
                    self.assertEqual(1, len(list_new_asset_ids), msg='one new asset added in pass two (NewAsset)')
                    asset_id = list_new_asset_ids[0]
                    asset_obj = self.OMS.read_asset(asset_id)
                    attributes = asset_obj.asset_attrs
                    associations = self.container.resource_registry.find_associations(subject=asset_id,
                                                                predicate=PRED.implementsAssetType,id_only=False)
                    #log.debug('\n\n[unit] associations: %s', associations)
                    self.assertEqual(1, len(associations), msg='one and only one associated type resource')
                    asset_type_id = ''
                    asset_type_id = associations[0].o
                    #log.debug('\n\n[unit] associations: %s', asset_type_id)
                    asset_type_obj = self.OMS.read_asset_type(asset_type_id)

                    #log.debug('\n\n[unit] asset_type_obj.attribute_specifications (%d): ', len(asset_type_obj.attribute_specifications))
                    base_names = asset_type_obj.attribute_specifications.keys()
                    attribute_keys = attributes.keys()
                    self.assertEqual(len(base_names), len(attributes), msg='number of attributes should equal len base attributes')
                    #log.debug('\n\n[unit] base_names(%d): %s', len(base_names), base_names)
                    # verify base attribute specification names are each in newly created NewAsset attributes
                    for name in base_names:
                        if name not in attribute_keys:
                            raise BadRequest('all attribute names in NewAsset must match Base type resource names')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(5, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_new_asset_base_one_attribute_no_types')

    # ----- unit test: test_new_asset_base_one_attribute_only
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_new_asset_base_one_attribute_only(self):

        # Create a new asset ('NewAsset') - provide attributes and extends base
        # Step 1. load a single spreadsheet with all sheets (test500.xlsx)
        # Step 2. load spread sheet with (two sheets - Assets, AssetAttributes) providing
        #           one asset ('NewAsset') and single 'descr' attribute value for 'NewAsset';
        #           expect defaults for all attributes other than 'descr'
        # NOTE: No attribute specifications sheet!!


        log.debug('\n\n***** Start : test_new_asset_base_one_attribute_only')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test500-new-asset-base-one-attribute-only.xlsx']
        current_file = ''

        sum_code_space_ids = sum_asset_type_ids = sum_asset_ids = sum_event_ids = sum_event_type_ids = []
        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)

                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)

                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_one_asset_ids = response['res_modified']['assets'][:]

                    log.debug('\n\n[unit] Pass %d - pass_one_asset_ids: %s', pass_count, pass_one_asset_ids)

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    #log.debug('\n\n[service] number of unique asset type ids: %d', len(list(set(sum_asset_type_ids))))
                    self.assertEqual(5, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                    if response['res_modified']:
                        if 'assets' in response['res_modified']:
                            pass_two_asset_ids = response['res_modified']['assets'][:]

                    new_asset_ids = set(pass_two_asset_ids) - set(pass_one_asset_ids)
                    list_new_asset_ids = list(new_asset_ids)
                    self.assertEqual(1, len(list_new_asset_ids), msg='one new asset added in pass two (NewAsset)')
                    asset_id = list_new_asset_ids[0]
                    asset_obj = self.OMS.read_asset(asset_id)
                    attributes = asset_obj.asset_attrs
                    associations = self.container.resource_registry.find_associations(subject=asset_id,
                                                                predicate=PRED.implementsAssetType,id_only=False)
                    #log.debug('\n\n[unit] associations: %s', associations)
                    self.assertEqual(1, len(associations), msg='one and only one associated type resource')
                    asset_type_id = ''
                    asset_type_id = associations[0].o
                    #log.debug('\n\n[unit] associations: %s', asset_type_id)
                    asset_type_obj = self.OMS.read_asset_type(asset_type_id)

                    #log.debug('\n\n[unit] asset_type_obj.attribute_specifications (%d): ', len(asset_type_obj.attribute_specifications))
                    base_names = asset_type_obj.attribute_specifications.keys()
                    attribute_keys = attributes.keys()
                    self.assertEqual(len(base_names), len(attributes), msg='number of attributes should equal len base attributes')
                    #log.debug('\n\n[unit] base_names(%d): %s', len(base_names), base_names)
                    # verify base attribute specification names are each in newly created NewAsset attributes
                    for name in base_names:
                        if name not in attribute_keys:
                            raise BadRequest('all attribute names in NewAsset must match Base type resource names')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids),     msg='cleanup rm_code_space_ids')
            self.assertEqual(5, len(rm_asset_ids),          msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),     msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),          msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),     msg='cleanup rm_event_type_ids')
            self.assertEqual(27, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted += len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if verbose: log.debug('\n\n[unit] total resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_new_asset_base_one_attribute_only')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Asset, AssetType and Attribute tests  (END)
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -



    # -----
    # ----- unit test: test_xls_mod_attribute
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_xls_mod_attribute(self):

        # Load spreadsheet to populate instances, then perform a second load to modify attribute
        # Step 1. load a single spreadsheet with all sheets (test505.xlsx) when there is no CodeSpace instance available
        # Step 2. load different spread sheet to modify attribute (test505-mod-attribute.xlsx)
        # Modify description attribute for SIM card to be 'Hot pink SIM card!'
        # todo add detailed asserts to check description value after modification

        log.debug('\n\n***** Start : test_xls_mod_attribute')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = True
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test505-mod-attribute.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)
                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)
                    if response['status'] != 'ok' or response['err_msg']:
                        raise BadRequest('Error in response: %s' % response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEqual(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEqual(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEqual(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEqual(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEqual(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    self.assertEqual(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEqual(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEqual(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEqual(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEqual(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEqual(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEqual(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEqual(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEqual(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # check uniqueness of Asset and Eevents altids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))
                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            if verbose: log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids), msg='cleanup rm_code_space_ids')
            self.assertEqual(4, len(rm_asset_ids),     msg='cleanup rm_asset_ids')
            self.assertEqual(4, len(rm_asset_type_ids),msg='cleanup rm_asset_type_ids')
            self.assertEqual(8, len(rm_event_ids),     msg='cleanup rm_event_ids')
            self.assertEqual(9, len(rm_event_type_ids),msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            if verbose:
                log.debug('\n\n rm_code_space_ids: %s', rm_code_space_ids)
                log.debug('\n\n rm_asset_ids: %s',      rm_asset_ids)
                log.debug('\n\n rm_asset_type_ids: %s', rm_asset_type_ids)
                log.debug('\n\n rm_event_ids: %s',      rm_event_ids)
                log.debug('\n\n rm_event_type_ids: %s', rm_event_type_ids)

            if rm_asset_type_ids:
                if verbose: log.debug('\n\n[unit] cleanup...asset_types...')
                total_resources_to_delete += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                if verbose: log.debug('\n\n[unit] cleanup...event_duration_types...')
                total_resources_to_delete += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                if verbose: log.debug('\n\n[unit] cleanup...assets...')
                total_resources_to_delete += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                if verbose: log.debug('\n\n[unit] cleanup...event durations...')
                total_resources_to_delete += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                if verbose: log.debug('\n\n[unit] cleanup...code_space_ids...%d', len(rm_code_space_ids))
                inx = 0
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] BadRequest (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] NotFound   (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception  (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_xls_mod_attribute')


    # test for development understanding only
    #@unittest.skip('targeting - attribute value encoding')
    @attr('UNIT', group='sa')
    def test_attribute_value_encoding(self):

        log.debug('\n\n***** Start : test_attribute_value_encoding')

        #log.debug('\n\n***** ValueTypeEnum = %s', OT.ValueTypeEnum.values) #__dict__)

        """
        def create_value(value=None) :

            constructor_map = {bool.__name__ : OT.BooleanValue, int.__name__ : OT.IntegerValue, float.__name__ : OT.RealValue, str.__name__ : OT.StringValue}

            if not value :
                raise BadRequest("No value provided to initialize requested value")

            if not type(value).__name__ in constructor_map :
                raise BadRequest("The type of value provided not supported")

            return IonObject(constructor_map[type(value).__name__],value=value)
        """
        attr_data = [('muss', True) ,('foo', 5) , ('foo', 6), ('blah', 1.2), ('shu', 'Hello World')]

        log.debug('\n\n[unit] attr_data: %s', attr_data)

        asset = IonObject(RT.Asset,name="Test Asset")

        log.debug('\n\n[unit] asset: %s', asset)

        for k,v in attr_data :
            log.debug('\n\n[unit] create_value v: %s ', v)
            value = self.create_value(v)
            if k in asset.asset_attrs :
                log.debug('\n\n[unit] create_value k: %s ', k)
                att = asset.asset_attrs[k]
                att.value.append(value)
                log.debug('\n\n[unit] att.value: %s', att.value)
            else:
                log.debug('\n\n[unit] make attr..')
                att = IonObject(OT.Attribute,name=k)
                att.value.append(value)                     # add list in Attribute definition
                asset.asset_attrs[k] = att
                log.debug('\n\n[unit] asset.asset_attrs[k]: %s', asset.asset_attrs[k])
                # asset.asset_attrs[k]: Attribute({, 'name': 'shu', 'value': [StringValue({, 'value': 'Hello World'})]})

        log.debug('\n\n***** Asset - asset: %s',asset)

        import json
        json_dumps = json.dumps
        #Used by json encoder
        def ion_object_encoder(obj):
            return obj.__dict__

        encoding = json_dumps(asset, default=ion_object_encoder, indent=2)

        log.debug('\n\n***** Asset - encoded asset: %s',encoding)

        log.debug('\n\n***** Completed : test_attribute_value_encoding')

        """
        This is example interface discovery

                def create_value(value_type=None,value_str='',value_constraints=[]) :
                    parsed_value = parse_value(value_str,value_type)
                    validated_value = constrain_value(parsed_value, value_constraints)
                    return IonObject(value_type,value=validated_value)

                value = create_value(attr_spec.value_type,value_str=input_value_str,attr_spec.value_constraints)

                assert(type(value).__name__ == attr_spec.value_type)

                def get_attr_value(attr_dict,attr_name,index)
                    assert(attr_name in)
                    assert(index within len)
                    dynamic_attrs[attr_name].value[index].value

        """

    # -----
    # ----- unit test: test_upload_all_sheets_twice
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_upload_event_asset_remove(self):

        # Test sheet EventAssetMap - 'add' association (Step 1); 'remove' association (Step 2); more in sheets 3 and 4
        # Step 1. load a single spreadsheet with all sheets (test400.xlsx) including sheet EventAssetMap
        # Step 2. load (again) but 'remove' instead of 'add' in EventAssetMap (test400-rm-association.xlsx)
        # Step 3. load (again) and 'add' association
        # Step 4. repeat Step 3, load again and expect failure to add association since it already exists:
        #  err_msg expected:
        #  'Association between 184eaac551524446a1c79eb67ff6e1cc and 42b28e2f1edf426e81eb6b22c7d9af29 with predicate hasVerificationEvent already exists'

        log.debug('\n\n***** Start : test_upload_event_asset_remove')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test500-rm-association.xlsx', 'test500.xlsx', 'test500.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 0
            for fid in input_files:
                pass_count += 1
                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)
                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)
                    if response['status'] != 'ok' or response['err_msg']:
                        if pass_count != 4:
                            raise BadRequest('Error in response: %s' % response['err_msg'])
                        else:
                            log.debug('\n\n[unit] received expected error_msg:\n%s', response['err_msg'])

                    if response['res_modified']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:
                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEquals(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEquals(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEquals(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEquals(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEquals(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEquals(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    self.assertEquals(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEquals(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEquals(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEquals(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEquals(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())



            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0

            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)

            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids), msg='cleanup rm_code_space_ids')
            self.assertEquals(4, len(rm_asset_ids),     msg='cleanup rm_asset_ids')
            self.assertEquals(4, len(rm_asset_type_ids),msg='cleanup rm_asset_type_ids')
            self.assertEquals(8, len(rm_event_ids),     msg='cleanup rm_event_ids')
            self.assertEquals(9, len(rm_event_type_ids),msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted +=len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1
            log.debug('\n\n[unit] total number of resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_event_asset_remove')

    # -----
    # ----- unit test: test_upload_multiple_event_asset
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_upload_multiple_event_asset(self):

        # Test sheet EventAssetMap - 'add' association (Step 1); 'remove' association (Step 2); more in sheets 3 and 4
        # Step 1. load a single spreadsheet with all sheets (test400.xlsx) includes sheet EventAssetMap
        # Step 2. load and add 2 events (remove Test event for Platform, a Verification Category and Repair,
        # a Location category) from Instrument 5010 using EventAssetMap (test410.xlsx)
        # Step 3. load and add 2 Deployment events (one each to Platform and Instrument 5010) in EventAssetMap (test410-multiple-location-events.xlsx)
        # Step 4. load same sheet as in Step 1 and expect error since only one Location event can be assigned at a time and
        #  err_msg expected:
        #  'an association (hasLocationEvent) already exists; cannot assign more than one association of the same type'
        log.debug('\n\n***** Start : test_upload_multiple_event_asset')

        #self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test500.xlsx', 'test510.xlsx', 'test500-multiple-location-events.xlsx', 'test500.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)
                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)
                    if response['status'] != 'ok' or response['err_msg']:
                        if pass_count != 4:
                            raise BadRequest('Error in response: %s' % response['err_msg'])
                        else:
                            log.debug('\n\n[unit] received expected error_msg:\n%s', response['err_msg'])
                    if response['res_modified']:

                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:

                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEquals(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEquals(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEquals(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEquals(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEquals(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEquals(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    self.assertEquals(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEquals(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEquals(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEquals(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEquals(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0
            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids), msg='cleanup rm_code_space_ids')
            self.assertEquals(4, len(rm_asset_ids),     msg='cleanup rm_asset_ids')
            self.assertEquals(4, len(rm_asset_type_ids),msg='cleanup rm_asset_type_ids')
            self.assertEquals(8, len(rm_event_ids),     msg='cleanup rm_event_ids')
            self.assertEquals(9, len(rm_event_type_ids),msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted +=len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1
            log.debug('\n\n[unit] total number of resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_upload_multiple_event_asset')

    # -----
    # ----- unit test: test_deployment_to_multiple_assets
    # -----
    #@unittest.skip('targeting')
    @attr('UNIT', group='sa')
    def test_deployment_to_multiple_assets(self):

        # Test sheet EventAssetMap - 'add' association (Step 1); 'remove' association (Step 2); more in sheets 3 and 4
        # Step 1. load a single spreadsheet with all sheets (test400.xlsx) includes sheet EventAssetMap
        # Step 2. load and add 2 events (remove Test event for Platform, a Verification Category and Repair,
        # a Location category) from Instrument 5010 using EventAssetMap (test410.xlsx)
        # Step 3. load and add 2 Deployment events (one each to Platform and Instrument 5010) in EventAssetMap (test410-multiple-location-events.xlsx)
        # Step 4. load same sheet as in Step 1 and expect error since only one Location event can be assigned at a time and
        #  err_msg expected:
        #  'an association (hasLocationEvent) already exists; cannot assign more than one association of the same type'
        log.debug('\n\n***** Start : test_deployment_to_multiple_assets')

        self._preload_scenario("BETA")      # not required, but should be included, for this test

        verbose = False
        breakpoint1A = False
        breakpoint2A = False
        breakpoint2B = False

        interactive = False
        if interactive:
            verbose = True
            breakpoint1A = True
            breakpoint2A = True
            breakpoint2B = True

        # Input and folder(s) and files for driving test
        input_files= ['test505.xlsx', 'test500-event-to-multiple-assets.xlsx']
        current_file = ''

        del_sum_code_space_ids = del_sum_asset_type_ids = del_sum_asset_ids = del_sum_event_ids = del_sum_event_type_ids = []
        rm_code_space_ids = rm_asset_type_ids = rm_asset_ids = rm_event_ids = rm_event_type_ids = []

        try:
            sum_code_space_ids = []
            sum_asset_type_ids = []
            sum_asset_ids = []
            sum_event_ids = []
            sum_event_type_ids = []
            code_space_ids = []
            pass_count = 1
            for fid in input_files:

                if verbose:
                    log.debug('\n- - - - - - - - - - - -- - - - - - - - - - -- - - - - - - -' + \
                              '\n- - - - - - - - - - - - Pass %d - - - - - - - - - - - - - -' + \
                              '\n- - - - - - - - - - - -- - - - - - - - - - - - - - - - - - ', pass_count)

                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                # Load marine assets into system from xslx file
                # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                current_file = TEST_XLS_FOLDER + fid
                response = self.load_marine_assets_from_xlsx(current_file)
                if response:

                    if verbose: log.debug('\n\n[unit] response - pass %d: %s', pass_count, response)
                    if response['status'] != 'ok' or response['err_msg']:
                        if pass_count != 4:
                            raise BadRequest('Error in response: %s' % response['err_msg'])
                        else:
                            log.debug('\n\n[unit] received expected error_msg:\n%s', response['err_msg'])
                    if response['res_modified']:

                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_modified']:
                            code_space_ids = response['res_modified']['codespaces'][:]
                            if code_space_ids:
                                sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_modified']:
                            asset_type_ids = response['res_modified']['asset_types']            # ids of resources created
                            if asset_type_ids:
                                sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_modified']:
                            asset_ids = response['res_modified']['assets']                      # ids of resources created
                            if asset_ids:
                                sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_modified']:
                            event_duration_type_ids = response['res_modified']['event_types']   # ids of resources created
                            if event_duration_type_ids:
                                sum_event_type_ids.extend(event_duration_type_ids)
                        if 'events' in response['res_modified']:
                            event_duration_ids = response['res_modified']['events']             # ids of resources created
                            if event_duration_ids:
                                sum_event_ids.extend(event_duration_ids)

                    if response['res_removed']:

                        code_space_ids = asset_type_ids = asset_ids = event_type_ids = event_ids = []
                        if 'codespaces' in response['res_removed']:
                            code_space_ids = response['res_removed']['codespaces'][:]
                            if code_space_ids:
                                del_sum_code_space_ids.extend(code_space_ids)
                        if 'asset_types' in response['res_removed']:
                            asset_type_ids = []
                            asset_type_ids = response['res_removed']['asset_types']     # ids of resources created
                            if asset_type_ids:
                                del_sum_asset_type_ids.extend(asset_type_ids)
                        if 'assets' in response['res_removed']:
                            asset_ids = response['res_removed']['assets']               # ids of resources created
                            if asset_ids:
                                del_sum_asset_ids.extend(asset_ids)
                        if 'event_types' in response['res_removed']:
                            event_type_ids = response['res_removed']['event_types']     # ids of resources created
                            if event_type_ids:
                                del_sum_event_type_ids.extend(event_type_ids)
                        if 'events' in response['res_removed']:
                            event_ids = response['res_removed']['events']               # ids of resources created
                            if event_ids:
                                del_sum_event_ids.extend(event_ids)

                # pass one 'add' all resources - full load
                # asserts specifically for this unit test
                if pass_count == 1:
                    self.assertEquals(1, len(sum_code_space_ids),    msg='pass 1: sum_code_space_ids')
                    self.assertEquals(4, len(sum_asset_ids),         msg='pass 1: sum_asset_ids')
                    self.assertEquals(4, len(sum_asset_type_ids),    msg='pass 1: sum_asset_type_ids')
                    self.assertEquals(8, len(sum_event_ids),         msg='pass 1: sum_event_ids')
                    self.assertEquals(9, len(sum_event_type_ids),    msg='pass 1: sum_event_type_ids')
                    self.assertEquals(0, len(del_sum_code_space_ids),msg='pass 1: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),     msg='pass 1: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),msg='pass 1: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),     msg='pass 1: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),msg='pass 1: del_sum_event_type_ids')

                # pass two - asserts specifically for this unit test
                if pass_count == 2:
                    self.assertEquals(4, len(list(set(sum_asset_ids))),     msg='pass 2: sum_asset_ids')
                    self.assertEquals(4, len(list(set(sum_asset_type_ids))),msg='pass 2: sum_asset_type_ids')
                    self.assertEquals(8, len(list(set(sum_event_ids))),     msg='pass 2: sum_event_ids')
                    self.assertEquals(9, len(list(set(sum_event_type_ids))),msg='pass 2: sum_event_type_ids')
                    self.assertEquals(0, len(del_sum_code_space_ids),       msg='pass 2: del_sum_code_space_ids')
                    self.assertEquals(0, len(del_sum_asset_ids),            msg='pass 2: del_sum_asset_ids')
                    self.assertEquals(0, len(del_sum_asset_type_ids),       msg='pass 2: del_sum_asset_type_ids')
                    self.assertEquals(0, len(del_sum_event_ids),            msg='pass 2: del_sum_event_ids')
                    self.assertEquals(0, len(del_sum_event_type_ids),       msg='pass 2: del_sum_event_type_ids')

                # set breakpoint for testing...
                if breakpoint1A:
                    log.debug('\n\n[unit] verify result of pass %d...', pass_count)
                    from pyon.util.breakpoint import breakpoint
                    breakpoint(locals(), globals())

                pass_count += 1

            # Check uniqueness of alt_ids
            unique = self.unique_altids(RT.Asset)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate Asset altids found')
                raise BadRequest('duplicate Asset altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all Asset altids unique')
            picklist = self.OMS.get_assets_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.Asset)
            self.assertEqual(len(picklist),len(altids), msg='duplicate Asset altids')

            unique = self.unique_altids(RT.EventDuration)
            if unique != True:
                if verbose: log.debug('\n\n[unit] duplicate EventDuration altids found')
                raise BadRequest('duplicate EventDuration altids found!')
            else:
                if verbose: log.debug('\n\n[unit] all EventDuration altids unique')
            picklist = self.OMS.get_events_picklist(id_only='False')
            altids = self.OMS.get_altids(RT.EventDuration)
            self.assertEqual(len(picklist),len(altids), msg='duplicate EventDuration altids')

            # summary and cleanup
            total_resources_to_delete = 0

            """
            if verbose:
                log.debug('\n\n[unit] Summary of items processed:\nAssets: %d\nAssetTypes: %d\nEvents: %d\nEventTypes: %d\nCodeSpaces: %d',
                    len(sum_asset_ids), len(sum_asset_type_ids), len(sum_event_ids), len(sum_event_type_ids),
                    len(sum_code_space_ids))

                log.debug('\n\n[unit] sum_asset_ids (%d): %s',      len(sum_asset_ids), sum_asset_ids)
                log.debug('\n\n[unit] sum_asset_type_ids (%d): %s', len(sum_asset_type_ids), sum_asset_type_ids)
                log.debug('\n\n[unit] sum_event_ids (%d): %s',      len(sum_event_ids), sum_event_ids)
                log.debug('\n\n[unit] sum_event_type_ids (%d): %s', len(sum_event_type_ids), sum_event_type_ids)
                log.debug('\n\n[unit] sum_code_space_ids (%d): %s', len(sum_code_space_ids), sum_code_space_ids)
            """
            total_resources_to_delete = 0
            rm_code_space_ids = list(set(sum_code_space_ids))
            rm_asset_ids = list(set(sum_asset_ids))
            rm_asset_type_ids = list(set(sum_asset_type_ids))
            rm_event_ids = list(set(sum_event_ids))
            rm_event_type_ids = list(set(sum_event_type_ids))
            total_resources_to_delete = len(rm_code_space_ids) + len(rm_asset_ids) + len(rm_asset_type_ids) + \
                                        len(rm_event_ids) + len(rm_event_type_ids)

            log.debug('\n\n[unit] total number of resources to delete: %d', total_resources_to_delete)

            # asserts specifically for this unit test
            self.assertEqual(1, len(rm_code_space_ids), msg='cleanup rm_code_space_ids')
            self.assertEquals(4, len(rm_asset_ids),     msg='cleanup rm_asset_ids')
            self.assertEquals(4, len(rm_asset_type_ids),msg='cleanup rm_asset_type_ids')
            self.assertEquals(8, len(rm_event_ids),     msg='cleanup rm_event_ids')
            self.assertEquals(9, len(rm_event_type_ids),msg='cleanup rm_event_type_ids')
            self.assertEqual(26, total_resources_to_delete, msg='summary of resources to delete')

            # Cleanup all resources (retire/force delete)
            total_resources_deleted = 0
            if rm_asset_type_ids:
                total_resources_deleted += len(rm_asset_type_ids)
                for id in rm_asset_type_ids:
                    self.OMS.force_delete_asset_type(id)
            if rm_event_type_ids:
                total_resources_deleted += len(rm_event_type_ids)
                for id in rm_event_type_ids:
                    self.OMS.force_delete_event_duration_type(id)
            if rm_asset_ids:
                total_resources_deleted += len(rm_asset_ids)
                for id in rm_asset_ids:
                    self.OMS.force_delete_asset(id)
            if rm_event_ids:
                total_resources_deleted += len(rm_event_ids)
                for id in rm_event_ids:
                    self.OMS.force_delete_event_duration(id)
            if rm_code_space_ids:
                inx = 0
                total_resources_deleted +=len(rm_code_space_ids)
                for code_space_id in rm_code_space_ids:
                    id = rm_code_space_ids[inx]
                    self.OMS.force_delete_code_space(id)
                    inx += 1
            log.debug('\n\n[unit] total number of resources deleted: %d', total_resources_deleted)
            self.assertEqual(total_resources_to_delete, total_resources_deleted, msg='number of resources deleted different from number of resources created')

            if breakpoint2B:
                log.debug('\n\n[unit] verify all resources have been deleted...')
                from pyon.util.breakpoint import breakpoint
                breakpoint(locals(), globals())

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise           # raise here to fail test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', current_file, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s)', current_file, exc_info=True)
            raise           # raise here to fail test case

        log.debug('\n\n***** Completed : test_deployment_to_multiple_assets')

    # -----
    # ----- UNIT TEST: test_alpha_preload
    # -----
    @unittest.skip('development support for UI')
    @attr('UNIT', group='sa')
    def test_alpha_preload(self):

        log.debug('\n\n***** Start : test_alpha_preload')
        self._preload_cfg("res/preload/r2_ioc/config/ooi_alpha.yml", path=TEST_PATH)
        log.debug('\n\n***** Completed : test_alpha_preload')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # helper functions
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _get_type_resource_by_name(self, res_name, res_type):

        if not res_name:
            raise BadRequest('res_name parameter is empty')
        if not res_type:
            raise BadRequest('res_type parameter is empty')
        if res_type != RT.AssetType and res_type != RT.EventDurationType:
            raise BadRequest('invalid res_type value (%s)' % res_type)

        res_objs, res_keys = self.container.resource_registry.find_resources_ext(alt_id_ns=res_type, alt_id=res_name, id_only=False)
        type_resource = ''
        if res_keys:
            if len(res_keys) == 1:
                type_resource = res_objs[0]

        return type_resource

    def _create_attribute_specification(self, value_type, name, source, constraints, pattern, codeset_name):

        if not value_type:
            raise BadRequest('value_type is empty')
        if not name:
            raise BadRequest('name is empty')
        if not source:
            raise BadRequest('source is empty')

        if value_type == 'CodeValue':
            if not codeset_name:
                raise BadRequest('_create_attribute_specification if value_type is CodeValue then a codeset_name')

        simple_types = ['BooleanValue', 'IntegerValue', 'RealValue', 'StringValue', 'DateValue', 'TimeValue', 'DateTimeValue']

        pattern_string = '[\w -_]{1,64}'
        pattern_real = '\d*\.?\d*'
        pattern_date = '\d{1,2}/\d{1,2}/(\d{4}|\d{2})'
        pattern_time = '\d{2}:\d{2}'
        pattern_datetime = '\d{1,2}/\d{1,2}/(\d{4}|\d{2}) \d{2}:\d{2}'
        pattern_integer = '\d*'
        pattern_codevalue = '[\w -_]{1,64}'

        constraint_string = ''
        constraint_real = 'min=0.00, max=10923.00'
        constraint_date = ''
        constraint_time = ''
        constraint_datetime = ''
        constraint_integer = ''
        if codeset_name:
            constraint_codevalue = 'set=MAM:' + codeset_name

        attribute_specification = IonObject(OT.AttributeSpecification)
        attribute_specification['id'] = name
        attribute_specification['description'] = 'some description of ' + name
        attribute_specification['value_type'] = value_type
        attribute_specification['group_label'] = 'Group Label'
        attribute_specification['attr_label'] = 'Some Attribute Label'
        attribute_specification['rank'] = '1.1'
        attribute_specification['visibility'] = 'True'
        attribute_specification['editable'] = 'False'
        attribute_specification['journal'] = 'False'
        attribute_specification['default_value'] = 'NONE'
        attribute_specification['uom'] = ''
        attribute_specification['cardinality'] = '1..1'
        attribute_specification['_source_id'] = source

        if not pattern:
            if value_type == 'StringValue':
                pattern = pattern_string
            elif value_type == 'RealValue':
                pattern = pattern_real
            elif value_type == 'DateValue':
                pattern = pattern_date
            elif value_type == 'TimeValue':
                pattern = pattern_time
            elif value_type == 'DateTimeValue':
                pattern = pattern_datetime
            elif value_type == 'IntegerValue':
                pattern = pattern_integer
            elif value_type == 'CodeValue':
                pattern = pattern_codevalue
            else:
                raise BadRequest('_create_attribute_specification unknown value_type to process pattern: %s' % value_type)

        if not constraints:
            if value_type == 'StringValue':
                constraints = constraint_string
            elif value_type == 'RealValue':
                constraints = constraint_real
            elif value_type == 'DateValue':
                constraints = constraint_date
            elif value_type == 'TimeValue':
                constraints = constraint_time
            elif value_type == 'DateTimeValue':
                constraints = constraint_datetime
            elif value_type == 'IntegerValue':
                constraints = constraint_integer
            elif value_type == 'CodeValue':
                constraints = constraint_codevalue
            else:
                raise BadRequest('_create_attribute_specification unknown value_type to process constraints: %s' % value_type)

        attribute_specification['value_constraints'] = constraints
        attribute_specification['value_pattern'] = pattern

        return attribute_specification

    def _create_attribute(self, value_type, name, value):

        if not value_type:
            raise BadRequest('value_type is empty')
        if not name:
            raise BadRequest('name is empty')

        simple_types = ['BooleanValue', 'IntegerValue', 'RealValue', 'StringValue', 'DateValue', 'TimeValue', 'DateTimeValue']
        value_string = 'hello world'
        value_real = '1.45'
        value_date = '12/25/2014'
        value_time = '23:17'
        value_datetime = '12/25/2014 23:17'
        value_integer = '5'

        if not value:
            if value_type == 'StringValue':
                value = value_string
            elif value_type == 'RealValue':
                value = value_real
            elif value_type == 'DateValue':
                value = value_date
            elif value_type == 'TimeValue':
                value = value_time
            elif value_type == 'DateTimeValue':
                value = value_datetime
            elif value_type == 'IntegerValue':
                value = value_integer
            elif value_type == 'CodeValue':
                raise BadRequest('_create_attribute requires a value, when value_type CodeValue')

        # Create Attribute
        attribute = IonObject(OT.Attribute)
        attribute['name'] = name
        if value_type in simple_types:
            return_value = self.create_value(value)
        else:
            log.debug('\n\n[unit] probably CodeValue...')
            if value_type == 'CodeValue':
                log.debug('\n\n[unit] definitely CodeValue...')
                return_value = self.create_complex_value(value_type, name, value)
            else:
                raise BadRequest('\n\n[unit] _create_attribute - unknown value_type: %s', value_type)

        attribute['value'] = [return_value]

        return attribute



    def load_marine_assets_from_xlsx(self, fid):

        # unit test helper function
        if not fid:
            raise BadRequest('fid parameter empty.')

        try:
            try:
                f = open(fid, 'r')
            except:
                log.error('failed to open xlsx file for read: ', exc_info=True)
                raise

            content = f.read()
            response = self.OMS.declare_asset_tracking_resources(binascii.b2a_hex(content),
                                                               content_type='file_descriptor.mimetype',
                                                               content_encoding='b2a_hex')
            f.close()

        except BadRequest, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise       # raise here to test case
        except NotFound, Arguments:
            log.debug('\n\n[unit] Exception (file: %s): %s', fid, Arguments.get_error_message())
            raise
        except:
            log.error('\n\n[unit] Exception (file: %s):', fid, exc_info=True)
            raise       # raise here to test case

        return response

    def unique_altids(self, res_type):
        # helper
        if not res_type:
            raise BadRequest('res_type param is empty')

        unique = True
        picklist = []
        altids = []
        if res_type == RT.Asset:
            picklist = self.OMS.get_assets_picklist(id_only='False')
        else:
            picklist = self.OMS.get_events_picklist(id_only='False')

        altids = self.OMS.get_altids(res_type)
        # test - force error
        # picklist[0][2].append('asset:junk')
        # verify one and only one altid per resource instance
        for id_list in picklist:
            if len(id_list[2]) != 1:
                unique = False
                break

        # test - force error
        #altids[0].append('asset:junk')
        for id_list in altids:
            if len(id_list) != 1:
                unique = False
                break

        # compare list of altids for resource instances created to number of resource instances created
        if len(picklist) != len(altids):
            unique = False

        # compare list of altids for tracking resource instances created to set()of same; if unique assert will pass
        # if non-unique res.alt_ids have been created then assert will fail
        if altids:
            len_altids = len(altids[0])             # len of all altids
            list_altids = []
            list_altids = list(set(altids[0]))      # len of unique altids
            if len(list_altids) != len_altids:
                unique = False

        return unique

    def create_value(self, value=None) :
        # helper
        constructor_map = {bool.__name__ : OT.BooleanValue, int.__name__ : OT.IntegerValue, float.__name__ : OT.RealValue, str.__name__ : OT.StringValue}

        if not value :
            raise BadRequest('value parameter is empty')

        if type(value).__name__ not in constructor_map :
            raise BadRequest('type of value provided not supported')

        return IonObject(constructor_map[type(value).__name__],value=value)

    def create_complex_value(self, type=None, name=None, value=None) :
        # helper
        constructor_map = {'CodeValue' : OT.CodeValue }
        #, int.__name__ : OT.IntegerValue, float.__name__ : OT.RealValue, str.__name__ : OT.StringValue}

        if not value :
            raise BadRequest('value parameter is empty')
        if not type :
            raise BadRequest('type parameter is empty')
        if not name :
            raise BadRequest('name parameter is empty')
        if type not in constructor_map :
            raise BadRequest('type provided is not supported')
        log.debug('\n\n[unit] name: %s, type: %s, value: %s', name, type, value)

        return IonObject(constructor_map[type], value=value)

