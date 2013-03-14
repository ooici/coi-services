#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
import string
import unittest

from pyon.util.containers import DotDict, get_ion_ts
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.public import RT, PRED
from pyon.public import IonObject
from pyon.event.event import EventPublisher
from pyon.agent.agent import ResourceAgentState
from ion.services.dm.utility.granule_utils import time_series_domain
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.coi.iorg_management_service import OrgManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

from nose.plugins.attrib import attr

from ion.services.sa.test.helpers import any_old


# some stuff for logging info to the console
log = DotDict()

def mk_logger(level):
    def logger(fmt, *args):
        print "%s %s" % (string.ljust("%s:" % level, 8), (fmt % args))

    return logger

log.debug = mk_logger("DEBUG")
log.info  = mk_logger("INFO")
log.warn  = mk_logger("WARNING")


class FakeProcess(LocalContextMixin):
    name = ''

 
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
        self.OMS = ObservatoryManagementServiceClient(node=self.container.node)
        self.org_management_service = OrgManagementServiceClient(node=self.container.node)
        self.IMS =  InstrumentManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()
        #print 'TestObservatoryManagementServiceIntegration: started services'

        self.event_publisher = EventPublisher()

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
    def test_resources_associations(self):
        resources = self._make_associations()
        self.destroy(resources)

    #@unittest.skip('targeting')
    def test_find_related_frames_of_reference(self):
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

        def create_under_org(resource_type):
            obj = any_old(resource_type)

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

        return ret

    #@unittest.skip("targeting")
    def test_create_observatory(self):
        observatory_obj = IonObject(RT.Observatory,
                                        name='TestFacility',
                                        description='some new mf')
        observatory_id = self.OMS.create_observatory(observatory_obj)
        self.OMS.force_delete_observatory(observatory_id)


    #@unittest.skip("targeting")
    def test_find_observatory_org(self):
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


    #@unittest.skip("in development...")
    @attr('EXT')
    def test_observatory_org_extended(self):

        stuff = self._make_associations()

        parsed_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',
                                                                                    id_only=True)
        parsed_stream_def_id = self.pubsubcli.create_stream_definition(name='parsed',
                                                                       parameter_dictionary_id=parsed_pdict_id)
        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()
        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id1 = self.dpclient.create_data_product(data_product=dp_obj,
                                                             stream_definition_id=parsed_stream_def_id)
        self.damsclient.assign_data_product(input_resource_id=stuff.instrument_device_id,
                                            data_product_id=data_product_id1)


        #--------------------------------------------------------------------------------
        # Get the extended Site (platformSite)
        #--------------------------------------------------------------------------------

        extended_site = self.OMS.get_site_extension(stuff.platform_site_id)
        log.debug("extended_site:  %s ", str(extended_site))
        self.assertEqual(1, len(extended_site.platform_devices))
        self.assertEqual(1, len(extended_site.platform_models))
        self.assertEqual(stuff.platform_device_id, extended_site.platform_devices[0]._id)
        self.assertEqual(stuff.platform_model_id, extended_site.platform_models[0]._id)

        #--------------------------------------------------------------------------------
        # Get the extended Org
        #--------------------------------------------------------------------------------
        #test the extended resource
        extended_org = self.org_management_service.get_marine_facility_extension(stuff.org_id)
        log.debug("test_observatory_org_extended: extended_org:  %s ", str(extended_org))
        #self.assertEqual(2, len(extended_org.instruments_deployed) )
        #self.assertEqual(1, len(extended_org.platforms_not_deployed) )
        self.assertEqual(2, extended_org.number_of_platforms)
        self.assertEqual(2, len(extended_org.platform_models) )

        self.assertEqual(2, extended_org.number_of_instruments)
        self.assertEqual(2, len(extended_org.instrument_models) )

        #test the extended resource of the ION org
        ion_org_id = self.org_management_service.find_org()
        extended_org = self.org_management_service.get_marine_facility_extension(ion_org_id._id, user_id=12345)
        log.debug("test_observatory_org_extended: extended_ION_org:  %s ", str(extended_org))
        self.assertEqual(0, len(extended_org.members))
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



