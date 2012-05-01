#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from ion.services.sa.observatory.observatory_management_service import ObservatoryManagementService
from interface.services.sa.iobservatory_management_service import IObservatoryManagementService, ObservatoryManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict, Inconsistent
from pyon.public import RT, PRED
#from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log

from ion.services.sa.test.helpers import any_old



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

        self.container.start_rel_from_url('res/deploy/r2deploy_no_bootstrap.yml')
        self.RR = ResourceRegistryServiceClient(node=self.container.node)
        self.OMS = ObservatoryManagementServiceClient(node=self.container.node)
        #print 'TestObservatoryManagementServiceIntegration: started services'

    @unittest.skip('this exists only for debugging the launch process')
    def test_just_the_setup(self):
        return

    
    def test_resources_associations(self):
        self._make_associations()


    @unittest.skip('needs refactoring')    
    def test_find_subordinate(self):
        # find_subordinates gives a dict of obj lists, convert objs to ids
        def idify(adict):
            ids = {}
            for k, v in adict.iteritems():
                ids[k] = []
                for obj in v:
                    ids[k].append(obj._id)

            return ids

        #set up associations first
        stuff = self._make_associations()

        #full traversal of tree down to instrument
        ret = self.OMS.find_subordinate_entity(stuff.observatory_id, [RT.InstrumentSite])
        ids = idify(ret)
        self.assertIn(RT.InstrumentSite, ids)
        self.assertIn(stuff.instrument_site_id, ids[RT.InstrumentSite])

        #partial traversal, only down to platform
        ret = self.OMS.find_subordinate_entity(stuff.observatory_id, [RT.Site, RT.PlatformSite])
        ids = idify(ret)
        self.assertIn(RT.PlatformSite, ids)
        self.assertIn(RT.Site, ids)
        self.assertIn(stuff.platform_site_id, ids[RT.PlatformSite])
        self.assertIn(stuff.platform_site2_id, ids[RT.PlatformSite])
        self.assertIn(stuff.site_id, ids[RT.Site])
        self.assertIn(stuff.site2_id, ids[RT.Site])
        self.assertNotIn(RT.InstrumentSite, ids)
        
    @unittest.skip('needs refactoring')    
    def test_find_superior(self):
        # find_superiors gives a dict of obj lists, convert objs to ids
        def idify(adict):
            ids = {}
            for k, v in adict.iteritems():
                ids[k] = [obj._id for obj in v]

            return ids

        #set up associations first
        stuff = self._make_associations()

        #full traversal of tree down to instrument
        ret = self.OMS.find_subordinate_entity(stuff.instrument_site_id, [RT.Observatory])
        ids = idify(ret)
        # self.assertIn(RT.Observatory, ids)
        # self.assertIn(stuff.observatory_id, ids[RT.Observatory])

        #partial traversal, only down to platform
        ret = self.OMS.find_subordinate_entity(stuff.instrument_site_id, [RT.Site, RT.PlatformSite])
        ids = idify(ret)
        self.assertIn(RT.PlatformSite, ids)
        self.assertIn(RT.Site, ids)
        self.assertIn(stuff.platform_site_id, ids[RT.PlatformSite])
        #self.assertIn(stuff.platform_site2_id, ids[RT.PlatformSite])
        self.assertIn(stuff.site_id, ids[RT.Site])
        self.assertIn(stuff.site2_id, ids[RT.Site])
        self.assertNotIn(RT.Observatory, ids)
        

    def _make_associations(self):
        """
        create one of each resource and association used by OMS
        to guard against problems in ion-definitions
        """

        #raise unittest.SkipTest("https://jira.oceanobservatories.org/tasks/browse/CISWCORE-41")
        
        #stuff we control
        instrument_site_id, _ = self.RR.create(any_old(RT.InstrumentSite))
        platform_site_id, _   = self.RR.create(any_old(RT.PlatformSite))
        platform_site2_id, _  = self.RR.create(any_old(RT.PlatformSite))
        observatory_id, _     = self.RR.create(any_old(RT.Observatory))
        subsite_id, _            = self.RR.create(any_old(RT.Subsite))
        subsite2_id, _           = self.RR.create(any_old(RT.Subsite))

        #stuff we associate to
        instrument_agent_id, _ =           self.RR.create(any_old(RT.InstrumentAgent))
        platform_agent_id, _ =             self.RR.create(any_old(RT.PlatformAgent))

        #instrument_site
        self.RR.create_association(instrument_site_id, PRED.hasAgent, instrument_agent_id)


        #platform_site
        self.RR.create_association(platform_site_id, PRED.hasSite, platform_site2_id)
        self.RR.create_association(platform_site_id, PRED.hasSite, instrument_site_id)
        self.RR.create_association(platform_site_id, PRED.hasAgent, platform_agent_id)


        #observatory
        self.RR.create_association(observatory_id, PRED.hasSite, subsite_id)
        if True: return DotDict()

        #site
        self.RR.create_association(subsite_id, PRED.hasSite, subsite2_id)
        self.RR.create_association(subsite2_id, PRED.hasPlatform, platform_site_id)
        

        ret = DotDict()
        ret.observatory_id      = observatory_id
        ret.subsite_id             = subsite_id
        ret.subsite2_id            = subsite2_id
        ret.platform_site_id    = platform_site_id
        ret.platform_site2_id   = platform_site2_id
        ret.instrument_site_id  = instrument_site_id
        
        return ret

    def test_create_observatory(self):
        observatory_obj = IonObject(RT.Observatory,
                                        name='TestFacility',
                                        description='some new mf')
        self.OMS.create_observatory(observatory_obj)

    #@unittest.skip('temporarily')
    def test_find_observatory_org(self):
        observatory_obj = IonObject(RT.Observatory,
                                        name='TestFacility',
                                        description='some new mf')
        observatory_id = self.OMS.create_marine_facility(observatory_obj)
        org_objs = self.OMS.find_org_by_observatory(observatory_id)
        self.assertEqual(1, len(org_objs))
        org_id = org_objs[0]._id
        print("org_id=<" + org_id + ">")

        #create a subsite with parent Observatory
        subsite_obj =  IonObject(RT.Subsite,
                                name= 'TestSubsite',
                                description = 'sample subsite')
        subsite_id = self.OMS.create_subsite(subsite_obj, observatory_id)
        self.assertIsNotNone(subsite_id, "Subsite not created.")


        # verify that Subsite is linked to Observatory
        mf_subsite_assoc = self.RR.get_association(observatory_id, PRED.hasSite, subsite_id)
        self.assertIsNotNone(mf_subsite_assoc, "Subsite not connected to Observatory.")


        # add the Subsite as a resource of this Observatory
        self.OMS.assign_resource_to_observatory(resource_id=subsite_id, observatory_id=observatory_id)
        # verify that Subsite is linked to Org
        org_subsite_assoc = self.RR.get_association(org_id, PRED.hasResource, subsite_id)
        self.assertIsNotNone(org_subsite_assoc, "Subsite not connected as resource to Org.")


        return

        #create a logical platform with parent Subsite
        platform_site_obj =  IonObject(RT.PlatformSite,
                                name= 'TestPlatformSite',
                                description = 'sample logical platform')
        platform_site_id = self.OMS.create_platform_site(platform_site_obj, subsite_id)
        self.assertIsNotNone(platform_site_id, "PlatformSite not created.")


        # verify that PlatformSite is linked to Site
        site_lp_assoc = self.RR.get_association(site_id, PRED.hasPlatform, platform_site_id)
        self.assertIsNotNone(site_lp_assoc, "PlatformSite not connected to Site.")


        # add the PlatformSite as a resource of this Observatory
        self.OMS.assign_resource_to_observatory(resource_id=platform_site_id, observatory_id=observatory_id)
        # verify that PlatformSite is linked to Org
        org_lp_assoc = self.RR.get_association(org_id, PRED.hasResource, platform_site_id)
        self.assertIsNotNone(org_lp_assoc, "PlatformSite not connected as resource to Org.")



        #create a logical instrument with parent logical platform
        instrument_site_obj =  IonObject(RT.InstrumentSite,
                                name= 'TestInstrumentSite',
                                description = 'sample logical instrument')
        instrument_site_id = self.OMS.create_instrument_site(instrument_site_obj, platform_site_id)
        self.assertIsNotNone(instrument_site_id, "InstrumentSite not created.")


        # verify that InstrumentSite is linked to PlatformSite
        li_lp_assoc = self.RR.get_association(platform_site_id, PRED.hasInstrument, instrument_site_id)
        self.assertIsNotNone(li_lp_assoc, "InstrumentSite not connected to PlatformSite.")


        # add the InstrumentSite as a resource of this Observatory
        self.OMS.assign_resource_to_observatory(resource_id=instrument_site_id, observatory_id=observatory_id)
        # verify that InstrumentSite is linked to Org
        org_li_assoc = self.RR.get_association(org_id, PRED.hasResource, instrument_site_id)
        self.assertIsNotNone(org_li_assoc, "InstrumentSite not connected as resource to Org.")


        # remove the InstrumentSite as a resource of this Observatory
        self.OMS.unassign_resource_from_observatory(instrument_site_id, observatory_id)
        # verify that InstrumentSite is linked to Org
        assocs,_ = self.RR.find_objects(org_id, PRED.hasResource, RT.InstrumentSite, id_only=True )
        self.assertEqual(len(assocs), 0)

        # remove the InstrumentSite
        self.OMS.delete_instrument_site(instrument_site_id)
        assocs, _ = self.RR.find_objects(platform_site_id, PRED.hasInstrument, RT.InstrumentSite, id_only=True )
        self.assertEqual(len(assocs), 0)


        # remove the PlatformSite as a resource of this Observatory
        self.OMS.unassign_resource_from_observatory(platform_site_id, observatory_id)
        # verify that PlatformSite is linked to Org
        assocs,_ = self.RR.find_objects(org_id, PRED.hasResource, RT.PlatformSite, id_only=True )
        self.assertEqual(len(assocs), 0)

        # remove the PlatformSite
        self.OMS.delete_platform_site(platform_site_id)
        assocs, _ = self.RR.find_objects(site_id, PRED.hasPlatform, RT.PlatformSite, id_only=True )
        self.assertEqual(len(assocs), 0)



        # remove the Site as a resource of this Observatory
        self.OMS.unassign_resource_from_observatory(site_id, observatory_id)
        # verify that Site is linked to Org
        assocs,_ = self.RR.find_objects(org_id, PRED.hasResource, RT.Site, id_only=True )
        self.assertEqual(len(assocs), 0)

        # remove the Site
        self.OMS.delete_site(site_id)
        assocs, _ = self.RR.find_objects(observatory_id, PRED.hasSite, RT.Site, id_only=True )
        self.assertEqual(len(assocs), 0)
