#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient

from ion.services.sa.test.helpers import any_old

from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from pyon.public import RT, PRED

from nose.plugins.attrib import attr

from ion.util.related_resources_crawler import RelatedResourcesCrawler

import string

# some stuff for logging info to the console
log = DotDict()

def mk_logger(level):
    def logger(fmt, *args):
        print "%s %s" % (string.ljust("%s:" % level, 8), (fmt % args))

    return logger

log.debug = mk_logger("DEBUG")
log.info  = mk_logger("INFO")
log.warn  = mk_logger("WARNING")

RT_SITE = "Site"
RT_SUBPLATFORMSITE = "SubPlatformSite"


@attr('INT', group='sa')
class TestFindRelatedResources(IonIntegrationTestCase):
    """
    assembly integration tests at the service level
    """

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.OMS = ObservatoryManagementServiceClient(node=self.container.node)

        self.RR   = ResourceRegistryServiceClient(node=self.container.node)

        self.care = {}
        self.dontcare = {}
        self.realtype = {}

#    @unittest.skip('this test just for debugging setup')
#    def test_just_the_setup(self):
#        return


    def create_any(self, resourcetype, first, label=None):
        rsrc_id, rev = self.RR.create(any_old(resourcetype))

        if label is None: label = resourcetype

        if first:
            if label in self.care:
                self.fail("tried to add a duplicate %s" % label)
            log.debug("Creating %s as %s", resourcetype, label)
            self.care[label] = rsrc_id
            self.realtype[label] = resourcetype
            self.assertIn(label, self.care)
            self.assertIn(label, self.realtype)
        else:
            if not resourcetype in self.dontcare: self.dontcare[resourcetype] = []
            self.dontcare[resourcetype].append(rsrc_id)

        return rsrc_id


    def create_observatory(self, first=False):
        obs_id = self.create_any(RT.Observatory, first)

        site_id1 = self.create_site(first)
        site_id2 = self.create_site(False)

        self.RR.create_association(subject=obs_id, predicate=PRED.hasSite, object=site_id1)
        self.RR.create_association(subject=obs_id, predicate=PRED.hasSite, object=site_id2)

        return obs_id


    def create_site(self, first=False):
        site_id = self.create_any(RT.Subsite, first, RT_SITE)

        subsite_id1 = self.create_subsite(first)
        subsite_id2 = self.create_subsite(False)

        self.RR.create_association(subject=site_id, predicate=PRED.hasSite, object=subsite_id1)
        self.RR.create_association(subject=site_id, predicate=PRED.hasSite, object=subsite_id2)

        return site_id


    def create_subsite(self, first=False):
        subsite_id = self.create_any(RT.Subsite, first)

        platformsite_id1 = self.create_platformsite(first)
        platformsite_id2 = self.create_platformsite(False)

        self.RR.create_association(subject=subsite_id, predicate=PRED.hasSite, object=platformsite_id1)
        self.RR.create_association(subject=subsite_id, predicate=PRED.hasSite, object=platformsite_id2)

        return subsite_id


    def create_platformsite(self, first=False):
        platform_model_id = self.create_any(RT.PlatformModel, False) # we never care about this level
        platformsite_id = self.create_any(RT.PlatformSite, first)

        subplatformsite_id1 = self.create_subplatformsite(first)
        subplatformsite_id2 = self.create_subplatformsite(False)

        self.RR.create_association(subject=platformsite_id, predicate=PRED.hasSite, object=subplatformsite_id1)
        self.RR.create_association(subject=platformsite_id, predicate=PRED.hasSite, object=subplatformsite_id2)

        self.RR.create_association(subject=platformsite_id, predicate=PRED.hasModel, object=platform_model_id)

        return platformsite_id


    def create_subplatformsite(self, first=False):
        platform_model_id = self.create_any(RT.PlatformModel, first)

        subplatformsite_id = self.create_any(RT.PlatformSite, first, RT_SUBPLATFORMSITE)
        self.RR.create_association(subject=subplatformsite_id, predicate=PRED.hasModel, object=platform_model_id)

        platformdevice_id = self.create_platform_device(platform_model_id, first)
        self.RR.create_association(subject=subplatformsite_id, predicate=PRED.hasDevice, object=platformdevice_id)

        instrumentsite_id1 = self.create_instrumentsite(platformdevice_id, first)
        instrumentsite_id2 = self.create_instrumentsite(platformdevice_id, False)

        self.RR.create_association(subject=subplatformsite_id, predicate=PRED.hasSite, object=instrumentsite_id1)
        self.RR.create_association(subject=subplatformsite_id, predicate=PRED.hasSite, object=instrumentsite_id2)


        return subplatformsite_id


    def create_instrumentsite(self, platform_device_id, first=False):
        instrument_model_id = self.create_any(RT.InstrumentModel, first)

        instrumentsite_id = self.create_any(RT.InstrumentSite, first)

        self.RR.create_association(subject=instrumentsite_id, predicate=PRED.hasModel, object=instrument_model_id)

        instrument_device_id = self.create_instrumentdevice(instrument_model_id, first)
        self.RR.create_association(subject=platform_device_id, predicate=PRED.hasDevice, object=instrument_device_id)
        self.RR.create_association(subject=instrumentsite_id, predicate=PRED.hasDevice, object=instrument_device_id)

        return instrumentsite_id


    def create_platform_device(self, platform_model_id, first=False):
        platformdevice_id = self.create_any(RT.PlatformDevice, first)
        self.RR.create_association(subject=platformdevice_id, predicate=PRED.hasModel, object=platform_model_id)

        return platformdevice_id


    def create_instrumentdevice(self, instrument_model_id, first=False):
        instrumentdevice_id = self.create_any(RT.InstrumentDevice, first)
        self.RR.create_association(subject=instrumentdevice_id, predicate=PRED.hasModel, object=instrument_model_id)

        return instrumentdevice_id


    def create_dummy_structure(self):
        """
        Create two observatories.
         - each observatory has 2 subsites
         - each subsite has 2 more subsites
         - each of those subsites has 2 platform sites
         - each of those platform sites has a model and 2 sub- platform sites
         - each of those sub- platform sites has a model, matching platform device, and 2 instrument sites
         - each of those instrument sites has a model and matching instrument device

         One of each resource type (observatory all the way down to instrument device/model) is what we "care" about
          - it goes in the self.care dict
         All the rest go in the self.dontcare dict

         To manage subsite/platform multiplicity, we alias them in the dict... the proper hierarchy is:
         Observatory-Site-Subsite-PlatformSite-SubPlatformSite-InstrumentSite

         self.realtype[alias] gives the real resource type of an alias
        """
        self.create_observatory(True)
        self.create_observatory(False)

        for rt in [RT.Observatory, RT_SITE, RT.Subsite,
                   RT.PlatformSite, RT_SUBPLATFORMSITE, RT.PlatformDevice, RT.PlatformModel,
                   RT.InstrumentSite, RT.InstrumentDevice, RT.InstrumentModel
                   ]:
            self.assertIn(rt, self.care)

        self.expected_associations = [
            (RT.Observatory, PRED.hasSite, RT_SITE),
            (RT.Site, PRED.hasSite, RT.Subsite),
            (RT.Subsite, PRED.hasSite, RT.PlatformSite),
            (RT.PlatformSite, PRED.hasSite, RT_SUBPLATFORMSITE),
            (RT_SUBPLATFORMSITE, PRED.hasSite, RT.InstrumentSite),

            (RT_SUBPLATFORMSITE, PRED.hasModel, RT.PlatformModel),
            (RT_SUBPLATFORMSITE, PRED.hasDevice, RT.PlatformDevice),
            (RT.PlatformDevice, PRED.hasModel, RT.PlatformModel),

            (RT.InstrumentSite, PRED.hasModel, RT.InstrumentModel),
            (RT.InstrumentSite, PRED.hasDevice, RT.InstrumentDevice),
            (RT.InstrumentDevice, PRED.hasModel, RT.InstrumentModel)
        ]

        log.info("Verifying created structure")
        for (st, p, ot) in self.expected_associations:
            rst = self.realtype[st]
            rot = self.realtype[ot]
            s = self.care[st]
            o = self.care[ot]
            log.debug("searching %s->%s->%s as %s->%s->%s" % (st, p, ot, rst, p, rot))
            log.debug(" - expecting %s %s" % (rot, o))
            a = self.RR.find_associations(subject=s, predicate=p, object=o)
            if not (0 < len(a) < 3):
                a2 = self.RR.find_associations(subject=s, predicate=p)
                a2content = [("(%s %s)" % (alt.ot, alt.o)) for alt in a2]
                self.fail("Expected 1-2 associations for %s->%s->%s, got %s: %s" % (st, p, ot, len(a2), a2content))
            self.assertIn(o, [aa.o for aa in a])
        log.info("CREATED STRUCTURE APPEARS CORRECT ===============================")

    def simplify_assn_resource_ids(self, assn_list):
        count = 0

        lookup = {}

        retval = []

        for a in assn_list:
            if not a.s in lookup:
                lookup[a.s] = count
                count += 1
            if not a.o in lookup:
                lookup[a.o] = count
                count += 1
            retval.append(DotDict({"s":lookup[a.s], "st":a.st, "p":a.p, "o":lookup[a.o], "ot":a.ot}))

        return retval

    def describe_assn_graph(self, assn_list):
        return [("%s %s -> %s -> %s %s" % (a.st, a.s, a.p, a.ot, a.o)) for a in assn_list]


    #@unittest.skip('refactoring')
    def test_related_resource_crawler(self):
        """

        """
        self.create_dummy_structure()

        r = RelatedResourcesCrawler()

        # test the basic forward-backward searches
        for (st, p, ot) in self.expected_associations:
            rst = self.realtype[st]
            rot = self.realtype[ot]
            s = self.care[st]
            o = self.care[ot]

            test_sto_fn = r.generate_get_related_resources_fn(self.RR, [rot], {p: (True, False)})
            sto_crawl = test_sto_fn(s, 1) # depth of 1
            if 2 < len(sto_crawl): # we get 2 because of care/dontcare
                self.fail("got %s" % self.describe_assn_graph(self.simplify_assn_resource_ids(sto_crawl)))

            self.assertIn(o, [t.o for t in sto_crawl])

            test_ots_fn = r.generate_get_related_resources_fn(self.RR, [rst], {p: (False, True)})
            ots_crawl = test_ots_fn(o, 1) # depth of 1
            if 1 != len(ots_crawl):
                self.fail("got %s" % self.describe_assn_graph(self.simplify_assn_resource_ids(ots_crawl)))


        # test a nontrivial lookup, in which we extract resources related to an instrument device
        rw = []
        pd = {}

        # we want things related to an instrument device
        rw.append(RT.PlatformModel)
        rw.append(RT.InstrumentModel)
        rw.append(RT.PlatformDevice)
        rw.append(RT.InstrumentSite)
        rw.append(RT.PlatformSite)
        rw.append(RT.Subsite)
        rw.append(RT.Observatory)
        rw.append(RT.InstrumentDevice)
        pd[PRED.hasModel] = (True, True)
        pd[PRED.hasDevice] = (False, True)
        pd[PRED.hasSite] = (False, True)

        test_real_fn = r.generate_get_related_resources_fn(self.RR, resource_whitelist=rw, predicate_dictionary=pd)
        related = test_real_fn(self.care[RT.InstrumentDevice])

        log.debug("========= Result is:")
        for l in self.describe_assn_graph(self.simplify_assn_resource_ids(related)):
            log.debug("    %s", l)

        # check that we only got things we care about
        for a in related:
            # special case for platform model, because we don't care about the top-level platform's model
            #  so it will blow up if we don't ignore it.  if we got an extra platform model, we'd have an
            #  extra platform anyway... so this special case is safe.
            if a.st != RT.PlatformModel:
                self.assertIn(a.s, self.care.values(), "%s %s not cared about" % (a.st, a.s))

            if a.ot != RT.PlatformModel:
                self.assertIn(a.o, self.care.values(), "%s %s not cared about" % (a.ot, a.o))
