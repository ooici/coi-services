#!/usr/bin/env python

"""
@author Ian Katz
"""
import os
from interface.objects import DeviceStatusType, AggregateStatusType, ComputedIntValue, ComputedValueAvailability, ComputedListValue
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from ion.services.sa.instrument.instrument_management_service import InstrumentManagementService
from ion.services.sa.instrument.status_builder import AgentStatusBuilder
from ion.services.sa.observatory.observatory_management_service import ObservatoryManagementService
from ion.services.sa.test.helpers import any_old
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from nose.plugins.attrib import attr

from pyon.public import log


#from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
import unittest
from pyon.core.exception import BadRequest
from pyon.ion.resource import RT, PRED
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase


unittest # block pycharm inspection

reverse_mapping = {"communications_status_roll_up" : AggregateStatusType.AGGREGATE_COMMS,
                   "power_status_roll_up"          : AggregateStatusType.AGGREGATE_POWER,
                   "data_status_roll_up"           : AggregateStatusType.AGGREGATE_DATA,
                   "location_status_roll_up"       : AggregateStatusType.AGGREGATE_LOCATION}


# to fake a resource agent client
class FakeAgent(object):

    def __init__(self):
        self.cmds = {}

    def get_agent(self, cmds):
        return dict([(c, self.cmds.get(c, None)) for c in cmds])

    def set_agent(self, key, val):
        self.cmds[key] = val

    def get_capabilities(self):
        return [DotDict({"name": k}) for k in self.cmds.keys()]

    def get_agent_state(self):
        return "FAKE"


class FakeAgentErroring(FakeAgent):
    def __init__(self, exn):
        self.exn = exn

    def get_agent(self, cmds):
        raise self.exn("FakeAgentErroring")


@attr('INT', group='sa')
@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
class TestRollups(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()

        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.RR   = ResourceRegistryServiceClient(node=self.container.node)
        self.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.OMS = ObservatoryManagementServiceClient(node=self.container.node)
        self.RR2 = EnhancedResourceRegistryClient(self.RR)


        self._setup_statuses()


    def _make_status(self, bad_items_dict=None):
        if bad_items_dict is None:
            bad_items_dict = {}
        ret = {}
        for k in reverse_mapping.values():
            if k in bad_items_dict:
                ret[k] = bad_items_dict[k]
            else:
                ret[k] = DeviceStatusType.STATUS_OK

        return ret


    def _setup_statuses(self):
        # set up according to https://docs.google.com/drawings/d/1kZ_L4xr4Be0OdqMDX6tiI50hROgvLHU4HcnD7e_NIKE/pub?w=1200z
        # https://confluence.oceanobservatories.org/display/syseng/CIAD+SA+OV+Observatory+Status+and+Events

        device_agents = {}

        ms = self._make_status

        # override the default "get agent" function and resource registyr
        IMS_SVC = self._get_svc(InstrumentManagementService)
        OMS_SVC = self._get_svc(ObservatoryManagementService)
        self.IMS_ASB = self._get_specific_attr(IMS_SVC, AgentStatusBuilder)
        self.OMS_ASB = self._get_specific_attr(OMS_SVC, AgentStatusBuilder)
        assert self.IMS_ASB
        assert self.OMS_ASB
        self.IMS_ASB.RR2 = IMS_SVC.RR2
        self.OMS_ASB.RR2 = OMS_SVC.RR2

        # create org
        org_id = self.OMS.create_marine_facility(any_old(RT.Org))
        obs_id = self.OMS.create_observatory(any_old(RT.Observatory), org_id)

        # create instrument and platform devices and sites
        pst = dict([(i + 1, self.RR2.create(any_old(RT.PlatformSite))) for i in range(8)])
        pdv = dict([(i + 1, self.RR2.create(any_old(RT.PlatformDevice))) for i in range(11)])
        ist = dict([(i + 1, self.RR2.create(any_old(RT.InstrumentSite))) for i in range(6)])
        idv = dict([(i + 1, self.RR2.create(any_old(RT.InstrumentDevice))) for i in range(6)])

        # create associations
        has_site = [
            (obs_id, pst[2]),
            (pst[2], pst[1]),
            (pst[1], ist[1]),
            (pst[2], pst[3]),
            (pst[3], ist[2]),
            (pst[3], ist[3]),
            (obs_id, pst[4]),
            (pst[4], pst[5]),
            (pst[4], pst[6]),
            (pst[6], pst[7]),
            (pst[7], ist[4]),
            (pst[6], pst[8]),
            (pst[8], ist[5]),
            (pst[8], ist[6]),
        ]

        has_device = [
            (pst[2], pdv[2]),
            (pst[1], pdv[1]),
            (ist[1], idv[1]),
            (pst[3], pdv[3]),
            (pdv[3], idv[2]),
            (pdv[3], idv[3]),
            (ist[2], idv[2]),
            (ist[3], idv[3]),
            (pst[4], pdv[4]),
            (pdv[4], pdv[5]),
            (pdv[5], pdv[6]),
            (pdv[5], pdv[7]),
            (pdv[7], idv[4]),
            (pst[6], pdv[5]),
            (pst[7], pdv[6]),
            (pst[8], pdv[7]),
            (ist[5], idv[4]),
            (pdv[8], pdv[9]),
            (pdv[9], pdv[10]),
            (pdv[10], idv[5]),
            (pdv[9], pdv[11]),
            (pdv[11], idv[6]),
        ]

        for (s, o) in has_site:
            self.RR2.create_association(s, PRED.hasSite, o)
            self.assertIn(o, self.RR2.find_objects(s, PRED.hasSite, None, id_only=True))

        for (s, o) in has_device:
            self.RR2.create_association(s, PRED.hasDevice, o)
            self.assertIn(o, self.RR2.find_objects(s, PRED.hasDevice, None, id_only=True))

        self.assertEqual(pdv[1], self.RR2.find_platform_device_id_of_platform_site_using_has_device(pst[1]))


        # preparing to create fake agents, shortcut to status names
        o = DeviceStatusType.STATUS_OK
        w = DeviceStatusType.STATUS_WARNING
        c = DeviceStatusType.STATUS_CRITICAL

        # expected status for instruments and platforms
        idv_stat = ["ignore", c, o, w, o, w, c]

        # make the fake instrument agents, with their statuses
        for i, id in idv.iteritems():
            idv_agent = FakeAgent()
            idv_agent.set_agent("aggstatus", ms({AggregateStatusType.AGGREGATE_DATA: idv_stat[i]}))
            device_agents[id] = idv_agent


        # create fake agents for platforms

        pdv1_agent = FakeAgent()
        pdv1_agent.set_agent("aggstatus", ms())
        pdv1_agent.set_agent("child_agg_status", {})
        device_agents[pdv[1]] = pdv1_agent

        pdv2_agent = FakeAgent()
        pdv2_agent.set_agent("aggstatus", ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_WARNING}))
        pdv2_agent.set_agent("child_agg_status", {})
        device_agents[pdv[2]] = pdv2_agent

        pdv3_agent = FakeAgent()
        pdv3_agent.set_agent("aggstatus", ms())
        pdv3_agent.set_agent("child_agg_status",
                {
                idv[2]: ms(),
                idv[3]: ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_WARNING}),
            })
        device_agents[pdv[3]] = pdv3_agent

        pdv4_agent = FakeAgent()
        pdv4_agent.set_agent("aggstatus", ms())
        pdv4_agent.set_agent("child_agg_status",
                {
                pdv[5]: ms(),
                pdv[6]: ms(),
                pdv[7]: ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_WARNING}),
                idv[4]: ms(),
                })
        device_agents[pdv[4]] = pdv4_agent

        pdv5_agent = FakeAgent()
        pdv5_agent.set_agent("aggstatus", ms())
        pdv5_agent.set_agent("child_agg_status",
                {
                pdv[6]: ms(),
                pdv[7]: ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_WARNING}),
                idv[4]: ms(),
                })
        device_agents[pdv[5]] = pdv5_agent

        pdv6_agent = FakeAgent()
        pdv6_agent.set_agent("aggstatus", ms())
        pdv6_agent.set_agent("child_agg_status", {})
        device_agents[pdv[6]] = pdv6_agent

        pdv7_agent = FakeAgent()
        pdv7_agent.set_agent("aggstatus", ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_WARNING}))
        pdv7_agent.set_agent("child_agg_status",
                {
                idv[4]: ms(),
                })
        device_agents[pdv[7]] = pdv7_agent

        pdv8_agent = FakeAgent()
        pdv8_agent.set_agent("aggstatus", ms())
        pdv8_agent.set_agent("child_agg_status",
                {
                pdv[9]: ms(),
                pdv[10]: ms(),
                idv[5]: ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_WARNING}),
                pdv[11]: ms(),
                idv[6]: ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_CRITICAL}),
                })
        device_agents[pdv[8]] = pdv8_agent

        pdv9_agent = FakeAgent()
        pdv9_agent.set_agent("aggstatus", ms())
        pdv9_agent.set_agent("child_agg_status",
                {
                pdv[10]: ms(),
                idv[5]: ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_WARNING}),
                pdv[11]: ms(),
                idv[6]: ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_CRITICAL}),
                })
        device_agents[pdv[9]] = pdv9_agent

        pdv10_agent = FakeAgent()
        pdv10_agent.set_agent("aggstatus", ms())
        pdv10_agent.set_agent("child_agg_status",
                {
                idv[5]: ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_WARNING}),
                })
        device_agents[pdv[10]] = pdv10_agent

        pdv11_agent = FakeAgent()
        pdv11_agent.set_agent("aggstatus", ms())
        pdv11_agent.set_agent("child_agg_status",
                {
                idv[6]: ms({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_CRITICAL}),
                })
        device_agents[pdv[8]] = pdv11_agent

        self.device_agents = device_agents

        self.IMS_ASB._get_agent_client = self.my_get_agent_client
        self.OMS_ASB._get_agent_client = self.my_get_agent_client

        # save created ids
        self.org_id = org_id
        self.obs_id = obs_id
        self.pst = pst
        self.pdv = pdv
        self.ist = ist
        self.idv = idv

        log.info("org ID:                 %s", org_id)
        log.info("observatory ID:         %s", obs_id)
        for k, v in self.pst.iteritems():
            log.info("platform site ID %s:     %s", k, v)
        for k, v in self.ist.iteritems():
            log.info("instrument site ID %s:   %s", k, v)
        for k, v in self.pdv.iteritems():
            log.info("platform device ID %s:   %s", k, v)
        for k, v in self.idv.iteritems():
            log.info("instrument device ID %s: %s", k, v)



    # define a function to get the agent client, using our fake agents
    def my_get_agent_client(self, device_id, **kwargs):
        try:
            return self.device_agents[device_id]
        except KeyError:
            raise BadRequest("Tried to retrieve status for undefined device '%s'" % device_id)


    # some quick checks to help us debug the structure and statuses, to isolate problems
    def check_structure_assumptions(self):
        # check that all objects exist in the RR
        for adict in [self.pst, self.pdv, self.ist, self.idv]:
            for id in adict: assert id

        # pst1 should have status critical, pdev1 should be ok, and idv1/ist1 should be critical
        self.assertEqual(self.pdv[1], self.RR2.find_platform_device_id_of_platform_site_using_has_device(self.pst[1]))
        self.assertEqual(self.ist[1], self.RR2.find_instrument_site_id_of_platform_site_using_has_site(self.pst[1]))
        self.assertEqual(self.idv[1], self.RR2.find_instrument_device_id_of_instrument_site_using_has_device(self.ist[1]))
        self.assertEqual(DeviceStatusType.STATUS_CRITICAL,
                         self.my_get_agent_client(self.idv[1]).get_agent(["aggstatus"])["aggstatus"][AggregateStatusType.AGGREGATE_DATA])

        # pdv4 should have status warning, coming from pdv7
        self.assertEqual(self.pdv[5], self.RR2.find_platform_device_id_of_platform_device_using_has_device(self.pdv[4]))
        self.assertIn(self.pdv[6], self.RR2.find_platform_device_ids_of_platform_device_using_has_device(self.pdv[5]))
        self.assertIn(self.pdv[7], self.RR2.find_platform_device_ids_of_platform_device_using_has_device(self.pdv[5]))
        self.assertEqual(self.idv[4], self.RR2.find_instrument_device_id_of_platform_device_using_has_device(self.pdv[7]))
        self.assertEqual(DeviceStatusType.STATUS_OK,
                         self.my_get_agent_client(self.idv[4]).get_agent(["aggstatus"])["aggstatus"][AggregateStatusType.AGGREGATE_DATA])
        self.assertEqual(DeviceStatusType.STATUS_WARNING,
                         self.my_get_agent_client(self.pdv[7]).get_agent(["aggstatus"])["aggstatus"][AggregateStatusType.AGGREGATE_DATA])


    @unittest.skip("errors in outil prevent this from passing")
    def test_complex_rollup_structure(self):

        self.check_structure_assumptions()

        o = DeviceStatusType.STATUS_OK
        u = DeviceStatusType.STATUS_UNKNOWN
        w = DeviceStatusType.STATUS_WARNING
        c = DeviceStatusType.STATUS_CRITICAL

        pst_stat = ["ignore", c, c, w, w, u, w, o, w]
        pdv_stat = ["ignore", o, w, w, w, w, o, w, c, c, w, c]
        ist_stat = ["ignore", c, o, w, u, o, u]
        idv_stat = ["ignore", c, o, w, o, w, c]

        for i, id in self.idv.iteritems():
            label = "InstrumentDevice %s" % i
            log.info("Checking rollup of %s", label)
            self.assertProperRollup(label, self.IMS.get_instrument_device_extension(id), idv_stat[i])

        for i, id in self.ist.iteritems():
            label = "InstrumentSite %s" % i
            log.info("Checking rollup of %s", label)
            self.assertProperRollup(label, self.OMS.get_site_extension(id), ist_stat[i])

        for i, id in self.pdv.iteritems():
            label = "PlatformDevice %s" % i
            log.info("Checking rollup of %s", label)
            self.assertProperRollup(label, self.IMS.get_platform_device_extension(id), pdv_stat[i])

        for i, id in self.pst.iteritems():
            label = "PlatformSite %s" % i
            log.info("Checking rollup of %s", label)
            self.assertProperRollup(label, self.OMS.get_site_extension(id), pst_stat[i])

        #TODO: check observatory and org rollups!



    #TODO: REMOVE THIS TEST when test_complex_rollup_structure is fixed
    #@unittest.skip("phasing out")
    def test_complex_rollup_structure_partially(self):

        o = DeviceStatusType.STATUS_OK
        u = DeviceStatusType.STATUS_UNKNOWN
        w = DeviceStatusType.STATUS_WARNING
        c = DeviceStatusType.STATUS_CRITICAL

        idv_stat = ["ignore", c, o, w, o, w, c]
        ist_stat = ["ignore", c, o, w, u, o, u]

        for i, id in self.idv.iteritems():
            label = "InstrumentDevice %s" % i
            log.info("Checking rollup of %s", label)
            self.assertProperRollup(label, self.IMS.get_instrument_device_extension(id), idv_stat[i])

        for i, id in self.ist.iteritems():
            label = "InstrumentSite %s" % i
            log.info("Checking rollup of %s", label)
            self.assertProperRollup(label, self.OMS.get_site_extension(id), ist_stat[i])



    def assertProperRollup(self, label, extended_resource, status):
        m = DeviceStatusType._str_map
        s = extended_resource.computed.data_status_roll_up.status
        v = extended_resource.computed.data_status_roll_up.value
        self.assertEqual(ComputedValueAvailability.PROVIDED, s)
        message = "Expected rollup status of %s to be %s but got %s" % (label, m[status], m.get(v, "?? %s" % v))
        self.assertEqual(status, v, message)

    # get an object of a specific type from within another python object
    def _get_specific_attr(self, parent_obj, attrtype):
        for d in dir(parent_obj):
            a = getattr(parent_obj, d)
            if isinstance(a, attrtype):
                return a

        return None


    # get a service of a given type from the capability container
    def _get_svc(self, service_cls):
        # get service from container proc manager
        relevant_services = [
        item[1] for item in self.container.proc_manager.procs.items()
        if isinstance(item[1], service_cls)
        ]

        assert (0 < len(relevant_services)),\
        "no services of type '%s' found running in container!" % service_cls

        service_itself = relevant_services[0]
        assert service_itself
        return service_itself
