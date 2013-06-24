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
from ion.services.sa.instrument.status_builder import AgentStatusBuilder, DriverTypingMethod
from ion.services.sa.observatory.observatory_management_service import ObservatoryManagementService
from ion.services.sa.test.helpers import any_old
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from mock import Mock
from nose.plugins.attrib import attr

from ooi.logging import log


#from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
import unittest
from pyon.core.exception import Unauthorized, NotFound
from pyon.ion.resource import RT, PRED
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase

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






@attr('UNIT', group='sa')
class TestAgentStatusBuilder(PyonTestCase):

    def setUp(self):
        self.ASB = AgentStatusBuilder(Mock())


    def test_crush_list(self):

        some_values = []
        self.assertEqual(DeviceStatusType.STATUS_UNKNOWN, self.ASB._crush_status_list(some_values))
        def add_and_check(onestatus):
            oldsize = len(some_values)
            some_values.append(onestatus)
            self.assertEqual(oldsize + 1, len(some_values))
            self.assertEqual(onestatus, self.ASB._crush_status_list(some_values))

        # each successive addition here should become the returned status -- they are done in increasing importance
        add_and_check(DeviceStatusType.STATUS_UNKNOWN)
        add_and_check(DeviceStatusType.STATUS_OK)
        add_and_check(DeviceStatusType.STATUS_WARNING)
        add_and_check(DeviceStatusType.STATUS_CRITICAL)

    def test_crush_dict(self):

        statuses = {AggregateStatusType.AGGREGATE_COMMS    : DeviceStatusType.STATUS_CRITICAL,
                    AggregateStatusType.AGGREGATE_POWER    : DeviceStatusType.STATUS_WARNING,
                    AggregateStatusType.AGGREGATE_DATA     : DeviceStatusType.STATUS_OK,
                    AggregateStatusType.AGGREGATE_LOCATION : DeviceStatusType.STATUS_UNKNOWN}

        ret = self.ASB._crush_status_dict(statuses)
        self.assertEqual(DeviceStatusType.STATUS_CRITICAL, ret)


    def test_set_status_computed_attributes(self):
        # set_status_computed_attributes(self, computed_attrs, values_dict=None, availability=None, reason=None)

        container = DotDict()

        log.debug("check null values")
        self.ASB.set_status_computed_attributes_notavailable(container,"this is the reason")

        for attr, enumval in reverse_mapping.iteritems():
            self.assertTrue(hasattr(container, attr))
            attrval = getattr(container, attr)
            self.assertIsInstance(attrval, ComputedIntValue)
            self.assertEqual(ComputedValueAvailability.NOTAVAILABLE, attrval.status)
            self.assertEqual("this is the reason", attrval.reason)

        log.debug("check provided values")

        statuses = {AggregateStatusType.AGGREGATE_COMMS    : DeviceStatusType.STATUS_CRITICAL,
                    AggregateStatusType.AGGREGATE_POWER    : DeviceStatusType.STATUS_WARNING,
                    AggregateStatusType.AGGREGATE_DATA     : DeviceStatusType.STATUS_OK,
                    AggregateStatusType.AGGREGATE_LOCATION : DeviceStatusType.STATUS_UNKNOWN}

        self.ASB.set_status_computed_attributes(container, statuses, ComputedValueAvailability.PROVIDED, "na")



        for attr, enumval in reverse_mapping.iteritems():
            self.assertTrue(hasattr(container, attr))
            attrval = getattr(container, attr)
            self.assertIsInstance(attrval, ComputedIntValue)
            self.assertEqual(ComputedValueAvailability.PROVIDED, attrval.status)
            self.assertEqual(statuses[enumval], attrval.value)


    def test_get_device_agent(self):
        # we expect to fail
        for bad in [None, "0"]:
            handle, reason = self.ASB.get_device_agent(bad)
            self.assertIsNone(handle)
            self.assertEqual(type(""), type("reason"))

        for exn in [Unauthorized, NotFound, AttributeError]:
            def make_exception(*args, **kwargs):
                raise exn()
            self.ASB._get_agent_client = make_exception
            handle, reason = self.ASB.get_device_agent("anything")
            self.assertIsNone(handle)
            self.assertNotEqual("", reason)

        # override resource agent client and try again
        self.ASB._get_agent_client = lambda *args, **kwargs : "happy"

        handle, reason = self.ASB.get_device_agent("anything")
        self.assertEqual("happy", handle)


    def test_compute_status_list(self):

        bad = self.ASB.compute_status_list(None, [1,2,3])
        self.assertIsInstance(bad, ComputedListValue)

        statusd1 = {AggregateStatusType.AGGREGATE_COMMS    : DeviceStatusType.STATUS_CRITICAL,
                    AggregateStatusType.AGGREGATE_POWER    : DeviceStatusType.STATUS_WARNING,
                    AggregateStatusType.AGGREGATE_DATA     : DeviceStatusType.STATUS_OK,
                    AggregateStatusType.AGGREGATE_LOCATION : DeviceStatusType.STATUS_UNKNOWN}

        statusd2 = {AggregateStatusType.AGGREGATE_POWER    : DeviceStatusType.STATUS_WARNING,
                    AggregateStatusType.AGGREGATE_DATA     : DeviceStatusType.STATUS_OK,
                    AggregateStatusType.AGGREGATE_LOCATION : DeviceStatusType.STATUS_UNKNOWN}

        childstatus = {"d1": statusd1, "d2": statusd2}

        ret = self.ASB.compute_status_list(childstatus, ["d1", "d2"])
        self.assertIsInstance(ret, ComputedListValue)
        self.assertEqual(ComputedValueAvailability.PROVIDED, ret.status)
        self.assertEqual([DeviceStatusType.STATUS_CRITICAL, DeviceStatusType.STATUS_WARNING], ret.value)





@attr('INT', group='sa')
@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
class TestAgentStatusBuilderIntegration(IonIntegrationTestCase):

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


    def _make_status(self, bad_items_dict):
        ret = {}
        for k in reverse_mapping.values():
            if k in bad_items_dict:
                ret[k] = bad_items_dict[k]
            else:
                ret[k] = DeviceStatusType.STATUS_OK

        return ret


    def _setup_statuses(self):

        device_agents = {}

        IMS_SVC = self._get_svc(InstrumentManagementService)
        OMS_SVC = self._get_svc(ObservatoryManagementService)

        self.IMS_ASB = self._get_specific_attr(IMS_SVC, AgentStatusBuilder)
        self.OMS_ASB = self._get_specific_attr(OMS_SVC, AgentStatusBuilder)

        assert self.IMS_ASB
        assert self.OMS_ASB

        self.IMS_ASB.RR2 = IMS_SVC.RR2
        self.OMS_ASB.RR2 = OMS_SVC.RR2

        # create one tree of devices
        self.grandparent1_device_id = self.RR2.create(any_old(RT.PlatformDevice))
        self.parent1_device_id = self.RR2.create(any_old(RT.PlatformDevice))
        self.child1_device_id = self.RR2.create(any_old(RT.InstrumentDevice))
        self.RR2.create_association(self.grandparent1_device_id, PRED.hasDevice, self.parent1_device_id)
        self.RR2.create_association(self.parent1_device_id, PRED.hasDevice, self.child1_device_id)
        g1_agent = FakeAgent()
        g1_stat = self._make_status({AggregateStatusType.AGGREGATE_COMMS: DeviceStatusType.STATUS_UNKNOWN})
        p1_stat = self._make_status({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_CRITICAL})
        c1_stat = self._make_status({AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_WARNING})
        g1_agent.set_agent("aggstatus", g1_stat)
        g1_agent.set_agent("child_agg_status", {self.parent1_device_id: p1_stat,
                                                self.child1_device_id: c1_stat})

        device_agents[self.grandparent1_device_id] = g1_agent

        c1_agent = FakeAgent()
        c1_agent.set_agent("aggstatus", c1_stat)
        device_agents[self.child1_device_id] = c1_agent


        # create second tree of devices
        self.grandparent2_device_id = self.RR2.create(any_old(RT.PlatformDevice))
        self.parent2_device_id = self.RR2.create(any_old(RT.PlatformDevice))
        self.child2_device_id = self.RR2.create(any_old(RT.InstrumentDevice))
        self.RR2.create_association(self.grandparent2_device_id, PRED.hasDevice, self.parent2_device_id)
        self.RR2.create_association(self.parent2_device_id, PRED.hasDevice, self.child2_device_id)
        g2_agent = FakeAgent()
        g2_stat = self._make_status({AggregateStatusType.AGGREGATE_COMMS: DeviceStatusType.STATUS_UNKNOWN})
        p2_stat = self._make_status({AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_CRITICAL})
        c2_stat = self._make_status({AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_WARNING})
        g2_agent.set_agent("aggstatus", g2_stat)
        g2_agent.set_agent("child_agg_status", {self.parent2_device_id: p2_stat,
                                                self.child2_device_id: c2_stat})

        device_agents[self.grandparent2_device_id] = g2_agent



        def my_get_agent_client(device_id, **kwargs):
            try:
                return device_agents[device_id]
            except KeyError:
                raise NotFound

        self.IMS_ASB._get_agent_client = my_get_agent_client


    @unittest.skip("hasDevice rollup is no longer supported")
    def test_get_device_rollup(self):
        iext = self.IMS.get_instrument_device_extension(self.child1_device_id)
        istatus = self._make_status({AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_WARNING})
        for attr, key in reverse_mapping.iteritems():
            log.debug("reading iext.computed.%s to get key '%s'", attr, key)
            compattr = getattr(iext.computed, attr)
            self.assertEqual(ComputedValueAvailability.PROVIDED, compattr.status)
            self.assertEqual(istatus[key], compattr.value)

        # this tests that the rollup is being completed successfully
        pext = self.IMS.get_platform_device_extension(self.grandparent1_device_id)
        pstatus = self._make_status({AggregateStatusType.AGGREGATE_COMMS: DeviceStatusType.STATUS_UNKNOWN,
                                     AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_CRITICAL,
                                     AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_WARNING})

        log.debug("expected status is %s", pstatus)
        for attr, key in reverse_mapping.iteritems():
            log.debug("reading pext.computed.%s to get key '%s'", attr, key)
            compattr = getattr(pext.computed, attr)
            self.assertEqual(ComputedValueAvailability.PROVIDED, compattr.status)
            log.debug("pext.computed.%s.value = %s", attr, compattr.value)
            self.assertEqual(pstatus[key], compattr.value)


    def test_get_cumulative_status_dict(self):

        # approved way
        self.IMS_ASB.dtm = DriverTypingMethod.ByRR
        status, _ = self.IMS_ASB.get_cumulative_status_dict(self.grandparent1_device_id)
        self.assertIn(self.grandparent1_device_id, status)
        self.assertIn(self.parent1_device_id, status)
        self.assertIn(self.child1_device_id, status)

        status, _ = self.IMS_ASB.get_cumulative_status_dict(self.child1_device_id)
        self.assertIn(self.child1_device_id, status)

        # bad client
        def bad_client(*args, **kwargs):
            raise NotFound
        self.IMS_ASB._get_agent_client = bad_client
        status, reason = self.IMS_ASB.get_cumulative_status_dict("anything")
        self.assertIsNone(status)
        self.assertNotEqual("", reason)

        # good client
        self.IMS_ASB.dtm = DriverTypingMethod.ByException
        fake_agent = FakeAgent()
        fake_agent.set_agent("aggstatus", {"foo": "bar"})
        self.IMS_ASB._get_agent_client = lambda *args, **kwargs : fake_agent
        status, reason = self.IMS_ASB.get_cumulative_status_dict("anything")
        self.assertNotEqual("", reason)
        self.assertIn("anything", status)
        astatus = status["anything"]
        self.assertIn("foo", astatus)
        self.assertEqual("bar", astatus["foo"])

        # good client
        self.IMS_ASB.dtm = DriverTypingMethod.ByAgent
        fake_agent = FakeAgent()
        fake_agent.set_agent("aggstatus", {"foo": "bar"})
        self.IMS_ASB._get_agent_client = lambda *args, **kwargs : fake_agent
        status, reason = self.IMS_ASB.get_cumulative_status_dict("anything")
        self.assertIn("anything", status)
        astatus = status["anything"]
        self.assertIn("foo", astatus)
        self.assertEqual("bar", astatus["foo"])

        # bad client
        self.IMS_ASB.dtm = DriverTypingMethod.ByException
        fake_agent = FakeAgentErroring(Unauthorized)
        self.IMS._get_agent_client = lambda *args, **kwargs : fake_agent
        status, reason = self.IMS_ASB.get_cumulative_status_dict("anything")
        self.assertIsNone(status)
        self.assertNotEqual("", reason)




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
