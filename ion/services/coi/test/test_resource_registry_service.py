#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from unittest import SkipTest
from nose.plugins.attrib import attr

from pyon.public import IonObject, RT, LCS, LCE, iex
from pyon.util.int_test import IonIntegrationTestCase

from ion.services.coi.resource_registry_service import ResourceRegistryService

@attr('INT', group='resource')
class TestResourceRegistry(IonIntegrationTestCase):

    def test_lifecycle(self):
        #self._start_container()
        #self._start_service("resource_registry",
        #                    "ion.services.coi.resource_registry_service.ResourceRegistryService",
        #                    {'resource_registry': {'persistent': True, 'force_clean': True}})

        rrs = ResourceRegistryService()
        rrs.CFG = {'resource_registry': {'persistent': True, 'force_clean': True}}
        rrs.init()

        att = IonObject("Attachment", name='mine', description='desc')

        rid,rev = rrs.create(att)

        att1 = rrs.read(rid)
        self.assertEquals(att1.name, att.name)
        self.assertEquals(att1.lcstate, LCS.DRAFT)

        new_state = rrs.execute_lifecycle_transition(rid, LCE.register)
        self.assertEquals(new_state, LCS.PLANNED)

        att2 = rrs.read(rid)
        self.assertEquals(att2.lcstate, LCS.PLANNED)

        self.assertRaises(iex.Inconsistent, rrs.execute_lifecycle_transition,
                                            resource_id=rid, transition_event=LCE.register)

        new_state = rrs.execute_lifecycle_transition(rid, LCE.develop, LCS.PLANNED)
        self.assertEquals(new_state, LCS.DEVELOPED)

        self.assertRaises(iex.Inconsistent, rrs.execute_lifecycle_transition,
                                            resource_id=rid, transition_event=LCE.develop, current_lcstate=LCS.PLANNED)
