#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from nose.plugins.attrib import attr

from pyon.public import RT
from pyon.util.int_test import IonIntegrationTestCase
import math
from ion.processes.bootstrap.ion_loader import TESTED_DOC

from interface.services.coi.idatastore_service import DatastoreServiceClient, DatastoreServiceProcessClient
import unittest

@attr('INT', group='loader')
class TestLoader(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

    def test_lca_load(self):
        config = dict(op="load", scenario="R2_DEMO", attachments="res/preload/r2_ioc/attachments")
        self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=config)

        # make sure contact entries were created correctly
        res,_ = self.container.resource_registry.find_resources(RT.Org, id_only=False)
        self.assertTrue(len(res) > 1)
        found = False
        for org in res:
            if org.name=='RSN':
                self.assertFalse(found, msg='Found more than one Org "RSN" -- should have preloaded one')
                found = True
                self.assertFalse(org.contact is None)
                self.assertEquals('Delaney', org.contact.individual_name_family)
        self.assertTrue(found, msg='Did not find Org "RSN" -- should have been preloaded')

        res,_ = self.container.resource_registry.find_resources(RT.DataProduct, name='CTDBP-1012-REC1 Raw Endurance OR Offshore Benthic Pkg Demo', id_only=False)
        self.assertEquals(1, len(res))
        dp = res[0]
        formats = dp.available_formats
        self.assertEquals(2, len(formats))
        self.assertEquals('csv', formats[0])

        res,_ = self.container.resource_registry.find_resources(RT.InstrumentSite, id_only=False)
        self.assertTrue(len(res) > 1)
        found = False
        for site in res:
            if site.name=='Logical instrument 1 Demo':
                self.assertFalse(found, msg='Found more than one InstrumentSite "Logical instrument 1 Demo" -- should have preloaded one')
                found = True
                self.assertFalse(site.constraint_list is None)
                self.assertEquals(2, len(site.constraint_list))
                con = site.constraint_list[0]
                self.assertTrue(math.fabs(con.geospatial_latitude_limit_north-32.88)<.01)
                self.assertTrue(math.fabs(con.geospatial_longitude_limit_east+117.23)<.01)
                con = site.constraint_list[1]
                self.assertEquals('TemporalBounds', con.type_)
                # check that coordinate system was loaded
                self.assertFalse(site.coordinate_reference_system is None)

        self.assertTrue(found, msg='Did not find InstrumentSite "Logical instrument 1 Demo" -- should have been preloaded')


        # check that InstrumentDevice contacts are loaded
        res,_ = self.container.resource_registry.find_resources(RT.InstrumentDevice, name='CTD Simulator 1 Demo', id_only=False)
        self.assertTrue(len(res) == 1)
        self.assertTrue(len(res[0].contacts)==1)
        self.assertEquals('Orcutt', res[0].contacts[0].individual_name_family)

        # check has attachments
        attachments,_ = self.container.resource_registry.find_attachments(res[0]._id)
        self.assertTrue(len(attachments)>0)
