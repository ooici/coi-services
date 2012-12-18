#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from nose.plugins.attrib import attr
from pyon.public import RT, PRED
from pyon.util.int_test import IonIntegrationTestCase
import math
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
import unittest

@attr('INT', group='loader')
class TestLoader(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.ingestion_management = IngestionManagementServiceClient()

    def test_lca_load(self):
        config = dict(op="load", scenario="BASE,R2_DEMO", attachments="res/preload/r2_ioc/attachments")
        self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=config)

        # make sure contact entries were created correctly
        res,_ = self.container.resource_registry.find_resources(RT.Org, id_only=False)
        self.assertTrue(len(res) > 1)
        found = False
        for org in res:
            if org.name=='Regional_Scale_Nodes':
                self.assertFalse(found, msg='Found more than one Org "RSN" -- should have preloaded one')
                found = True
                self.assertFalse(org.contacts is None)
                self.assertEquals('Delaney', org.contacts[0].individual_name_family)
                self.assertEquals('primary', org.contacts[0].roles[0])
        self.assertTrue(found, msg='Did not find Org "RSN" -- should have been preloaded')

        # check data product
#        res,_ = self.container.resource_registry.find_resources(RT.DataProduct, name='CTDBP-1012-REC1 Raw', id_only=False)
        target_name = 'NSF Demo - CTDBP-1012-REC1 Raw'
        res,_ = self.container.resource_registry.find_resources(RT.DataProduct, name=target_name, id_only=False)
        self.assertEquals(1, len(res), msg='failed to find data product: '+target_name)
        dp = res[0]
        formats = dp.available_formats
        self.assertEquals(2, len(formats))
        self.assertEquals('csv', formats[0])
        # should be persisted
        streams, _ = self.container.resource_registry.find_objects(dp._id, PRED.hasStream, RT.Stream, True)
        self.assertTrue(streams)
        self.assertEquals(1, len(streams))
        self.assertTrue(self.ingestion_management.is_persisted(streams[0]))
        self.assertTrue(math.fabs(dp.geospatial_bounds.geospatial_latitude_limit_north-44.7)<.01)

        # but L1 data product should not be persisted
        res,_ = self.container.resource_registry.find_resources(RT.DataProduct, name='Conductivity L1', id_only=True)
        self.assertEquals(1, len(res))
        dpid = res[0]
        streams, _ = self.container.resource_registry.find_objects(dpid, PRED.hasStream, RT.Stream, True)
        self.assertEquals(1, len(streams))
        self.assertTrue(streams)
        self.assertFalse(self.ingestion_management.is_persisted(streams[0]))

        res,_ = self.container.resource_registry.find_resources(RT.InstrumentSite, id_only=False)
        self.assertTrue(len(res) > 1)
        found = False
        for site in res:
            if site.name=='Instrument site 1 Demo':
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
        self.assertTrue(len(res[0].contacts)==2)
        self.assertEquals('Ampe', res[0].contacts[0].individual_name_family)

        # check has attachments
        attachments,_ = self.container.resource_registry.find_attachments(res[0]._id)
        self.assertTrue(len(attachments)>0)

        # check for platform agents
        res,_ = self.container.resource_registry.find_resources(RT.PlatformAgent, id_only=False)
        self.assertTrue(len(res)>0)
        agent = None
        for pa in res:
            if pa.name=='RSN Platform Agent':
                agent = pa
                break
        self.assertTrue(agent)
        self.assertEquals(2, len(agent.stream_configurations))
        parsed = agent.stream_configurations[1]
        self.assertEquals('platform_eng_parsed', parsed.parameter_dictionary_name)

        # check for platform agents
        res,_ = self.container.resource_registry.find_resources(RT.PlatformAgentInstance, id_only=False)
        self.assertTrue(len(res)>0)
        agent_instance = None
        for pai in res:
            if pai.name=='Platform Agent':
                agent_instance = pai
                break
        self.assertTrue(agent_instance)
