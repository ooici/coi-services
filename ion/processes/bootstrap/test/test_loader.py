#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from nose.plugins.attrib import attr
from pyon.public import RT, PRED
from pyon.util.int_test import IonIntegrationTestCase
import math
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
import unittest
from ion.processes.bootstrap.ion_loader import TESTED_DOC, IONLoader

class TestLoader(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.ingestion_management = IngestionManagementServiceClient()

    def assert_can_load(self, scenarios, loadui=False, loadooi=False,
            path=TESTED_DOC, ui_path='default'):
        """ perform preload for given scenarios and raise exception if there is a problem with the data """
        config = dict(op="load",
                      scenario=scenarios,
                      attachments="res/preload/r2_ioc/attachments",
                      loadui=loadui,
                      loadooi=loadooi,
                      path=path, ui_path=ui_path,
                      assets='res/preload/r2_ioc/ooi_assets',
                      bulk=loadooi,
                      ooiexclude='DataProduct,DataProductLink')
        self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=config)

    @attr('PRELOAD')
    def test_ui_valid(self):
        """ make sure UI assets are valid using DEFAULT_UI_ASSETS = 'https://userexperience.oceanobservatories.org/database-exports/' """
        self.assert_can_load("BASE,BETA", loadui=True, ui_path='default')

    @attr('PRELOAD')
    def test_ui_candidates_valid(self):
        """ make sure UI assets are valid using DEFAULT_UI_ASSETS = 'https://userexperience.oceanobservatories.org/database-exports/Candidates' """
        self.assert_can_load("BASE,BETA", loadui=True, ui_path='candidate')

    @attr('PRELOAD')
    def test_assets_valid(self):
        """ make sure can load asset DB """
        self.assert_can_load("BASE,BETA,DEVS", path='master', loadooi=True)

    @attr('PRELOAD')
    def test_alpha_valid(self):
        """ make sure R2_DEMO scenario in master google doc
            is valid and self-contained (doesn't rely on rows from other scenarios except BASE and BETA)
            NOTE: test will pass/fail based on current google doc, not just code changes.
        """
        self.assert_can_load("BASE,BETA,ALPHA_SYS", path='master')

    @attr('PRELOAD')
    def test_beta_valid(self):
        """ make sure R2_DEMO scenario in master google doc
            is valid and self-contained (doesn't rely on rows from other scenarios except BASE and BETA)
            NOTE: test will pass/fail based on current google doc, not just code changes.
        """
        self.assert_can_load("BASE,BETA,BETA_SYS", path='master')

    @attr('PRELOAD')
    def test_devs_valid(self):
        """ make sure DEVS scenario in master google doc
            is valid and self-contained (doesn't rely on rows from other scenarios except BASE and BETA)
            NOTE: test will pass/fail based on current google doc, not just code changes.
        """
        self.assert_can_load("BASE,BETA,DEVS", path='master')

    def find_object_by_name(self, name, resource_type):
        objects,_ = self.container.resource_registry.find_resources(resource_type, id_only=False)
        self.assertGreaterEqual(len(objects), 1)

        filtered_objs = [obj for obj in objects if obj.name == name]
        self.assertEquals(len(filtered_objs), 1)

        return filtered_objs[0]


    @attr('UNIT', group='loader')
    def test_parse_alarms(self):
        loader = IONLoader()
        out = loader._parse_alarm('5<temp<10')
        self.assertEqual('<', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual('<', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])

        out = loader._parse_alarm('5<=temp<10')
        self.assertEqual('<=', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual('<', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])

        out = loader._parse_alarm('5<temp<=10')
        self.assertEqual('<', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual('<=', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])

        out = loader._parse_alarm('5<=temp<=10')
        self.assertEqual('<=', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual('<=', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])

        out = loader._parse_alarm('5<temp')
        self.assertEqual('<', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual(3, len(out), msg='value: %r'%out)
        self.assertEqual('temp', out['value_id'])

        out = loader._parse_alarm('5<=temp')
        self.assertEqual('<=', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual('temp', out['value_id'])
        self.assertEqual(3, len(out))

        out = loader._parse_alarm('temp<10')
        self.assertEqual('<', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])
        self.assertEqual(3, len(out))

        out = loader._parse_alarm('temp<=10')
        self.assertEqual('<=', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])
        self.assertEqual(3, len(out))

    @attr('INT', group='loader')
    @attr('SMOKE', group='loader')
    def test_row_values(self):
        """ use only rows from NOSE scenario for specific names and details included in this test.
            rows in NOSE may rely on entries in BASE and BETA scenarios,
            but should not specifically test values from those scenarios.
        """

        # first make sure this scenario loads successfully
        self.assert_can_load("BASE,BETA,NOSE")

        # check for an Org
        org = self.find_object_by_name('CASPER', RT.Org)
        self.assertFalse(org.contacts is None)
        self.assertEquals('Userbrough', org.contacts[0].individual_name_family)
        self.assertEquals('primary', org.contacts[0].roles[0])

        # check data product
        dp = self.find_object_by_name('Test DP L0 CTD', RT.DataProduct)
        formats = dp.available_formats
        self.assertEquals(2, len(formats))
        self.assertEquals('csv', formats[0])
        # should be persisted
        streams, _ = self.container.resource_registry.find_objects(dp._id, PRED.hasStream, RT.Stream, True)
        self.assertTrue(streams)
        self.assertEquals(1, len(streams))
        self.assertTrue(self.ingestion_management.is_persisted(streams[0]))
        self.assertAlmostEqual(32.88237, dp.geospatial_bounds.geospatial_latitude_limit_north,places=3)

        # but L1 data product should not be persisted
        dp = self.find_object_by_name('Test DP L1 conductivity', RT.DataProduct)
        streams, _ = self.container.resource_registry.find_objects(dp._id, PRED.hasStream, RT.Stream, True)
        self.assertEquals(1, len(streams))
        self.assertTrue(streams)
        self.assertFalse(self.ingestion_management.is_persisted(streams[0]))

        site = self.find_object_by_name('Test Instrument Site', RT.InstrumentSite)
        self.assertFalse(site.constraint_list is None)
        self.assertEquals(2, len(site.constraint_list))
        con = site.constraint_list[0]
        self.assertAlmostEqual(  32.88237, con.geospatial_latitude_limit_north, places=3)
        self.assertAlmostEqual(-117.23214, con.geospatial_longitude_limit_east, places=3)
        con = site.constraint_list[1]
        self.assertEquals('TemporalBounds', con.type_)
        # check that coordinate system was loaded
        self.assertFalse(site.coordinate_reference_system is None)

        # check that InstrumentDevice contacts are loaded
        dev = self.find_object_by_name('Unit Test SMB37', RT.InstrumentDevice)
        self.assertTrue(len(dev.contacts)==2)
        self.assertEquals('Userbrough', dev.contacts[0].individual_name_family)

        # check has attachments
        attachments = self.container.resource_registry.find_attachments(dev._id)
        self.assertTrue(len(attachments)>0)

        # check for platform agents
        agent = self.find_object_by_name('Unit Test Platform Agent', RT.PlatformAgent)
        self.assertEquals(2, len(agent.stream_configurations))
        parsed = agent.stream_configurations[1]
#        self.assertEquals('platform_eng_parsed', parsed.parameter_dictionary_name)
        self.assertEquals('ctd_parsed_param_dict', parsed.parameter_dictionary_name)
        # check that alarm was added to StreamConfig
        self.assertEquals(1, len(parsed.alarms), msg='alarms: %r'%parsed.alarms)
        self.assertEquals('temp', parsed.alarms[0]['kwargs']['value_id'])

        # check for platform agents
        self.find_object_by_name('Unit Test Platform Agent Instance', RT.PlatformAgentInstance)

        # check for platform model boolean values
        model = self.find_object_by_name('Nose Testing Platform Model', RT.PlatformModel)
        self.assertEquals(True, model.shore_networked)
        self.assertNotEqual('str', model.shore_networked.__class__.__name__)

        # check for data process definition
        self.find_object_by_name("Logical Transform Definition", RT.DataProcessDefinition)

        iai = self.find_object_by_name("Test InstrumentAgentInstance", RT.InstrumentAgentInstance)
        self.assertEqual(iai.startup_config, {'SCHEDULER': {'VERSION': 3.0, 'CLOCK_SYNC': 48.2},
                                              'PARAMETERS': {'TXWAVEBURST': 'false', 'TXREALTIME': True}})

        orgs, _ = self.container.resource_registry.find_subjects(RT.Org, PRED.hasResource, iai._id, True)
        self.assertEqual(1, len(orgs))
        self.assertEqual(org._id, orgs[0])
