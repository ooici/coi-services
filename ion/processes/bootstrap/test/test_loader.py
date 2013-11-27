#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from nose.plugins.attrib import attr
from pyon.public import RT, PRED, OT, log
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
import math
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
import unittest
from ion.processes.bootstrap.ion_loader import TESTED_DOC, IONLoader


class TestLoaderAlgo(PyonTestCase):

    @attr('UNIT', group='loader')
    def test_parse_alert_ranges(self):
        loader = IONLoader()
        out = loader._parse_alert_range('5<temp<10')
        self.assertEqual('<', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual('<', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])

        out = loader._parse_alert_range('5<=temp<10')
        self.assertEqual('<=', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual('<', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])

        out = loader._parse_alert_range('5<temp<=10')
        self.assertEqual('<', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual('<=', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])

        out = loader._parse_alert_range('5<=temp<=10')
        self.assertEqual('<=', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual('<=', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])

        out = loader._parse_alert_range('5<temp')
        self.assertEqual('<', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual(3, len(out), msg='value: %r'%out)
        self.assertEqual('temp', out['value_id'])

        out = loader._parse_alert_range('5<=temp')
        self.assertEqual('<=', out['lower_rel_op'])
        self.assertEqual(5, out['lower_bound'])
        self.assertEqual('temp', out['value_id'])
        self.assertEqual(3, len(out))

        out = loader._parse_alert_range('temp<10')
        self.assertEqual('<', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])
        self.assertEqual(3, len(out))

        out = loader._parse_alert_range('temp<=10')
        self.assertEqual('<=', out['upper_rel_op'])
        self.assertEqual(10, out['upper_bound'])
        self.assertEqual('temp', out['value_id'])
        self.assertEqual(3, len(out))

TEST_PATH = TESTED_DOC

class TestLoader(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.ingestion_management = IngestionManagementServiceClient()
        self.rr = self.container.resource_registry

    def _perform_preload(self, load_cfg):
        self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=load_cfg)

    def _preload_instrument(self, inst_scenario):
        load_cfg = dict(op="load",
                        scenario=inst_scenario,
                        attachments="res/preload/r2_ioc/attachments",
                        assets='res/preload/r2_ioc/ooi_assets',
                        )
        self._perform_preload(load_cfg)

    def _preload_ui(self, ui_path="default"):
        load_cfg = dict(op="load",
                        loadui=True,
                        ui_path=ui_path,
                        )
        self._perform_preload(load_cfg)

    def _preload_cfg(self, cfg, path=TESTED_DOC):
        load_cfg = dict(cfg=cfg,
                        path=path)
        self._perform_preload(load_cfg)

    def _preload_scenario(self, scenario, path=TESTED_DOC, idmap=False):
        load_cfg = dict(op="load",
                        scenario=scenario,
                        attachments="res/preload/r2_ioc/attachments",
                        path=path,
                        idmap=idmap)
        self._perform_preload(load_cfg)

    def _preload_ooi(self, path=TESTED_DOC):
        load_cfg = dict(op="load",
                        loadooi=True,
                        assets="res/preload/r2_ioc/ooi_assets",
                        path=path,
                        ooiuntil="12/31/2013",
                        )
        self._perform_preload(load_cfg)

    @attr('PRELOAD')
    def test_ui_valid(self):
        """ make sure UI assets are valid using DEFAULT_UI_ASSETS = 'http://userexperience.oceanobservatories.org/database-exports/Stable' """
        self._preload_ui(ui_path='default')
        obj_list,_ = self.rr.find_resources(restype=RT.UISpec, name="ION UI Specs", id_only=False)
        self.assertEquals(len(obj_list), 1)

    @attr('PRELOAD')
    def test_ui_candidates_valid(self):
        """ make sure UI assets are valid using DEFAULT_UI_ASSETS = 'http://userexperience.oceanobservatories.org/database-exports/Candidates' """
        self._preload_ui(ui_path='candidate')
        obj_list,_ = self.rr.find_resources(restype=RT.UISpec, name="ION UI Specs", id_only=False)
        self.assertEquals(len(obj_list), 1)

    @attr('PRELOAD')
    def test_betademo_valid(self):
        """ make sure can load asset DB """
        self._preload_scenario("BETA,R2_DEMO,RSN_OMS", path=TEST_PATH)
        self._preload_ooi(path=TEST_PATH)

        # check that deployment port assignments subobject  created correctly

        #collect a set of deployments
        deploy_list = []
        #DEP3 of PDEV3
        obj_list,_ = self.rr.find_resources(restype=RT.Deployment, name="Platform Deployment", id_only=False)
        deploy_list.extend(obj_list)
        log.debug('test_betademo_valid DEP3:  %s ', obj_list)
        #DEP4 of PDEV4
        obj_list,_ = self.rr.find_resources(restype=RT.Deployment, name="dep4", id_only=False)
        log.debug('test_betademo_valid DEP4:  %s ', obj_list)
        deploy_list.extend(obj_list)
        self.assertEquals(len(deploy_list), 2)

        for dply_obj in deploy_list:
            # CabledNode should have just one item in the assignments list
            self.assertEquals(len(dply_obj.port_assignments), 1)

            for dev_id, platform_port in dply_obj.port_assignments.iteritems():
                # all values in the port assignments dict should be PlatformPort objects
                self.assertEquals(platform_port.type_, OT.PlatformPort)

    @attr('PRELOAD')
    def test_alpha_valid(self):
        """ make sure R2_DEMO scenario in master google doc
            is valid and self-contained (doesn't rely on rows from other scenarios except BETA)
            NOTE: test will pass/fail based on current google doc, not just code changes.
        """
        self._preload_cfg("res/preload/r2_ioc/config/ooi_alpha.yml", path=TEST_PATH)

    @attr('PRELOAD')
    def test_beta_valid(self):
        """ make sure R2_DEMO scenario in master google doc
            is valid and self-contained (doesn't rely on rows from other scenarios except BETA)
            NOTE: test will pass/fail based on current google doc, not just code changes.
        """
        self._preload_cfg("res/preload/r2_ioc/config/ooi_beta.yml", path=TEST_PATH)

    @attr('PRELOAD')
    def test_incremental(self):
        """ make sure R2_DEMO scenario in master google doc
            is valid and self-contained (doesn't rely on rows from other scenarios except BETA)
            NOTE: test will pass/fail based on current google doc, not just code changes.
        """
        self._preload_cfg("res/preload/r2_ioc/config/ooi_load_config.yml", path=TEST_PATH)
        self._preload_scenario("OOIR2_DEMO", path=TEST_PATH, idmap=True)

        dp_list1,_ = self.rr.find_resources(restype=RT.DataProduct, id_only=True)
        ia_list1,_ = self.rr.find_resources(restype=RT.InstrumentAgent, id_only=True)

        self._preload_cfg("res/preload/r2_ioc/config/ooi_instruments.yml", path=TEST_PATH)

        ia_list2,_ = self.rr.find_resources(restype=RT.InstrumentAgent, id_only=True)
        self.assertGreater(len(ia_list2), len(ia_list1))
        dp_list2,_ = self.rr.find_resources(restype=RT.DataProduct, id_only=True)
        self.assertGreater(len(dp_list2), len(dp_list1))
        id_list2,_ = self.rr.find_resources(restype=RT.InstrumentDevice, id_only=True)

        self._preload_ooi(path=TEST_PATH)

        dp_list3,_ = self.rr.find_resources(restype=RT.DataProduct, id_only=True)
        self.assertGreater(len(dp_list3), len(dp_list2))
        id_list3,_ = self.rr.find_resources(restype=RT.InstrumentDevice, id_only=True)
        self.assertEquals(len(id_list3), len(id_list2))

        self._preload_ooi(path=TEST_PATH)

        dp_list4,_ = self.rr.find_resources(restype=RT.DataProduct, id_only=True)
        self.assertEquals(len(dp_list4), len(dp_list3))
        id_list4,_ = self.rr.find_resources(restype=RT.InstrumentDevice, id_only=True)
        self.assertEquals(len(id_list4), len(id_list3))

    def find_object_by_name(self, name, resource_type):
        objects,_ = self.container.resource_registry.find_resources(resource_type, name=name, id_only=False)
        self.assertEquals(len(objects), 1)

        return objects[0]

    @attr('INT', group='loader')
    @attr('SMOKE', group='loader')
    def test_row_values(self):
        """ use only rows from NOSE scenario for specific names and details included in this test.
            rows in NOSE may rely on entries in BETA scenarios,
            but should not specifically test values from those scenarios.
        """

        # first make sure this scenario loads successfully
        self._preload_scenario("BETA,NOSE")

        # check for ExternalDataset
        eds = self.find_object_by_name('Test External CTD Dataset', RT.ExternalDataset)
        edm1 = self.find_object_by_name('Test External CTD Dataset Model', RT.ExternalDatasetModel)
        edm2,_ = self.container.resource_registry.find_objects(eds._id, PRED.hasModel, RT.ExternalDatasetModel, True)
        self.assertEquals(edm1._id, edm2[0])

        inst = self.find_object_by_name('Test External CTD Agent Instance', RT.ExternalDatasetAgentInstance)
        self.assertEquals('value1', inst.driver_config['key1'], msg='driver_config[key1] is not value1:\n%r' % inst.driver_config)

        # check for an Org
        org = self.find_object_by_name('CASPER', RT.Org)
        self.assertFalse(org.contacts is None)
        self.assertEquals('Userbrough', org.contacts[0].individual_name_family)
        self.assertEquals('primary', org.contacts[0].roles[0])

        # check data product
        dp = self.find_object_by_name('Test DP L0 CTD', RT.DataProduct)
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
        # OBSOLETE: check that alarm was added to StreamConfig
#        self.assertEquals(1, len(parsed.alarms), msg='alarms: %r'%parsed.alarms)
#        self.assertEquals('temp', parsed.alarms[0]['kwargs']['value_id'])

        # check for platform agents
        self.find_object_by_name('Unit Test Platform Agent Instance', RT.PlatformAgentInstance)

        # check for platform model boolean values
        model = self.find_object_by_name('Nose Testing Platform Model', RT.PlatformModel)
        self.assertEquals(True, model.shore_networked)
        self.assertNotEqual('str', model.shore_networked.__class__.__name__)

        iai = self.find_object_by_name("Test InstrumentAgentInstance", RT.InstrumentAgentInstance)
        self.assertEqual({'SCHEDULER': {'VERSION': {'number': 3.0}, 'CLOCK_SYNC': 48.2, 'ACQUIRE_STATUS': {}},
                          'PARAMETERS': {"TXWAVESTATS": False, 'TXWAVEBURST': 'false', 'TXREALTIME': True}},
                        iai.startup_config)
        self.assertEqual(2, len(iai.alerts))

        pai = self.find_object_by_name("Unit Test Platform Agent Instance", RT.PlatformAgentInstance)
        self.assertEqual(1, len(pai.alerts))
        self.assertTrue(pai.agent_config.has_key('platform_config'))

        orgs, _ = self.container.resource_registry.find_subjects(RT.Org, PRED.hasResource, iai._id, True)
        self.assertEqual(1, len(orgs))
        self.assertEqual(org._id, orgs[0])

        entries ,_ = self.container.resource_registry.find_resources(RT.SchedulerEntry, id_only=False)
        self.assertGreaterEqual(len(entries), 1)

    @attr('PRELOAD')
    def test_ooi_preload_valid(self):
        """ make sure R2_DEMO scenario in master google doc
            is valid and self-contained (doesn't rely on rows from other scenarios except BETA)
            NOTE: test will pass/fail based on current google doc, not just code changes.
        """
        self._preload_cfg("res/preload/r2_ioc/config/ooi_alpha.yml", path=TEST_PATH)
