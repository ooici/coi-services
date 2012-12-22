#!/usr/bin/env python

__author__ = 'Michael Meisinger'

import unittest
from mock import Mock, patch
from nose.plugins.attrib import attr

from pyon.public import log, RT
from pyon.util.unit_test import PyonTestCase

from ion.core.mockutil import MockUtil
from ion.services.sa.observatory.observatory_util import ObservatoryUtil

@attr('UNIT', group='saob')
class TestObservatoryUtil(unittest.TestCase):

    def setUp(self):
        self.mu = MockUtil()
        self.process_mock = self.mu.create_process_mock()
        self.container_mock = self.mu.create_container_mock()

    res_list = [
        dict(rt='Org', _id='Org_1', attr={}),
        dict(rt='Observatory', _id='Obs_1', attr={}),
        dict(rt='Observatory', _id='Obs_2', attr={}),
        dict(rt='Subsite', _id='Sub_1', attr={}),
        dict(rt='Subsite', _id='Sub_2', attr={}),
        dict(rt='PlatformSite', _id='PS_1', attr={}),
        dict(rt='InstrumentSite', _id='IS_1', attr={}),

        dict(rt='PlatformDevice', _id='PD_1', attr={}),
        dict(rt='InstrumentDevice', _id='ID_1',attr={}),
    ]

    assoc_list = [
        ['Obs_1', 'hasSite', 'Sub_1'],
        ['Sub_1', 'hasSite', 'PS_1'],
        ['PS_1', 'hasSite', 'IS_1'],
    ]
    assoc_list1 = [
        ['Org_1', 'hasResource', 'Obs_1'],
        ['Org_1', 'hasResource', 'Obs_2'],
        ['Obs_2', 'hasSite', 'Sub_2'],
    ]
    assoc_list2 = [
        ['PS_1', 'hasDevice', 'PD_1'],
        ['IS_1', 'hasDevice', 'ID_1'],

        ['PD_1', 'hasDevice', 'ID_1'],
    ]

    event_list = [
        dict(et='EventType', o='origin', ot='origin_type', st='sub_type', att={})
    ]


    def test_get_child_sites(self):
        self.mu.load_mock_resources(self.res_list)
        self.mu.load_mock_associations(self.assoc_list)

        self.mu.assign_mockres_find_associations(filter_predicate="hasSite")

        self.obs_util = ObservatoryUtil(self.process_mock, self.container_mock)

        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id='Obs_1', include_parents=False, id_only=True)
        self.assertEquals(len(child_sites), 3)
        self.assertEquals(len(site_ancestors), 3)
        self.assertIn('Sub_1', child_sites)
        self.assertIn('PS_1', child_sites)
        self.assertIn('IS_1', child_sites)
        self.assertNotIn('Obs_1', child_sites)
        self.assertEquals(len([v for v in child_sites.values() if v is None]), 3)

        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id='Obs_1', include_parents=False, id_only=False)
        self.assertEquals(len(child_sites), 3)
        self.assertEquals(len(site_ancestors), 3)
        self.assertEquals(len([v for v in child_sites.values() if v is None]), 0)
        self.assertEquals(child_sites['Sub_1']._get_type(), RT.Subsite)

        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id='Obs_1', include_parents=True)
        self.assertEquals(len(child_sites), 4)
        self.assertEquals(len(site_ancestors), 3)
        self.assertIn('Obs_1', child_sites)

        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id='Sub_1', include_parents=False)
        self.assertEquals(len(child_sites), 2)
        self.assertEquals(len(site_ancestors), 2)
        self.assertNotIn('Sub_1', child_sites)

        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id='Sub_1', include_parents=True)
        self.assertEquals(len(child_sites), 4)
        self.assertEquals(len(site_ancestors), 3)
        self.assertIn('Sub_1', child_sites)
        self.assertIn('Obs_1', child_sites)

        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id='PS_1', include_parents=False)
        self.assertEquals(len(child_sites), 1)
        self.assertEquals(len(site_ancestors), 1)

        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id='PS_1', include_parents=True)
        self.assertEquals(len(child_sites), 4)
        self.assertEquals(len(site_ancestors), 3)

        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id='IS_1', include_parents=False)
        self.assertEquals(len(child_sites), 0)
        self.assertEquals(len(site_ancestors), 0)

        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id='IS_1', include_parents=True)
        self.assertEquals(len(child_sites), 4)
        self.assertEquals(len(site_ancestors), 3)

        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id='XXX', include_parents=True)
        self.assertEquals(len(child_sites), 1)
        self.assertEquals(len(site_ancestors), 0)

    def test_get_child_sites_org(self):
        self.mu.load_mock_resources(self.res_list)
        self.mu.load_mock_associations(self.assoc_list + self.assoc_list1)

        self.mu.assign_mockres_find_associations(filter_predicate="hasSite")
        self.mu.assign_mockres_find_objects(filter_predicate="hasResource")

        self.obs_util = ObservatoryUtil(self.process_mock, self.container_mock)

        child_sites, site_ancestors = self.obs_util.get_child_sites(org_id='Org_1', include_parents=False, id_only=True)
        self.assertEquals(len(child_sites), 6)
        self.assertEquals(len(site_ancestors), 5)
        self.assertIn('Sub_1', child_sites)
        self.assertIn('PS_1', child_sites)
        self.assertIn('IS_1', child_sites)
        self.assertIn('Obs_1', child_sites)
        self.assertIn('Obs_2', child_sites)

        child_sites, site_ancestors = self.obs_util.get_child_sites(org_id='Org_1', include_parents=True, id_only=True)
        self.assertEquals(len(child_sites), 7)
        self.assertEquals(len(site_ancestors), 5)

        child_sites, site_ancestors = self.obs_util.get_child_sites(org_id='Org_1', include_parents=True, id_only=False)
        self.assertEquals(len(child_sites), 7)
        self.assertEquals(len(site_ancestors), 5)
        self.assertEquals(len([v for v in child_sites.values() if v is None]), 0)
        self.assertEquals(child_sites['Org_1']._get_type(), RT.Org)

    def test_get_site_devices(self):
        self.mu.load_mock_resources(self.res_list)
        self.mu.load_mock_associations(self.assoc_list2)

        self.mu.assign_mockres_find_associations(filter_predicate="hasDevice")

        self.obs_util = ObservatoryUtil(self.process_mock, self.container_mock)
        site_devices = self.obs_util.get_site_devices(['Sub_1', 'PS_1', 'IS_1'])
        self.assertEquals(len(site_devices), 3)
        self.assertEquals(site_devices['Sub_1'], None)
        self.assertEquals(site_devices['IS_1'], ('InstrumentSite', 'ID_1', 'InstrumentDevice'))

    def test_get_child_devices(self):
        self.mu.load_mock_resources(self.res_list)
        self.mu.load_mock_associations(self.assoc_list2)
        self.mu.assign_mockres_find_associations(filter_predicate="hasDevice")

        self.obs_util = ObservatoryUtil(self.process_mock, self.container_mock)
        child_devices = self.obs_util.get_child_devices('PD_1')
        self.assertEquals(len(child_devices), 1)
        self.assertEquals(child_devices['PD_1'][0][1], 'ID_1')

        child_devices = self.obs_util.get_child_devices('ID_1')
        self.assertEquals(len(child_devices), 1)
        self.assertEquals(child_devices['ID_1'], [])

        child_devices = self.obs_util.get_child_devices('Sub_1')
        self.assertEquals(len(child_devices), 1)
        self.assertEquals(child_devices['Sub_1'], [])

        child_devices = self.obs_util.get_child_devices('XXX')
        self.assertEquals(len(child_devices), 1)
