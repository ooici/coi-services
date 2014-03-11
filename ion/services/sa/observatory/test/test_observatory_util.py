#!/usr/bin/env python

__author__ = 'Michael Meisinger'

import unittest
from nose.plugins.attrib import attr

from pyon.public import RT, log
from pyon.util.unit_test import IonUnitTestCase

from ion.services.sa.observatory.mockutil import MockUtil
from ion.services.sa.observatory.observatory_util import ObservatoryUtil

from interface.objects import DeviceStatusType, DeviceCommsType, AggregateStatusType
DST = DeviceStatusType


def _devstat(dev_id, power, comms, data, loc):
    res_dict = dict(device_id = dev_id,
                    agg_status={
                        AggregateStatusType.AGGREGATE_POWER: dict(status=power),
                        AggregateStatusType.AGGREGATE_COMMS: dict(status=comms),
                        AggregateStatusType.AGGREGATE_DATA: dict(status=data),
                        AggregateStatusType.AGGREGATE_LOCATION: dict(status=loc),
                    })
    return res_dict

@attr('UNIT', group='sa')
class TestObservatoryUtil(IonUnitTestCase):

    def setUp(self):
        self.mu = MockUtil()
        self.process_mock = self.mu.create_process_mock()
        self.container_mock = self.mu.create_container_mock()
        self.dsm_mock = self.mu.create_device_status_manager_mock()

    res_list = [
        dict(rt='Org', _id='Org_1', attr={}),
        dict(rt='Observatory', _id='Obs_1', attr={}),
        dict(rt='Observatory', _id='Obs_2', attr={}),
        dict(rt='Subsite', _id='Sub_1', attr={}),
        dict(rt='Subsite', _id='Sub_2', attr={}),
        dict(rt='PlatformSite', _id='PS_1', attr={}),
        dict(rt='InstrumentSite', _id='IS_1', attr={}),

        dict(rt='PlatformDevice', _id='PD_1', attr={}),
        dict(rt='InstrumentDevice', _id='ID_1', attr={}),
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

    def spy_get_child_sites(self, parent_site_id=None, org_id=None, exclude_types=None, include_parents=True, id_only=True):
        child_sites, site_ancestors = self.obs_util.get_child_sites(parent_site_id=parent_site_id,
                                                                    org_id=org_id,
                                                                    exclude_types=exclude_types,
                                                                    include_parents=include_parents,
                                                                    id_only=id_only)

        print "child_sites of", parent_site_id, "are", child_sites
        print "site_ancestors of", parent_site_id, "are", site_ancestors

        return child_sites, site_ancestors

    def test_get_child_sites(self):
        self.mu.load_mock_resources(self.res_list)
        self.mu.load_mock_associations(self.assoc_list)

        self.obs_util = ObservatoryUtil(self.process_mock, self.container_mock)

        site_resources, site_children = self.spy_get_child_sites(parent_site_id='Obs_1', include_parents=False, id_only=True)
        self.assertEquals(len(site_resources), 3)
        self.assertEquals(len(site_children), 3)
        self.assertIn('Sub_1', site_resources)
        self.assertIn('PS_1', site_resources)
        self.assertIn('IS_1', site_resources)
        self.assertNotIn('Obs_1', site_resources)
        self.assertEquals(len([v for v in site_resources.values() if v is None]), 3)

        site_resources, site_children = self.spy_get_child_sites(parent_site_id='Obs_1', include_parents=False, id_only=False)
        self.assertEquals(len(site_resources), 3)
        self.assertEquals(len(site_children), 3)
        self.assertEquals(len([v for v in site_resources.values() if v is None]), 0)
        self.assertEquals(site_resources['Sub_1']._get_type(), RT.Subsite)

        site_resources, site_children = self.spy_get_child_sites(parent_site_id='Obs_1', include_parents=True)
        self.assertEquals(len(site_resources), 4)
        self.assertEquals(len(site_children), 3)
        self.assertIn('Obs_1', site_resources)

        site_resources, site_children = self.spy_get_child_sites(parent_site_id='Sub_1', include_parents=False)
        self.assertEquals(len(site_resources), 2)
        self.assertEquals(len(site_children), 2)
        self.assertNotIn('Sub_1', site_resources)

        site_resources, site_children = self.spy_get_child_sites(parent_site_id='Sub_1', include_parents=True)
        self.assertEquals(len(site_resources), 4)
        self.assertEquals(len(site_children), 3)
        self.assertIn('Sub_1', site_resources)
        self.assertIn('Obs_1', site_resources)

        site_resources, site_children = self.spy_get_child_sites(parent_site_id='PS_1', include_parents=False)
        self.assertEquals(len(site_resources), 1)
        self.assertEquals(len(site_children), 1)

        site_resources, site_children = self.spy_get_child_sites(parent_site_id='PS_1', include_parents=True)
        self.assertEquals(len(site_resources), 4)
        self.assertEquals(len(site_children), 3)

        site_resources, site_children = self.spy_get_child_sites(parent_site_id='IS_1', include_parents=False)
        self.assertEquals(len(site_resources), 0)
        self.assertEquals(len(site_children), 0)

        site_resources, site_children = self.spy_get_child_sites(parent_site_id='IS_1', include_parents=True)
        self.assertEquals(len(site_resources), 4)
        self.assertEquals(len(site_children), 3)

        site_resources, site_children = self.spy_get_child_sites(parent_site_id='XXX', include_parents=True)
        self.assertEquals(len(site_resources), 1)
        self.assertEquals(len(site_children), 0)

    def test_get_child_sites_org(self):
        self.mu.load_mock_resources(self.res_list)
        self.mu.load_mock_associations(self.assoc_list + self.assoc_list1)

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

        self.obs_util = ObservatoryUtil(self.process_mock, self.container_mock)
        site_devices = self.obs_util.get_site_devices(['Sub_1', 'PS_1', 'IS_1'])
        self.assertEquals(len(site_devices), 3)
        self.assertEquals(site_devices['Sub_1'], [])
        self.assertEquals(site_devices['IS_1'], [('InstrumentSite', 'ID_1', 'InstrumentDevice')])

    def test_get_child_devices(self):
        self.mu.load_mock_resources(self.res_list)
        self.mu.load_mock_associations(self.assoc_list2)

        self.obs_util = ObservatoryUtil(self.process_mock, self.container_mock)
        child_devices = self.obs_util.get_child_devices('PD_1')
        self.assertEquals(len(child_devices), 2)
        self.assertEquals(child_devices['PD_1'][0][1], 'ID_1')

        child_devices = self.obs_util.get_child_devices('ID_1')
        self.assertEquals(len(child_devices), 1)
        self.assertEquals(child_devices['ID_1'], [])

        child_devices = self.obs_util.get_child_devices('Sub_1')
        self.assertEquals(len(child_devices), 1)
        self.assertEquals(child_devices['Sub_1'], [])

        child_devices = self.obs_util.get_child_devices('XXX')
        self.assertEquals(len(child_devices), 1)

    def test_get_device_data_products(self):
        self.mu.load_mock_resources(self.res_list + self.res_list1)
        self.mu.load_mock_associations(self.assoc_list + self.assoc_list1 + self.assoc_list2 + self.assoc_list3)

        self.mu.assign_mockres_find_objects(filter_predicate="hasResource")

        self.obs_util = ObservatoryUtil(self.process_mock, self.container_mock)
        res_dict = self.obs_util.get_site_data_products('Obs_1', RT.Observatory)
        self.assertGreaterEqual(len(res_dict), 6)
        self.assertIsNone(res_dict['data_product_resources'])
        self.assertIn('ID_1', res_dict['device_data_products'])
        self.assertEquals(len(res_dict['device_data_products']['ID_1']), 3)
        self.assertIn('DP_1', res_dict['device_data_products']['ID_1'])
        self.assertIn('PD_1', res_dict['device_data_products'])
        self.assertEquals(len(res_dict['device_data_products']['PD_1']), 3)

        res_dict = self.obs_util.get_site_data_products('PS_1', RT.PlatformSite)
        self.assertEquals(len(res_dict['device_data_products']['ID_1']), 3)
        self.assertIn('ID_1', res_dict['device_data_products'])
        self.assertIn('DP_1', res_dict['device_data_products']['ID_1'])
        self.assertIn('PD_1', res_dict['device_data_products'])
        self.assertEquals(len(res_dict['device_data_products']['PD_1']), 3)

        res_dict = self.obs_util.get_site_data_products('Org_1', RT.Org)
        self.assertIn('DP_1', res_dict['device_data_products']['ID_1'])

        res_dict = self.obs_util.get_site_data_products('PS_1', RT.PlatformSite, include_data_products=True)
        self.assertIsNotNone(res_dict['data_product_resources'])
        self.assertIn('DP_1', res_dict['data_product_resources'])

        #import pprint
        #pprint.pprint(res_dict)


    status_by_device_1 = {
        "ID_1": _devstat("ID_1", DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK),
        "PD_1": _devstat("PD_1", DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK),
    }
    status_by_device_2 = {
        "ID_1": _devstat("ID_1", DST.STATUS_WARNING, DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK),
        "PD_1": _devstat("PD_1", DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK),
    }
    status_by_device_3 = {
        "ID_1": _devstat("ID_1", DST.STATUS_WARNING, DST.STATUS_WARNING, DST.STATUS_OK, DST.STATUS_OK),
        "PD_1": _devstat("PD_1", DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK),
    }
    status_by_device_4 = {
        "ID_1": _devstat("ID_1", DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK, DST.STATUS_OK),
        "PD_1": _devstat("PD_1", DST.STATUS_WARNING, DST.STATUS_WARNING, DST.STATUS_OK, DST.STATUS_OK),
    }


    def test_get_status_roll_ups(self):
        self.mu.load_mock_resources(self.res_list)
        self.mu.load_mock_associations(self.assoc_list + self.assoc_list1 + self.assoc_list2)
        self.mu.load_mock_device_statuses(self.status_by_device_1)

        self.obs_util = ObservatoryUtil(self.process_mock, self.container_mock, device_status_mgr=self.dsm_mock)

        # No problems
        status_rollups = self.obs_util.get_status_roll_ups('ID_1', RT.InstrumentDevice)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'ID_1')

        status_rollups = self.obs_util.get_status_roll_ups('PD_1', RT.PlatformDevice)
        self.assertIn('PD_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'PD_1')
        self._assert_status(status_rollups, 'ID_1')

        status_rollups = self.obs_util.get_status_roll_ups('IS_1', RT.InstrumentSite)
        self.assertEquals(len(status_rollups), 6)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'IS_1')
        self._assert_status(status_rollups, 'ID_1')

        status_rollups = self.obs_util.get_status_roll_ups('PS_1', RT.PlatformSite)
        self.assertEquals(len(status_rollups), 6)
        self.assertIn('PS_1', status_rollups)
        self.assertIn('PD_1', status_rollups)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'PS_1')
        self._assert_status(status_rollups, 'PD_1')
        self._assert_status(status_rollups, 'IS_1')
        self._assert_status(status_rollups, 'ID_1')

        status_rollups = self.obs_util.get_status_roll_ups('Sub_1', RT.Subsite)
        self.assertIn('Sub_1', status_rollups)
        self.assertIn('PS_1', status_rollups)
        self.assertIn('PD_1', status_rollups)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'Sub_1')
        self._assert_status(status_rollups, 'PS_1')
        self._assert_status(status_rollups, 'PD_1')
        self._assert_status(status_rollups, 'IS_1')
        self._assert_status(status_rollups, 'ID_1')

        status_rollups = self.obs_util.get_status_roll_ups('Obs_1', RT.Observatory)
        self.assertIn('Obs_1', status_rollups)
        self.assertIn('Sub_1', status_rollups)
        self.assertIn('PS_1', status_rollups)
        self.assertIn('PD_1', status_rollups)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'Obs_1')
        self._assert_status(status_rollups, 'Sub_1')
        self._assert_status(status_rollups, 'PS_1')
        self._assert_status(status_rollups, 'PD_1')
        self._assert_status(status_rollups, 'IS_1')
        self._assert_status(status_rollups, 'ID_1')

        # ID_1 power warning
        self.mu.load_mock_device_statuses(self.status_by_device_2)
        status_rollups = self.obs_util.get_status_roll_ups('ID_1', RT.InstrumentDevice)
        self._assert_status(status_rollups, 'ID_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING)

        status_rollups = self.obs_util.get_status_roll_ups('PD_1', RT.PlatformDevice)
        self._assert_status(status_rollups, 'PD_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'ID_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING)

        status_rollups = self.obs_util.get_status_roll_ups('IS_1', RT.InstrumentSite)
        self.assertIn('IS_1', status_rollups)
        self._assert_status(status_rollups, 'IS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING)

        status_rollups = self.obs_util.get_status_roll_ups('PS_1', RT.PlatformSite)
        self.assertIn('PS_1', status_rollups)
        self.assertIn('IS_1', status_rollups)
        self._assert_status(status_rollups, 'PS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'IS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING)

        # ID_1 power+comms warning
        self.mu.load_mock_device_statuses(self.status_by_device_3)
        status_rollups = self.obs_util.get_status_roll_ups('ID_1', RT.InstrumentDevice)
        self._assert_status(status_rollups, 'ID_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)

        status_rollups = self.obs_util.get_status_roll_ups('PD_1', RT.PlatformDevice)
        self._assert_status(status_rollups, 'PD_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'ID_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)

        status_rollups = self.obs_util.get_status_roll_ups('IS_1', RT.InstrumentSite)
        self.assertEquals(len(status_rollups), 6)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'IS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'ID_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)

        status_rollups = self.obs_util.get_status_roll_ups('PS_1', RT.PlatformSite)
        self.assertEquals(len(status_rollups), 6)
        self.assertIn('PS_1', status_rollups)
        self.assertIn('PD_1', status_rollups)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'PS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'PD_1')
        self._assert_status(status_rollups, 'IS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'ID_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)

        status_rollups = self.obs_util.get_status_roll_ups('Sub_1', RT.Subsite)
        self.assertIn('Sub_1', status_rollups)
        self.assertIn('PS_1', status_rollups)
        self.assertIn('PD_1', status_rollups)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'Sub_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'PS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'PD_1')
        self._assert_status(status_rollups, 'IS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'ID_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)

        status_rollups = self.obs_util.get_status_roll_ups('Obs_1', RT.Observatory)
        self.assertIn('Obs_1', status_rollups)
        self.assertIn('Sub_1', status_rollups)
        self.assertIn('PS_1', status_rollups)
        self.assertIn('PD_1', status_rollups)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'Obs_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'Sub_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'PS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'PD_1')
        self._assert_status(status_rollups, 'IS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'ID_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)

    def test_get_status_roll_ups_platform_warn(self):
        self.mu.load_mock_resources(self.res_list)
        self.mu.load_mock_associations(self.assoc_list + self.assoc_list1 + self.assoc_list2)
        self.mu.load_mock_device_statuses(self.status_by_device_4)

        self.obs_util = ObservatoryUtil(self.process_mock, self.container_mock, device_status_mgr=self.dsm_mock)

        # PD_1 power+comms warning
        status_rollups = self.obs_util.get_status_roll_ups('ID_1', RT.InstrumentDevice)
        self._assert_status(status_rollups, 'ID_1')

        status_rollups = self.obs_util.get_status_roll_ups('PD_1', RT.PlatformDevice)
        #log.warn("status %s" % status_rollups)
        self._assert_status(status_rollups, 'PD_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'ID_1')

        status_rollups = self.obs_util.get_status_roll_ups('IS_1', RT.InstrumentSite)
        self.assertEquals(len(status_rollups), 6)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'IS_1')
        self._assert_status(status_rollups, 'ID_1')

        status_rollups = self.obs_util.get_status_roll_ups('PS_1', RT.PlatformSite)
        self.assertEquals(len(status_rollups), 6)
        self.assertIn('PS_1', status_rollups)
        self.assertIn('PD_1', status_rollups)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'PS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'PD_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'IS_1')
        self._assert_status(status_rollups, 'ID_1')

        status_rollups = self.obs_util.get_status_roll_ups('Sub_1', RT.Subsite)
        self.assertIn('Sub_1', status_rollups)
        self.assertIn('PS_1', status_rollups)
        self.assertIn('PD_1', status_rollups)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'Sub_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'PS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'PD_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'IS_1')
        self._assert_status(status_rollups, 'ID_1')

        status_rollups = self.obs_util.get_status_roll_ups('Obs_1', RT.Observatory)
        self.assertIn('Obs_1', status_rollups)
        self.assertIn('Sub_1', status_rollups)
        self.assertIn('PS_1', status_rollups)
        self.assertIn('PD_1', status_rollups)
        self.assertIn('IS_1', status_rollups)
        self.assertIn('ID_1', status_rollups)
        self._assert_status(status_rollups, 'Obs_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'Sub_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'PS_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'PD_1', agg=DST.STATUS_WARNING, power=DST.STATUS_WARNING, comms=DST.STATUS_WARNING)
        self._assert_status(status_rollups, 'IS_1')
        self._assert_status(status_rollups, 'ID_1')

    def _assert_status(self, status_rollups, res_id=None, agg=DST.STATUS_OK, loc=DST.STATUS_OK,
                       data=DST.STATUS_OK, comms=DST.STATUS_OK, power=DST.STATUS_OK):
        res_status = status_rollups[res_id] if res_id else status_rollups
        log.debug("_assert_status(%s) = %s", res_id, res_status)
        self.assertEquals(len(res_status), 5)
        if agg is not None:
            self.assertEquals(res_status['agg'], agg)
        if loc is not None:
            self.assertEquals(res_status[AggregateStatusType.AGGREGATE_LOCATION], loc)
        if data is not None:
            self.assertEquals(res_status[AggregateStatusType.AGGREGATE_DATA], data)
        if comms is not None:
            self.assertEquals(res_status[AggregateStatusType.AGGREGATE_COMMS], comms)
        if power is not None:
            self.assertEquals(res_status[AggregateStatusType.AGGREGATE_POWER], power)

    res_list1 = [
        dict(rt='DataProduct', _id='DP_1', attr={}),
        dict(rt='DataProduct', _id='DP_2', attr={}),
        dict(rt='DataProduct', _id='DP_3', attr={}),
        dict(rt='DataProduct', _id='DP_4', attr={}),
        dict(rt='DataProduct', _id='DP_5', attr={}),
        ]

    assoc_list3 = [
        ['DP_1', 'hasSource', 'ID_1'],
        ['DP_1', 'hasSource', 'IS_1'],
        ['DP_2', 'hasSource', 'ID_1'],
        ['DP_3', 'hasSource', 'ID_1'],
        ['DP_3', 'hasSource', 'PD_1'],
        ['DP_4', 'hasSource', 'PD_1'],
        ['DP_5', 'hasSource', 'PD_1'],
        ]
