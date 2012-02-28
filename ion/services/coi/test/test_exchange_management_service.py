#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Dave Foster <dfoster@asascience.com>'
__license__ = 'Apache 2.0'

from pyon.core import exception
from pyon.ion import exchange
from interface.objects import ExchangeSpace, ExchangePoint, ExchangeName
from mock import Mock, patch, sentinel
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

import unittest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT
from ion.services.coi.exchange_management_service import ExchangeManagementService


@attr('UNIT', group='coi')
class TestExchangeManagementService(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('exchange_management')

        self.exchange_management_service = ExchangeManagementService()
        self.exchange_management_service.clients = mock_clients

        self.exchange_management_service.container = Mock()

        # Rename to save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_read = mock_clients.resource_registry.read
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_create_association = mock_clients.resource_registry.create_association
        self.mock_delete_association = mock_clients.resource_registry.delete_association
        self.mock_find_objects = mock_clients.resource_registry.find_objects
        self.mock_find_resources = mock_clients.resource_registry.find_resources
        self.mock_find_subjects = mock_clients.resource_registry.find_subjects

        # Exchange Space
        self.exchange_space = Mock()
        self.exchange_space.name = "Foo"


    def test_create_exchange_space(self):
        self.mock_create.return_value = ['111', 1]

        #TODO - Need to mock an org to pass in an valid Org_id

        exchange_space_id = self.exchange_management_service.create_exchange_space(self.exchange_space, "1233")

        assert exchange_space_id == '111'
        self.mock_create.assert_called_once_with(self.exchange_space)

    def test_xs_create_bad_params(self):
        self.assertRaises(exception.BadRequest, self.exchange_management_service.create_exchange_space)
        self.assertRaises(exception.BadRequest, self.exchange_management_service.create_exchange_space, exchange_space=sentinel.ex_space)
        self.assertRaises(exception.BadRequest, self.exchange_management_service.create_exchange_space, org_id=sentinel.org_id)

    def test_read_and_update_exchange_space(self):
        self.mock_read.return_value = self.exchange_space

        exchange_space = self.exchange_management_service.read_exchange_space('111')

        assert exchange_space is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        exchange_space.name = 'Bar'

        self.mock_update.return_value = ['111', 2]

        self.exchange_management_service.update_exchange_space(exchange_space)

        self.mock_update.assert_called_once_with(exchange_space)

    def test_delete_exchange_space(self):
        self.mock_read.return_value = self.exchange_space
        self.mock_find_subjects.return_value = (None, [])
        self.mock_find_objects.return_value = (None, [])

        self.exchange_management_service.delete_exchange_space('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with('111')

    def test_read_exchange_space_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.exchange_management_service.read_exchange_space('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Exchange Space bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_exchange_space_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.exchange_management_service.delete_exchange_space('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'Exchange Space bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')


@attr('INT', group='coi')
class TestExchangeManagementServiceInt(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        self.ems = self.container.proc_manager.procs_by_name['exchange_management']

        self.rr = ResourceRegistryServiceClient()
        orglist, _ = self.rr.find_resources(RT.Org)
        if not len(orglist) == 1:
            raise StandardError("Unexpected number of orgs found")

        self.org_id = orglist[0]._id

    def test_xs_create_delete(self):
        exchange_space = ExchangeSpace(name="bobo")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        # should be able to pull from RR an exchange space
        es2 = self.rr.read(esid)
        self.assertEquals(exchange_space.name, es2.name)

        # should have an exchange declared on the broker (how to test)
        # @TODO

        # should have an assoc to an org
        orglist, _ = self.rr.find_subjects(RT.Org, PRED.hasExchangeSpace, esid, id_only=True)
        self.assertEquals(len(orglist), 1)
        self.assertEquals(orglist[0], self.org_id)

        self.ems.delete_exchange_space(esid)

        # should no longer have that id in the RR
        self.assertRaises(NotFound, self.rr.read, esid)

        # should no longer have an assoc to an org
        orglist2, _ = self.rr.find_subjects(RT.Org, PRED.hasExchangeSpace, esid, id_only=True)
        self.assertEquals(len(orglist2), 0)

        # should no longer have that exchange declared (how to test)
        # @TODO

    def test_xs_delete_without_create(self):
        self.assertRaises(NotFound, self.ems.delete_exchange_space, '123')

    def test_xp_create_delete(self):

        # xp needs an xs first
        exchange_space = ExchangeSpace(name="doink")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_point = ExchangePoint(name="hammer")
        epid = self.ems.create_exchange_point(exchange_point, esid)

        # should be in RR
        ep2 = self.rr.read(epid)
        self.assertEquals(exchange_point.name, ep2.name)

        # should be associated to the XS as well
        xslist, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, epid, id_only=True)
        self.assertEquals(len(xslist), 1)
        self.assertEquals(xslist[0], esid)

        # should exist on broker (both xp and xs)
        # @TODO

        self.ems.delete_exchange_point(epid)
        self.ems.delete_exchange_space(esid)

        # should no longer be in RR
        self.assertRaises(NotFound, self.rr.read, epid)

        # should no longer be associated
        xslist2, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, epid, id_only=True)
        self.assertEquals(len(xslist2), 0)

        # should no longer exist on broker (both xp and xs)
        # @TODO

    def test_xp_create_then_delete_xs(self):

        # xp needs an xs first
        exchange_space = ExchangeSpace(name="doink")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_point = ExchangePoint(name="hammer")
        epid = self.ems.create_exchange_point(exchange_point, esid)

        # delete xs
        self.ems.delete_exchange_space(esid)

        # should no longer have an association
        xslist2, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, epid, id_only=True)
        self.assertEquals(len(xslist2), 0)

        # TEST ONLY: have to clean up the xp or we leave junk on the broker
        # we have to do it manually because the xs is gone
        #self.ems.delete_exchange_point(epid)
        xs = exchange.ExchangeSpace(self.container.ex_manager, exchange_space.name)
        xp = exchange.ExchangePoint(self.container.ex_manager, exchange_point.name, xs, 'ttree')
        self.container.ex_manager.delete_xp(xp, use_ems=False)

    def test_xs_create_update(self):
        raise unittest.SkipTest("Test not implemented yet")

    def test_xn_declare(self):

        # xn needs an xs first
        exchange_space = ExchangeSpace(name="bozo")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_name = ExchangeName(name='shoes', xn_type="XN_PROCESS")
        enid = self.ems.declare_exchange_name(exchange_name, esid)

        # should be in RR
        en2 = self.rr.read(enid)
        self.assertEquals(exchange_name.name, en2.name)

        # should have an assoc from XN to XS
        xnlist, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, enid, id_only=True)
        self.assertEquals(len(xnlist), 1)
        self.assertEquals(xnlist[0], esid)

        # container API got called (but is a noop)

        # TEST ONLY: have to clean up the xs or we leave junk on the broker
        self.ems.delete_exchange_space(esid)

    def test_xn_declare_no_xs(self):
        exchange_name = ExchangeName(name="shoez", xn_type='XN_PROCESS')
        self.assertRaises(NotFound, self.ems.declare_exchange_name, exchange_name, '11')

    def test_xn_undeclare_without_declare(self):
        raise unittest.SkipTest("Undeclare exchange name not implemented yet")

    def test_xn_declare_and_undeclare(self):
        raise unittest.SkipTest("Undeclare exchange name not implemented yet")

    def test_xn_declare_then_delete_xs(self):

        # xn needs an xs first
        exchange_space = ExchangeSpace(name="bozo")
        esid = self.ems.create_exchange_space(exchange_space, self.org_id)

        exchange_name = ExchangeName(name='shnoz', xn_type="XN_PROCESS")
        enid = self.ems.declare_exchange_name(exchange_name, esid)

        # delete the XS
        self.ems.delete_exchange_space(esid)

        # no longer should have assoc from XS to XN
        xnlist, _ = self.rr.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, enid, id_only=True)
        self.assertEquals(len(xnlist), 0)
