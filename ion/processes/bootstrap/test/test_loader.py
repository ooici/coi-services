#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from nose.plugins.attrib import attr

from pyon.public import RT
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.idatastore_service import DatastoreServiceClient, DatastoreServiceProcessClient
import unittest

@attr('INT', group='loader')
class TestLoader(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

    def test_lca_load(self):
        config = dict(op="load",
            path="https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdG82NHZfSEJJOGdQTkgzb05aRjkzMEE&output=xls",
            scenario="R2_DEMO",
            attachments="res/preload/r2_ioc/attachments")
        self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=config)

        res,_ = self.container.resource_registry.find_resources(RT.Org, id_only=True)
        self.assertTrue(len(res) > 1)

        res,_ = self.container.resource_registry.find_resources(RT.DataProduct, id_only=True)
        self.assertTrue(len(res) > 1)
