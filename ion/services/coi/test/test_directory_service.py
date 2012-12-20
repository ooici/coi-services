#!/usr/bin/env python

__author__ = 'Thomas Lennan'

from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.idirectory_service import DirectoryServiceClient, DirectoryServiceProcessClient

@attr('INT', group='coi')
class TestDirectoryService(IonIntegrationTestCase):
    
    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        # Now create client to bank service
        self.directory_service = DirectoryServiceClient(node=self.container.node)

    def test_directory_service(self):
        # Lookup of non-existent entry is benign
        ret = self.directory_service.lookup("/Foo")
        self.assertTrue(ret == None)

        # Find isn't implemented
        with self.assertRaises(BadRequest) as cm:
            self.directory_service.find("/", "Foo")
        self.assertTrue(cm.exception.message == "Not Implemented")

        # Unregister doesn't raise error if not found
        self.directory_service.unregister("/", "Foo")

        value = {"field1": 1, "field2": "ABC"}
        self.directory_service.register("/Foo/Bar", "SomeKey", value)
        ret = self.directory_service.lookup("/Foo/Bar/SomeKey")
        self.assertTrue(ret == value)

        self.directory_service.unregister("/Foo/Bar", "SomeKey")
        ret = self.directory_service.lookup("/Foo/Bar/SomeKey")
        self.assertTrue(ret == None)

        self.directory_service.reset_ui_specs()
        status = self.directory_service.get_ui_specs()
        self.assertNotEqual(status, None)

