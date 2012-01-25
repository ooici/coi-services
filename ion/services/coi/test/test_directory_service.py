#!/usr/bin/env python

__author__ = 'Thomas Lennan'

from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.coi.idirectory_service import DirectoryServiceClient, DirectoryServiceProcessClient

@attr('INT', group='directory')
class TestDirectory(IonIntegrationTestCase):
    
    def setUp(self):
        # Start container
        self._start_container()

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        container_client.start_rel_from_url('res/deploy/r2coi.yml')

        # Now create client to bank service
        self.directory_service = DirectoryServiceClient(node=self.container.node)

    def tearDown(self):
        self._stop_container()

    def test_directory(self):
        # Lookup of non-existent entry is benign
        ret = self.directory_service.lookup("Foo")
        self.assertTrue(ret == None)

        # Find isn't implemented
        find_failed = False
        try:
            self.directory_service.find("/", "Foo")
        except BadRequest as ex:
            self.assertTrue(ex.message == "Not Implemented")
            find_failed = True
        self.assertTrue(find_failed)

        # Unregister doesn't raise error if not found
        self.directory_service.unregister("/", "Foo")

        value = {"field1": 1, "field2": "ABC"}
        self.directory_service.register("/Foo/Bar", "SomeKey", value)
        ret = self.directory_service.lookup("/Foo/Bar/SomeKey")
        self.assertTrue(ret == value)

        self.directory_service.unregister("/Foo/Bar", "SomeKey")
        ret = self.directory_service.lookup("/Foo/Bar/SomeKey")
        self.assertTrue(ret == None)
        
