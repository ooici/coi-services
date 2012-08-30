#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Mon Aug 20 15:54:48 EDT 2012
@file ion/services/dm/preservation/test/test_persistence.py
@brief Testing for file-system level persistence
'''

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
from interface.objects import File
from interface.services.dm.ipreservation_management_service import PreservationManagementServiceClient
from pyon.datastore.datastore import DataStore
import unittest
import hashlib

@attr('INT',group='dm')
class FilePersistenceIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.preservation_management = PreservationManagementServiceClient()

    def test_simple_file_persist(self):
        datastore = self.container.datastore_manager.get_datastore('filesystem', DataStore.DS_PROFILE.FILESYSTEM)
        example_data = 'hello world\n'
        digest = hashlib.sha224(example_data).hexdigest()
        metadata = File(name='/examples/hello',extension='.txt')
        file_id = self.preservation_management.persist_file(file_data=example_data, digest=digest, metadata=metadata)

        ret_data, ret_digest = self.preservation_management.read_file(file_id)
        self.assertEquals(ret_data, example_data)
        self.assertEquals(ret_digest, digest)


