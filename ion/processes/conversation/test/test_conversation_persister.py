#!/usr/bin/env python
from pyon.datastore.datastore import DatastoreManager

__author__ = 'Prashant Kediyal <pkediyal@ucsd.edu>'


import time

from nose.plugins.attrib import attr
from pyon.ion.conversation_log import ConvRepository
from pyon.util.int_test import IonIntegrationTestCase
from pyon.net.endpoint import Publisher
import unittest
import os

@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test in CEI_LAUNCH_TEST mode as it uses a special yaml \
        with conversation persister')
@attr('INT',group='conversation_log')
class TestConversations(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        # start the conversation_persister
        self.container.start_rel_from_url('res/deploy/r2convlog.yml')


    def test_sub(self):

        # publish 2 messages
        pub = Publisher()
        pub.publish(to_name='anyone', msg="hello1")
        pub.publish(to_name='anyone', msg="hello2")

        dsm = self.container.datastore_manager
        ds = dsm.get_datastore("conversations")

        # give at least 2 seconds for the persister to save in the repository
        # test may fail if it does not wait long enough for the persister
        no_of_conv = 0
        retried = 0

        while (no_of_conv != 2 and retried < 5):
            time.sleep(2)
            # assert that the 2 messages have been persisted
            no_of_conv = len(ds.list_objects())
            retried = retried + 1

        self.assertEquals(no_of_conv, 2)

