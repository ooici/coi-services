#!/usr/bin/env python
from pyon.datastore.datastore import DatastoreManager

__author__ = 'Prashant Kediyal <pkediyal@ucsd.edu>'
__license__ = 'Apache 2.0'

import time

from nose.plugins.attrib import attr
from pyon.ion.conversation_log import ConvRepository
from pyon.util.int_test import IonIntegrationTestCase
from pyon.net.endpoint import Publisher


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
        time.sleep(2)

        # assert that the 2 messages have been persisted
        no_of_conv = len(ds.list_objects())
        self.assertEquals(no_of_conv, 2)

