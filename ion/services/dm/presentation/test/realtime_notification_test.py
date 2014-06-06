#!/usr/bin/env python

__author__ = 'Brian McKenna <bmckenna@asascience.com>'

from collections import Counter
import gevent
from nose.plugins.attrib import attr
import os
import smtplib
import time
import unittest

from pyon.event.event import EventPublisher
from pyon.public import IonObject, OT, CFG
from pyon.util.int_test import IonIntegrationTestCase

from interface.objects import UserInfo
from interface.objects import DeliveryModeEnum, NotificationFrequencyEnum
from interface.objects import NotificationTypeEnum
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient


outbox=[] # sent emails wind up here

@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
@attr('LOCOINT')
@attr('INT', group='dm')
class RealTimeNotificationTestCase(IonIntegrationTestCase):

    def setUp(self):

        self._start_container()

        # patch the CFG service.user_notification.max_daily_notifications value so we only test 10
        original_CFG_max = CFG.get_safe("service.user_notification.max_daily_notifications", 1000)
        CFG['service']['user_notification']['max_daily_notifications'] = 10

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.object_store = self.container.object_store
        self.resource_registry = self.container.resource_registry
        self.user_notification = UserNotificationServiceClient()
        self.event_publisher = EventPublisher()

        # create UserInfo object (user)
        user = UserInfo()
        user.name = 'Iceman'
        user.contact.email = 'iceman@example.com'
        user_id, _ = self.resource_registry.create(user)
        self.user = self.resource_registry.read(user_id)

        # create NotificationRequest objects (notifications)
        # 4 notifications are created:
        #     REAL_TIME, EMAIL(user default via UserInfo)
        #     REAL_TIME, EMAIL(in DeliveryConfiguration)
        #     DISABLED, EMAIL(in DeliveryConfiguration)
        #     REAL_TIME, SMS(in DeliveryConfiguration)

        # REAL_TIME, EMAIL(user default via UserInfo)
        delivery_configuration = IonObject(OT.DeliveryConfiguration,
                                 mode=DeliveryModeEnum.EMAIL,
                                 frequency=NotificationFrequencyEnum.REAL_TIME)
        notification_request = IonObject(OT.NotificationRequest,
                               name='REAL_TIME to default UserInfo email',
                               type=NotificationTypeEnum.SIMPLE,
                               origin='Miramar',
                               event_type=OT.ResourceLifecycleEvent,
                               delivery_configurations=[delivery_configuration])
        # store this notification_id to check disabled_by_system status later
        self.notification_id = self.user_notification.create_notification(notification=notification_request, user_id=self.user._id)
        
        # REAL_TIME, EMAIL(in DeliveryConfiguration), 10 notifications/day max
        delivery_configuration = IonObject(OT.DeliveryConfiguration,
                                 email='slider@example.com',
                                 mode=DeliveryModeEnum.EMAIL,
                                 frequency=NotificationFrequencyEnum.REAL_TIME)
        notification_request = IonObject(OT.NotificationRequest,
                               name='REAL_TIME to alternate email, 10 notifications/day max',
                               type=NotificationTypeEnum.SIMPLE,
                               origin="Miramar",
                               event_type=OT.ResourceLifecycleEvent,
                               delivery_configurations=[delivery_configuration])
        self.user_notification.create_notification(notification=notification_request, user_id=self.user._id)

        # DISABLED, EMAIL(in DeliveryConfiguration)
        delivery_configuration = IonObject(OT.DeliveryConfiguration,
                                 email='charlie@example.com',
                                 mode=DeliveryModeEnum.EMAIL,
                                 frequency=NotificationFrequencyEnum.DISABLED)
        notification_request = IonObject(OT.NotificationRequest,
                               name='DISABLED to alternate email',
                               type=NotificationTypeEnum.SIMPLE,
                               origin="Miramar",
                               event_type=OT.ResourceLifecycleEvent,
                               delivery_configurations=[delivery_configuration])
        self.user_notification.create_notification(notification=notification_request, user_id=self.user._id)

        # REAL_TIME, SMS(in DeliveryConfiguration)
        delivery_configuration = IonObject(OT.DeliveryConfiguration,
                                 email='snot_nosed_jockey@example.com',
                                 mode=DeliveryModeEnum.SMS,
                                 frequency=NotificationFrequencyEnum.REAL_TIME)
        notification_request = IonObject(OT.NotificationRequest,
                               name='SMS to alternate email',
                               type=NotificationTypeEnum.SIMPLE,
                               origin="Miramar",
                               event_type=OT.ResourceLifecycleEvent,
                               delivery_configurations=[delivery_configuration])
        self.user_notification.create_notification(notification=notification_request, user_id=self.user._id)

    def test_realtime_notifications(self):

        # monkey patch smtplib.SMTP to capture sent emails
        original_SMTP = smtplib.SMTP # store original for restoration

        class MonkeyPatchSMTP(object):
            def __init__(self, address, host):
                self.address = address
                self.host = host
            def login(self,username,password):
                self.username = username
                self.password = password
            def sendmail(self,from_addr, to_addrs, msg):
                global outbox
                outbox.append((from_addr, to_addrs, msg,time.time()))
                return []
            def quit(self):
                pass

        smtplib.SMTP=MonkeyPatchSMTP

        # patch the CFG service.user_notification.max_daily_notifications value so we only test 10
        original_CFG_max = CFG.get_safe("service.user_notification.max_daily_notifications", 1000)
        CFG['service']['user_notification']['max_daily_notifications'] = 10

        # publish event(s) - one should trigger notifications, the other not
        self.event_publisher.publish_event(origin='Miramar', event_type=OT.ResourceLifecycleEvent)
        self.event_publisher.publish_event(origin='Hong Kong', event_type=OT.ResourceLifecycleEvent)
        time.sleep(1) # wait some non-trivial time for events to be processed by NotificationWorker

        # outbox now contains tuple(from_addr, to_addrs, msg,time.time()) for each message sent

        # verifies the alternate email requirement (eg. email specified in DeliveryConfiguration)
        self.assertEqual(len([t[1] for t in outbox if t[1] == 'iceman@example.com']), 1) # 1 message to Iceman
        self.assertEqual(len([t[1] for t in outbox if t[1] == 'slider@example.com']), 1) # 1 message to Slider
        self.assertEqual(len([t[1] for t in outbox if t[1] == 'charlie@example.com']), 0 ) # no messages to Charlie (CIV)

        # check SMS sent
        self.assertEqual(len([t[1] for t in outbox if t[1] == 'snot_nosed_jockey@example.com']), 1 )
        # check SMS <= 140 characters
        self.assertLessEqual(len([(t[2]) for t in outbox if t[1] == 'snot_nosed_jockey@example.com'][0]), 140)

        # publish 9 more events (already have 1), notification should be disabled at number 10
        for x in xrange(9):
            self.event_publisher.publish_event(origin='Miramar', event_type=OT.ResourceLifecycleEvent)
        time.sleep(2) # give system time to update NotificationRequest (1 sec saw occasional fail)
        # publish 1 more event, should NOT trigger any additional notifications
        self.event_publisher.publish_event(origin='Miramar', event_type=OT.ResourceLifecycleEvent)
        time.sleep(1) # wait some non-trivial time for events to be processed by NotificationWorker

        # check there are 10 for Iceman, not 11 even though 11 have been published
        # verifies the max_daily notification limit
        self.assertEquals(len([t[1] for t in outbox if t[1] == 'iceman@example.com']), 10) # 10 to Iceman (reached limit)

        # check NotificationRequest has been disabled_by_system
        notification = self.resource_registry.read(self.notification_id)
        self.assertTrue(notification.disabled_by_system)

        # MONKEY PATCH time.time() fast forward to trigger NotificationSentScanner to persist counts
        CONSTANT_TIME = time.time() + 600 # forward 10 minutes (from now FREEZES time.time())
        def new_time():
            return CONSTANT_TIME
        old_time = time.time
        time.time = new_time

        # sleep 5s longer than interval taken from ion/processes/event/event_persister.py
        time.sleep(float(CFG.get_safe("process.event_persister.persist_interval", 1.0))+5)

        # get notification_counts from ObjectStore
        notification_counts_obj = self.object_store.read('notification_counts')
        # persisted as standard dicts, convert to Counter objects
        notification_counts = {k:Counter(v) for k,v in notification_counts_obj.items() if not (k == '_id' or k == '_rev') }
        self.assertEqual(int(notification_counts.get(self.user._id).get('all')), 30) # 3 DeliveryConfigurations * 10 notifications

        # restore MONKEY PATCHed time (so we can go ahead from actual wall time)
        time.time = old_time

        # MONKEY PATCH time.time() fast forward to trigger NotificationSentScanner to flush/reset counts
        CONSTANT_TIME = time.time() + 86400 # forward 1 day (from now FREEZES time.time())
        def new_time():
            return CONSTANT_TIME
        old_time = time.time
        time.time = new_time

        # sleep 5s longer than interval taken from ion/processes/event/event_persister.py
        time.sleep(float(CFG.get_safe("process.event_persister.persist_interval", 1.0))+5)

        # get notification_counts from ObjectStore
        notification_counts_obj = self.object_store.read('notification_counts')
        # persisted as standard dicts, convert to Counter objects
        notification_counts = {k:Counter(v) for k,v in notification_counts_obj.items() if not (k == '_id' or k == '_rev') }
        self.assertEqual(notification_counts.get(self.user._id, None), None)

        # restore MONKEY PATCHed time
        time.time = old_time

        # restore original smtplib.SMTP and CFG value
        smtplib.SMTP = original_SMTP
        CFG['service']['user_notification']['max_daily_notifications'] = original_CFG_max

