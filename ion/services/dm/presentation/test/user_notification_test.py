'''
@author Bill Bollenbacher
@file ion/services/dm/presentation/test/user_notification_test.py
@description Unit and Integration test implementations for the user notification service class.
'''
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from ion.services.dm.presentation.user_notification_service import UserNotificationService
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.public import IonObject, RT, PRED
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log
from pyon.event.event import ResourceLifecycleEventPublisher, DataEventPublisher
import gevent

@attr('UNIT',group='dm')
@unittest.skip('not working')
class UserNotificationTest(PyonTestCase):
    def setUp(self):
        mock_clients = self._create_service_mock('user_notification')
        self.user_notification = UserNotificationService()
        self.user_notification.clients = mock_clients

        self.mock_rr_client = self.user_notification.clients.resource_registry
        self.notification_object = IonObject(RT.NotificationRequest, name="notification")

    def test_create_one_user_notification(self):
        # mocks
        self.mock_rr_client.create.return_value = ('notification_id','rev')
        self.mock_rr_client.read.return_value = ('user_1_info')

        # execution
        notification_id = self.user_notification.create_notification(self.notification_object, 'user_1')

        # assertions
        self.assertEquals(notification_id,'notification_id')
        self.assertTrue(self.mock_rr_client.create.called)

    def test_update_user_notification(self):
        # mocks

        # execution

        # assertions
        pass
    
    def test_delete_user_notification(self):
        # mocks

        # execution

        # assertions
        pass


@attr('INT', group='dm')
class UserNotificationIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.unsc = UserNotificationServiceClient(node=self.container.node)
        self.rrc = ResourceRegistryServiceClient(node=self.container.node)
        self.imc = IdentityManagementServiceClient(node=self.container.node)
        
    def Xtest_find_event_types_for_resource(self):
        # create a dataset object in the RR to pass into the UNS method
        dataset_object = IonObject(RT.DataSet, name="dataset1")
        dataset_id, version = self.rrc.create(dataset_object)
        
        # get the list of event types for the dataset
        events = self.unsc.find_event_types_for_resource(dataset_id)
        log.debug("dataset events = " + str(events))
        if not events == ['dataset_supplement_added', 'dataset_change']:
            self.fail("failed to return correct list of event types")
            
        # try to pass in an id of a resource that doesn't exist (should fail)
        try:
            events = self.unsc.find_event_types_for_resource("bogus_id")
            self.fail("failed to detect non-existant resource")
        except:
            pass
        
    def Xtest_create_two_user_notifications(self):
        # create user with email address in RR
        user_identty_object = IonObject(RT.UserIdentity, name="user1")
        user_id = self.imc.create_user_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'user1_email@someplace.com'}})
        self.imc.create_user_info(user_id, user_info_object)
        
        # create first notification
        notification_object1 = IonObject(RT.NotificationRequest, {"name":"notification1",
                                                                 "origin_list":['Some_Resource_Agent_ID1'],
                                                                 "events_list":['resource_lifecycle']})
        notification_id1 = self.unsc.create_notification(notification_object1, user_id)
        # create second notification
        notification_object2 = IonObject(RT.NotificationRequest, {"name":"notification2",
                                                                 "origin_list":['Some_Resource_Agent_ID2'],
                                                                 "events_list":['data']})
        notification_id2 = self.unsc.create_notification(notification_object2, user_id)
        
        # read the notifications back and check that they are correct
        n1 = self.unsc.read_notification(notification_id1)
        if n1.name != notification_object1.name or \
           n1.origin_list != notification_object1.origin_list or \
           n1.events_list != notification_object1.events_list:
            self.fail("notification was not correct")
        n2 = self.unsc.read_notification(notification_id2)
        if n2.name != notification_object2.name or \
           n2.origin_list != notification_object2.origin_list or \
           n2.events_list != notification_object2.events_list:
            self.fail("notification was not correct")

    def Xtest_delete_user_notifications(self):
        # create user with email address in RR
        user_identty_object = IonObject(RT.UserIdentity, name="user1")
        user_id = self.imc.create_user_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'user1_email@someplace.com'}})
        self.imc.create_user_info(user_id, user_info_object)
        
        # create first notification
        notification_object1 = IonObject(RT.NotificationRequest, {"name":"notification1",
                                                                 "origin_list":['Some_Resource_Agent_ID1'],
                                                                 "events_list":['resource_lifecycle']})
        notification1_id = self.unsc.create_notification(notification_object1, user_id)
        # create second notification
        notification_object2 = IonObject(RT.NotificationRequest, {"name":"notification2",
                                                                 "origin_list":['Some_Resource_Agent_ID2'],
                                                                 "events_list":['data']})
        notification2_id = self.unsc.create_notification(notification_object2, user_id)
        
        # delete both notifications
        self.unsc.delete_notification(notification1_id)
        self.unsc.delete_notification(notification2_id)
        
        # check that the notifications are not there
        try:
            n1 = self.unsc.read_notification(notification1_id)
        except:
            try:
                n2 = self.unsc.read_notification(notification2_id)
            except:
                return
        self.fail("failed to delete notifications")      
        
    def Xtest_find_user_notifications(self):
        # create user with email address in RR
        user_identty_object = IonObject(RT.UserIdentity, name="user1")
        user_id = self.imc.create_user_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'user1_email@someplace.com'}})
        self.imc.create_user_info(user_id, user_info_object)

        # create first notification
        notification_object = IonObject(RT.NotificationRequest, {"name":"notification1",
                                                                 "origin_list":['Some_Resource_Agent_ID1'],
                                                                 "events_list":['resource_lifecycle']})

        self.unsc.create_notification(notification_object, user_id)
        # create second notification
        notification_object = IonObject(RT.NotificationRequest, {"name":"notification2",
                                                                 "origin_list":['Some_Resource_Agent_ID2'],
                                                                 "events_list":['data']})
        self.unsc.create_notification(notification_object, user_id)
        
        # try to find all notifications for user
        notifications = self.unsc.find_notifications_by_user(user_id)
        if len(notifications) != 2:
            self.fail("failed to find all notifications")  

    def Xtest_update_user_notification(self):
        # create user with email address in RR
        user_identty_object = IonObject(RT.UserIdentity, name="user1")
        user_id = self.imc.create_user_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'user1_email@someplace.com'}})
        self.imc.create_user_info(user_id, user_info_object)

        # create a notification
        notification_object = IonObject(RT.NotificationRequest, {"name":"notification1",
                                                                 "origin_list":['Some_Resource_Agent_ID1'],
                                                                 "events_list":['resource_lifecycle']})
        notification_id = self.unsc.create_notification(notification_object, user_id)
        print("n_id = " + notification_id)
        
        # read back the notification and change it
        notification = self.unsc.read_notification(notification_id)
        notification.origin_list = ['Some_Resource_Agent_ID5']
        self.unsc.update_notification(notification)
        
        # read back the notification and check that it got changed
        notification = self.unsc.read_notification(notification_id)
        if notification.origin_list != ['Some_Resource_Agent_ID5']:
            self.fail("failed to change notification")          

    def Xtest_send_notification_emails(self):
        # create user with email address in RR
        user_identty_object = IonObject(RT.UserIdentity, name="user1")
        user_id = self.imc.create_user_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'myooici@gmail.com'}})
        self.imc.create_user_info(user_id, user_info_object)

        # create first notification
        notification_object = IonObject(RT.NotificationRequest, {"name":"notification1",
                                                                 "origin_list":['Some_Resource_Agent_ID1'],
                                                                 "events_list":['resource_lifecycle']})
        self.unsc.create_notification(notification_object, user_id)
        # create second notification
        notification_object = IonObject(RT.NotificationRequest, {"name":"notification2",
                                                                 "origin_list":['Some_Resource_Agent_ID2'],
                                                                 "events_list":['data']})
        self.unsc.create_notification(notification_object, user_id)
        
        # publish an event for each notification to generate the emails
        # this can't be easily check in SW so need to check for these at the myooici@gmail.com account
        rle_publisher = ResourceLifecycleEventPublisher()
        rle_publisher.create_and_publish_event(origin='Some_Resource_Agent_ID1', description="RLE test event")
        de_publisher = DataEventPublisher()
        de_publisher.create_and_publish_event(origin='Some_Resource_Agent_ID2', description="DE test event")
        gevent.sleep(1)

    def test_find_events(self):
        # publish some events for the event repository
        rle_publisher = ResourceLifecycleEventPublisher(event_repo=self.container.event_repository)
        de_publisher = DataEventPublisher(event_repo=self.container.event_repository)
        rle_publisher.create_and_publish_event(origin='Some_Resource_Agent_ID1', description="RLE test event1")
        rle_publisher.create_and_publish_event(origin='Some_Resource_Agent_ID1', description="RLE test event2")
        rle_publisher.create_and_publish_event(origin='Some_Resource_Agent_ID1', description="RLE test event3")
        de_publisher.create_and_publish_event(origin='Some_Resource_Agent_ID2', description="DE test event1")
        de_publisher.create_and_publish_event(origin='Some_Resource_Agent_ID2', description="DE test event2")
        de_publisher.create_and_publish_event(origin='Some_Resource_Agent_ID2', description="DE test event3")
        
        # find all events for the originator 'Some_Resource_Agent_ID1'
        events = self.unsc.find_events(origin='Some_Resource_Agent_ID1')
        if len(events) != 3:
            self.fail("failed to find all events")  
        for event in events:
            log.debug("event=" + str(event))
            if event[1][0] != 'Some_Resource_Agent_ID1':
                self.fail("failed to find correct events")
                  
        # find all events for the originator 'DataEvent'
        events = self.unsc.find_events(type='DataEvent')
        if len(events) != 3:
            self.fail("failed to find all events")  
        for event in events:
            log.debug("event=" + str(event))
            if event[1][0] != 'DataEvent':
                self.fail("failed to find correct events") 
                 
        # find 2 events for the originator 'Some_Resource_Agent_ID1'
        events = self.unsc.find_events(origin='Some_Resource_Agent_ID2', limit=2)
        if len(events) != 2:
            self.fail("failed to find all events")  
        for event in events:
            log.debug("event=" + str(event))
            if event[1][0] != 'Some_Resource_Agent_ID2':
                self.fail("failed to find correct events")
            
        # find all events for the originator 'Some_Resource_Agent_ID1' in reverse time order
        events = self.unsc.find_events(origin='Some_Resource_Agent_ID1', descending=True)
        if len(events) != 3:
            self.fail("failed to find all events")  
        for event in events:
            log.debug("event=" + str(event))
            if event[1][0] != 'Some_Resource_Agent_ID1':
                self.fail("failed to find correct events")
