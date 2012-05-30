'''
@author Bill Bollenbacher
@file ion/services/dm/presentation/test/user_notification_test.py
@description Unit and Integration test implementations for the user notification service class.
'''
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from ion.services.dm.presentation.user_notification_service import UserNotificationService
from interface.objects import DeliveryMode, UserInfo, DeliveryConfig, DetectionFilterConfig
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.public import IonObject, RT, PRED, Container
from pyon.core.exception import NotFound, BadRequest
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log
from pyon.event.event import EventPublisher
import gevent
from mock import Mock, mocksignature
from interface.objects import NotificationRequest

import gevent
from gevent.timeout import Timeout

@attr('UNIT',group='dm')
class UserNotificationTest(PyonTestCase):
    def setUp(self):


        mock_clients = self._create_service_mock('user_notification')
        self.user_notification = UserNotificationService()

        self.user_notification.event_processors = {}

        self.user_notification.smtp_server = 'smtp_server'
        self.user_notification.clients = mock_clients

        self.mock_rr_client = self.user_notification.clients.resource_registry

    @unittest.skip('Bad test - figure out how to patch out the greenlet start...')
    def test_create_one_user_notification(self):
        # mocks
        user = Mock()
        objects = [Mock()]
        user_id = 'user_id'

        self.mock_rr_client.create.return_value = ('notification_id','rev')
        self.mock_rr_client.read.return_value = user
        self.mock_rr_client.find_objects.return_value = objects, None

        delivery_config = DeliveryConfig()

        # Create a notification object
        notification_request = NotificationRequest(name='Setting_email',
                                                    origin = 'origin',
                                                    origin_type = 'origin_type',
                                                    event_type= 'event_type',
                                                    event_subtype = 'event_subtype' ,
                                                    delivery_config= delivery_config)

        # execution
        notification_id = self.user_notification.create_notification(notification_request, user_id)

        # assertions
        #@todo - change to asserting called with!
        self.assertEquals('notification_id', notification_id)
        self.assertTrue(self.mock_rr_client.create.called)
        self.assertTrue(self.mock_rr_client.find_objects.return_value)


    def test_create_notification_validation(self):

        #------------------------------------------------------------------------------------------------------
        # Test with no user provided
        #------------------------------------------------------------------------------------------------------

        delivery_config = DeliveryConfig()

        # Create a notification object
        notification_request = NotificationRequest(name='Setting_email',
            origin = 'origin',
            origin_type = 'origin_type',
            event_type= 'event_type',
            event_subtype = 'event_subtype' ,
            delivery_config= delivery_config)

        with self.assertRaises(BadRequest) as br:
            notification_id =  self.user_notification.create_notification(notification=notification_request)

        self.assertEquals(
            br.exception.message,
            '''User id not provided.'''
        )

        #@todo when validation for subscription properties is added test it here...


    def test_create_email(self):

        #------------------------------------------------------------------------------------------------------
        #Setup for the create email test
        #------------------------------------------------------------------------------------------------------

        cn = Mock()

        notification_id = 'an id'
        args_list = {}
        kwargs_list = {}

        def side_effect(*args, **kwargs):

            args_list.update(args)
            kwargs_list.update(kwargs)
            return notification_id

        cn.side_effect = side_effect
        self.user_notification.create_notification = cn

        #------------------------------------------------------------------------------------------------------
        # Test with complete arguments
        #------------------------------------------------------------------------------------------------------

        res = self.user_notification.create_email(event_type='event_type',
                                                    event_subtype='event_subtype',
                                                    origin='origin',
                                                    origin_type='origin_type',
                                                    user_id='user_id',
                                                    email='email',
                                                    mode = DeliveryMode.DIGEST,
                                                    message_header='message_header',
                                                    parser='parser',
                                                    period=2323)

        #------------------------------------------------------------------------------------------------------
        # Assert results about complete arguments
        #------------------------------------------------------------------------------------------------------

        self.assertEquals(res, notification_id)

        notification_request = kwargs_list['notification']
        user_id = kwargs_list['user_id']

        self.assertEquals(user_id, 'user_id')
        self.assertEquals(notification_request.delivery_config.delivery['email'], 'email')
        self.assertEquals(notification_request.delivery_config.delivery['mode'], DeliveryMode.DIGEST)
        self.assertEquals(notification_request.delivery_config.delivery['period'], 2323)

        self.assertEquals(notification_request.delivery_config.processing['message_header'], 'message_header')
        self.assertEquals(notification_request.delivery_config.processing['parsing'], 'parser')

        self.assertEquals(notification_request.event_type, 'event_type')
        self.assertEquals(notification_request.event_subtype, 'event_subtype')
        self.assertEquals(notification_request.origin, 'origin')
        self.assertEquals(notification_request.origin_type, 'origin_type')



        #------------------------------------------------------------------------------------------------------
        # Test with email missing...
        #------------------------------------------------------------------------------------------------------

        with self.assertRaises(BadRequest):
            res = self.user_notification.create_email(event_type='event_type',
                                                    event_subtype='event_subtype',
                                                    origin='origin',
                                                    origin_type='origin_type',
                                                    user_id='user_id',
                                                    mode = DeliveryMode.DIGEST,
                                                    message_header='message_header',
                                                    parser='parser')

        #------------------------------------------------------------------------------------------------------
        # Test with user id missing - that is caught in the create_notification method
        #------------------------------------------------------------------------------------------------------

        res = self.user_notification.create_email(event_type='event_type',
                                                event_subtype='event_subtype',
                                                origin='origin',
                                                origin_type='origin_type',
                                                email='email',
                                                mode = DeliveryMode.DIGEST,
                                                message_header='message_header',
                                                parser='parser')

        notification_request = kwargs_list['notification']
        user_id = kwargs_list['user_id']

        self.assertEquals(user_id, '')
        self.assertEquals(notification_request.delivery_config.processing['message_header'], 'message_header')
        self.assertEquals(notification_request.delivery_config.processing['parsing'], 'parser')


        #------------------------------------------------------------------------------------------------------
        # Test with no mode - bad request?
        #------------------------------------------------------------------------------------------------------

        with self.assertRaises(BadRequest):
            res = self.user_notification.create_email(event_type='event_type',
                                                        event_subtype='event_subtype',
                                                        origin='origin',
                                                        origin_type='origin_type',
                                                        user_id='user_id',
                                                        email='email',
                                                        message_header='message_header',
                                                        parser='parser')


    def test_create_sms(self):

        #------------------------------------------------------------------------------------------------------
        #Setup for the create sms test
        #------------------------------------------------------------------------------------------------------

        cn = Mock()

        notification_id = 'an id'
        args_list = {}
        kwargs_list = {}

        def side_effect(*args, **kwargs):

            args_list.update(args)
            kwargs_list.update(kwargs)
            return notification_id

        cn.side_effect = side_effect
        self.user_notification.create_notification = cn

        #------------------------------------------------------------------------------------------------------
        # Test with complete arguments
        #------------------------------------------------------------------------------------------------------

        res = self.user_notification.create_sms(event_type='event_type',
                                                event_subtype='event_subtype',
                                                origin='origin',
                                                origin_type='origin_type',
                                                user_id='user_id',
                                                phone='401-XXX-XXXX',
                                                provider='provider',
                                                message_header='message_header',
                                                parser='parser')

        #------------------------------------------------------------------------------------------------------
        # Assert results about complete arguments
        #------------------------------------------------------------------------------------------------------

        self.assertEquals(res, notification_id)

        notification_request = kwargs_list['notification']
        user_id = kwargs_list['user_id']

        self.assertEquals(user_id, 'user_id')
        self.assertEquals(notification_request.delivery_config.delivery['phone_number'], '401-XXX-XXXX')
        self.assertEquals(notification_request.delivery_config.delivery['provider'], 'provider')

        self.assertEquals(notification_request.delivery_config.processing['message_header'], 'message_header')
        self.assertEquals(notification_request.delivery_config.processing['parsing'], 'parser')

        self.assertEquals(notification_request.event_type, 'event_type')
        self.assertEquals(notification_request.event_subtype, 'event_subtype')
        self.assertEquals(notification_request.origin, 'origin')
        self.assertEquals(notification_request.origin_type, 'origin_type')

        #------------------------------------------------------------------------------------------------------
        # Test with phone missing - what should that do? - bad request?
        #------------------------------------------------------------------------------------------------------

        with self.assertRaises(BadRequest):
            res = self.user_notification.create_sms(event_type='event_type',
                event_subtype='event_subtype',
                origin='origin',
                origin_type='origin_type',
                user_id='user_id',
                provider='provider',
                message_header='message_header',
                parser='parser')

        #------------------------------------------------------------------------------------------------------
        # Test with provider missing - what should that do? - bad request?
        #------------------------------------------------------------------------------------------------------

        with self.assertRaises(BadRequest):
            res = self.user_notification.create_sms(event_type='event_type',
                event_subtype='event_subtype',
                origin='origin',
                origin_type='origin_type',
                user_id='user_id',
                phone = '401-XXX-XXXX',
                message_header='message_header',
                parser='parser')


    def test_create_detection_filter(self):
        self.user_notification.create_notification = mocksignature(self.user_notification.create_notification)

        notification_id = 'an id'

        self.user_notification.create_notification.return_value = notification_id

        res = self.user_notification.create_detection_filter(
            event_type='event_type',
            event_subtype='event_subtype',
            origin='origin',
            origin_type='origin_type',
            user_id='user_id',
            filter_config = 'filter_config')

        self.assertEquals(res, notification_id)

    def test_update_user_notification(self):
        pass
        #@todo implement test for update

    def test_delete_user_notification(self):
        pass
        #@todo implement test for delete


ION_NOTIFICATION_EMAIL_ADDRESS = 'ION_notifications-do-not-reply@oceanobservatories.org'

@attr('INT', group='dm')
class UserNotificationIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.unsc = UserNotificationServiceClient(node=self.container.node)
        self.rrc = ResourceRegistryServiceClient(node=self.container.node)
        self.imc = IdentityManagementServiceClient(node=self.container.node)

    def test_email(self):

        proc1 = self.container.proc_manager.procs_by_name['user_notification']

        # Create a user and get the user_id
        user = UserInfo(name = 'new_user')
        user_id, _ = self.rrc.create(user)

#        # set up....
#        notification_id = self.unsc.create_email(event_type='ResourceLifecycleEvent',
#            event_subtype=None,
#            origin='Some_Resource_Agent_ID1',
#            origin_type=None,
#            user_id=user_id,
#            email='email@email.com',
#            mode = DeliveryMode.DIGEST,
#            message_header='message_header',
#            parser='parser',
#            period=1)
#
#        #------------------------------------------------------------------------------------------------------
#        # Setup so as to be able to get the message and headers going into the
#        # subscription callback method of the EmailEventProcessor
#        #------------------------------------------------------------------------------------------------------
#
#        # publish an event for each notification to generate the emails
#        rle_publisher = EventPublisher("ResourceLifecycleEvent")
#        rle_publisher.publish_event(origin='Some_Resource_Agent_ID1', description="RLE test event")
#
#
#        msg_tuple = proc1.event_processors[notification_id].smtp_client.sentmail.get(timeout=4)
#
#        #@todo assert that the queue is now empty
#
#        message = msg_tuple[2]
#        list_lines = message.split("\n")
#
#        #@todo assert the to and from for the message
#
#        #-------------------------------------------------------
#        # parse the message body
#        #-------------------------------------------------------
#
#        message_dict = {}
#        for line in list_lines:
#            key_item = line.split(": ")
#            if key_item[0] == 'Subject':
#                message_dict['Subject'] = key_item[1] + key_item[2]
#            else:
#                try:
#                    message_dict[key_item[0]] = key_item[1]
#                except Exception as exc:
#                    #@todo Why do you except on Exception here? That is bad practice!
#                    # these exceptions happen only because the message sometimes
#                    # has successive /r/n (i.e. new lines) and therefore,
#                    # the indexing goes out of range. These new lines
#                    # can just be ignored. So we ignore the exceptions here.
#                    pass
#
#        #-------------------------------------------------------
#        # make assertions
#        #-------------------------------------------------------
#
#        self.assertEquals(msg_tuple[1], 'email@email.com' )
#
#        self.assertEquals(message_dict['From'], ION_NOTIFICATION_EMAIL_ADDRESS)
#        self.assertEquals(message_dict['To'], 'email@email.com')
#        self.assertEquals(message_dict['Event'].rstrip('\r'), 'ResourceLifecycleEvent')
#        self.assertEquals(message_dict['Originator'].rstrip('\r'), 'Some_Resource_Agent_ID1')
#        self.assertEquals(message_dict['Description'].rstrip('\r'), 'RLE test event')

    def test_sms_notification(self):
        pass
        #@todo Implement the test - similar to the email test


    def test_event_detection_notification(self):

        proc1 = self.container.proc_manager.procs_by_name['user_notification']

        # Create a user and get the user_id
        user = UserInfo(name = 'new_user')
        user_id, _ = self.rrc.create(user)

        dfilt = DetectionFilterConfig()

        dfilt.processing['condition'] = 5
        dfilt.processing['comparator'] = '>'
        dfilt.processing['filter_field'] = 'voltage'

        dfilt.delivery['message'] = 'I got my detection event!'

        # set up....
        notification_id = self.unsc.create_detection_filter(event_type='ResourceLifecycleEvent',
            event_subtype=None,
            origin='Some_Resource_Agent_ID1',
            origin_type=None,
            user_id=user_id,
            filter_config=dfilt
            )

        # Create detection notification
        # Create event subscription for resulting detection event

        # Send event that is not detected
        # Assert that no detection event is sent

        # Send Event that is detected
        # check the async result for the received event


    @unittest.skip('interface has changed!')
    def test_find_event_types_for_resource(self):
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

    @unittest.skip('interface has changed!')
    def test_create_two_user_notifications(self):
        # create user with email address in RR
        user_identty_object = IonObject(RT.ActorIdentity, name="user1")
        user_id = self.imc.create_actor_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'user1_email@someplace.com'}})
        self.imc.create_user_info(user_id, user_info_object)
        
        # create first notification
        notification_object1 = IonObject(RT.NotificationRequest, {"name":"notification1",
                                                                 "origin_list":['Some_Resource_Agent_ID1'],
                                                                 "events_list":['ResourceLifecycleEvent']})
        notification_id1 = self.unsc.create_notification(notification_object1, user_id)
        # create second notification
        notification_object2 = IonObject(RT.NotificationRequest, {"name":"notification2",
                                                                 "origin_list":['Some_Resource_Agent_ID2'],
                                                                 "events_list":['DataEvent']})
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

    @unittest.skip('interface has changed!')
    def test_delete_user_notifications(self):
        # create user with email address in RR
        user_identty_object = IonObject(RT.ActorIdentity, name="user1")
        user_id = self.imc.create_actor_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'user1_email@someplace.com'}})
        self.imc.create_user_info(user_id, user_info_object)
        
        # create first notification
        notification_object1 = IonObject(RT.NotificationRequest, {"name":"notification1",
                                                                 "origin_list":['Some_Resource_Agent_ID1'],
                                                                 "events_list":['ResourceLifecycleEvent']})
        notification1_id = self.unsc.create_notification(notification_object1, user_id)
        # create second notification
        notification_object2 = IonObject(RT.NotificationRequest, {"name":"notification2",
                                                                 "origin_list":['Some_Resource_Agent_ID2'],
                                                                 "events_list":['DataEvent']})
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

    @unittest.skip('interface has changed!')
    def test_find_user_notifications(self):
        # create user with email address in RR
        user_identty_object = IonObject(RT.ActorIdentity, name="user1")
        user_id = self.imc.create_actor_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'user1_email@someplace.com'}})
        self.imc.create_user_info(user_id, user_info_object)

        # create first notification
        notification_object = IonObject(RT.NotificationRequest, {"name":"notification1",
                                                                 "origin_list":['Some_Resource_Agent_ID1'],
                                                                 "events_list":['ResourceLifecycleEvent']})

        self.unsc.create_notification(notification_object, user_id)
        # create second notification
        notification_object = IonObject(RT.NotificationRequest, {"name":"notification2",
                                                                 "origin_list":['Some_Resource_Agent_ID2'],
                                                                 "events_list":['DataEvent']})
        self.unsc.create_notification(notification_object, user_id)
        
        # try to find all notifications for user
        notifications = self.unsc.find_notifications_by_user(user_id)
        if len(notifications) != 2:
            self.fail("failed to find all notifications")

    @unittest.skip('interface has changed!')
    def test_update_user_notification(self):
        # create user with email address in RR
        user_identty_object = IonObject(RT.ActorIdentity, name="user1")
        user_id = self.imc.create_actor_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'user1_email@someplace.com'}})
        self.imc.create_user_info(user_id, user_info_object)

        # create a notification
        notification_object = IonObject(RT.NotificationRequest, {"name":"notification1",
                                                                 "origin_list":['Some_Resource_Agent_ID1'],
                                                                 "events_list":['ResourceLifecycleEvent']})
        notification_id = self.unsc.create_notification(notification_object, user_id)
        
        # read back the notification and change it
        notification = self.unsc.read_notification(notification_id)
        notification.origin_list = ['Some_Resource_Agent_ID5']
        self.unsc.update_notification(notification)
        
        # read back the notification and check that it got changed
        notification = self.unsc.read_notification(notification_id)
        if notification.origin_list != ['Some_Resource_Agent_ID5']:
            self.fail("failed to change notification")

    @unittest.skip('interface has changed!')
    def test_send_notification_emails(self):
        # create user with email address in RR
        user_identty_object = IonObject(RT.ActorIdentity, name="user1")
        user_id = self.imc.create_actor_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'myooici@gmail.com'}})
        self.imc.create_user_info(user_id, user_info_object)

        # create first notification
        notification_object = IonObject(RT.NotificationRequest, {"name":"notification1",
                                                                 "origin_list":['Some_Resource_Agent_ID1'],
                                                                 "events_list":['ResourceLifecycleEvent']})
        self.unsc.create_notification(notification_object, user_id)
        # create second notification
        notification_object = IonObject(RT.NotificationRequest, {"name":"notification2",
                                                                 "origin_list":['Some_Resource_Agent_ID2'],
                                                                 "events_list":['DataEvent']})
        self.unsc.create_notification(notification_object, user_id)
        
        # publish an event for each notification to generate the emails
        # this can't be easily check in SW so need to check for these at the myooici@gmail.com account
        rle_publisher = EventPublisher("ResourceLifecycleEvent")
        rle_publisher.publish_event(origin='Some_Resource_Agent_ID1', description="RLE test event")
        de_publisher = EventPublisher("DataEvent")
        de_publisher.publish_event(origin='Some_Resource_Agent_ID2', description="DE test event")
        gevent.sleep(1)

    @unittest.skip('interface has changed!')
    def test_find_events(self):
        # publish some events for the event repository
        rle_publisher = EventPublisher("ResourceLifecycleEvent")
        de_publisher = EventPublisher("DataEvent")
        rle_publisher.publish_event(origin='Some_Resource_Agent_ID1', description="RLE test event1")
        rle_publisher.publish_event(origin='Some_Resource_Agent_ID1', description="RLE test event2")
        rle_publisher.publish_event(origin='Some_Resource_Agent_ID1', description="RLE test event3")
        de_publisher.publish_event(origin='Some_Resource_Agent_ID2', description="DE test event1")
        de_publisher.publish_event(origin='Some_Resource_Agent_ID2', description="DE test event2")
        de_publisher.publish_event(origin='Some_Resource_Agent_ID2', description="DE test event3")
        
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
