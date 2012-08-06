'''
@author Bill Bollenbacher
@author Swarbhanu Chatterjee
@author David Stuebe
@file ion/services/dm/presentation/test/user_notification_test.py
@description Unit and Integration test implementations for the user notification service class.
'''
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from pyon.public import IonObject, RT, PRED, Container, CFG
from pyon.core.exception import NotFound, BadRequest
from pyon.core.bootstrap import get_sys_name
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.cei.ischeduler_service import SchedulerServiceClient
from ion.services.dm.presentation.user_notification_service import UserNotificationService
from interface.objects import UserInfo, DeliveryConfig
from interface.objects import DeviceEvent
from ion.services.cei.scheduler_service import SchedulerService
from interface.services.cei.ischeduler_service import SchedulerServiceClient
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log
from pyon.event.event import EventPublisher, EventSubscriber
import gevent
from mock import Mock, mocksignature
from interface.objects import NotificationRequest
from ion.services.dm.inventory.index_management_service import IndexManagementService
from ion.services.dm.presentation.user_notification_service import EmailEventProcessor
from ion.processes.bootstrap.index_bootstrap import STD_INDEXES
import os, time
from gevent import event, queue
from gevent.timeout import Timeout
import elasticpy as ep

use_es = CFG.get_safe('system.elasticsearch',False)

@attr('UNIT',group='dm')
class UserNotificationTest(PyonTestCase):
    def setUp(self):


        mock_clients = self._create_service_mock('user_notification')
        self.user_notification = UserNotificationService()
        self.user_notification.clients = mock_clients
        self.user_notification.container = DotDict()
        self.user_notification.container.node = Mock()

        self.user_notification.container['spawn_process'] = Mock()
        self.user_notification.container['id'] = 'mock_container_id'
        self.user_notification.container['proc_manager'] = DotDict()
        self.user_notification.container.proc_manager['terminate_process'] = Mock()
        self.user_notification.container.proc_manager['procs'] = {}

        self.mock_cc_spawn = self.user_notification.container.spawn_process
        self.mock_cc_terminate = self.user_notification.container.proc_manager.terminate_process
        self.mock_cc_procs = self.user_notification.container.proc_manager.procs

        self.mock_rr_client = self.user_notification.clients.resource_registry

        self.user_notification.smtp_server = 'smtp_server'
        self.user_notification.smtp_client = 'smtp_client'
        self.user_notification.event_publisher = EventPublisher()
        self.user_notification.event_processor = EmailEventProcessor('an_smtp_client')


    #    @unittest.skip('Bad test - figure out how to patch out the greenlet start...')
    def test_create_notification(self):
        '''
        Test creating a notification
        '''
        user_id = 'user_id_1'

        self.mock_rr_client.create = mocksignature(self.mock_rr_client.create)
        self.mock_rr_client.create.return_value = ('notification_id_1','rev_1')

        self.mock_rr_client.find_resources = mocksignature(self.mock_rr_client.find_resources)
        self.mock_rr_client.find_resources.return_value = [],[]

        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = 'notification'

        self.user_notification.event_processor.add_notification_for_user = mocksignature(self.user_notification.event_processor.add_notification_for_user)

        self.user_notification.event_publisher.publish_event = mocksignature(self.user_notification.event_publisher.publish_event)

        #-------------------------------------------------------------------------------------------------------------------
        # Create a notification object
        #-------------------------------------------------------------------------------------------------------------------

        notification_request = NotificationRequest(name='a name',
            origin = 'origin_1',
            origin_type = 'origin_type_1',
            event_type= 'event_type_1',
            event_subtype = 'event_subtype_1' )

        #-------------------------------------------------------------------------------------------------------------------
        # execution
        #-------------------------------------------------------------------------------------------------------------------

        notification_id = self.user_notification.create_notification(notification_request, user_id)

        #-------------------------------------------------------------------------------------------------------------------
        # assertions
        #-------------------------------------------------------------------------------------------------------------------

        self.assertEquals('notification_id_1', notification_id)
        self.mock_rr_client.create.assert_called_once_with(notification_request)
        self.user_notification.event_processor.add_notification_for_user.assert_called_once_with('notification', user_id)


    def test_create_notification_validation(self):
        '''
        Test that creating a notification without a providing a user_id results in an error
        '''

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

    def test_update_notification(self):
        '''
        Test updating a notification
        '''

        notification = 'notification'
        user_id = 'user_id_1'

        self.mock_rr_client.update = mocksignature(self.mock_rr_client.update)
        self.mock_rr_client.update.return_value = ''

        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = notification

        self.user_notification.update_user_info_object = mocksignature(self.user_notification.update_user_info_object)
        self.user_notification.update_user_info_object.return_value = 'user'

        self.user_notification.update_user_info_dictionary = mocksignature(self.user_notification.update_user_info_dictionary)
        self.user_notification.update_user_info_dictionary.return_value = ''

        self.user_notification.event_publisher.publish_event = mocksignature(self.user_notification.event_publisher.publish_event)

        #-------------------------------------------------------------------------------------------------------------------
        # Create a notification object
        #-------------------------------------------------------------------------------------------------------------------

        notification_request = NotificationRequest(name='a name',
            origin = 'origin_1',
            origin_type = 'origin_type_1',
            event_type= 'event_type_1',
            event_subtype = 'event_subtype_1' )

        notification_request._id = 'an id'

        #-------------------------------------------------------------------------------------------------------------------
        # execution
        #-------------------------------------------------------------------------------------------------------------------

        self.user_notification.update_notification(notification_request, user_id)

        #-------------------------------------------------------------------------------------------------------------------
        # assertions
        #-------------------------------------------------------------------------------------------------------------------

        self.mock_rr_client.update.assert_called_once_with(notification_request)
        self.user_notification.update_user_info_object.assert_called_once_with(user_id, notification, notification)
        self.user_notification.update_user_info_dictionary.assert_called_once_with('user', notification, notification)


    def test_delete_user_notification(self):
        '''
        Test deleting a notification
        '''

        notification_id = 'notification_id_1'

        self.mock_rr_client.delete = mocksignature(self.mock_rr_client.update())
        self.mock_rr_client.delete.return_value = (notification_id,'rev_1')

        self.user_notification.delete_notification_from_user_info = mocksignature(self.user_notification.delete_notification_from_user_info)
        self.user_notification.delete_notification_from_user_info.return_value = ''

        self.user_notification.event_processor.stop_notification_subscriber = mocksignature(self.user_notification.event_processor.stop_notification_subscriber)

        self.user_notification.event_publisher.publish_event = mocksignature(self.user_notification.event_publisher.publish_event)

        #-------------------------------------------------------------------------------------------------------------------
        # Create a notification object
        #-------------------------------------------------------------------------------------------------------------------

        notification_request = NotificationRequest(name='a name',
            origin = 'origin_1',
            origin_type = 'origin_type_1',
            event_type= 'event_type_1',
            event_subtype = 'event_subtype_1' )

        #-------------------------------------------------------------------------------------------------------------------
        # execution
        #-------------------------------------------------------------------------------------------------------------------

        self.user_notification.delete_notification(notification_id=notification_id)

        #-------------------------------------------------------------------------------------------------------------------
        # assertions
        #-------------------------------------------------------------------------------------------------------------------

        self.mock_rr_client.delete.assert_called_once_with(notification_id)
        self.user_notification.delete_notification_from_user_info.assert_called_once_with(notification_id)

@attr('INT', group='dm')
class UserNotificationIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(UserNotificationIntTest, self).setUp()
        config = DotDict()
        config.bootstrap.use_es = True

        self._start_container()
        self.addCleanup(UserNotificationIntTest.es_cleanup)
        self.container.start_rel_from_url('res/deploy/r2deploy.yml', config)

        self.unsc = UserNotificationServiceClient()
        self.rrc = ResourceRegistryServiceClient()
        self.imc = IdentityManagementServiceClient()
        self.discovery = DiscoveryServiceClient()
        self.scheduler = SchedulerServiceClient()

        self.ION_NOTIFICATION_EMAIL_ADDRESS = 'ION_notifications-do-not-reply@oceanobservatories.org'

    @staticmethod
    def es_cleanup():
        es_host = CFG.get_safe('server.elasticsearch.host', 'localhost')
        es_port = CFG.get_safe('server.elasticsearch.port', '9200')
        es = ep.ElasticSearch(
            host=es_host,
            port=es_port,
            timeout=10
        )
        indexes = STD_INDEXES.keys()
        indexes.append('%s_resources_index' % get_sys_name().lower())
        indexes.append('%s_events_index' % get_sys_name().lower())

        for index in indexes:
            IndexManagementService._es_call(es.river_couchdb_delete,index)
            IndexManagementService._es_call(es.index_delete,index)

    def poll(self, tries, callback, *args, **kwargs):
        '''
        Polling wrapper for queries
        Elasticsearch may not index and cache the changes right away so we may need
        a couple of tries and a little time to go by before the results show.
        '''
        for i in xrange(tries):
            retval = callback(*args, **kwargs)
            if retval:
                return retval
            time.sleep(0.2)
        return None

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_pub_reload_user_info_event(self):
        '''
        Test that the publishing of reload user info event occurs every time a create, update
        or delete notification occurs.
        '''
        proc1 = self.container.proc_manager.procs_by_name['user_notification']

        #--------------------------------------------------------------------------------------
        # Create subscribers for reload events
        #--------------------------------------------------------------------------------------

        queue = gevent.queue.Queue()

        def reload_event_received(message, headers):
            queue.put(message)

        reload_event_subscriber = EventSubscriber(origin="UserNotificationService",
            event_type="ReloadUserInfoEvent",
            callback=reload_event_received)
        reload_event_subscriber.start()

        #--------------------------------------------------------------------------------------
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(   name= 'notification_1',
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(   name='notification_2',
            origin="instrument_2",
            origin_type="type_2",
            event_type='DetectionEvent')

        #--------------------------------------------------------------------------------------
        # Create a user and get the user_id
        #--------------------------------------------------------------------------------------

        user = UserInfo()
        user.name = 'new_user'
        user.contact.email = 'new_user@yahoo.com'

        user_id, _ = self.rrc.create(user)

        #--------------------------------------------------------------------------------------
        # Create notification
        #--------------------------------------------------------------------------------------

        notification_id_1 = self.unsc.create_notification(notification=notification_request_1, user_id=user_id)
        notification_id_2 = self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        notifications = set([notification_id_1, notification_id_2])

        #--------------------------------------------------------------------------------------
        # Check the publishing
        #--------------------------------------------------------------------------------------

        received_event_1 = queue.get()
        received_event_2 = queue.get()

        notifications_received = set([received_event_1.notification_id, received_event_2.notification_id])

        self.assertEquals(notifications, notifications_received)

        #--------------------------------------------------------------------------------------
        # Update notification
        #--------------------------------------------------------------------------------------

        notification_id_1 = self.unsc.create_notification(notification=notification_request_1, user_id=user_id)
        notification_id_2 = self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        notifications = set([notification_id_1, notification_id_2])

        #--------------------------------------------------------------------------------------
        # Check that the correct events were published
        #--------------------------------------------------------------------------------------

        received_event_1 = queue.get()
        received_event_2 = queue.get()

        notifications_received = set([received_event_1.notification_id, received_event_2.notification_id])

        self.assertEquals(notifications, notifications_received)

        #--------------------------------------------------------------------------------------
        # Delete notification
        #--------------------------------------------------------------------------------------

        notification_id_1 = self.unsc.create_notification(notification=notification_request_1, user_id=user_id)
        notification_id_2 = self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        notifications = set([notification_id_1, notification_id_2])

        #--------------------------------------------------------------------------------------
        # Check that the correct events were published
        #--------------------------------------------------------------------------------------

        received_event_1 = queue.get()
        received_event_2 = queue.get()

        notifications_received = set([received_event_1.notification_id, received_event_2.notification_id])

        self.assertEquals(notifications, notifications_received)


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_user_info_UNS(self):
        '''
        Test that the user info dictionary maintained by the notification workers get updated when
        a notification is created, updated, or deleted by UNS
        '''

        proc1 = self.container.proc_manager.procs_by_name['user_notification']

        #--------------------------------------------------------------------------------------
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(   name = 'notification_1',
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent',
            event_subtype = 'subtype_1')

        notification_request_2 = NotificationRequest(   name = 'notification_2',
            origin="instrument_2",
            origin_type="type_2",
            event_type='DetectionEvent',
            event_subtype = 'subtype_2')


        #--------------------------------------------------------------------------------------
        # Create users and make user_ids
        #--------------------------------------------------------------------------------------

        user_1 = UserInfo()
        user_1.name = 'user_1'
        user_1.contact.email = 'user_1@gmail.com'


        user_2 = UserInfo()
        user_2.name = 'user_2'
        user_2.contact.email = 'user_2@gmail.com'

        user_id_1, _ = self.rrc.create(user_1)
        user_id_2, _ = self.rrc.create(user_2)


        #--------------------------------------------------------------------------------------
        # Create a notification
        #--------------------------------------------------------------------------------------

        notification_id_1 = self.unsc.create_notification(notification=notification_request_1, user_id=user_id_1)
        notification_id_2 = self.unsc.create_notification(notification=notification_request_2, user_id=user_id_2)

        #--------------------------------------------------------------------------------------
        # Check the user_info and reverse_user_info got reloaded
        #--------------------------------------------------------------------------------------

        # Check in UNS ------------>

        # read back the registered notification request objects
        notification_request_1 = self.rrc.read(notification_id_1)
        notification_request_2 = self.rrc.read(notification_id_2)

        # check user_info dictionary
        self.assertEquals(proc1.event_processor.user_info['user_1']['user_contact'].email, 'user_1@gmail.com' )
        self.assertEquals(proc1.event_processor.user_info['user_1']['notifications'], [notification_request_1])

        self.assertEquals(proc1.event_processor.user_info['user_2']['user_contact'].email, 'user_2@gmail.com' )
        self.assertEquals(proc1.event_processor.user_info['user_2']['notifications'], [notification_request_2])

        self.assertEquals(proc1.event_processor.reverse_user_info['event_origin']['instrument_1'], ['user_1'])
        self.assertEquals(proc1.event_processor.reverse_user_info['event_origin']['instrument_2'], ['user_2'])

        self.assertEquals(proc1.event_processor.reverse_user_info['event_type']['ResourceLifecycleEvent'], ['user_1'])
        self.assertEquals(proc1.event_processor.reverse_user_info['event_type']['DetectionEvent'], ['user_2'])

        self.assertEquals(proc1.event_processor.reverse_user_info['event_subtype']['subtype_1'], ['user_1'])
        self.assertEquals(proc1.event_processor.reverse_user_info['event_subtype']['subtype_2'], ['user_2'])

        self.assertEquals(proc1.event_processor.reverse_user_info['event_origin_type']['type_1'], ['user_1'])
        self.assertEquals(proc1.event_processor.reverse_user_info['event_origin_type']['type_2'], ['user_2'])

        log.debug("The event processor received the notification topics after a create_notification() for two users")
        log.debug("Verified that the event processor correctly updated its user info dictionaries")
        #--------------------------------------------------------------------------------------
        # Create another notification for the first user
        #--------------------------------------------------------------------------------------

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_1)

        # Check in UNS ------------>
        self.assertEquals(proc1.event_processor.user_info['user_1']['user_contact'].email, 'user_1@gmail.com' )

        self.assertEquals(proc1.event_processor.user_info['user_1']['notifications'], [notification_request_1, notification_request_2])

        self.assertEquals(proc1.event_processor.reverse_user_info['event_origin']['instrument_1'], ['user_1'])
        self.assertEquals(proc1.event_processor.reverse_user_info['event_origin']['instrument_2'], ['user_2', 'user_1'])

        self.assertEquals(proc1.event_processor.reverse_user_info['event_type']['ResourceLifecycleEvent'], ['user_1'])
        self.assertEquals(proc1.event_processor.reverse_user_info['event_type']['DetectionEvent'], ['user_2', 'user_1'])

        self.assertEquals(proc1.event_processor.reverse_user_info['event_subtype']['subtype_1'], ['user_1'])
        self.assertEquals(proc1.event_processor.reverse_user_info['event_subtype']['subtype_2'], ['user_2', 'user_1'])

        self.assertEquals(proc1.event_processor.reverse_user_info['event_origin_type']['type_1'], ['user_1'])
        self.assertEquals(proc1.event_processor.reverse_user_info['event_origin_type']['type_2'], ['user_2', 'user_1'])

        log.debug("The event processor received the notification topics after another create_notification() for the first user")
        log.debug("Verified that the event processor correctly updated its user info dictionaries")

        #--------------------------------------------------------------------------------------
        # Update notification and check that the user_info and reverse_user_info in UNS got reloaded
        #--------------------------------------------------------------------------------------

        notification_request_1.origin = "newly_changed_instrument"

        self.unsc.update_notification(notification=notification_request_1, user_id=user_id_1)

        # Check for UNS ------->

        # user_info
        notification_request_1 = self.rrc.read(notification_id_1)

        # check that the updated notification is in the user info dictionary
        self.assertTrue(notification_request_1 in proc1.event_processor.user_info['user_1']['notifications'] )

        # check that the notifications in the user info dictionary got updated
        update_worked = False
        for notification in proc1.event_processor.user_info['user_1']['notifications']:
            if notification.origin == "newly_changed_instrument":
                update_worked = True
                break

        self.assertTrue(update_worked)

        # reverse_user_info
        self.assertTrue('user_1' in proc1.event_processor.reverse_user_info['event_origin']["newly_changed_instrument"])

        log.debug("Verified that the event processor correctly updated its user info dictionaries after an update_notification()")

        #--------------------------------------------------------------------------------------
        # Delete notification and check that the user_info and reverse_user_info in UNS got reloaded
        #--------------------------------------------------------------------------------------

        self.unsc.delete_notification(notification_id_2)

        # Check for UNS ------->

        # check that the notification is not there anymore in the resource registry
        with self.assertRaises(NotFound):
            notification = self.rrc.read(notification_id_2)

        # check that the user_info dictionary for the user is not holding the notification anymore
        self.assertFalse(notification_request_2 in proc1.event_processor.user_info['user_1']['notifications'])

        log.debug("Verified that the event processor correctly updated its user info dictionaries after an delete_notification()")

        log.debug("REQ: L4-CI-DM-RQ-56 was satisfied here for UNS")

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_user_info_notification_worker(self):
        '''
        Test the user_info and reverse user info dictionary capability of the notification worker
        '''

        #--------------------------------------------------------------------------------------
        # Create a user and get the user_id
        #--------------------------------------------------------------------------------------

        user = UserInfo()
        user.name = 'new_user'
        user.contact.email = 'new_user@gmail.com'

        # this part of code is in the beginning to allow enough time for users_index creation

        user_id, _ = self.rrc.create(user)

        # confirm that users_index got created by discovery
        search_string = 'search "name" is "*" from "users_index"'

        results = self.poll(9, self.discovery.parse,search_string)
        self.assertIsNotNone(results, 'Results not found')

        #--------------------------------------------------------------------------------------
        # Create notification workers
        #--------------------------------------------------------------------------------------

        pids = self.unsc.create_worker(number_of_workers=1)
        self.assertIsNotNone(pids, 'No workers were created')

        #--------------------------------------------------------------------------------------
        # Make notification request objects -- Remember to put names
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(   name = "notification_1",
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent',
            event_subtype = 'subtype_1')

        notification_request_2 = NotificationRequest(   name = "notification_2",
            origin="instrument_2",
            origin_type="type_2",
            event_type='DetectionEvent',
            event_subtype = 'subtype_2')


        #--------------------------------------------------------------------------------------
        # Create a notification
        #--------------------------------------------------------------------------------------

        notification_id_1 = self.unsc.create_notification(notification=notification_request_1, user_id=user_id)

        #--------------------------------------------------------------------------------------
        # Check the user_info and reverse_user_info got reloaded
        #--------------------------------------------------------------------------------------
        proc1 = self.container.proc_manager.procs_by_name[pids[0]]

        ar_1 = gevent.event.AsyncResult()
        ar_2 = gevent.event.AsyncResult()

        def received_reload(msg, headers):
            ar_1.set(msg)
            ar_2.set(headers)


        proc1.test_hook = received_reload

        reloaded_user_info = ar_1.get(timeout=10)
        reloaded_reverse_user_info = ar_2.get(timeout=10)

        # read back the registered notification request objects
        notification_request_1 = self.rrc.read(notification_id_1)

        self.assertEquals(reloaded_user_info['new_user']['notifications'], [notification_request_1] )
        self.assertEquals(reloaded_user_info['new_user']['user_contact'].email, 'new_user@gmail.com')

        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_type']['ResourceLifecycleEvent'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_1'], ['new_user'] )

        log.debug("Verified that the notification worker correctly updated its user info dictionaries after a create_notification()")

        #--------------------------------------------------------------------------------------
        # Create another notification
        #--------------------------------------------------------------------------------------

        notification_id_2 = self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        ar_1 = gevent.event.AsyncResult()
        ar_2 = gevent.event.AsyncResult()

        reloaded_user_info = ar_1.get(timeout=10)
        reloaded_reverse_user_info = ar_2.get(timeout=10)

        notification_request_2 = self.rrc.read(notification_id_2)


        self.assertEquals(reloaded_user_info['new_user']['notifications'], [notification_request_1, notification_request_2] )

        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_2'], ['new_user'] )

        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_2'], ['new_user'] )

        self.assertEquals(reloaded_reverse_user_info['event_type']['ResourceLifecycleEvent'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_type']['DetectionEvent'], ['new_user'] )

        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_2'], ['new_user'] )

        log.debug("Verified that the notification worker correctly updated its user info dictionaries after another create_notification()")


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_batch_notifications(self):
        '''
        Test that batch notifications work
        '''

        #--------------------------------------------------------------------------------------
        # Publish events corresponding to the notification requests just made
        # These events will get stored in the event repository allowing UNS to batch process
        # them later for batch notifications
        #--------------------------------------------------------------------------------------

        event_publisher = EventPublisher()

        # this part of code is in the beginning to allow enough time for the events_index creation

        for i in xrange(10):
            event_publisher.publish_event( ts_created= i ,
                origin="instrument_1",
                origin_type="type_1",
                event_type='ResourceLifecycleEvent')

            event_publisher.publish_event( ts_created= i ,
                origin="instrument_3",
                origin_type="type_3",
                event_type='ResourceLifecycleEvent')

        #----------------------------------------------------------------------------------------
        # Create users and get the user_ids
        #----------------------------------------------------------------------------------------

        # user_1
        user_1 = UserInfo()
        user_1.name = 'user_1'
        user_1.contact.email = 'user_1@gmail.com'

        # user_2
        user_2 = UserInfo()
        user_2.name = 'user_2'
        user_2.contact.email = 'user_2@gmail.com'

        # user_3
        user_3 = UserInfo()
        user_3.name = 'user_3'
        user_3.contact.email = 'user_3@gmail.com'

        # this part of code is in the beginning to allow enough time for the users_index creation

        user_id_1, _ = self.rrc.create(user_1)
        user_id_2, _ = self.rrc.create(user_2)
        user_id_3, _ = self.rrc.create(user_3)

        #--------------------------------------------------------------------------------------
        # Grab the UNS process
        #--------------------------------------------------------------------------------------

        proc1 = self.container.proc_manager.procs_by_name['user_notification']

        #--------------------------------------------------------------------------------------
        # Make notification request objects -- Remember to put names
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(   name = "notification_1",
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(   name = "notification_2",
            origin="instrument_2",
            origin_type="type_2",
            event_type='DetectionEvent')


        notification_request_3 = NotificationRequest(   name = "notification_3",
            origin="instrument_3",
            origin_type="type_3",
            event_type='ResourceLifecycleEvent')



        #--------------------------------------------------------------------------------------
        # Create a notification using UNS. This should cause the user_info to be updated
        #--------------------------------------------------------------------------------------

        self.unsc.create_notification(notification=notification_request_1, user_id=user_id_1)
        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_1)

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_2)

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_3)
        self.unsc.create_notification(notification=notification_request_3, user_id=user_id_3)

        #--------------------------------------------------------------------------------------
        # Do a process_batch() in order to start the batch notifications machinery
        #--------------------------------------------------------------------------------------

        test_start_time = 5
        test_end_time = 8
        self.unsc.process_batch(start_time=test_start_time, end_time= test_end_time)

        #--------------------------------------------------------------------------------------
        # Check that the emails were sent to the users. This is done using the fake smtp client
        # Make assertions....
        #--------------------------------------------------------------------------------------

        self.assertFalse(proc1.smtp_client.sent_mail.empty())

        email_list = []

        while not proc1.smtp_client.sent_mail.empty():
            email_tuple = proc1.smtp_client.sent_mail.get()
            email_list.append(email_tuple)

        self.assertEquals(len(email_list), 2)

        for email_tuple in email_list:
            msg_sender, msg_recipient, msg = email_tuple

            self.assertEquals(msg_sender, CFG.get_safe('server.smtp.sender') )
            self.assertTrue(msg_recipient in ['user_1@gmail.com', 'user_2@gmail.com', 'user_3@gmail.com'])

            lines = msg.split("\r\n")

            maps = []

            for line in lines:

                maps.extend(line.split(','))

            event_time = ''
            for map in maps:
                fields = map.split(":")
                if fields[0].find("ts_created") > -1:
                    event_time = int(fields[1].strip(" "))
                    break

            # Check that the events sent in the email had times within the user specified range
            self.assertTrue(event_time >= test_start_time)
            self.assertTrue(event_time <= test_end_time)


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_worker_send_email(self):
        '''
        Test that the workers process the notification event and send email using the
        fake smtp client
        '''

        #-------------------------------------------------------
        # Create users and get the user_ids
        #-------------------------------------------------------

        # user_1
        user_1 = UserInfo()
        user_1.name = 'user_1'
        user_1.contact.email = 'user_1@gmail.com'

        # user_2
        user_2 = UserInfo()
        user_2.name = 'user_2'
        user_2.contact.phone = 'user_2@gmail.com'


        user_id_1, _ = self.rrc.create(user_1)
        user_id_2, _ = self.rrc.create(user_2)

        #--------------------------------------------------------------------------------------
        # Create notification workers
        #--------------------------------------------------------------------------------------

        pids = self.unsc.create_worker(number_of_workers=1)
        self.assertEquals(len(pids), 1)

        proc1 = self.container.proc_manager.procs_by_name[pids[0]]

        #--------------------------------------------------------------------------------------
        # Make notification request objects -- Remember to put names
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(   name = "notification_1",
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(   name = "notification_2",
            origin="instrument_2",
            origin_type="type_2",
            event_type='DetectionEvent')


        #--------------------------------------------------------------------------------------
        # Create notifications using UNS.
        #--------------------------------------------------------------------------------------

        self.unsc.create_notification(notification=notification_request_1, user_id=user_id_1)
        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_1)

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_2)

        #--------------------------------------------------------------------------------------
        # Publish events
        #--------------------------------------------------------------------------------------

        event_publisher = EventPublisher()

        event_publisher.publish_event(  ts_created= 5,
            event_type = "ResourceLifecycleEvent",
            origin="instrument_1",
            origin_type="type_1")

        event_publisher.publish_event(  ts_created= 10,
            event_type = "DetectionEvent",
            origin="instrument_2",
            origin_type="type_2")

        #--------------------------------------------------------------------------------------
        # Check that the workers processed the events
        #--------------------------------------------------------------------------------------

        # check fake smtp client for emails sent
        self.assertFalse(proc1.smtp_client.sent_mail.empty())

        email_list = []

        while not proc1.smtp_client.sent_mail.empty():
            email_tuple = proc1.smtp_client.sent_mail.get()
            email_list.append(email_tuple)

        # check that one user got the email
        self.assertEquals(len(email_list), 1)

        for email_tuple in email_list:
            msg_sender, msg_recipient, msg = email_tuple

            self.assertEquals(msg_sender, CFG.get_safe('server.smtp.sender') )
            self.assertTrue(msg_recipient in ['user_1@gmail.com', 'user_2@gmail.com'])

            maps = []
            maps = msg.split(",")

            event_time = ''
            for map in maps:
                fields = map.split(":")
                if fields[0].find("Time stamp") > -1:
                    event_time = int(fields[1].strip(" "))
                    break

            # Check that the event sent in the email had time within the user specified range
            self.assertEquals(event_time, 5)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_create_read_user_notifications(self):
        '''
        Test the create and read notification methods
        '''

        #--------------------------------------------------------------------------------------
        # create user with email address in RR
        #--------------------------------------------------------------------------------------

        user = UserInfo()
        user.name = 'user_1'
        user.contact.email = 'user_1@gmail.com'

        user_id, _ = self.rrc.create(user)


        #--------------------------------------------------------------------------------------
        # Make notification request objects -- Remember to put names
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(   name = "notification_1",
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(   name = "notification_2",
            origin="instrument_2",
            origin_type="type_2",
            event_type='DetectionEvent')


        #--------------------------------------------------------------------------------------
        # Create notifications using UNS.
        #--------------------------------------------------------------------------------------

        notification_id1 =  self.unsc.create_notification(notification=notification_request_1, user_id=user_id)
        notification_id2 =  self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        #--------------------------------------------------------------------------------------
        # Make assertions
        #--------------------------------------------------------------------------------------

        n1 = self.unsc.read_notification(notification_id1)
        n2 = self.unsc.read_notification(notification_id2)

        self.assertEquals(n1.event_type, notification_request_1.event_type)
        self.assertEquals(n1.origin, notification_request_1.origin)
        self.assertEquals(n1.origin_type, notification_request_1.origin_type)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_delete_user_notifications(self):
        '''
        Test deleting a notification
        '''

        #--------------------------------------------------------------------------------------
        # create user with email address in RR
        #--------------------------------------------------------------------------------------

        user = UserInfo()
        user.name = 'user_1'
        user.contact.email = 'user_1@gmail.com'

        user_id, _ = self.rrc.create(user)

        #--------------------------------------------------------------------------------------
        # Make notification request objects - Remember to put names
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(   name = "notification_1",
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(   name = "notification_2",
            origin="instrument_2",
            origin_type="type_2",
            event_type='DetectionEvent')

        notification_id1 =  self.unsc.create_notification(notification=notification_request_1, user_id=user_id)
        notification_id2 =  self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        # delete both notifications
        self.unsc.delete_notification(notification_id1)
        self.unsc.delete_notification(notification_id2)

        # check that the notifications are not there
        with self.assertRaises(NotFound):
            notific1 = self.unsc.read_notification(notification_id1)
        with self.assertRaises(NotFound):
            notific2 = self.unsc.read_notification(notification_id2)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_update_user_notification(self):
        '''
        Test updating a user notification
        '''

        #--------------------------------------------------------------------------------------
        # create user with email address in RR
        #--------------------------------------------------------------------------------------

        user = UserInfo()
        user.name = 'user_1'
        user.contact.email = 'user_1@gmail.com'

        user_id, _ = self.rrc.create(user)

        #--------------------------------------------------------------------------------------
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        #--------------------------------------------------------------------------------------
        # Make notification request objects - Remember to put names
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(   name = "notification_1",
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_id =  self.unsc.create_notification(notification=notification_request_1, user_id=user_id)

        # read back the notification and change it
        notification = self.unsc.read_notification(notification_id)
        notification.origin_type = 'new_type'

        self.unsc.update_notification(notification, user_id)

        # read back the notification and check that it got changed
        notification = self.unsc.read_notification(notification_id)

        self.assertEquals(notification.origin_type, 'new_type')
        self.assertEquals(notification.event_type, 'ResourceLifecycleEvent')
        self.assertEquals(notification.origin, 'instrument_1')


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_find_events(self):
        '''
        Test the find events functionality of UNS
        '''

        # publish some events for the event repository
        event_publisher_1 = EventPublisher("ResourceLifecycleEvent")
        event_publisher_2 = EventPublisher("ReloadUserInfoEvent")

        for i in xrange(10):
            event_publisher_1.publish_event(origin='Some_Resource_Agent_ID1', ts_created = i)
            event_publisher_2.publish_event(origin='Some_Resource_Agent_ID2', ts_created = i)

        # allow elastic search to populate the indexes. This gives enough time for the reload of user_info
        gevent.sleep(4)
        events = self.unsc.find_events(origin='Some_Resource_Agent_ID1', min_datetime=4, max_datetime=7)

        self.assertEquals(len(events), 4)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_create_several_workers(self):
        '''
        Create more than one worker. Test that they process events in round robin
        '''
        pids = self.unsc.create_worker(number_of_workers=2)

        self.assertEquals(len(pids), 2)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_publish_event_on_time(self):
        '''
        Test the publish_event method of UNS
        '''
        interval_timer_params = {'interval':3,
                                'number_of_intervals':4}

        #--------------------------------------------------------------------------------
        # Create an event object
        #--------------------------------------------------------------------------------
        event = DeviceEvent(  origin= "origin_1",
            origin_type='origin_type_1',
            sub_type= 'sub_type_1',
            ts_created = 2)

        #--------------------------------------------------------------------------------
        # Set up a subscriber to listen for that event
        #--------------------------------------------------------------------------------
        def received_event(event, headers):
            log.debug("received the event in the test: %s" % event)

            #--------------------------------------------------------------------------------
            # check that the event was published
            #--------------------------------------------------------------------------------
            self.assertEquals(event.origin, "origin_1")
            self.assertEquals(event.type_, 'DeviceEvent')
            self.assertEquals(event.origin_type, 'origin_type_1')
            self.assertEquals(event.ts_created, 2)
            self.assertEquals(event.sub_type, 'sub_type_1')

        event_subscriber = EventSubscriber( event_type = 'DeviceEvent',
                                            origin="origin_1",
                                            callback=received_event)
        event_subscriber.start()

        #--------------------------------------------------------------------------------
        # Use the UNS publish_event
        #--------------------------------------------------------------------------------

        self.unsc.publish_event(event=event, interval_timer_params = interval_timer_params )

