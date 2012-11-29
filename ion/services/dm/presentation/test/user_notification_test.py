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
from pyon.public import IonObject, RT, OT, PRED, Container, CFG
from pyon.core.exception import NotFound, BadRequest
from pyon.core.bootstrap import get_sys_name
from ion.services.dm.utility.granule_utils import time_series_domain
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from ion.services.dm.presentation.user_notification_service import UserNotificationService
from interface.objects import UserInfo, DeliveryConfig, ComputedListValue, ComputedValueAvailability
from interface.objects import DeviceEvent
from pyon.util.context import LocalContextMixin
from interface.services.cei.ischeduler_service import SchedulerServiceProcessClient
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log
from pyon.event.event import EventPublisher, EventSubscriber
import gevent
from mock import Mock, mocksignature
from interface.objects import NotificationRequest, TemporalBounds
from ion.services.dm.inventory.index_management_service import IndexManagementService
from ion.services.dm.presentation.user_notification_service import EmailEventProcessor
from ion.processes.bootstrap.index_bootstrap import STD_INDEXES
import os, time, uuid
from gevent import event, queue
from gevent.timeout import Timeout
from gevent.event import Event
import elasticpy as ep
from datetime import datetime, timedelta
from sets import Set

use_es = CFG.get_safe('system.elasticsearch',False)

def now():
    '''
    This method defines what the UNS uses as its "current" time
    '''
    return datetime.utcnow()

class FakeProcess(LocalContextMixin):
    name = 'scheduler_for_user_notification_test'
    id = 'scheduler_client'
    process_type = 'simple'

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
        self.user_notification.event_processor = EmailEventProcessor()

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

        self.user_notification.notifications = {}

        self.user_notification.event_processor.add_notification_for_user = mocksignature(self.user_notification.event_processor.add_notification_for_user)
        self.user_notification.update_user_info_dictionary = mocksignature(self.user_notification.update_user_info_dictionary)
        self.user_notification.event_publisher.publish_event = mocksignature(self.user_notification.event_publisher.publish_event)

        self.user_notification._notification_in_notifications = mocksignature(self.user_notification._notification_in_notifications)
        self.user_notification._notification_in_notifications.return_value = None

        self.mock_rr_client.create_association = mocksignature(self.mock_rr_client.create_association)

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

        # Create a notification object
        notification_request = NotificationRequest(name='Setting_email',
            origin = 'origin',
            origin_type = 'origin_type',
            event_type= 'event_type',
            event_subtype = 'event_subtype')

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

        self.user_notification.notifications = []

        self.user_notification._update_notification_in_notifications_dict = mocksignature(self.user_notification._update_notification_in_notifications_dict)
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
        self.user_notification.update_user_info_dictionary.assert_called_once_with('user_id_1', notification, notification)


    def test_delete_user_notification(self):
        '''
        Test deleting a notification
        '''

        notification_id = 'notification_id_1'

        self.user_notification.event_publisher.publish_event = mocksignature(self.user_notification.event_publisher.publish_event)
        self.user_notification.user_info = {}

        #-------------------------------------------------------------------------------------------------------------------
        # Create a notification object
        #-------------------------------------------------------------------------------------------------------------------

        notification_request = NotificationRequest(name='a name',
            origin = 'origin_1',
            origin_type = 'origin_type_1',
            event_type= 'event_type_1',
            event_subtype = 'event_subtype_1',
            temporal_bounds = TemporalBounds())
        notification_request.temporal_bounds.start_datetime = ''

        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = notification_request

        self.mock_rr_client.update = mocksignature(self.mock_rr_client.update)
        self.mock_rr_client.update.return_value = ''

        #-------------------------------------------------------------------------------------------------------------------
        # execution
        #-------------------------------------------------------------------------------------------------------------------

        self.user_notification.delete_notification(notification_id=notification_id)

        #-------------------------------------------------------------------------------------------------------------------
        # assertions
        #-------------------------------------------------------------------------------------------------------------------

        self.mock_rr_client.read.assert_called_once_with(notification_id, '')

        notification_request.temporal_bounds.end_datetime = self.user_notification.makeEpochTime(now())
        self.mock_rr_client.update.assert_called_once_with(notification_request)

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

        self.event = Event()
        self.number_event_published = 0

        process = FakeProcess()
        self.ssclient = SchedulerServiceProcessClient(node=self.container.node, process=process)

        self.ION_NOTIFICATION_EMAIL_ADDRESS = 'ION_notifications-do-not-reply@oceanobservatories.org'

    def event_poll(self, poller, timeout):
        success = False
        with gevent.timeout.Timeout(timeout):
            while not success:
                success = poller()
                gevent.sleep(0.1) # Let the sockets close by yielding this greenlet
        return success




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
        self.addCleanup(reload_event_subscriber.stop)

        #--------------------------------------------------------------------------------------
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        notification_request_correct = NotificationRequest(   name= 'notification_1',
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

        notification_id_1 = self.unsc.create_notification(notification=notification_request_correct, user_id=user_id)
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

        notification_id_1 = self.unsc.create_notification(notification=notification_request_correct, user_id=user_id)
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

        notification_id_1 = self.unsc.create_notification(notification=notification_request_correct, user_id=user_id)
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

        notification_request_correct = NotificationRequest(   name = 'notification_1',
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

        notification_id_1 = self.unsc.create_notification(notification=notification_request_correct, user_id=user_id_1)
        notification_id_2 = self.unsc.create_notification(notification=notification_request_2, user_id=user_id_2)

        #--------------------------------------------------------------------------------------
        # Check the user_info and reverse_user_info got reloaded
        #--------------------------------------------------------------------------------------

        # Check in UNS ------------>

        # read back the registered notification request objects
        notification_request_correct = self.rrc.read(notification_id_1)
        notification_request_2 = self.rrc.read(notification_id_2)

        # check user_info dictionary
        self.assertEquals(proc1.user_info[user_id_1]['user_contact'].email, 'user_1@gmail.com' )
        self.assertEquals(proc1.user_info[user_id_1]['notifications'], [notification_request_correct])

        self.assertEquals(proc1.user_info[user_id_2]['user_contact'].email, 'user_2@gmail.com' )
        self.assertEquals(proc1.user_info[user_id_2]['notifications'], [notification_request_2])

        self.assertEquals(proc1.reverse_user_info['event_origin']['instrument_1'], [user_id_1])
        self.assertEquals(proc1.reverse_user_info['event_origin']['instrument_2'], [user_id_2])

        self.assertEquals(proc1.reverse_user_info['event_type']['ResourceLifecycleEvent'], [user_id_1])
        self.assertEquals(proc1.reverse_user_info['event_type']['DetectionEvent'], [user_id_2])

        self.assertEquals(proc1.reverse_user_info['event_subtype']['subtype_1'], [user_id_1])
        self.assertEquals(proc1.reverse_user_info['event_subtype']['subtype_2'], [user_id_2])

        self.assertEquals(proc1.reverse_user_info['event_origin_type']['type_1'], [user_id_1])
        self.assertEquals(proc1.reverse_user_info['event_origin_type']['type_2'], [user_id_2])

        log.debug("The event processor received the notification topics after a create_notification() for two users")
        log.debug("Verified that the event processor correctly updated its user info dictionaries")
        #--------------------------------------------------------------------------------------
        # Create another notification for the first user
        #--------------------------------------------------------------------------------------

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_1)

        # Check in UNS ------------>
        self.assertEquals(proc1.user_info[user_id_1]['user_contact'].email, 'user_1@gmail.com' )

        notifications = proc1.user_info[user_id_1]['notifications']
        origins = []
        event_types = []
        for notific in notifications:
            origins.append(notific.origin)
            event_types.append(notific.event_type)

        self.assertEquals(set(origins), set(['instrument_1', 'instrument_2']))
        self.assertEquals(set(event_types), set(['ResourceLifecycleEvent', 'DetectionEvent']))

        self.assertEquals(proc1.reverse_user_info['event_origin']['instrument_1'], [user_id_1])
        self.assertEquals(set(proc1.reverse_user_info['event_origin']['instrument_2']), set([user_id_2, user_id_1]))

        self.assertEquals(proc1.reverse_user_info['event_type']['ResourceLifecycleEvent'], [user_id_1])
        self.assertEquals(set(proc1.reverse_user_info['event_type']['DetectionEvent']), set([user_id_2, user_id_1]))

        self.assertEquals(proc1.reverse_user_info['event_subtype']['subtype_1'], [user_id_1])
        self.assertEquals(set(proc1.reverse_user_info['event_subtype']['subtype_2']), set([user_id_2, user_id_1]))

        self.assertEquals(proc1.reverse_user_info['event_origin_type']['type_1'], [user_id_1])
        self.assertEquals(set(proc1.reverse_user_info['event_origin_type']['type_2']), set([user_id_2, user_id_1]))

        log.debug("The event processor received the notification topics after another create_notification() for the first user")
        log.debug("Verified that the event processor correctly updated its user info dictionaries")

        #--------------------------------------------------------------------------------------
        # Update notification and check that the user_info and reverse_user_info in UNS got reloaded
        #--------------------------------------------------------------------------------------

        notification_request_correct.origin = "newly_changed_instrument"

        self.unsc.update_notification(notification=notification_request_correct, user_id=user_id_1)

        # Check for UNS ------->

        # user_info
        notification_request_correct = self.rrc.read(notification_id_1)

        # check that the updated notification is in the user info dictionary
        self.assertTrue(notification_request_correct in proc1.user_info[user_id_1]['notifications'] )

        # check that the notifications in the user info dictionary got updated
        update_worked = False
        for notification in proc1.user_info[user_id_1]['notifications']:
            if notification.origin == "newly_changed_instrument":
                update_worked = True
                break

        self.assertTrue(update_worked)

        # reverse_user_info
        self.assertTrue(user_id_1 in proc1.reverse_user_info['event_origin']["newly_changed_instrument"])

        log.debug("Verified that the event processor correctly updated its user info dictionaries after an update_notification()")

        #--------------------------------------------------------------------------------------------------------------------------------------
        # Delete notification and check. Whether the user_info and reverse_user_info in UNS got reloaded is done in test_get_subscriptions()
        #--------------------------------------------------------------------------------------------------------------------------------------

        self.unsc.delete_notification(notification_id_2)

        notific = self.rrc.read(notification_id_2)
        # This checks that the notification has been retired.
        self.assertNotEquals(notific.temporal_bounds.end_datetime, '')

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

        notification_request_correct = NotificationRequest(   name = "notification_1",
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

        notification_id_1 = self.unsc.create_notification(notification=notification_request_correct, user_id=user_id)

        #--------------------------------------------------------------------------------------
        # Check the user_info and reverse_user_info got reloaded
        #--------------------------------------------------------------------------------------
        proc1 = self.container.proc_manager.procs.get(pids[0])

        ar_1 = gevent.event.AsyncResult()
        ar_2 = gevent.event.AsyncResult()

        def received_reload(msg, headers):
            ar_1.set(msg)
            ar_2.set(headers)


        proc1.test_hook = received_reload

        reloaded_user_info = ar_1.get(timeout=10)
        reloaded_reverse_user_info = ar_2.get(timeout=10)

        # read back the registered notification request objects
        notification_request_correct = self.rrc.read(notification_id_1)

        self.assertEquals(reloaded_user_info[user_id]['notifications'], [notification_request_correct] )
        self.assertEquals(reloaded_user_info[user_id]['user_contact'].email, 'new_user@gmail.com')

        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_1'], [user_id] )
        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_1'], [user_id] )
        self.assertEquals(reloaded_reverse_user_info['event_type']['ResourceLifecycleEvent'], [user_id] )
        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_1'], [user_id] )

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

        #--------------------------------------------------------------------------------------------------------------------------
        # Check that the two notifications created for the same user got properly reloaded in the user_info dictionaries of the workers
        #--------------------------------------------------------------------------------------------------------------------------
        notifications = reloaded_user_info[user_id]['notifications']
        origins = []
        event_types = []
        for notific in notifications:
            origins.append(notific.origin)
            event_types.append(notific.event_type)

        shouldbe_origins = []
        shouldbe_event_types = []
        for notific in [notification_request_correct, notification_request_2]:
            shouldbe_origins.append(notific.origin)
            shouldbe_event_types.append(notific.event_type)

        self.assertEquals(set(origins), set(shouldbe_origins))
        self.assertEquals(set(event_types), set(shouldbe_event_types))

        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_1'], [user_id] )
        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_2'], [user_id] )

        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_1'], [user_id] )
        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_2'], [user_id] )

        self.assertEquals(reloaded_reverse_user_info['event_type']['ResourceLifecycleEvent'], [user_id] )
        self.assertEquals(reloaded_reverse_user_info['event_type']['DetectionEvent'], [user_id] )

        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_1'], [user_id] )
        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_2'], [user_id] )

        log.debug("Verified that the notification worker correctly updated its user info dictionaries after another create_notification()")


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_process_batch(self):
        '''
        Test that the process_batch() method works
        '''


        test_start_time = UserNotificationIntTest.makeEpochTime(datetime.utcnow())
        test_end_time = UserNotificationIntTest.makeEpochTime(datetime.utcnow() + timedelta(seconds=10))

        #--------------------------------------------------------------------------------------
        # Publish events corresponding to the notification requests just made
        # These events will get stored in the event repository allowing UNS to batch process
        # them later for batch notifications
        #--------------------------------------------------------------------------------------

        event_publisher = EventPublisher()

        # this part of code is in the beginning to allow enough time for the events_index creation

        for i in xrange(10):

            t = now()
            t = UserNotificationIntTest.makeEpochTime(t)

            event_publisher.publish_event( ts_created= t ,
                origin="instrument_1",
                origin_type="type_1",
                event_type='ResourceLifecycleEvent')

            event_publisher.publish_event( ts_created= t ,
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

        notification_request_correct = NotificationRequest(   name = "notification_1",
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

        self.unsc.create_notification(notification=notification_request_correct, user_id=user_id_1)
        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_1)

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_2)

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_3)
        self.unsc.create_notification(notification=notification_request_3, user_id=user_id_3)

        #--------------------------------------------------------------------------------------
        # Do a process_batch() in order to start the batch notifications machinery
        #--------------------------------------------------------------------------------------

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
        user_2.contact.phones = ['5551212']


        user_id_1, _ = self.rrc.create(user_1)
        user_id_2, _ = self.rrc.create(user_2)

        #--------------------------------------------------------------------------------------
        # Create notification workers
        #--------------------------------------------------------------------------------------

        '''
        Since notification workers are being created in bootstrap, we dont need to generate any here
        '''
        #        pids = self.unsc.create_worker(number_of_workers=1)
        #        self.assertEquals(len(pids), 1)

        #--------------------------------------------------------------------------------------
        # Get the list of notification worker processes existing in the container
        # This will enable us to get the fake smtp client objects they are using,
        # which in turn will allow us to check what the notification emails they are sending
        #--------------------------------------------------------------------------------------

        procs = []

        for process_name in self.container.proc_manager.procs.iterkeys():
            # if the process is a notification worker process, add its pid to the list of pids
            if process_name.find("notification_worker") != -1:
                proc = self.container.proc_manager.procs[process_name]

                log.debug("Got the following notification worker process with name: %s, process: %s" % (process_name, proc))

                procs.append(proc)

        #--------------------------------------------------------------------------------------
        # Make notification request objects -- Remember to put names
        #--------------------------------------------------------------------------------------

        notification_request_correct = NotificationRequest(   name = "notification_1",
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

        self.unsc.create_notification(notification=notification_request_correct, user_id=user_id_1)
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

        email_sent_by_a_worker = False
        worker_that_sent_email = None
        for proc in procs:
            if not proc.smtp_client.sent_mail.empty():
                email_sent_by_a_worker = True
                worker_that_sent_email = proc
                break

        log.debug("Was email sent by any worker?: %s" % email_sent_by_a_worker)

        # check fake smtp client for emails sent
        self.assertTrue(email_sent_by_a_worker)

        email_tuple = None
        if email_sent_by_a_worker:
            email_tuple = worker_that_sent_email.smtp_client.sent_mail.get()

        # Parse the email sent and check and make assertions about email body. Make assertions about the sender and recipient
        msg_sender, msg_recipient, msg = email_tuple

        self.assertEquals(msg_sender, CFG.get_safe('server.smtp.sender') )
        self.assertTrue(msg_recipient in ['user_1@gmail.com', 'user_2@gmail.com'])

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

        #--------------------------------------------------------------------------------------
        # Create the same notification request again using UNS. Check that no duplicate notification request is made
        #--------------------------------------------------------------------------------------

        notification_again_id =  self.unsc.create_notification(notification=notification_request_1, user_id=user_id)
        notification_again = self.rrc.read(notification_again_id)

        self.assertEquals(notification_again.event_type, notification_request_1.event_type)
        self.assertEquals(notification_again.origin, notification_request_1.origin)
        self.assertEquals(notification_again.origin_type, notification_request_1.origin_type)

        # assert that the old id is unchanged
        self.assertEquals(notification_again_id, notification_id1)

        proc = self.container.proc_manager.procs_by_name['user_notification']

        self.assertEquals(len(proc.notifications.values()), 2)


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

        notification_request_correct = NotificationRequest(   name = "notification_1",
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(   name = "notification_2",
            origin="instrument_2",
            origin_type="type_2",
            event_type='DetectionEvent')

        notification_id1 =  self.unsc.create_notification(notification=notification_request_correct, user_id=user_id)
        notification_id2 =  self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        # delete both notifications
        self.unsc.delete_notification(notification_id1)
        self.unsc.delete_notification(notification_id2)

        notific_1 = self.rrc.read(notification_id1)
        notific_2 = self.rrc.read(notification_id2)
        # This checks that the notifications have been retired.
        self.assertNotEquals(notific_1.temporal_bounds.end_datetime, '')
        self.assertNotEquals(notific_2.temporal_bounds.end_datetime, '')

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

        notification_request_correct = NotificationRequest(   name = "notification_1",
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_id =  self.unsc.create_notification(notification=notification_request_correct, user_id=user_id)

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
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_find_events(self):
        # Test the find events functionality of UNS

        # publish some events for the event repository
        event_publisher_1 = EventPublisher("PlatformEvent")
        event_publisher_2 = EventPublisher("ReloadUserInfoEvent")

        for i in xrange(10):
            event_publisher_1.publish_event(origin='my_special_find_events_origin', ts_created = i)
            event_publisher_2.publish_event(origin='another_origin', ts_created = i)

        def poller():
            events = self.unsc.find_events(origin='my_special_find_events_origin', type = 'PlatformEvent', min_datetime= 4, max_datetime=7)
            return len(events) >= 4

        success = self.event_poll(poller, 10)

        self.assertTrue(success)


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_find_events_extended(self):
        '''
        Test the find events functionality of UNS
        '''

        # publish some events for the event repository
        event_publisher_1 = EventPublisher("PlatformEvent")
        event_publisher_2 = EventPublisher("ReloadUserInfoEvent")

        for i in xrange(10):
            event_publisher_1.publish_event(origin='Some_Resource_Agent_ID1', ts_created = i)
            event_publisher_2.publish_event(origin='Some_Resource_Agent_ID2', ts_created = i)

        # allow elastic search to populate the indexes. This gives enough time for the reload of user_info
        def poller():
            events = self.unsc.find_events_extended(origin='Some_Resource_Agent_ID1', min_time=4, max_time=7)
            return len(events) >= 4

        success = self.event_poll(poller, 10)
        self.assertTrue(success)

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
    def test_publish_event(self):
        '''
        Test the publish_event method of UNS
        '''
        #--------------------------------------------------------------------------------
        # Create an event object
        #--------------------------------------------------------------------------------
        event = DeviceEvent(  origin= "origin_1",
            origin_type='origin_type_1',
            sub_type= 'sub_type_1',
            ts_created = 2)

        # create async result to wait on in test
        ar = gevent.event.AsyncResult()

        #--------------------------------------------------------------------------------
        # Set up a subscriber to listen for that event
        #--------------------------------------------------------------------------------
        def received_event(result, event, headers):
            log.debug("received the event in the test: %s" % event)

            #--------------------------------------------------------------------------------
            # check that the event was published
            #--------------------------------------------------------------------------------
            self.assertEquals(event.origin, "origin_1")
            self.assertEquals(event.type_, 'DeviceEvent')
            self.assertEquals(event.origin_type, 'origin_type_1')
            self.assertEquals(event.ts_created, 2)
            self.assertEquals(event.sub_type, 'sub_type_1')

            result.set(True)

        event_subscriber = EventSubscriber( event_type = 'DeviceEvent',
            origin="origin_1",
            callback=lambda m, h: received_event(ar, m, h))
        event_subscriber.start()
        self.addCleanup(event_subscriber.stop)

        #--------------------------------------------------------------------------------
        # Use the UNS publish_event
        #--------------------------------------------------------------------------------

        self.unsc.publish_event(event=event)

        ar.wait(timeout=10)


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_batch_notifications(self):
        '''
        Test how the UNS listens to timer events and through the call back runs the process_batch()
        with the correct arguments.
        '''

        #--------------------------------------------------------------------------------------------
        # The operator sets up the process_batch_key. The UNS will listen for scheduler created
        # timer events with origin = process_batch_key
        #--------------------------------------------------------------------------------------------
        # generate a uuid
        newkey = 'batch_processing_' + str(uuid.uuid4())
        self.unsc.set_process_batch_key(process_batch_key = newkey)

        #--------------------------------------------------------------------------------
        # Set up a time for the scheduler to trigger timer events
        #--------------------------------------------------------------------------------
        # Trigger the timer event 10 seconds later from now
        time_now = datetime.utcnow() + timedelta(seconds=15)
        times_of_day =[{'hour': str(time_now.hour),'minute' : str(time_now.minute), 'second':str(time_now.second) }]

        #--------------------------------------------------------------------------------
        # Publish the events that the user will later be notified about
        #--------------------------------------------------------------------------------
        event_publisher = EventPublisher()

        # this part of code is in the beginning to allow enough time for the events_index creation
        times_of_events_published = Set()


        def publish_events():
            for i in xrange(3):
                t = now()
                t = UserNotificationIntTest.makeEpochTime(t)

                event_publisher.publish_event( ts_created= t ,
                    origin="instrument_1",
                    origin_type="type_1",
                    event_type='ResourceLifecycleEvent')

                event_publisher.publish_event( ts_created= t ,
                    origin="instrument_2",
                    origin_type="type_2",
                    event_type='ResourceLifecycleEvent')

                times_of_events_published.add(t)
                self.number_event_published += 2
                self.event.set()
                #            time.sleep(1)
                log.debug("Published events of origins = instrument_1, instrument_2 with ts_created: %s" % t)

        publish_events()

        self.assertTrue(self.event.wait(10))
        #----------------------------------------------------------------------------------------
        # Create users and get the user_ids
        #----------------------------------------------------------------------------------------

        # user_1
        user_1 = UserInfo()
        user_1.name = 'user_1'
        user_1.contact.email = 'user_1@gmail.com'

        # this part of code is in the beginning to allow enough time for the users_index creation

        user_id_1, _ = self.rrc.create(user_1)

        #--------------------------------------------------------------------------------------
        # Make notification request objects -- Remember to put names
        #--------------------------------------------------------------------------------------

        notification_request_correct = NotificationRequest(   name = "notification_1",
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(   name = "notification_2",
            origin="instrument_2",
            origin_type="type_2",
            event_type='ResourceLifecycleEvent')

        #--------------------------------------------------------------------------------------
        # Create a notification using UNS. This should cause the user_info to be updated
        #--------------------------------------------------------------------------------------

        self.unsc.create_notification(notification=notification_request_correct, user_id=user_id_1)
        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_1)


        #--------------------------------------------------------------------------------
        # Set up the scheduler to publish daily events that should kick off process_batch()
        #--------------------------------------------------------------------------------
        sid = self.ssclient.create_time_of_day_timer(   times_of_day=times_of_day,
            expires=time.time()+25200+60,
            event_origin= newkey,
            event_subtype="")
        def cleanup_timer(scheduler, schedule_id):
            """
            Do a friendly cancel of the scheduled event.
            If it fails, it's ok.
            """
            try:
                scheduler.cancel_timer(schedule_id)
            except:
                log.warn("Couldn't cancel")

        self.addCleanup(cleanup_timer, self.ssclient, sid)

        #--------------------------------------------------------------------------------
        # Assert that emails were sent
        #--------------------------------------------------------------------------------

        proc = self.container.proc_manager.procs_by_name['user_notification']

        ar_1 = gevent.event.AsyncResult()
        ar_2 = gevent.event.AsyncResult()

        def send_email(events_for_message, user_id):
            log.warning("(in asyncresult) events_for_message: %s" % events_for_message)
            ar_1.set(events_for_message)
            ar_2.set(user_id)

        proc.format_and_send_email = send_email

        events_for_message = ar_1.get(timeout=20)
        user_id = ar_2.get(timeout=20)

        log.warning("user_id: %s" % user_id)

        origins_of_events = Set()
        times = Set()

        for event in events_for_message:
            origins_of_events.add(event.origin)
            times.add(event.ts_created)

        #--------------------------------------------------------------------------------
        # Make assertions on the events mentioned in the formatted email
        #--------------------------------------------------------------------------------

        self.assertEquals(len(events_for_message), self.number_event_published)
        self.assertEquals(times, times_of_events_published)
        self.assertEquals(origins_of_events, Set(['instrument_1', 'instrument_2']))

    @staticmethod
    def makeEpochTime(date_time):
        """
        provides the seconds since epoch give a python datetime object.

        @param date_time: Python datetime object
        @return: seconds_since_epoch:: int
        """
        date_time = date_time.isoformat().split('.')[0].replace('T',' ')
        #'2009-07-04 18:30:47'
        pattern = '%Y-%m-%d %H:%M:%S'
        seconds_since_epoch = int(time.mktime(time.strptime(date_time, pattern)))

        return seconds_since_epoch

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_get_user_notification(self):
        '''
        Test that the get_user_notifications() method returns the notifications for a user
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

        notification_request_correct = NotificationRequest(   name = "notification_1",
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

        notification_id1 =  self.unsc.create_notification(notification=notification_request_correct, user_id=user_id)
        notification_id2 =  self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        #--------------------------------------------------------------------------------------
        # Get the notifications for the user
        #--------------------------------------------------------------------------------------

        ret= self.unsc.get_user_notifications(user_id=user_id)

        self.assertIsInstance(ret, ComputedListValue)
        notifications = ret.value
        self.assertEquals(ret.status, ComputedValueAvailability.PROVIDED)

        names = []
        origins = []
        origin_types = []
        event_types = []
        for notification in notifications:
            names.append(notification.name)
            origins.append(notification.origin)
            origin_types.append(notification.origin_type)
            event_types.append(notification.event_type)

        self.assertEquals(Set(names), Set(['notification_1', 'notification_2']) )
        self.assertEquals(Set(origins), Set(['instrument_1', 'instrument_2']) )
        self.assertEquals(Set(origin_types), Set(['type_1', 'type_2']) )
        self.assertEquals(Set(event_types), Set(['ResourceLifecycleEvent', 'DetectionEvent']) )


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_get_recent_events(self):
        '''
        Test that the get_recent_events(resource_id, limit) method returns the events whose origin is
        the specified resource.
        '''

        #--------------------------------------------------------------------------------------
        # create user with email address in RR
        #--------------------------------------------------------------------------------------

        # publish some events for the event repository
        event_publisher_1 = EventPublisher("PlatformEvent")
        event_publisher_2 = EventPublisher("PlatformEvent")

        def publish_events():
            x = 0
            for i in xrange(10):
                event_publisher_1.publish_event(origin='my_unique_test_recent_events_origin', ts_created = i)
                event_publisher_2.publish_event(origin='Another_recent_events_origin', ts_created = i)
                x += 1
            self.event.set()

        publish_events()

        self.assertTrue(self.event.wait(10))

        #--------------------------------------------------------------------------------------
        # Test with specified limit
        #--------------------------------------------------------------------------------------
        def poller():
            ret = self.unsc.get_recent_events(resource_id='my_unique_test_recent_events_origin', limit = 5)
            events = ret.value
            return len(events) >= 5

        success = self.event_poll(poller, 10)
        self.assertTrue(success)


        #--------------------------------------------------------------------------------------
        # Test without specified limit
        #--------------------------------------------------------------------------------------

        def poller():
            ret = self.unsc.get_recent_events(resource_id='Another_recent_events_origin')
            events = ret.value
            return len(events) >= 10

        success = self.event_poll(poller, 10)
        self.assertTrue(success)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_get_subscriptions(self):
        '''
        Test that the get_subscriptions works correctly
        '''

        #--------------------------------------------------------------------------------------
        # Create users
        #--------------------------------------------------------------------------------------

        user_ids = []
        for i in xrange(5):
            user = UserInfo()
            user.name = 'user_%s' % i
            user.contact.email = 'user_%s@gmail.com' % i

            user_id, _ = self.rrc.create(user)
            user_ids.append(user_id)

        #--------------------------------------------------------------------------------------
        # Make a data product
        #--------------------------------------------------------------------------------------
        data_product_management = DataProductManagementServiceClient()
        dataset_management = DatasetManagementServiceClient()
        pubsub = PubsubManagementServiceClient()
        
        pdict_id = dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        streamdef_id = pubsub.create_stream_definition(name="test_subscriptions", parameter_dictionary_id=pdict_id)

        tdom, sdom = time_series_domain()
        tdom, sdom = tdom.dump(), sdom.dump()

        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id = data_product_management.create_data_product(data_product=dp_obj, stream_definition_id=streamdef_id)

        #--------------------------------------------------------------------------------------
        # Make notification request objects -- Remember to put names
        #--------------------------------------------------------------------------------------

        notification_active_1 = NotificationRequest(   name = "notification_1",
            origin=data_product_id,
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_active_2 = NotificationRequest(   name = "notification_2",
            origin=data_product_id,
            origin_type="type_2",
            event_type='ResourceLifecycleEvent')

        notification_past_1 = NotificationRequest(   name = "notification_3_to_be_retired",
            origin=data_product_id,
            origin_type="type_3",
            event_type='DetectionEvent')

        notification_past_2 = NotificationRequest(   name = "notification_4_to_be_retired",
            origin=data_product_id,
            origin_type="type_4",
            event_type='DetectionEvent')

        #--------------------------------------------------------------------------------------
        # Create notifications using UNS.
        #--------------------------------------------------------------------------------------
        active_notification_ids = set()
        past_notification_ids = set()

        for user_id in user_ids:
            notification_id_active_1 =  self.unsc.create_notification(notification=notification_active_1, user_id=user_id)
            notification_id_active_2 =  self.unsc.create_notification(notification=notification_active_2, user_id=user_id)

            # Store the ids for the active notifications in a set
            active_notification_ids.add(notification_id_active_1)
            active_notification_ids.add(notification_id_active_2)

            notification_id_past_1 =  self.unsc.create_notification(notification=notification_past_1, user_id=user_id)
            notification_id_past_2 =  self.unsc.create_notification(notification=notification_past_2, user_id=user_id)

            # Store the ids for the retired-to-be notifications in a set
            past_notification_ids.add(notification_id_past_1)
            past_notification_ids.add(notification_id_past_2)

        log.debug("Number of active notification ids: %s" % len(active_notification_ids))
        log.debug("Number of past notification ids: %s" % len(past_notification_ids))

        # Retire the retired-to-be notifications
        for notific_id in past_notification_ids:
            self.unsc.delete_notification(notification_id=notific_id)

        #--------------------------------------------------------------------------------------
        # Use UNS to get the subscriptions
        #--------------------------------------------------------------------------------------
        res_notifs= self.unsc.get_subscriptions(resource_id=data_product_id, include_nonactive=False)

        log.debug("Result for subscriptions: %s" % res_notifs)
        log.debug("Number of subscriptions returned: %s" % len(res_notifs))

        self.assertEquals(len(res_notifs), 2)

        for notific in res_notifs:
            self.assertEquals(notific.origin, data_product_id)
            self.assertEquals(notific.temporal_bounds.end_datetime, '')


        #--------------------------------------------------------------------------------------
        # Use UNS to get the all subscriptions --- including retired
        #--------------------------------------------------------------------------------------
        res_notifs = self.unsc.get_subscriptions(resource_id=data_product_id, include_nonactive=True)

        for notific in res_notifs:
            self.assertEquals(notific.origin, data_product_id)

        self.assertEquals(len(res_notifs), 4)

        log.debug("Number of subscriptions including retired notifications: %s" % len(res_notifs))
