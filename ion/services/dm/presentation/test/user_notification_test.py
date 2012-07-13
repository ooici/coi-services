'''
@author Bill Bollenbacher
@author Swarbhanu Chatterjee
@author David Stuebe
@file ion/services/dm/presentation/test/user_notification_test.py
@description Unit and Integration test implementations for the user notification service class.
'''
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from ion.services.dm.presentation.user_notification_service import UserNotificationService
from interface.objects import DeliveryMode, UserInfo, DeliveryConfig, DetectionFilterConfig
from interface.objects import ResourceEvent
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from pyon.public import IonObject, RT, PRED, Container, CFG
from pyon.core.exception import NotFound, BadRequest
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log
from pyon.event.event import EventPublisher, EventSubscriber
import gevent
from mock import Mock, mocksignature
from interface.objects import NotificationRequest, NotificationType, ExampleDetectableEvent, Frequency
from ion.services.dm.utility.query_language import QueryLanguage
import os, time, datetime
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


#    @unittest.skip('Bad test - figure out how to patch out the greenlet start...')
    def test_create_notification(self):

        user_id = 'user_id_1'

        self.mock_rr_client.create = mocksignature(self.mock_rr_client.create)
        self.mock_rr_client.create.return_value = ('notification_id_1','rev_1')

        self.user_notification._update_user_with_notification = mocksignature(self.user_notification._update_user_with_notification)
        self.user_notification._update_user_with_notification.return_value = ''

        self.user_notification.create_event_processor = mocksignature(self.user_notification.create_event_processor)

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
        self.user_notification._update_user_with_notification.assert_called_once_with(user_id, notification_request)
        self.user_notification.create_event_processor.assert_called_once_with(notification_request, user_id, 'smtp_client')


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

    def test_update_notification(self):

        user_id = 'user_id_1'

        self.mock_rr_client.update = mocksignature(self.mock_rr_client.update())
        self.mock_rr_client.update.return_value = ('notification_id_1','rev_1')

        self.user_notification._update_user_with_notification = mocksignature(self.user_notification._update_user_with_notification)
        self.user_notification._update_user_with_notification.return_value = ''

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

        self.user_notification.update_notification(notification_request, user_id)

        #-------------------------------------------------------------------------------------------------------------------
        # assertions
        #-------------------------------------------------------------------------------------------------------------------

        self.mock_rr_client.update.assert_called_once_with(notification_request)
        self.user_notification._update_user_with_notification.assert_called_once_with(user_id, notification_request)

    def test_delete_user_notification(self):

        notification_id = 'notification_id_1'

        self.mock_rr_client.delete = mocksignature(self.mock_rr_client.update())
        self.mock_rr_client.delete.return_value = (notification_id,'rev_1')

        self.user_notification.delete_notification_from_user_info = mocksignature(self.user_notification.delete_notification_from_user_info)
        self.user_notification.delete_notification_from_user_info.return_value = ''

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

    def test_match(self):
        '''
        Tests the query language parser.

        #todo - I think these tests dont belong here but belong in query_test.py
        '''
        parser = QueryLanguage()

        #------------------------------------------------------------------------------------------------------
        # Check that when field is outside range (less than lower bound), match() returns false
        #------------------------------------------------------------------------------------------------------

        field = 'voltage'
        lower_bound = 5
        upper_bound = 10
        instrument = 'instrument_1'
        search_string1 = "SEARCH '%s' VALUES FROM %s TO %s FROM '%s'"\
        % (field, lower_bound, upper_bound, instrument)
        query = parser.parse(search_string1)

        event = ExampleDetectableEvent('TestEvent', voltage=4)
        self.assertFalse(QueryLanguage.match(event, query['query']))

        #------------------------------------------------------------------------------------------------------
        # Check that when field is outside range (is higher than upper bound), match() returns false
        #------------------------------------------------------------------------------------------------------

        event = ExampleDetectableEvent('TestEvent', voltage=11)
        self.assertFalse(QueryLanguage.match(event, query['query']))

        #------------------------------------------------------------------------------------------------------
        # Check that when field is inside range, match() returns true
        #------------------------------------------------------------------------------------------------------

        event = ExampleDetectableEvent('TestEvent', voltage=6)
        self.assertTrue(QueryLanguage.match(event, query['query']))

        #------------------------------------------------------------------------------------------------------
        # Check that when field is exactly of the value mentioned in a query, match() returns true
        #------------------------------------------------------------------------------------------------------

        value = 15
        search_string2 = "search '%s' is '%s' from '%s'" % (field, value, instrument)
        query = parser.parse(search_string2)

        event = ExampleDetectableEvent('TestEvent', voltage=15)
        self.assertTrue(QueryLanguage.match(event, query['query']))

        #------------------------------------------------------------------------------------------------------
        # Check that when value is not exactly what is mentioned in a query, match() returns false
        #------------------------------------------------------------------------------------------------------

        event = ExampleDetectableEvent('TestEvent', voltage=14)
        self.assertFalse(QueryLanguage.match(event, query['query']))

    def test_evaluate_condition(self):
        '''
        Tests the query language parser.

        #todo - I think these tests dont belong here but belong in query_test.py
        '''

        parser = QueryLanguage()

        #------------------------------------------------------------------------------------------------------
        # Set up the search strings for different queries:
        # These include main query, a list of or queries and a list of and queries
        #------------------------------------------------------------------------------------------------------

        field = 'voltage'
        instrument = 'instrument'

        #------------------------------------------------------------------------------------------------------
        # main query
        #------------------------------------------------------------------------------------------------------

        lower_bound = 5
        upper_bound = 10
        search_string1 = "SEARCH '%s' VALUES FROM %s TO %s FROM '%s'"\
        % (field, lower_bound, upper_bound, instrument)

        #------------------------------------------------------------------------------------------------------
        # or queries
        #------------------------------------------------------------------------------------------------------

        value = 15
        search_string2 = "or search '%s' is '%s' from '%s'" % (field, value, instrument)

        value = 17
        search_string3 = "or search '%s' is '%s' from '%s'" % (field, value, instrument)

        lower_bound = 20
        upper_bound = 30
        search_string4 = "or SEARCH '%s' VALUES FROM %s TO %s FROM '%s'"\
        % (field, lower_bound, upper_bound, instrument)

        #------------------------------------------------------------------------------------------------------
        # and queries
        #------------------------------------------------------------------------------------------------------

        lower_bound = 5
        upper_bound = 6
        search_string5 = "and SEARCH '%s' VALUES FROM %s TO %s FROM '%s'"\
        % (field, lower_bound, upper_bound, instrument)

        lower_bound = 6
        upper_bound = 7
        search_string6 = "and SEARCH '%s' VALUES FROM %s TO %s FROM '%s'"\
        % (field, lower_bound, upper_bound, instrument)

        #------------------------------------------------------------------------------------------------------
        # Construct queries by parsing different search strings and test the evaluate_condition()
        # for each such complex query
        #------------------------------------------------------------------------------------------------------
        search_string = search_string1+search_string2+search_string3+search_string4+search_string5+search_string6
        query = parser.parse(search_string)

        # the main query as well as the 'and' queries pass for this case
        event = ExampleDetectableEvent('TestEvent', voltage=6)
        self.assertTrue(QueryLanguage.evaluate_condition(event, query))

        # check true conditions. If any one of the 'or' conditions passes, evaluate_condition()
        # will return True
        event = ExampleDetectableEvent('TestEvent', voltage=15)
        self.assertTrue(QueryLanguage.evaluate_condition(event, query))

        event = ExampleDetectableEvent('TestEvent', voltage=17)
        self.assertTrue(QueryLanguage.evaluate_condition(event, query))

        event = ExampleDetectableEvent('TestEvent', voltage=25)
        self.assertTrue(QueryLanguage.evaluate_condition(event, query))

        # check fail conditions arising from the 'and' condition (happens if any one of the 'and' conditions fail)
        # note: the 'and' queries are attached to the main query
        event = ExampleDetectableEvent('TestEvent', voltage=5)
        self.assertFalse(QueryLanguage.evaluate_condition(event, query))

        event = ExampleDetectableEvent('TestEvent', voltage=7)
        self.assertFalse(QueryLanguage.evaluate_condition(event, query))

        event = ExampleDetectableEvent('TestEvent', voltage=9)
        self.assertFalse(QueryLanguage.evaluate_condition(event, query))

    def test_create_detection_filter(self):
        notification_id = 'an id'

        self.user_notification.create_notification = mocksignature(self.user_notification.create_notification)
        self.user_notification.create_notification.return_value = notification_id


        res = self.user_notification.create_detection_filter(
            event_type='event_type',
            event_subtype='event_subtype',
            origin='origin',
            origin_type='origin_type',
            user_id='user_id',
            filter_config = 'filter_config')

        self.assertEquals(res, notification_id)
        self.assertTrue(self.user_notification.create_notification.called)


#    def test_create_email(self):
#
#        #------------------------------------------------------------------------------------------------------
#        #Setup for the create email test
#        #------------------------------------------------------------------------------------------------------
#
#        cn = Mock()
#
#        notification_id = 'an id'
#        args_list = {}
#        kwargs_list = {}
#
#        def side_effect(*args, **kwargs):
#
#            args_list.update(args)
#            kwargs_list.update(kwargs)
#            return notification_id
#
#        cn.side_effect = side_effect
#        self.user_notification.create_notification = cn
#
#        #------------------------------------------------------------------------------------------------------
#        # Test with complete arguments
#        #------------------------------------------------------------------------------------------------------
#
#        res = self.user_notification.create_email(event_type='event_type',
#            event_subtype='event_subtype',
#            origin='origin',
#            origin_type='origin_type',
#            user_id='user_id',
#            email='email',
#            mode = DeliveryMode.DIGEST,
#            message_header='message_header',
#            parser='parser'
#        )
#
#        #------------------------------------------------------------------------------------------------------
#        # Assert results about complete arguments
#        #------------------------------------------------------------------------------------------------------
#
#        self.assertEquals(res, notification_id)
#
#        notification_request = kwargs_list['notification']
#        user_id = kwargs_list['user_id']
#
#        self.assertEquals(user_id, 'user_id')
#        self.assertEquals(notification_request.delivery_config.delivery['email'], 'email')
#        self.assertEquals(notification_request.delivery_config.delivery['mode'], DeliveryMode.DIGEST)
#
#        self.assertEquals(notification_request.delivery_config.processing['message_header'], 'message_header')
#        self.assertEquals(notification_request.delivery_config.processing['parsing'], 'parser')
#
#        self.assertEquals(notification_request.event_type, 'event_type')
#        self.assertEquals(notification_request.event_subtype, 'event_subtype')
#        self.assertEquals(notification_request.origin, 'origin')
#        self.assertEquals(notification_request.origin_type, 'origin_type')
#
#
#
#        #------------------------------------------------------------------------------------------------------
#        # Test with email missing...
#        #------------------------------------------------------------------------------------------------------
#
#        with self.assertRaises(BadRequest):
#            res = self.user_notification.create_email(event_type='event_type',
#                event_subtype='event_subtype',
#                origin='origin',
#                origin_type='origin_type',
#                user_id='user_id',
#                mode = DeliveryMode.DIGEST,
#                message_header='message_header',
#                parser='parser')
#
#        #------------------------------------------------------------------------------------------------------
#        # Test with user id missing - that is caught in the create_notification method
#        #------------------------------------------------------------------------------------------------------
#
#        res = self.user_notification.create_email(event_type='event_type',
#            event_subtype='event_subtype',
#            origin='origin',
#            origin_type='origin_type',
#            email='email',
#            mode = DeliveryMode.DIGEST,
#            message_header='message_header',
#            parser='parser')
#
#        notification_request = kwargs_list['notification']
#        user_id = kwargs_list['user_id']
#
#        self.assertEquals(user_id, '')
#        self.assertEquals(notification_request.delivery_config.processing['message_header'], 'message_header')
#        self.assertEquals(notification_request.delivery_config.processing['parsing'], 'parser')
#        self.assertEquals(notification_request.type, NotificationType.EMAIL)
#


@attr('INT', group='dm')
class UserNotificationIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(UserNotificationIntTest, self).setUp()
        config = DotDict()
        config.bootstrap.use_es = True

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml', config)

        self.unsc = UserNotificationServiceClient()
        self.rrc = ResourceRegistryServiceClient()
        self.imc = IdentityManagementServiceClient()
        self.discovery = DiscoveryServiceClient()

        self.ION_NOTIFICATION_EMAIL_ADDRESS = 'ION_notifications-do-not-reply@oceanobservatories.org'

#    @staticmethod
#    def es_cleanup():
#        es_host = CFG.get_safe('server.elasticsearch.host', 'localhost')
#        es_port = CFG.get_safe('server.elasticsearch.port', '9200')
#        es = ep.ElasticSearch(
#            host=es_host,
#            port=es_port,
#            timeout=10
#        )
#        indexes = STD_INDEXES.keys()
#        indexes.append('%s_resources_index' % get_sys_name().lower())
#        indexes.append('%s_events_index' % get_sys_name().lower())
#
#        for index in indexes:
#            IndexManagementService._es_call(es.river_couchdb_delete,index)
#            IndexManagementService._es_call(es.index_delete,index)

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

        notification_request_1 = NotificationRequest(origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(origin="instrument_2",
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

        notification_request_1 = NotificationRequest(origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent',
            event_subtype = 'subtype_1')

        notification_request_2 = NotificationRequest(origin="instrument_2",
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

        self.unsc.create_notification(notification=notification_request_1, user_id=user_id_1)
        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_2)

        #--------------------------------------------------------------------------------------
        # Check the user_info and reverse_user_info got reloaded
        #--------------------------------------------------------------------------------------

        # Check in UNS ------------>

        # check user_info dictionary
        self.assertEquals(proc1.user_info['user_1']['user_contact'].email, 'user_1@gmail.com' )
        self.assertEquals(proc1.user_info['user_1']['notifications'], [notification_request_1])

        self.assertEquals(proc1.user_info['user_2']['user_contact'].email, 'user_2@gmail.com' )
        self.assertEquals(proc1.user_info['user_2']['notifications'], [notification_request_2])


        self.assertEquals(proc1.reverse_user_info['event_origin']['instrument_1'], ['user_1'])
        self.assertEquals(proc1.reverse_user_info['event_origin']['instrument_2'], ['user_2'])

        self.assertEquals(proc1.reverse_user_info['event_type']['ResourceLifecycleEvent'], ['user_1'])
        self.assertEquals(proc1.reverse_user_info['event_type']['DetectionEvent'], ['user_2'])

        self.assertEquals(proc1.reverse_user_info['event_subtype']['subtype_1'], ['user_1'])
        self.assertEquals(proc1.reverse_user_info['event_subtype']['subtype_2'], ['user_2'])

        self.assertEquals(proc1.reverse_user_info['event_origin_type']['type_1'], ['user_1'])
        self.assertEquals(proc1.reverse_user_info['event_origin_type']['type_2'], ['user_2'])


        #--------------------------------------------------------------------------------------
        # Create another notification for the first user
        #--------------------------------------------------------------------------------------

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_1)

        # Check in UNS ------------>
        self.assertEquals(proc1.user_info['user_1']['user_contact'].email, 'user_1@gmail.com' )
        self.assertEquals(proc1.user_info['user_1']['notifications'], [notification_request_1, notification_request_2])

        self.assertEquals(proc1.reverse_user_info['event_origin']['instrument_1'], ['user_1'])
        self.assertEquals(proc1.reverse_user_info['event_origin']['instrument_2'], ['user_2', 'user_1'])

        self.assertEquals(proc1.reverse_user_info['event_type']['ResourceLifecycleEvent'], ['user_1'])
        self.assertEquals(proc1.reverse_user_info['event_type']['DetectionEvent'], ['user_2', 'user_1'])

        self.assertEquals(proc1.reverse_user_info['event_subtype']['subtype_1'], ['user_1'])
        self.assertEquals(proc1.reverse_user_info['event_subtype']['subtype_2'], ['user_2', 'user_1'])

        self.assertEquals(proc1.reverse_user_info['event_origin_type']['type_1'], ['user_1'])
        self.assertEquals(proc1.reverse_user_info['event_origin_type']['type_2'], ['user_2', 'user_1'])

        #--------------------------------------------------------------------------------------
        # Update notification and check that the user_info and reverse_user_info in UNS got reloaded
        #--------------------------------------------------------------------------------------

        #todo The update method for UNS

#        notification_request_1 = notification_request_2
#        self.unsc.update_notification(notification=notification_request_1, user_id=user_id)
#
#        # Check for UNS ------->
#
#        # user_info
#        self.assertEquals(proc1.user_info['new_user']['user_contact'].email, 'new_user@gmail.com' )
#        self.assertEquals(proc1.user_info['new_user']['notifications'], [notification_request_2])

        # reverse_user_info


        #--------------------------------------------------------------------------------------
        # Delete notification and check that the user_info and reverse_user_info in UNS got reloaded
        #--------------------------------------------------------------------------------------

        #todo The delete method for UNS

        # Check for UNS ------->

        # user_info

        # reverse_user_info

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
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(origin="instrument_1",
            origin_type="type_1",
            event_subtype='subtype_1',
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(origin="instrument_2",
            origin_type="type_2",
            event_subtype='subtype_2',
            event_type='DetectionEvent')

        #--------------------------------------------------------------------------------------
        # Create a notification
        #--------------------------------------------------------------------------------------

        self.unsc.create_notification(notification=notification_request_1, user_id=user_id)

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



        self.assertEquals(reloaded_user_info['new_user']['notifications'], [notification_request_1] )
        self.assertEquals(reloaded_user_info['new_user']['user_contact'].email, 'new_user@gmail.com')

        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_type']['ResourceLifecycleEvent'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_1'], ['new_user'] )

        #--------------------------------------------------------------------------------------
        # Create another notification
        #--------------------------------------------------------------------------------------

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        ar_1 = gevent.event.AsyncResult()
        ar_2 = gevent.event.AsyncResult()

        reloaded_user_info = ar_1.get(timeout=10)
        reloaded_reverse_user_info = ar_2.get(timeout=10)

        self.assertEquals(reloaded_user_info['new_user']['notifications'], [notification_request_1, notification_request_2] )

        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_2'], ['new_user'] )

        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_2'], ['new_user'] )

        self.assertEquals(reloaded_reverse_user_info['event_type']['ResourceLifecycleEvent'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_type']['DetectionEvent'], ['new_user'] )

        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_2'], ['new_user'] )


    @attr('LOCOINT')
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
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(origin="instrument_1",
                                                    origin_type="type_1",
                                                    event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(origin="instrument_2",
                                                    origin_type="type_2",
                                                    event_type='DetectionEvent')

        notification_request_3 = NotificationRequest(origin="instrument_3",
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
            self.assertTrue(event_time > test_start_time)
            self.assertTrue(event_time <= test_end_time)


    @attr('LOCOINT')
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
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(origin="instrument_2",
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

        event_publisher.publish_event( ts_created= 5,
            event_type = "ResourceLifecycleEvent",
            origin="instrument_1",
            origin_type="type_1")

        event_publisher.publish_event( ts_created= 10,
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
    @unittest.skip('SMS is being deprecated')
    #    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_sms(self):

        proc1 = self.container.proc_manager.procs_by_name['user_notification']

        #--------------------------------------------------------------------------------------
        # Create a user and get the user_id
        #--------------------------------------------------------------------------------------

        user = UserInfo(name = 'new_user')
        user_id, _ = self.rrc.create(user)

        #--------------------------------------------------------------------------------------
        # set up....
        #--------------------------------------------------------------------------------------

        notification_id = self.unsc.create_email(event_type='ResourceLifecycleEvent',
            event_subtype=None,
            origin='Some_Resource_Agent_ID1',
            origin_type=None,
            user_id=user_id,
            phone = '401-XXX-XXXX',
            provider='T-Mobile',
            message_header='message_header',
            parser='parser',
        )

        #------------------------------------------------------------------------------------------------------
        # Setup so as to be able to get the message and headers going into the
        # subscription callback method of the EmailEventProcessor
        #------------------------------------------------------------------------------------------------------

        # publish an event for each notification to generate the emails
        rle_publisher = EventPublisher("ResourceLifecycleEvent")
        rle_publisher.publish_event(origin='Some_Resource_Agent_ID1', description="RLE test event")

        msg_tuple = proc1.event_processors[notification_id].smtp_client.sentmail.get(timeout=4)

        self.assertTrue(proc1.event_processors[notification_id].smtp_client.sentmail.empty())

        message = msg_tuple[2]
        list_lines = message.split("\n")

        #-------------------------------------------------------
        # parse the message body
        #-------------------------------------------------------

        message_dict = {}
        for line in list_lines:
            key_item = line.split(": ")
            if key_item[0] == 'Subject':
                message_dict['Subject'] = key_item[1] + key_item[2]
            else:
                try:
                    message_dict[key_item[0]] = key_item[1]
                except IndexError as exc:
                    # these IndexError exceptions happen only because the message sometimes
                    # has successive /r/n (i.e. new lines) and therefore,
                    # the indexing goes out of range. These new lines
                    # can just be ignored. So we ignore the exceptions here.
                    pass

        #-------------------------------------------------------
        # make assertions
        #-------------------------------------------------------

        self.assertEquals(msg_tuple[1], '401-XXX-XXXX@tmomail.net' )
        #self.assertEquals(msg_tuple[0], ION_NOTIFICATION_EMAIL_ADDRESS)
        self.assertEquals(message_dict['Description'].rstrip('\r'), 'RLE test event')

    @attr('LOCOINT')
    @unittest.skip('Event Detection is being deprecated')
#    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_event_detection(self):

        proc1 = self.container.proc_manager.procs_by_name['user_notification']

        #--------------------------------------------------------------------------------------
        # Create a user and get the user_id
        #--------------------------------------------------------------------------------------

        user = UserInfo(name = 'new_user')
        user_id, _ = self.rrc.create(user)

        #--------------------------------------------------------------------------------------
        # Create detection notification
        #--------------------------------------------------------------------------------------

        dfilt = DetectionFilterConfig()

        field = 'voltage'
        lower_bound = 5
        upper_bound = 10
        instrument = 'instrument_1'
        search_string1 = "SEARCH '%s' VALUES FROM %s TO %s FROM '%s'"\
        % (field, lower_bound, upper_bound, instrument)

        field = 'voltage'
        value = 15
        instrument = 'instrument_2'
        search_string2 = "or search '%s' is '%s' from '%s'" % (field, value, instrument)

        field = 'voltage'
        lower_bound = 8
        upper_bound = 14
        instrument = 'instrument_3'
        search_string3 = "and SEARCH '%s' VALUES FROM %s TO %s FROM '%s'"\
        % (field, lower_bound, upper_bound, instrument)


        dfilt.processing['search_string'] = search_string1 + search_string2 + search_string3

        dfilt.delivery['message'] = 'I got my detection event!'

        notification_id = self.unsc.create_detection_filter(event_type='ExampleDetectableEvent',
            event_subtype=None,
            origin='Some_Resource_Agent_ID1',
            origin_type=None,
            user_id=user_id,
            filter_config=dfilt
        )

        #---------------------------------------------------------------------------------
        # Create event subscription for resulting detection event
        #---------------------------------------------------------------------------------

        # Create an email notification so that when the DetectionEventProcessor
        # detects an event and fires its own output event, this will caught by an
        # EmailEventProcessor and an email will be sent to the user

        notification_id_2 = self.unsc.create_email(event_type='DetectionEvent',
            event_subtype=None,
            origin='DetectionEventProcessor',
            origin_type=None,
            user_id=user_id,
            email='email@email.com',
            mode = DeliveryMode.UNFILTERED,
            message_header='Detection event',
            parser='parser'
        )


        rle_publisher = EventPublisher("ExampleDetectableEvent")

        #------------------------------------------------------------------------------------------------
        # this event will not be detected because the 'and query' will fail the match condition,
        #------------------------------------------------------------------------------------------------

        rle_publisher.publish_event(origin='Some_Resource_Agent_ID1',
            description="RLE test event",
            voltage = 5)

        # The smtp_client's sentmail queue should now empty
        self.assertTrue(proc1.event_processors[notification_id_2].smtp_client.sentmail.empty())

        #----------------------------------------------------------------------
        # This event will generate an event because it will pass the OR query
        #----------------------------------------------------------------------

        rle_publisher.publish_event(origin='Some_Resource_Agent_ID1',
            description="RLE test event",
            voltage = 15)

        msg_tuple = proc1.event_processors[notification_id_2].smtp_client.sentmail.get(timeout=4)
        # check that a non empty message was generated for email
        self.assertEquals(msg_tuple[1], 'email@email.com' )
        # check that the sentmail queue is empty again after having extracted the message
        self.assertTrue(proc1.event_processors[notification_id_2].smtp_client.sentmail.empty())

        #----------------------------------------------------------------------
        # This event WILL not be detected because it will fail all the queries
        #----------------------------------------------------------------------

        rle_publisher = EventPublisher("ExampleDetectableEvent")
        rle_publisher.publish_event(origin='Some_Resource_Agent_ID1',
            description="RLE test event",
            voltage = 4)

        # The smtp_client's sentmail queue should now empty
        self.assertTrue(proc1.event_processors[notification_id_2].smtp_client.sentmail.empty())

        #------------------------------------------------------------------------------
        # this event WILL be detected. It will pass the main query and the AND query
        #------------------------------------------------------------------------------

        rle_publisher.publish_event(origin='Some_Resource_Agent_ID1',
            description="RLE test event",
            voltage = 8)

        msg_tuple = proc1.event_processors[notification_id_2].smtp_client.sentmail.get(timeout=4)

        #----------------------------------------------------------------------
        # Make assertions regarding the message generated for email
        #----------------------------------------------------------------------

        self.assertEquals(msg_tuple[1], 'email@email.com' )
        #self.assertEquals(msg_tuple[0], ION_NOTIFICATION_EMAIL_ADDRESS)

        # parse the message body
        message = msg_tuple[2]
        list_lines = message.split("\n")

        message_dict = {}
        for line in list_lines:
            key_item = line.split(": ")
            if key_item[0] == 'Subject':
                message_dict['Subject'] = key_item[1] + key_item[2]
            else:
                try:
                    message_dict[key_item[0]] = key_item[1]
                except IndexError as exc:
                    # these IndexError exceptions happen only because the message sometimes
                    # has successive /r/n (i.e. new lines) and therefore,
                    # the indexing goes out of range. These new lines
                    # can just be ignored. So we ignore the exceptions here.
                    pass


        #self.assertEquals(message_dict['From'], ION_NOTIFICATION_EMAIL_ADDRESS)
        self.assertEquals(message_dict['To'], 'email@email.com')
        self.assertEquals(message_dict['Event'].rstrip('\r'), 'DetectionEvent')
        self.assertEquals(message_dict['Originator'].rstrip('\r'), 'DetectionEventProcessor')
        self.assertEquals(message_dict['Description'].rstrip('\r'), 'Event was detected by DetectionEventProcessor')

    def test_create_read_user_notifications(self):
        # create user with email address in RR
        user = UserInfo()
        user.name = 'user_1'
        user.contact.email = 'user_1@gmail.com'

        user_id, _ = self.rrc.create(user)


        #--------------------------------------------------------------------------------------
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(origin="instrument_2",
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

    def test_delete_user_notifications(self):

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

        notification_request_1 = NotificationRequest(origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(origin="instrument_2",
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

    def test_update_user_notification(self):
        # create user with email address in RR
        user = UserInfo()
        user.name = 'user_1'
        user.contact.email = 'user_1@gmail.com'

        user_id, _ = self.rrc.create(user)

        #--------------------------------------------------------------------------------------
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(origin="instrument_1",
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

    def test_find_events(self):
        # publish some events for the event repository
        event_publisher_1 = EventPublisher("ResourceLifecycleEvent")
        event_publisher_2 = EventPublisher("DataEvent")

        for i in xrange(10):
            event_publisher_1.publish_event(origin='Some_Resource_Agent_ID1', ts_created = i)
            event_publisher_2.publish_event(origin='Some_Resource_Agent_ID2', ts_created = i)

        # allow elastic search to populate the indexes. This gives enough time for the reload of user_info
        gevent.sleep(4)
        events = self.unsc.find_events(origin='Some_Resource_Agent_ID1', min_datetime=4, max_datetime=7)

        self.assertEquals(len(events), 3)

    def test_create_several_workers(self):
        '''
        Create more than one worker. Test that they process events in round robin
        '''
        pids = self.unsc.create_worker(number_of_workers=2)

        self.assertEquals(len(pids), 2)

    def test_publish_event_on_time(self):
        '''
        Test the publish_event method of UNS
        '''

        current_time = datetime.datetime.today()

        if current_time.second < 50:
            future_time = [current_time.year, current_time.month, current_time.day,\
                           current_time.hour, current_time.minute, current_time.second + 4]
        else:
            future_time = [current_time.year, current_time.month, current_time.day,\
                           current_time.hour, current_time.minute, 4]

        # Create an event object
        event = ResourceEvent(  origin= "origin_1",
                                origin_type='origin_type_1',
                                sub_type= 'sub_type_1',
                                ts_created = 2)

        # Set up a subscriber to listen for that event

        ar = gevent.event.AsyncResult()
        def received_event(event, headers):
            ar.set(event)

        event_subscriber = EventSubscriber( origin="origin_1", callback=received_event)
        event_subscriber.start()

        # Use the UNS publish_event
        self.unsc.publish_event(event=event, publish_time=future_time)

        event_in = ar.get(timeout=20)

        # check that the event was published
        self.assertEquals(event_in.origin, "origin_1")
        self.assertEquals(event_in.type_, 'ResourceEvent')
        self.assertEquals(event_in.origin_type, 'origin_type_1')
        self.assertEquals(event_in.ts_created, 2)
        self.assertEquals(event_in.sub_type, 'sub_type_1')



#    @attr('LOCOINT')
#    @unittest.skip("Changed interface. Create email may get deprecated soon")
#    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
#    def test_create_email(self):
#        '''
#        Test the functionality of the create_email method of the User Notification Service
#
#        '''
#
#        proc1 = self.container.proc_manager.procs_by_name['user_notification']
#
#        #--------------------------------------------------------------------------------------
#        # Create a user and get the user_id
#        #--------------------------------------------------------------------------------------
#
#        user = UserInfo(name = 'new_user')
#        user_id, _ = self.rrc.create(user)
#
#        #--------------------------------------------------------------------------------------
#        # set up....
#        #--------------------------------------------------------------------------------------
#
#        notification_id = self.unsc.create_email(event_type='ResourceLifecycleEvent',
#            event_subtype=None,
#            origin='Some_Resource_Agent_ID1',
#            origin_type=None,
#            user_id=user_id,
#            email='email@email.com',
#            mode = DeliveryMode.DIGEST,
#            frequency= Frequency.REAL_TIME,
#            message_header='message_header',
#            parser='parser')
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
#        msg_tuple = proc1.event_processors[notification_id].smtp_client.sentmail.get(timeout=4)
#
#        self.assertTrue(proc1.event_processors[notification_id].smtp_client.sentmail.empty())
#
#        message = msg_tuple[2]
#        list_lines = message.split("\n")
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
#                except IndexError as exc:
#                    # these IndexError exceptions happen only because the message sometimes
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
#        #self.assertEquals(msg_tuple[0], ION_NOTIFICATION_EMAIL_ADDRESS)
#
#        #self.assertEquals(message_dict['From'], ION_NOTIFICATION_EMAIL_ADDRESS)
#        self.assertEquals(message_dict['To'], 'email@email.com')
#        self.assertEquals(message_dict['Event'].rstrip('\r'), 'ResourceLifecycleEvent')
#        self.assertEquals(message_dict['Originator'].rstrip('\r'), 'Some_Resource_Agent_ID1')
#        self.assertEquals(message_dict['Description'].rstrip('\r'), 'RLE test event')

