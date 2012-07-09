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
from ion.services.dm.inventory.index_management_service import IndexManagementService
from interface.objects import DeliveryMode, UserInfo, DeliveryConfig, DetectionFilterConfig
from interface.objects import NotificationRequest, InstrumentDevice
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
            parser='parser'
        )

        #------------------------------------------------------------------------------------------------------
        # Assert results about complete arguments
        #------------------------------------------------------------------------------------------------------

        self.assertEquals(res, notification_id)

        notification_request = kwargs_list['notification']
        user_id = kwargs_list['user_id']

        self.assertEquals(user_id, 'user_id')
        self.assertEquals(notification_request.delivery_config.delivery['email'], 'email')
        self.assertEquals(notification_request.delivery_config.delivery['mode'], DeliveryMode.DIGEST)

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
        self.assertEquals(notification_request.type, NotificationType.EMAIL)

    def test_match(self):

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
    @unittest.skip("")
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_create_email(self):
        '''
        Test the functionality of the create_email method of the User Notification Service

        '''

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
            email='email@email.com',
            mode = DeliveryMode.DIGEST,
            frequency= Frequency.REAL_TIME,
            message_header='message_header',
            parser='parser')

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

        self.assertEquals(msg_tuple[1], 'email@email.com' )
        #self.assertEquals(msg_tuple[0], ION_NOTIFICATION_EMAIL_ADDRESS)

        #self.assertEquals(message_dict['From'], ION_NOTIFICATION_EMAIL_ADDRESS)
        self.assertEquals(message_dict['To'], 'email@email.com')
        self.assertEquals(message_dict['Event'].rstrip('\r'), 'ResourceLifecycleEvent')
        self.assertEquals(message_dict['Originator'].rstrip('\r'), 'Some_Resource_Agent_ID1')
        self.assertEquals(message_dict['Description'].rstrip('\r'), 'RLE test event')


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

        #todo Do the same thing for update and delete notifications

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


        # check the reverse user info dictionary
        log.warning("The reverse user_info: %s" % proc1.reverse_user_info )

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


        # reverse_user_info
        log.warning("The reverse user_info: %s" % proc1.reverse_user_info )

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

        #todo The update method for UNS is not yet implementable without ids inside notification workers

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

        #todo whether the user_info contains the user_ids or the user_names need to be sorted

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

        user_id, _ = self.rrc.create(user)

        # confirm that users_index got created by discovery
        search_string = 'search "name" is "*" from "users_index"'

        results = self.poll(9, self.discovery.parse,search_string)
        log.warning("results : %s" % results)
        self.assertIsNotNone(results, 'Results not found')

        #--------------------------------------------------------------------------------------
        # Create notification workers
        #--------------------------------------------------------------------------------------

        pids = self.unsc.create_worker(number_of_workers=1)
        self.assertIsNotNone(pids, 'No workers were created')
        log.warning("pids: %s" % pids)

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
            log.warning("msg: %s" % msg)
            log.warning("headers: %s" % headers)
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


        log.warning("reloaded_reverse_user_info: %s" % reloaded_reverse_user_info)
        log.warning("keys: %s" % reloaded_reverse_user_info.keys())

        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_2'], ['new_user'] )

        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_2'], ['new_user'] )

        self.assertEquals(reloaded_reverse_user_info['event_type']['ResourceLifecycleEvent'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_type']['DetectionEvent'], ['new_user'] )

        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_1'], ['new_user'] )
        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_2'], ['new_user'] )


    @attr('LOCOINT')
    @unittest.skip("")
#    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_batch_notifications(self):
        '''
        Test that batch notifications work
        '''

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
        user_2.contact.phone = 'user_2@gmail.com'

        # user_3
        user_3 = UserInfo()
        user_3.name = 'user_3'
        user_3.contact.phone = 'user_3@gmail.com'

        user_id_1, _ = self.rrc.create(user_1)
        user_id_2, _ = self.rrc.create(user_2)
        user_id_3, _ = self.rrc.create(user_3)


        #--------------------------------------------------------------------------------------
        # Create a notification using UNS. This should cause the user_info to be updated
        #--------------------------------------------------------------------------------------

        self.unsc.create_notification(notification=notification_request_1, user_id=user_id_1)
        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_1)

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_2)

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_3)
        self.unsc.create_notification(notification=notification_request_3, user_id=user_id_3)


        #--------------------------------------------------------------------------------------
        # Publish events corresponding to the notification requests just made
        # These events will get stored in the event repository allowing UNS to batch process
        # them later for batch notifications
        #--------------------------------------------------------------------------------------

        event_publisher = EventPublisher("ResourceLifecycleEvent")

        for i in xrange(10):
            event_publisher.publish_event( ts_created= float(i) ,
                                            origin="instrument_1",
                                            origin_type="type_1",
                                            event_type='ResourceLifecycleEvent')

            event_publisher.publish_event( ts_created= float(i) ,
                                            origin="instrument_3",
                                            origin_type="type_3",
                                            event_type='ResourceLifecycleEvent')

        # allow enough time for elastic search to populate the events_index.
        gevent.sleep(4)

        #--------------------------------------------------------------------------------------
        # Do a process_batch() in order to start the batch notifications machinery
        #--------------------------------------------------------------------------------------

        self.unsc.process_batch(start_time=5.0, end_time= 8.0)

        #--------------------------------------------------------------------------------------
        # Check that the emails were sent to the users. This is done using the fake smtp client
        # Make assertions....
        #--------------------------------------------------------------------------------------


        #todo: add assertions when batch_notifications is completed

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_worker_send_email(self):
        '''
        Test that the workers process the notification event and send email using the
        fake smtp client
        '''

        #--------------------------------------------------------------------------------------
        # Create notification workers
        #--------------------------------------------------------------------------------------

        pids = self.unsc.create_worker(number_of_workers=3)

        log.warning("pids: %s" % pids)

        #--------------------------------------------------------------------------------------
        # Make notification request objects
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        notification_request_2 = NotificationRequest(origin="instrument_2",
            origin_type="type_2",
            event_type='DetectionEvent')

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
        # Create notifications using UNS.
        #--------------------------------------------------------------------------------------

        self.unsc.create_notification(notification=notification_request_1, user_id=user_id_1)
        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_1)

        self.unsc.create_notification(notification=notification_request_2, user_id=user_id_2)


        # allow elastic search to populate the users_index. This gives enough time for the reload of user_info
        gevent.sleep(2)

        #--------------------------------------------------------------------------------------
        # Publish events
        #--------------------------------------------------------------------------------------

        event_publisher = EventPublisher("ResourceLifecycleEvent")

        event_publisher.publish_event( ts_created= float(i) ,
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        event_publisher.publish_event( ts_created= float(i) ,
            origin="instrument_2",
            origin_type="type_2",
            event_type='DetectionEvent')

        #todo check whether we really need to have a sleep here
        gevent.sleep(2)

        #--------------------------------------------------------------------------------------
        # Check that the workers processed the events
        #--------------------------------------------------------------------------------------

        # check fake smtp client for emails sent

        # check if the correct users were sent email to regarding the correct events

        #todo check if the workers took the events from the queue in round robin



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
    @unittest.skip('SMS is being deprecated for now')
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
        if n1.name != notification_object1.name or\
           n1.origin_list != notification_object1.origin_list or\
           n1.events_list != notification_object1.events_list:
            self.fail("notification was not correct")
        n2 = self.unsc.read_notification(notification_id2)
        if n2.name != notification_object2.name or\
           n2.origin_list != notification_object2.origin_list or\
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
