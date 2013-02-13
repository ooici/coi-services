'''
@author Bill Bollenbacher
@author Swarbhanu Chatterjee
@author David Stuebe
@file ion/services/dm/presentation/test/user_notification_test.py
@description Unit and Integration test implementations for the user notification service class.
'''

from nose.plugins.attrib import attr
import unittest
import gevent
from mock import Mock, mocksignature
import os, time, uuid
from gevent import event, queue
from gevent.timeout import Timeout
from gevent.event import Event
import elasticpy as ep
from datetime import datetime, timedelta
from sets import Set

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict, get_ion_ts
from pyon.public import IonObject, RT, OT, PRED, Container
from pyon.core.exception import NotFound, BadRequest
from pyon.core.bootstrap import get_sys_name, CFG
from pyon.util.context import LocalContextMixin
from pyon.util.log import log
from pyon.event.event import EventPublisher, EventSubscriber

from ion.processes.bootstrap.index_bootstrap import STD_INDEXES
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.presentation.user_notification_service import UserNotificationService
from ion.services.dm.inventory.index_management_service import IndexManagementService
from ion.services.dm.presentation.user_notification_service import EmailEventProcessor

from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.objects import UserInfo, DeliveryConfig, ComputedListValue, ComputedValueAvailability
from interface.objects import DeviceEvent, NotificationPreferences, NotificationDeliveryModeEnum
from interface.services.cei.ischeduler_service import SchedulerServiceProcessClient
from interface.objects import NotificationRequest, TemporalBounds, DeviceStatusType

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
        # Test creating a notification

        user_id = 'user_id_1'

        self.mock_rr_client.create = mocksignature(self.mock_rr_client.create)
        self.mock_rr_client.create.return_value = ('notification_id_1','rev_1')

        self.mock_rr_client.find_resources = mocksignature(self.mock_rr_client.find_resources)
        self.mock_rr_client.find_resources.return_value = [],[]

        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
        self.mock_rr_client.read.return_value = 'notification'

        self.mock_rr_client.find_associations = mocksignature(self.mock_rr_client.find_associations)
        self.mock_rr_client.find_associations.return_value = []

        self.mock_rr_client.create_association = mocksignature(self.mock_rr_client.create_association)
        self.mock_rr_client.create_association.return_value = None


        self.user_notification.notifications = {}

        self.user_notification.event_processor.add_notification_for_user = mocksignature(self.user_notification.event_processor.add_notification_for_user)
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
        # Test that creating a notification without a providing a user_id results in an error

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

#    def test_update_notification(self):
#
#        # Test updating a notification
#
#        notification = 'notification'
#        user_id = 'user_id_1'
#
#        self.mock_rr_client.read = mocksignature(self.mock_rr_client.read)
#        self.mock_rr_client.read.return_value = notification
#
#        self.user_notification.update_user_info_object = mocksignature(self.user_notification.update_user_info_object)
#        self.user_notification.update_user_info_object.return_value = 'user'
#
#        self.user_notification.notifications = []
#
#        self.user_notification._update_notification_in_notifications_dict = mocksignature(self.user_notification._update_notification_in_notifications_dict)
#
#        self.user_notification.event_publisher.publish_event = mocksignature(self.user_notification.event_publisher.publish_event)
#
#        #-------------------------------------------------------------------------------------------------------------------
#        # Create a notification object
#        #-------------------------------------------------------------------------------------------------------------------
#
#        notification_request = NotificationRequest(name='a name',
#            origin = 'origin_1',
#            origin_type = 'origin_type_1',
#            event_type= 'event_type_1',
#            event_subtype = 'event_subtype_1' )
#
#        notification_request._id = 'an id'
#
#        #-------------------------------------------------------------------------------------------------------------------
#        # execution
#        #-------------------------------------------------------------------------------------------------------------------
#
#        self.user_notification.update_notification(notification_request, user_id)
#
#        #-------------------------------------------------------------------------------------------------------------------
#        # assertions
#        #-------------------------------------------------------------------------------------------------------------------
#
#        self.user_notification.update_user_info_object.assert_called_once_with(user_id, notification, notification)


    def test_delete_user_notification(self):
        # Test deleting a notification

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

        self.mock_rr_client.find_subjects = mocksignature(self.mock_rr_client.find_subjects)
        self.mock_rr_client.find_subjects.return_value = [], ''

        #-------------------------------------------------------------------------------------------------------------------
        # execution
        #-------------------------------------------------------------------------------------------------------------------

        self.user_notification.delete_notification(notification_id=notification_id)

        #-------------------------------------------------------------------------------------------------------------------
        # assertions
        #-------------------------------------------------------------------------------------------------------------------

        self.mock_rr_client.read.assert_called_once_with(notification_id, '')

        notification_request.temporal_bounds.end_datetime = get_ion_ts()
        self.mock_rr_client.update.assert_called_once_with(notification_request)


@attr('UNIT', group='evt')
class UserNotificationEventsTest(PyonTestCase):
    def _load_mock_events(self, event_list):
        for cnt, event_entry in enumerate(event_list):
            origin = event_entry.get('o', None)
            origin_type = event_entry.get('ot', None)
            sub_type = event_entry.get('st', None)
            attr = event_entry.get('attr', {})
            evt_obj = IonObject(event_entry['et'], origin=origin, origin_type=origin_type, sub_type=sub_type, ts_created=get_ion_ts(), **attr)
            evt_obj._id = str(cnt)
            self.events.append(evt_obj)

    def setUp(self):
        self.events = []
        self.uns = UserNotificationService()
        self.uns.find_events = Mock()
        def side_effect(origin=None, limit=None, **kwargs):
            evt_list = [evt for evt in reversed(self.events) if evt.origin == origin]
            if limit:
                evt_list = evt_list[:limit]
            return evt_list
        self.uns.find_events.side_effect = side_effect

    event_list1 = [
        dict(et='ResourceLifecycleEvent', o='ID_1', ot='InstrumentDevice', st='DEPLOYED_AVAILABLE',
            attr=dict(new_state="DEPLOYED_AVAILABLE",
                  old_state="DEPLOYED_PRIVATE",
                  resource_type="",
                  transition_event="")),

        dict(et='ResourceModifiedEvent', o='ID_1', ot='InstrumentDevice', st='CREATE',
            attr=dict(mod_type=1)),

        dict(et='ResourceAgentStateEvent', o='ID_1', ot='InstrumentDevice', st='',
            attr=dict(state="RESOURCE_AGENT_STATE_UNINITIALIZED")),

        dict(et='ResourceAgentResourceStateEvent', o='ID_1', ot='InstrumentDevice', st='',
            attr=dict(state="DRIVER_STATE_UNCONFIGURED")),

        dict(et='ResourceAgentResourceConfigEvent', o='ID_1', ot='InstrumentDevice', st='',
            attr=dict(config={'CCALDATE':[0,1,2], 'CG': -0.987093, 'CH': 0.1417895})),

        dict(et='ResourceAgentCommandEvent', o='ID_1', ot='InstrumentDevice', st='',
            attr=dict(args=[],
                kwargs={},
                command="execute_resource",
                execute_command="DRIVER_EVENT_STOP_AUTOSAMPLE",
                result=None)),

       dict(et='DeviceStatusEvent', o='ID_1', ot='PlatformDevice', st='input_voltage',
            attr=dict(state=DeviceStatusType.OK,
                description="Event to deliver the status of instrument.")),
    ]

    def test_get_recent_events(self):
        self._load_mock_events(self.event_list1)

        res_list = self.uns.get_recent_events(resource_id="ID_1")
        self.assertEquals(res_list.status, ComputedValueAvailability.PROVIDED)
        self.assertEquals(len(res_list.value), len(self.event_list1))

        self.assertTrue(all([eca.event_id == res_list.value[i]._id for (i, eca) in enumerate(res_list.computed_list)]))
        self.assertTrue(all([eca.event_summary for eca in res_list.computed_list]))

        #import pprint
        #pprint.pprint([eca.__dict__ for eca in res_list.computed_list])


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

        self.ION_NOTIFICATION_EMAIL_ADDRESS = 'data_alerts@oceanobservatories.org'

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
        # Test that the publishing of reload user info event occurs every time a create, update
        # or delete notification occurs.

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

        received_event_1 = queue.get(timeout=10)
        received_event_2 = queue.get(timeout=10)

        notifications_received = set([received_event_1.notification_id, received_event_2.notification_id])

        self.assertEquals(notifications, notifications_received)

#        #--------------------------------------------------------------------------------------
#        # Update notification
#        #--------------------------------------------------------------------------------------
#        notification_request_correct = self.unsc.read_notification(notification_id_1)
#        notification_request_correct.origin = 'instrument_correct'
#
#        notification_request_2 = self.unsc.read_notification(notification_id_2)
#        notification_request_2.origin = 'instrument_2_correct'
#
#        self.unsc.update_notification(notification=notification_request_correct, user_id=user_id)
#        self.unsc.update_notification(notification=notification_request_2, user_id=user_id)
#
#        #--------------------------------------------------------------------------------------
#        # Check that the correct events were published
#        #--------------------------------------------------------------------------------------
#
#        received_event_1 = queue.get(timeout=10)
#        received_event_2 = queue.get(timeout=10)
#
#        notifications_received = set([received_event_1.notification_id, received_event_2.notification_id])
#
#        self.assertEquals(notifications, notifications_received)

        #--------------------------------------------------------------------------------------
        # Delete notification
        #--------------------------------------------------------------------------------------

        self.unsc.delete_notification(notification_id_1)
        self.unsc.delete_notification(notification_id_2)

        notifications = set([notification_id_1, notification_id_2])

        #--------------------------------------------------------------------------------------
        # Check that the correct events were published
        #--------------------------------------------------------------------------------------

        received_event_1 = queue.get(timeout=10)
        received_event_2 = queue.get(timeout=10)

        notifications_received = set([received_event_1.notification_id, received_event_2.notification_id])

        self.assertEquals(notifications, notifications_received)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_notification_preferences(self):
        #--------------------------------------------------------------------------------------
        # Make a notification request object
        #--------------------------------------------------------------------------------------

        notification_request = NotificationRequest(   name= 'notification_1',
            origin="instrument_1",
            origin_type="type_1",
            event_type='ResourceLifecycleEvent')

        #--------------------------------------------------------------------------------------
        # Create user 1
        #--------------------------------------------------------------------------------------

        notification_preferences_1 = NotificationPreferences()
        notification_preferences_1.delivery_mode = NotificationDeliveryModeEnum.REALTIME

        user_1 = UserInfo()
        user_1.name = 'user_1'
        user_1.contact.email = 'user_1@yahoo.com'
        user_1.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences_1})

        user_id_1, _ = self.rrc.create(user_1)

        #--------------------------------------------------------------------------------------
        # user 2
        #--------------------------------------------------------------------------------------

        notification_preferences_2 = NotificationPreferences()
        notification_preferences_2.delivery_mode = NotificationDeliveryModeEnum.BATCH
        notification_preferences_2.delivery_enabled = False


        user_2 = UserInfo()
        user_2.name = 'user_2'
        user_2.contact.email = 'user_2@yahoo.com'
        user_2.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences_2})

        user_id_2, _ = self.rrc.create(user_2)

        #--------------------------------------------------------------------------------------
        # Create notification
        #--------------------------------------------------------------------------------------

        notification_id_1 = self.unsc.create_notification(notification=notification_request, user_id=user_id_1)
        notification_id_2 = self.unsc.create_notification(notification=notification_request, user_id=user_id_2)

        notifications = set([notification_id_1, notification_id_2])

        proc1 = self.container.proc_manager.procs_by_name['user_notification']

        #--------------------------------------------------------------------------------------------------------------------------------------
        # check user_info dictionary to see that the notification preferences are properly loaded to the user info dictionaries
        #--------------------------------------------------------------------------------------------------------------------------------------
        self.assertEquals(proc1.user_info[user_id_1]['notification_preferences'], notification_preferences_1)
        self.assertEquals(proc1.user_info[user_id_2]['notification_preferences'], notification_preferences_2)


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_user_info_UNS(self):
        # Test that the user info dictionary maintained by the notification workers get updated when
        # a notification is created, updated, or deleted by UNS

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

        proc1 = self.container.proc_manager.procs_by_name['user_notification']

        def found_user_info_dicts(proc1,*args, **kwargs):
            reloaded_user_info = proc1.user_info
            reloaded_reverse_user_info = proc1.reverse_user_info

            notifications = proc1.user_info[user_id_1]['notifications']
            origins = []
            event_types = []

            log.debug("Within the poll, got notifications here :%s", notifications)

            if notifications:
                for notific in notifications:
                    self.assertTrue(notific._id != '')
                    origins.append(notific.origin)
                    event_types.append(notific.event_type)

            if set(origins) == set(['instrument_1', 'instrument_2']) and set(event_types) == set(['ResourceLifecycleEvent', 'DetectionEvent']):
                return reloaded_user_info, reloaded_reverse_user_info
            else:
                return None

        reloaded_user_info,  reloaded_reverse_user_info= self.poll(9, found_user_info_dicts, proc1)

        # Check in UNS ------------>
        self.assertEquals(reloaded_user_info[user_id_1]['user_contact'].email, 'user_1@gmail.com' )

        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_1'], [user_id_1])
        self.assertEquals(set(reloaded_reverse_user_info['event_origin']['instrument_2']), set([user_id_2, user_id_1]))

        self.assertEquals(reloaded_reverse_user_info['event_type']['ResourceLifecycleEvent'], [user_id_1])
        self.assertEquals(set(reloaded_reverse_user_info['event_type']['DetectionEvent']), set([user_id_2, user_id_1]))

        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_1'], [user_id_1])
        self.assertEquals(set(reloaded_reverse_user_info['event_subtype']['subtype_2']), set([user_id_2, user_id_1]))

        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_1'], [user_id_1])
        self.assertEquals(set(reloaded_reverse_user_info['event_origin_type']['type_2']), set([user_id_2, user_id_1]))

        log.debug("The event processor received the notification topics after another create_notification() for the first user")
        log.debug("Verified that the event processor correctly updated its user info dictionaries")

#        #--------------------------------------------------------------------------------------
#        # Update notification and check that the user_info and reverse_user_info in UNS got reloaded
#        #--------------------------------------------------------------------------------------
#
#        notification_request_correct.origin = "newly_changed_instrument"
#
#        self.unsc.update_notification(notification=notification_request_correct, user_id=user_id_1)
#
#        # Check for UNS ------->
#
#        # user_info
#        notification_request_correct = self.rrc.read(notification_id_1)
#
#        # check that the updated notification is in the user info dictionary
#        self.assertTrue(notification_request_correct in proc1.user_info[user_id_1]['notifications'] )
#
#        # check that the notifications in the user info dictionary got updated
#        update_worked = False
#        for notification in proc1.user_info[user_id_1]['notifications']:
#            if notification.origin == "newly_changed_instrument":
#                update_worked = True
#                break
#
#        self.assertTrue(update_worked)
#
#        # reverse_user_info
#        self.assertTrue(user_id_1 in proc1.reverse_user_info['event_origin']["newly_changed_instrument"])
#
#        log.debug("Verified that the event processor correctly updated its user info dictionaries after an update_notification()")

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
        # Test the user_info and reverse user info dictionary capability of the notification worker

        #--------------------------------------------------------------------------------------
        # Create a user subscribed to REALTIME notifications
        #--------------------------------------------------------------------------------------
        notification_preferences = NotificationPreferences()
        notification_preferences.delivery_mode = NotificationDeliveryModeEnum.REALTIME
        notification_preferences.delivery_enabled = True

        user = UserInfo()
        user.name = 'new_user'
        user.contact.email = 'new_user@gmail.com'
        user.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences})

        #--------------------------------------------------------------------------------------
        # Create a user subscribed to BATCH notifications
        #--------------------------------------------------------------------------------------
        notification_preferences_2 = NotificationPreferences()
        notification_preferences_2.delivery_mode = NotificationDeliveryModeEnum.BATCH
        notification_preferences_2.delivery_enabled = True

        user_batch = UserInfo()
        user_batch.name = 'user_batch'
        user_batch.contact.email = 'user_batch@gmail.com'
        user_batch.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences_2})

        #--------------------------------------------------------------------------------------
        # Create a user subscribed to REALTIME notifications but with delivery turned OFF
        #--------------------------------------------------------------------------------------
        notification_preferences_3 = NotificationPreferences()
        notification_preferences_3.delivery_mode = NotificationDeliveryModeEnum.REALTIME
        notification_preferences_3.delivery_enabled = False

        user_disabled = UserInfo()
        user_disabled.name = 'user_disabled'
        user_disabled.contact.email = 'user_disabled@gmail.com'
        user_disabled.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences_3})


        # this part of code is in the beginning to allow enough time for users_index creation

        user_id, _ = self.rrc.create(user)
        user_batch_id, _ = self.rrc.create(user_batch)
        user_disabled_id, _ = self.rrc.create(user_disabled)

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
        notification_id_batch = self.unsc.create_notification(notification=notification_request_correct, user_id=user_batch_id)
        notification_id_disabled = self.unsc.create_notification(notification=notification_request_correct, user_id=user_disabled_id)

        #--------------------------------------------------------------------------------------
        # Check the user_info and reverse_user_info got reloaded
        #--------------------------------------------------------------------------------------

        processes =self.container.proc_manager.procs

        def found_user_info_dicts(processes, qsize,*args, **kwargs):
            for key in processes:
                if key.startswith('notification_worker'):
                    proc1 = processes[key]
                    queue = proc1.q

                    if queue.qsize() >= qsize:
                        log.debug("the name of the process: %s" % key)

                        reloaded_user_info, reloaded_reverse_user_info = queue.get(timeout=10)
                        proc1.q.queue.clear()
                        return reloaded_user_info, reloaded_reverse_user_info

        reloaded_user_info,  reloaded_reverse_user_info= self.poll(20, found_user_info_dicts, processes, 3)
        notification_id_2 = self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        self.assertIsNotNone(reloaded_user_info)
        self.assertIsNotNone(reloaded_reverse_user_info)

        # read back the registered notification request objects
        notification_request_correct = self.rrc.read(notification_id_1)

        self.assertEquals(reloaded_user_info[user_id]['notifications'], [notification_request_correct] )
        self.assertEquals(reloaded_user_info[user_id]['notification_preferences'].delivery_mode, notification_preferences.delivery_mode )
        self.assertEquals(reloaded_user_info[user_id]['notification_preferences'].delivery_enabled, notification_preferences.delivery_enabled )

        self.assertEquals(reloaded_user_info[user_batch_id]['notification_preferences'].delivery_mode, notification_preferences_2.delivery_mode )
        self.assertEquals(reloaded_user_info[user_batch_id]['notification_preferences'].delivery_enabled, notification_preferences_2.delivery_enabled )

        self.assertEquals(reloaded_user_info[user_disabled_id]['notification_preferences'].delivery_mode, notification_preferences_3.delivery_mode )
        self.assertEquals(reloaded_user_info[user_disabled_id]['notification_preferences'].delivery_enabled, notification_preferences_3.delivery_enabled )

        self.assertEquals(reloaded_user_info[user_id]['user_contact'].email, 'new_user@gmail.com')

        self.assertEquals(reloaded_reverse_user_info['event_origin']['instrument_1'], [user_id] )
        self.assertEquals(reloaded_reverse_user_info['event_subtype']['subtype_1'], [user_id] )
        self.assertEquals(reloaded_reverse_user_info['event_type']['ResourceLifecycleEvent'], [user_id] )
        self.assertEquals(reloaded_reverse_user_info['event_origin_type']['type_1'], [user_id] )

        log.debug("Verified that the notification worker correctly updated its user info dictionaries after a create_notification()")

        #--------------------------------------------------------------------------------------
        # Create another notification
        #--------------------------------------------------------------------------------------

        reloaded_user_info,  reloaded_reverse_user_info= self.poll(20, found_user_info_dicts, processes, 1)

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
        # Test that the process_batch() method works

        test_start_time = get_ion_ts() # Note this time is in milliseconds
        test_end_time = str(int(get_ion_ts()) + 10000) # Adding 10 seconds

        #--------------------------------------------------------------------------------------
        # Publish events corresponding to the notification requests just made
        # These events will get stored in the event repository allowing UNS to batch process
        # them later for batch notifications
        #--------------------------------------------------------------------------------------

        event_publisher = EventPublisher()

        # this part of code is in the beginning to allow enough time for the events_index creation

        for i in xrange(10):

            event_publisher.publish_event(
                origin="instrument_1",
                origin_type="type_1",
                event_type='ResourceLifecycleEvent')

            event_publisher.publish_event(
                origin="instrument_3",
                origin_type="type_3",
                event_type='ResourceLifecycleEvent')

        #----------------------------------------------------------------------------------------
        # Create users and get the user_ids
        #----------------------------------------------------------------------------------------

        # user_1  -- default notification preferences
        user_1 = UserInfo()
        user_1.name = 'user_1'
        user_1.contact.email = 'user_1@gmail.com'

        # user_2   --- prefers BATCH notification
        notification_preferences_2 = NotificationPreferences()
        notification_preferences_2.delivery_mode = NotificationDeliveryModeEnum.BATCH
        notification_preferences_2.delivery_enabled = True

        user_2 = UserInfo()
        user_2.name = 'user_2'
        user_2.contact.email = 'user_2@gmail.com'
        user_2.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences_2})

        # user_3  --- delivery enabled at default
        notification_preferences_3 = NotificationPreferences()
        notification_preferences_3.delivery_mode = NotificationDeliveryModeEnum.BATCH

        user_3 = UserInfo()
        user_3.name = 'user_3'
        user_3.contact.email = 'user_3@gmail.com'
        user_3.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences_3})

        # user_4   --- prefers REALTIME notification

        notification_preferences_4 = NotificationPreferences()
        notification_preferences_4.delivery_mode = NotificationDeliveryModeEnum.REALTIME
        notification_preferences_4.delivery_enabled = True

        user_4 = UserInfo()
        user_4.name = 'user_4'
        user_4.contact.email = 'user_4@gmail.com'
        user_4.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences_4})

        # user_5   --- delivery disabled

        notification_preferences_5 = NotificationPreferences()
        notification_preferences_5.delivery_mode = NotificationDeliveryModeEnum.BATCH
        notification_preferences_5.delivery_enabled = False

        user_5 = UserInfo()
        user_5.name = 'user_5'
        user_5.contact.email = 'user_5@gmail.com'
        user_5.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences_5})

        # this part of code is in the beginning to allow enough time for the users_index creation

        user_id_1, _ = self.rrc.create(user_1)
        user_id_2, _ = self.rrc.create(user_2)
        user_id_3, _ = self.rrc.create(user_3)
        user_id_4, _ = self.rrc.create(user_4)
        user_id_5, _ = self.rrc.create(user_5)

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

        self.unsc.create_notification(notification=notification_request_correct, user_id=user_id_4)
        self.unsc.create_notification(notification=notification_request_3, user_id=user_id_4)

        self.unsc.create_notification(notification=notification_request_correct, user_id=user_id_5)
        self.unsc.create_notification(notification=notification_request_3, user_id=user_id_5)

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
            email_tuple = proc1.smtp_client.sent_mail.get(timeout=10)
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
                if fields[0].find("Time of event") > -1:
                    event_time = fields[1].strip(" ")
                    break

            self.assertIsNotNone(event_time)

#            # Check that the events sent in the email had times within the user specified range
#            self.assertTrue(event_time >= test_start_time)
#            self.assertTrue(event_time <= test_end_time)


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_worker_send_email(self):
        # Test that the workers process the notification event and send email using the
        # fake smtp client

        #-------------------------------------------------------
        # Create users and get the user_ids
        #-------------------------------------------------------

        # user_1
        notification_preferences = NotificationPreferences()
        notification_preferences.delivery_mode = NotificationDeliveryModeEnum.REALTIME
        notification_preferences.delivery_enabled = True

        user_1 = UserInfo()
        user_1.name = 'user_1'
        user_1.contact.email = 'user_1@gmail.com'
        user_1.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences})

        # user_2
        user_2 = UserInfo()
        user_2.name = 'user_2'
        user_2.contact.email = 'user_2@gmail.com'

        # user_3
        notification_preferences = NotificationPreferences()
        notification_preferences.delivery_mode = NotificationDeliveryModeEnum.REALTIME
        notification_preferences.delivery_enabled = False

        user_3 = UserInfo()
        user_3.name = 'user_3'
        user_3.contact.email = 'user_3@gmail.com'
        user_3.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences})

        # user_4
        notification_preferences = NotificationPreferences()
        notification_preferences.delivery_mode = NotificationDeliveryModeEnum.BATCH
        notification_preferences.delivery_enabled = True

        user_4 = UserInfo()
        user_4.name = 'user_4'
        user_4.contact.email = 'user_4@gmail.com'
        user_4.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences})


        user_id_1, _ = self.rrc.create(user_1)
        user_id_2, _ = self.rrc.create(user_2)
        user_id_3, _ = self.rrc.create(user_3)
        user_id_4, _ = self.rrc.create(user_4)

        #--------------------------------------------------------------------------------------
        # Make notification request objects -- Remember to put names
        #--------------------------------------------------------------------------------------

        notification_request_1 = NotificationRequest(   name = "notification_1",
            origin="instrument_1",
            event_type='ResourceLifecycleEvent',
        )

        notification_request_2 = NotificationRequest(   name = "notification_2",
            origin="instrument_2",
            event_type='DeviceStatusEvent',
        )

        notification_request_3 = NotificationRequest(   name = "notification_3",
            origin="instrument_3",
            event_type='DeviceCommsEvent',
        )

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
        # Create notifications using UNS.
        #--------------------------------------------------------------------------------------

        q = gevent.queue.Queue()

        id1 = self.unsc.create_notification(notification=notification_request_1, user_id=user_id_1)
        q.put(id1)

        id2 = self.unsc.create_notification(notification=notification_request_2, user_id=user_id_2)
        q.put(id2)

        id3 = self.unsc.create_notification(notification=notification_request_3, user_id=user_id_2)
        q.put(id3)

        id4 = self.unsc.create_notification(notification=notification_request_1, user_id=user_id_3)
        q.put(id4)

        id5 = self.unsc.create_notification(notification=notification_request_1, user_id=user_id_4)
        q.put(id5)

        # Wait till all the notifications have been created....
        for i in xrange(5):
            q.get(timeout = 10)

        #--------------------------------------------------------------------------------------
        # Publish events
        #--------------------------------------------------------------------------------------

        event_publisher = EventPublisher()

        event_publisher.publish_event(
            event_type = "ResourceLifecycleEvent",
            origin="instrument_1")

        event_publisher.publish_event(
            event_type = "DeviceStatusEvent",
            origin="instrument_2",
            time_stamps = [get_ion_ts(), str(int(get_ion_ts()) + 60*20*1000)])

        event_publisher.publish_event(
            event_type = "DeviceCommsEvent",
            origin="instrument_3",
            time_stamp = get_ion_ts())


        #--------------------------------------------------------------------------------------
        # Check that the workers processed the events
        #--------------------------------------------------------------------------------------

        worker_that_sent_email = None
        for proc in procs:
            if not proc.smtp_client.sent_mail.empty():
                worker_that_sent_email = proc
                break

        email_tuples = []

        while not worker_that_sent_email.smtp_client.sent_mail.empty():
            email_tuple  = worker_that_sent_email.smtp_client.sent_mail.get(timeout=20)
            email_tuples.append(email_tuple)
            log.debug("size of sent_mail queue: %s" % worker_that_sent_email.smtp_client.sent_mail.qsize())
            log.debug("email tuple::: %s" % str(email_tuple))

        for email_tuple in email_tuples:
            # Parse the email sent and check and make assertions about email body. Make assertions about the sender and recipient
            msg_sender, msg_recipient, msg = email_tuple

            self.assertEquals(msg_sender, CFG.get_safe('server.smtp.sender') )
            self.assertTrue(msg_recipient in ['user_1@gmail.com', 'user_2@gmail.com'])

            # The below users did not want real time notifications or disabled delivery
            self.assertTrue(msg_recipient not in ['user_3@gmail.com', 'user_4@gmail.com'])

            maps = msg.split(",")

            event_type = ''
            for map in maps:
                fields = map.split(":")
                log.debug("fields::: %s" % fields)
                if fields[0].find("type_") > -1:
                    event_type = fields[1].strip(" ").strip("'")
                    break
    #            if fields[0].find("Time stamp") > -1:
    #                event_time = int(fields[1].strip(" "))
    #                break
            if msg_recipient == 'user_1@gmail.com':
                self.assertTrue(event_type in ['ResourceLifecycleEvent', 'DeviceStatusEvent'])
            elif msg_recipient == 'user_2@gmail.com':
                self.assertTrue(event_type in ['DeviceCommsEvent', 'DeviceStatusEvent'])
            else:
                self.fail('Got email sent to msg recipient who did not set a correct notification preference.')


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_create_read_user_notifications(self):
        # Test the create and read notification methods

        #--------------------------------------------------------------------------------------
        # create user with email address in RR
        #--------------------------------------------------------------------------------------

        user = UserInfo()
        user.name = 'user_1'
        user.contact.email = 'user_1@gmail.com'

        user_id, _ = self.rrc.create(user)

        user_2 = UserInfo()
        user_2.name = 'user_2'
        user_2.contact.email = 'user_2@gmail.com'

        user_id_2, _ = self.rrc.create(user_2)

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

        # a notification is created for user 1
        notification_id1 =  self.unsc.create_notification(notification=notification_request_1, user_id=user_id)
        notification_id2 =  self.unsc.create_notification(notification=notification_request_2, user_id=user_id)

        #--------------------------------------------------------------------------------------
        # Check the resource registry
        #--------------------------------------------------------------------------------------

        n1 = self.unsc.read_notification(notification_id1)

        self.assertEquals(n1.event_type, notification_request_1.event_type)
        self.assertEquals(n1.origin, notification_request_1.origin)
        self.assertEquals(n1.origin_type, notification_request_1.origin_type)

        # Check the user notification service process
        proc = self.container.proc_manager.procs_by_name['user_notification']

        self.assertEquals(len(proc.notifications.values()), 2)

        # Check the user info dictionary of the UNS process
        user_info = proc.user_info
        notifications_held = user_info[user_id]['notifications']

        self.assertEquals(len(notifications_held), 2)

        def _compare_notifications(notifications):

            log.debug("notification insider here:: %s", notifications)
            for notif in notifications:

                self.assertTrue(notif._id==notification_id1 or notif._id==notification_id2)
                if notif._id==notification_id1:
                    self.assertEquals(notif.event_type, notification_request_1.event_type)
                    self.assertEquals(notif.origin, notification_request_1.origin)
                    self.assertEquals(notif.origin_type, notification_request_1.origin_type)
                    self.assertEquals(notif._id, notification_id1)
                else:
                    self.assertEquals(notif.event_type, notification_request_2.event_type)
                    self.assertEquals(notif.origin, notification_request_2.origin)
                    self.assertEquals(notif.origin_type, notification_request_2.origin_type)
                    self.assertEquals(notif._id, notification_id2)

        _compare_notifications(notifications_held)


        #--------------------------------------------------------------------------------------
        # Create the same notification request again using UNS. Check that no duplicate notification request is made
        #--------------------------------------------------------------------------------------

        notification_again_id =  self.unsc.create_notification(notification=notification_request_1, user_id=user_id)
        notification_again = self.rrc.read(notification_again_id)

        # Check the resource registry to see that the common notification request is being used
        self.assertEquals(notification_again.event_type, notification_request_1.event_type)
        self.assertEquals(notification_again.origin, notification_request_1.origin)
        self.assertEquals(notification_again.origin_type, notification_request_1.origin_type)
        # assert that the old id is unchanged
        self.assertEquals(notification_again_id, notification_id1)

        # Check the user info object
        user = self.rrc.read(user_id)
        notifs_of_user = [item['value'] for item in user.variables if item['name']=='notifications'][0]
        self.assertTrue(len(notifs_of_user), 2)

        _compare_notifications(notifs_of_user)

        #--------------------------------------------------------------------------------------
        # now have the other user subscribe to the same notification request
        #--------------------------------------------------------------------------------------

        notification_id_user_2 =  self.unsc.create_notification(notification=notification_request_1, user_id=user_id_2)

        ##########-------------------------------------------------------------------------------------------------------
        # Now check if subscriptions of user 1 are getting overwritten because user 2 subscribed to the same notification
        ##########-------------------------------------------------------------------------------------------------------

        #--------------------------------------------------------------------------------------
        # Check the resource registry
        #--------------------------------------------------------------------------------------

        n2 = self.unsc.read_notification(notification_id_user_2)
        self.assertEquals(n2.event_type, notification_request_1.event_type)
        self.assertEquals(n2.origin, notification_request_1.origin)
        self.assertEquals(n2.origin_type, notification_request_1.origin_type)

        self.assertEquals(len(proc.notifications.values()), 2)

        #--------------------------------------------------------------------------------------
        # Check the user info dictionary of the UNS process
        #--------------------------------------------------------------------------------------
        user_info = proc.user_info

        # For the first user, his subscriptions should be unchanged
        notifications_held_1 = user_info[user_id]['notifications']

        self.assertEquals(len(notifications_held_1), 2)
        _compare_notifications(notifications_held_1)

        # For the second user, he should have got a new subscription
        notifications_held_2 = user_info[user_id_2]['notifications']

        self.assertEquals(len(notifications_held_2), 1)

        notif = notifications_held_2[0]
        self.assertTrue(notif._id==notification_id1 or notif._id==notification_id2)

        if notif._id==notification_id1:
            self.assertEquals(notif.event_type, notification_request_1.event_type)
            self.assertEquals(notif.origin, notification_request_1.origin)
            self.assertEquals(notif.origin_type, notification_request_1.origin_type)
            self.assertEquals(notif._id, notification_id1)

        #--------------------------------------------------------------------------------------
        # Check the user info objects
        #--------------------------------------------------------------------------------------

        # Check the first user's info object
        user = self.rrc.read(user_id)
        notifs_of_user = [item['value'] for item in user.variables if item['name']=='notifications'][0]
        self.assertTrue(len(notifs_of_user), 2)

        _compare_notifications(notifs_of_user)

        # Check the second user's info object
        user = self.rrc.read(user_id_2)
        notifs_of_user = [item['value'] for item in user.variables if item['name']=='notifications'][0]
        self.assertTrue(len(notifs_of_user), 1)

        notif = notifs_of_user[0]
        self.assertTrue(notif._id==notification_id1 or notif._id==notification_id2)

        if notif._id==notification_id1:
            self.assertEquals(notif.event_type, notification_request_1.event_type)
            self.assertEquals(notif.origin, notification_request_1.origin)
            self.assertEquals(notif.origin_type, notification_request_1.origin_type)
            self.assertEquals(notif._id, notification_id1)

        #--------------------------------------------------------------------------------------
        # Check the associations... check that user 1 is associated with the same two notifications as before
        # and that user 2 is associated with one notification
        #--------------------------------------------------------------------------------------

        not_ids, _ = self.rrc.find_objects(subject=user_id,
                                predicate=PRED.hasNotification,
                                id_only=True)

        log.debug("not_ids::: %s", not_ids)

        self.assertEquals(set(not_ids), set([notification_id1,notification_id2]))

        not_ids_2, _ = self.rrc.find_objects(subject=user_id_2,
            predicate=PRED.hasNotification,
            id_only=True)

        log.debug("not_ids_2::: %s", not_ids_2)

        self.assertEquals(set(not_ids_2), set([notification_id1]))




    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_delete_user_notifications(self):
        # Test deleting a notification

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

        # Now check the user info object has the notifications
        user = self.rrc.read(user_id)
        vars = user.variables

        for var in vars:
            if var['name'] == 'notifications':
                self.assertEquals(len(var['value']), 2)
                for notif in var['value']:
                    self.assertTrue(notif.name in  ["notification_1", "notification_2"])
                    self.assertTrue(notif.origin in ["instrument_1", "instrument_2"])
                    self.assertTrue(notif.origin_type in ["type_1", "type_2"])
                    self.assertTrue(notif.event_type, ["ResourceLifecycleEvent", "DetectionEvent"])


        #--------------------------------------------------------------------------------------
        # Delete notification 2
        #--------------------------------------------------------------------------------------
        self.unsc.delete_notification(notification_id2)

        notific_2 = self.rrc.read(notification_id2)
        # This checks that the notifications have been retired.
        self.assertNotEquals(notific_2.temporal_bounds.end_datetime, '')

        # Now check the user info object has the notifications
        user = self.rrc.read(user_id)
        vars = user.variables

        for var in vars:
            if var['name'] == 'notifications':
                self.assertEquals(len(var['value']), 2)
                for notif in var['value']:
                    self.assertTrue(notif.name in  ["notification_1", "notification_2"])
                    if notif.origin == "instrument_2":
                        self.assertNotEquals(notif.temporal_bounds.end_datetime, '')
                    elif notif.origin == "instrument_1":
                        self.assertEquals(notif.temporal_bounds.end_datetime, '')
                    else:
                        self.fail("ACHTUNG: A completely different notification is being stored in the user info object")

        #--------------------------------------------------------------------------------------
        # Delete notification 1
        #--------------------------------------------------------------------------------------
        self.unsc.delete_notification(notification_id1)
        notific_1 = self.rrc.read(notification_id1)
        self.assertNotEquals(notific_1.temporal_bounds.end_datetime, '')

        # Now check the user info object has the notifications
        user = self.rrc.read(user_id)
        vars = user.variables

        for var in vars:
            if var['name'] == 'notifications':
                self.assertEquals(len(var['value']), 2)
                for notif in var['value']:
                    self.assertTrue(notif.name in  ["notification_1", "notification_2"])
                    if notif.origin == "instrument_2":
                        self.assertNotEquals(notif.temporal_bounds.end_datetime, '')
                    elif notif.origin == "instrument_1":
                        self.assertNotEquals(notif.temporal_bounds.end_datetime, '')
                    else:
                        self.fail("ACHTUNG: A completely different notification is being stored in the user info object")

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_update_user_notification(self):
        # Test updating a user notification

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

#        self.unsc.update_notification(notification, user_id)
#
#        # read back the notification and check that it got changed
#        notification = self.unsc.read_notification(notification_id)
#
#        # Assert that the notification resource in the datastore does not get overwritten
#        self.assertEquals(notification.origin_type, 'type_1')
#        self.assertEquals(notification.event_type, 'ResourceLifecycleEvent')
#        self.assertEquals(notification.origin, 'instrument_1')
#
#        # Check that the UserInfo object is updated
#
#        # Check that the user info dictionary is updated

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_find_events(self):
        # Test the find events functionality of UNS

        # publish some events for the event repository
        event_publisher_1 = EventPublisher("PlatformEvent")
        event_publisher_2 = EventPublisher("ReloadUserInfoEvent")

        min_datetime = get_ion_ts()

        for i in xrange(10):
            event_publisher_1.publish_event(origin='my_special_find_events_origin', ts_created = get_ion_ts())
            event_publisher_2.publish_event(origin='another_origin', ts_created = get_ion_ts())

        max_datetime = get_ion_ts()

        def poller():
            events = self.unsc.find_events(origin='my_special_find_events_origin', type = 'PlatformEvent', min_datetime= min_datetime, max_datetime=max_datetime)
            log.debug("(UNS) got events: %s", events)
            return len(events) >= 4

        success = self.event_poll(poller, 10)

        self.assertTrue(success)


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_find_events_extended(self):
        # Test the find events functionality of UNS

        # publish some events for the event repository
        event_publisher_1 = EventPublisher("PlatformEvent")
        event_publisher_2 = EventPublisher("ReloadUserInfoEvent")

        min_time = get_ion_ts()

        for i in xrange(10):
            event_publisher_1.publish_event(origin='Some_Resource_Agent_ID1', ts_created = get_ion_ts())
            event_publisher_2.publish_event(origin='Some_Resource_Agent_ID2', ts_created = get_ion_ts())

        max_time = get_ion_ts()

        # allow elastic search to populate the indexes. This gives enough time for the reload of user_info
        def poller():
            events = self.unsc.find_events_extended(origin='Some_Resource_Agent_ID1', min_time=min_time, max_time=max_time)
            return len(events) >= 4

        success = self.event_poll(poller, 10)
        self.assertTrue(success)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_create_several_workers(self):
        # Create more than one worker. Test that they process events in round robin

        pids = self.unsc.create_worker(number_of_workers=2)

        self.assertEquals(len(pids), 2)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_publish_event_object(self):
        # Test the publish_event_object() method of UNS

        event_recvd_count = 0
        #--------------------------------------------------------------------------------
        # Create an event object
        #--------------------------------------------------------------------------------
        event_1 = DeviceEvent(  origin= "origin_1",
            origin_type='origin_type_1',
            sub_type= 'sub_type_1')

        event_with_ts_created = event_1
        event_with_ts_created.ts_created = get_ion_ts()

        # create async result to wait on in test
        ar = gevent.event.AsyncResult()

        #--------------------------------------------------------------------------------
        # Set up a subscriber to listen for that event
        #--------------------------------------------------------------------------------
        def received_event(result, event_recvd_count, event, headers):
            log.debug("received the event in the test: %s" % event)

            #--------------------------------------------------------------------------------
            # check that the event was published
            #--------------------------------------------------------------------------------
            self.assertEquals(event.origin, "origin_1")
            self.assertEquals(event.type_, 'DeviceEvent')
            self.assertEquals(event.origin_type, 'origin_type_1')
            self.assertNotEquals(event.ts_created, '')
            self.assertEquals(event.sub_type, 'sub_type_1')

            event_recvd_count += 1

            if event_recvd_count == 2:
                result.set(True)

        event_subscriber = EventSubscriber( event_type = 'DeviceEvent',
            origin="origin_1",
            callback=lambda m, h: received_event(ar, event_recvd_count, m, h))
        event_subscriber.start()
        self.addCleanup(event_subscriber.stop)

        #--------------------------------------------------------------------------------
        # Use the UNS publish_event
        #--------------------------------------------------------------------------------

        self.unsc.publish_event_object(event=event_1)
        self.unsc.publish_event_object(event=event_with_ts_created)

        ar.wait(timeout=10)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_publish_event(self):
        # Test the publish_event() method of UNS

        type = "PlatformTelemetryEvent"
        origin= "origin_1"
        origin_type='origin_type_1'
        sub_type= 'sub_type_1'
        event_attrs = {'status': 'OK'}

        # create async result to wait on in test
        ar_1 = gevent.event.AsyncResult()
        ar_2 = gevent.event.AsyncResult()

        #--------------------------------------------------------------------------------
        # Set up a subscriber to listen for that event
        #--------------------------------------------------------------------------------
        def received_event(result, event, headers):
            log.debug("received the event in the test: %s" % event)

            #--------------------------------------------------------------------------------
            # check that the event was published
            #--------------------------------------------------------------------------------
            self.assertEquals(event.origin, "origin_1")
            self.assertEquals(event.type_, 'PlatformTelemetryEvent')
            self.assertEquals(event.origin_type, 'origin_type_1')
            self.assertNotEquals(event.ts_created, '')
            self.assertEquals(event.sub_type, 'sub_type_1')

            result.set(True)

        event_subscriber_1 = EventSubscriber( event_type = 'PlatformTelemetryEvent',
            origin="origin_1",
            callback=lambda m, h: received_event(ar_1, m, h))
        event_subscriber_1.start()
        self.addCleanup(event_subscriber_1.stop)

        event_subscriber_2 = EventSubscriber( event_type = 'PlatformTelemetryEvent',
            origin="origin_1",
            callback=lambda m, h: received_event(ar_2, m, h))
        event_subscriber_2.start()
        self.addCleanup(event_subscriber_2.stop)

        #--------------------------------------------------------------------------------
        # Use the UNS publish_event
        #--------------------------------------------------------------------------------

        self.unsc.publish_event(
            event_type=type,
            origin=origin,
            origin_type=origin_type,
            sub_type=sub_type,
            description="a description",
            event_attrs = event_attrs
        )

        ar_1.wait(timeout=10)

        # Not passing the event_attrs this time
        self.unsc.publish_event(
            event_type=type,
            origin=origin,
            origin_type=origin_type,
            sub_type=sub_type,
            description="a description"
        )

        ar_2.wait(timeout=10)


    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_batch_notifications(self):
        # Test how the UNS listens to timer events and through the call back runs the process_batch()
        # with the correct arguments.

        #--------------------------------------------------------------------------------------------
        # The operator sets up the process_batch_key. The UNS will listen for scheduler created
        # timer events with origin = process_batch_key
        #--------------------------------------------------------------------------------------------
        # generate a uuid
        newkey = 'batch_processing_' + str(uuid.uuid4())
        self.unsc.set_process_batch_key(process_batch_key = newkey)

        #--------------------------------------------------------------------------------
        # Publish the events that the user will later be notified about
        #--------------------------------------------------------------------------------
        event_publisher = EventPublisher()

        # this part of code is in the beginning to allow enough time for the events_index creation
        times_of_events_published = Set()


        def publish_events():
            for i in xrange(3):
                t = get_ion_ts()

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
        notification_preferences_1 = NotificationPreferences()
        notification_preferences_1.delivery_mode = NotificationDeliveryModeEnum.BATCH
        notification_preferences_1.delivery_enabled = True

        user_1 = UserInfo()
        user_1.name = 'user_1'
        user_1.contact.email = 'user_1@gmail.com'
        user_1.variables.append({'name' : 'notification_preferences', 'value' : notification_preferences_1})

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

        # Set up a time for the scheduler to trigger timer events
        # Trigger the timer event 15 seconds later from now
        time_now = datetime.utcnow() + timedelta(seconds=15)
        times_of_day =[{'hour': str(time_now.hour),'minute' : str(time_now.minute), 'second':str(time_now.second) }]

        sid = self.ssclient.create_time_of_day_timer(   times_of_day=times_of_day,
            expires=time.time()+25200+60,
            event_origin= newkey,
            event_subtype="")

        log.debug("created the timer id: %s", sid)

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

        def send_email(events_for_message, user_id, *args, **kwargs):
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

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_get_user_notifications(self):
        # Test that the get_user_notifications() method returns the notifications for a user

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
        # Get the notifications for the user
        #--------------------------------------------------------------------------------------

        notifications= self.unsc.get_user_notifications(user_id)
        self.assertEquals(len(notifications),2)

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

        #--------------------------------------------------------------------------------------
        # Now delete a notification and verify that it wont get picked up by get_user_notifications()
        #--------------------------------------------------------------------------------------
        self.unsc.delete_notification(notification_id=notification_id2)

        # Get the notifications for the user
        notifications = self.unsc.get_user_notifications(user_id)
        self.assertEquals(len(notifications),1)
        notification = notifications[0]

        self.assertEquals(notification.name, 'notification_1' )
        self.assertEquals(notification.origin, 'instrument_1' )
        self.assertEquals(notification.origin_type, 'type_1')
        self.assertEquals(notification.event_type, 'ResourceLifecycleEvent' )

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_get_recent_events(self):
        # Test that the get_recent_events(resource_id, limit) method returns the events whose origin is
        # the specified resource.

        #--------------------------------------------------------------------------------------
        # create user with email address in RR
        #--------------------------------------------------------------------------------------

        # publish some events for the event repository
        event_publisher_1 = EventPublisher("PlatformEvent")
        event_publisher_2 = EventPublisher("PlatformEvent")

        def publish_events():
            x = 0
            for i in xrange(10):
                t = get_ion_ts()
                event_publisher_1.publish_event(origin='my_unique_test_recent_events_origin', ts_created = t)
                event_publisher_2.publish_event(origin='Another_recent_events_origin', ts_created = t)
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
        # Test that the get_subscriptions works correctly

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

        notification_active_3 = NotificationRequest(   name = "notification_2",
            origin='wrong_origin',
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

        notification_past_3 = NotificationRequest(   name = "notification_4_to_be_retired",
            origin='wrong_origin_2',
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
            notification_id_active_3 =  self.unsc.create_notification(notification=notification_active_3, user_id=user_id)

            # Store the ids for the active notifications in a set
            active_notification_ids.add(notification_id_active_1)
            active_notification_ids.add(notification_id_active_2)
            active_notification_ids.add(notification_id_active_3)

            notification_id_past_1 =  self.unsc.create_notification(notification=notification_past_1, user_id=user_id)
            notification_id_past_2 =  self.unsc.create_notification(notification=notification_past_2, user_id=user_id)
            notification_id_past_3 =  self.unsc.create_notification(notification=notification_past_3, user_id=user_id)

            # Store the ids for the retired-to-be notifications in a set
            past_notification_ids.add(notification_id_past_1)
            past_notification_ids.add(notification_id_past_2)
            past_notification_ids.add(notification_id_past_3)

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

            notific_in_db = self.rrc.read(notific._id)
            self.assertTrue(notific_in_db)

            self.assertEquals(notific.origin, data_product_id)
            self.assertEquals(notific.temporal_bounds.end_datetime, '')
            self.assertTrue(notific.origin_type == 'type_1' or  notific.origin_type =='type_2')
            self.assertEquals(notific.event_type, 'ResourceLifecycleEvent')

            self.assertEquals(notific_in_db.origin, data_product_id)
            self.assertEquals(notific_in_db.temporal_bounds.end_datetime, '')
            self.assertTrue(notific_in_db.origin_type == 'type_1' or  notific_in_db.origin_type =='type_2')
            self.assertEquals(notific_in_db.event_type, 'ResourceLifecycleEvent')


        #--------------------------------------------------------------------------------------
        # Use UNS to get the all subscriptions --- including retired
        #--------------------------------------------------------------------------------------
        res_notifs = self.unsc.get_subscriptions(resource_id=data_product_id, include_nonactive=True)

        for notific in res_notifs:
            log.debug("notif.origin_type:: %s", notific.origin_type)

            notific_in_db = self.rrc.read(notific._id)
            self.assertTrue(notific_in_db)

            self.assertEquals(notific.origin, data_product_id)
            self.assertTrue(notific.origin_type in ['type_1', 'type_2', 'type_3', 'type_4'])
            self.assertTrue(notific.event_type in ['ResourceLifecycleEvent', 'DetectionEvent'])

            self.assertEquals(notific_in_db.origin, data_product_id)
            self.assertTrue(notific_in_db.origin_type in ['type_1', 'type_2', 'type_3', 'type_4'])
            self.assertTrue(notific_in_db.event_type in ['ResourceLifecycleEvent', 'DetectionEvent'])


        self.assertEquals(len(res_notifs), 4)

    @attr('LOCOINT')
    @unittest.skipIf(not use_es, 'No ElasticSearch')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_get_subscriptions_for_user(self):
        # Test that the get_subscriptions works correctly

        #--------------------------------------------------------------------------------------
        # Create 2 users
        #--------------------------------------------------------------------------------------

        user_ids = []
        for i in xrange(2):
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

        # ACTIVE

        # user 1
        notification_active_1 = NotificationRequest(   name = "notification_1",
            origin=data_product_id,
            origin_type="active_1",
            event_type='ResourceLifecycleEvent')

        # user 2
        notification_active_2 = NotificationRequest(   name = "notification_2",
            origin=data_product_id,
            origin_type="active_2",
            event_type='ResourceLifecycleEvent')

        # wrong origin
        notification_active_3 = NotificationRequest(   name = "notification_2",
            origin='wrong_origin',
            origin_type="active_3",
            event_type='ResourceLifecycleEvent')

        # PAST

        # user 1 - past
        notification_past_1 = NotificationRequest(   name = "notification_3_to_be_retired",
            origin=data_product_id,
            origin_type="past_1",
            event_type='DetectionEvent')

        # user 2 - past
        notification_past_2 = NotificationRequest(   name = "notification_4_to_be_retired",
            origin=data_product_id,
            origin_type="past_2",
            event_type='DetectionEvent')

        # wrong origin - past
        notification_past_3 = NotificationRequest(   name = "notification_4_to_be_retired",
            origin='wrong_origin_2',
            origin_type="past_3",
            event_type='DetectionEvent')

        #--------------------------------------------------------------------------------------
        # Create notifications using UNS.
        #--------------------------------------------------------------------------------------
        active_notification_ids = set()
        past_notification_ids = set()

        user_id_1 = user_ids[0]
        user_id_2 = user_ids[1]

        #--------------------------------------------------------------------------------------
        # Create notifications that will stay active for the following users
        #--------------------------------------------------------------------------------------

        # user 1
        notification_id_active_1 =  self.unsc.create_notification(notification=notification_active_1, user_id=user_id_1)
        # and the  notification below for the wrong origin
        notification_id_active_31 =  self.unsc.create_notification(notification=notification_active_3, user_id=user_id_1)

        #### Therefore, only one active notification for user_1 has been created so far

        #user 2
        notification_id_active_2 =  self.unsc.create_notification(notification=notification_active_2, user_id=user_id_2)
        # below we create notification for a different resource id
        notification_id_active_32 =  self.unsc.create_notification(notification=notification_active_3, user_id=user_id_2)

        #### Therefore, only one active notification for user_2 created so far

        # Store the ids for the active notifications in a set
        active_notification_ids.add(notification_id_active_1)
        active_notification_ids.add(notification_id_active_2)
        active_notification_ids.add(notification_id_active_31)
        active_notification_ids.add(notification_id_active_32)

        #--------------------------------------------------------------------------------------
        # Create notifications that will be RETIRED for the following users
        #--------------------------------------------------------------------------------------

        # user 1
        notification_id_past_1 =  self.unsc.create_notification(notification=notification_past_1, user_id=user_id_1)
        # the one below for a different resource id
        notification_id_past_31 =  self.unsc.create_notification(notification=notification_past_3, user_id=user_id_1)

        # user 2
        notification_id_past_2 =  self.unsc.create_notification(notification=notification_past_2, user_id=user_id_2)
        # the one below for a different resource id
        notification_id_past_32 =  self.unsc.create_notification(notification=notification_past_3, user_id=user_id_2)

        # Store the ids for the retired-to-be notifications in a set
        past_notification_ids.add(notification_id_past_1)
        past_notification_ids.add(notification_id_past_2)
        past_notification_ids.add(notification_id_past_31)
        past_notification_ids.add(notification_id_past_32)

        log.debug("Number of active notification ids: %s" % len(active_notification_ids)) # should be 3
        log.debug("Number of past notification ids: %s" % len(past_notification_ids))     # should be 3

        self.assertEquals(len(active_notification_ids), 3)
        self.assertEquals(len(past_notification_ids), 3)

        # Retire the retired-to-be notifications
        for notific_id in past_notification_ids:
            self.unsc.delete_notification(notification_id=notific_id)

        # now we should be left wih 1 active and 1 past notification FOR THE RELEVANT RESOURCE ID AS ORIGIN for each user

        #--------------------------------------------------------------------------------------
        # Use UNS to get the subscriptions
        #--------------------------------------------------------------------------------------

        n_for_user_1 = self.unsc.get_subscriptions(resource_id=data_product_id, user_id = user_id_1, include_nonactive=False)
        n_for_user_2 = self.unsc.get_subscriptions(resource_id=data_product_id, user_id = user_id_2, include_nonactive=False)

        self.assertEquals(len(n_for_user_1), 1)
        self.assertEquals(len(n_for_user_2), 1)

        for notif in n_for_user_1:
            notific_in_db = self.rrc.read(notif._id)
            self.assertTrue(notific_in_db)

            self.assertEquals(notif.origin, data_product_id)
            self.assertEquals(notif.temporal_bounds.end_datetime, '')
            self.assertEquals(notif.origin_type, 'active_1')
            self.assertEquals(notif.event_type, 'ResourceLifecycleEvent')

            self.assertEquals(notific_in_db.origin, data_product_id)
            self.assertEquals(notific_in_db.temporal_bounds.end_datetime, '')
            self.assertEquals(notific_in_db.origin_type, 'active_1')
            self.assertEquals(notific_in_db.event_type, 'ResourceLifecycleEvent')

        for notif in n_for_user_2:
            notific_in_db = self.rrc.read(notif._id)
            self.assertTrue(notific_in_db)

            self.assertEquals(notif.origin, data_product_id)
            self.assertEquals(notif.temporal_bounds.end_datetime, '')
            self.assertEquals(notif.origin_type, 'active_2')
            self.assertEquals(notif.event_type, 'ResourceLifecycleEvent')

            self.assertEquals(notific_in_db.origin, data_product_id)
            self.assertEquals(notific_in_db.temporal_bounds.end_datetime, '')
            self.assertEquals(notific_in_db.origin_type, 'active_2')
            self.assertEquals(notific_in_db.event_type, 'ResourceLifecycleEvent')


        #--------------------------------------------------------------------------------------
        # Use UNS to get the all subscriptions --- including retired
        #--------------------------------------------------------------------------------------
        notifs_for_user_1 = self.unsc.get_subscriptions(resource_id=data_product_id, user_id = user_id_1, include_nonactive=True)
        notifs_for_user_2 = self.unsc.get_subscriptions(resource_id=data_product_id, user_id = user_id_2, include_nonactive=True)

        log.debug("number of returned notif object: %s", len(notifs_for_user_1))
        self.assertEquals(len(notifs_for_user_1), 2)

        log.debug("number of returned notif object for user 2: %s", len(notifs_for_user_2))
        self.assertEquals(len(notifs_for_user_2), 2)

        for notif in notifs_for_user_1:
            notific_in_db = self.rrc.read(notif._id)
            self.assertTrue(notific_in_db)

            self.assertEquals(notif.origin, data_product_id)
            self.assertTrue(notif.origin_type == 'active_1' or notif.origin_type == 'past_1')
            self.assertTrue(notif.event_type== 'ResourceLifecycleEvent' or notif.event_type=='DetectionEvent')

            self.assertEquals(notific_in_db.origin, data_product_id)
            self.assertTrue(notific_in_db.origin_type == 'active_1' or notific_in_db.origin_type == 'past_1')
            self.assertTrue(notific_in_db.event_type== 'ResourceLifecycleEvent' or notific_in_db.event_type=='DetectionEvent')



        for notif in notifs_for_user_2:
            self.assertEquals(notif.origin, data_product_id)

            notific_in_db = self.rrc.read(notif._id)
            self.assertTrue(notific_in_db)

            self.assertTrue(notif.origin_type == 'active_2' or notif.origin_type == 'past_2')
            self.assertTrue(notif.event_type== 'ResourceLifecycleEvent' or notif.event_type=='DetectionEvent')

            self.assertTrue(notific_in_db.origin_type == 'active_2' or notific_in_db.origin_type == 'past_2')
            self.assertTrue(notific_in_db.event_type== 'ResourceLifecycleEvent' or notific_in_db.event_type=='DetectionEvent')

