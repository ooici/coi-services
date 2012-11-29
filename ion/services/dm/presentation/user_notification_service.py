#!/usr/bin/env python
"""
@author Bill Bollenbacher
@author Swarbhanu Chatterjee
@author David Stuebe
@file ion/services/dm/presentation/user_notification_service.py
@description Implementation of the UserNotificationService
"""

from pyon.core.exception import BadRequest, IonException
from pyon.public import RT, PRED, get_sys_name, Container, CFG, OT, IonObject
from pyon.util.async import spawn
from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.event.event import EventPublisher, EventSubscriber
from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ComputedValueAvailability

import string
import time
from email.mime.text import MIMEText
import elasticpy as ep
from datetime import datetime

from ion.services.dm.presentation.sms_providers import sms_providers
from interface.objects import ProcessDefinition, UserInfo, TemporalBounds, NotificationRequest
from interface.services.dm.iuser_notification_service import BaseUserNotificationService
from ion.services.dm.utility.uns_utility_methods import send_email, setting_up_smtp_client
from ion.services.dm.utility.uns_utility_methods import calculate_reverse_user_info


"""
For every user that has existing notification requests (who has called
create_notification()) the UNS will contain a local EventProcessor
instance that contains the user's notification information (email address)
and all of the user's notifications (along with their event subscribers).
The EventProcessors are maintained local to the UNS in a dictionary
indexed by the user's resourceID.  When a notification is created the user's
EventProcessor will be created if it doesn't already exist , and it will
be deleted when the user deletes their last notification.

The user's EventProcessor will encapsulate a list of notification objects
that the user has requested, along with user information needed for send notifications
(email address for LCA). It will also encapsulate a subscriber callback method
that is passed to all event subscribers for each notification the user has created.

Each notification object will encapsulate the notification information and a
list of event subscribers (only one for LCA) that listen for the events in the notification.
"""


class EmailEventProcessor(object):
    """
    A class that helps to get user subscribed to notifications
    """

    def __init__(self):
        # the resource registry
        self.rr = ResourceRegistryServiceClient()

    def add_notification_for_user(self, notification_request, user_id):
        """
        Add a notification to the user's list of subscribed notifications
        @param notification_request NotificationRequest
        @param user_id str
        """

        user = self.rr.read(user_id)

        # Add the notification into the user info object
        user = self.put_notification_in_user_object(user, notification_request)

        # return the updated user object
        return user

    def put_notification_in_user_object(self, user, notification_request):
        """
        Add the notification into the user info object.

        @param user UserInfo
        @param notification_request NotificationRequest

        @retval user UserInfo
        """

        user_variables_has_notifications = False

        for item in user.variables:
            if item.has_key('name') and item['name']=='notifications':
                item['value'].append(notification_request)
                user_variables_has_notifications = True

        if not user_variables_has_notifications:
            user.variables.append({'name' : 'notifications', 'value' : [notification_request]})

        self.rr.update(user)

        return user

#----------------------------------------------------------------------------------------------------------------
# Keep this note for the time when we need to also include sms delivery via email to sms providers
#        provider_email = sms_providers[provider] # self.notification.delivery_config.delivery['provider']
#        self.msg_recipient = notification_request.delivery_config.delivery['phone_number'] + provider_email
#----------------------------------------------------------------------------------------------------------------

class UserNotificationService(BaseUserNotificationService):
    """
    A service that provides users with an API for CRUD methods for notifications.
    """

    def __init__(self, *args, **kwargs):
        self._subscribers = []
        self._schedule_ids = []
        BaseUserNotificationService.__init__(self, *args, **kwargs)

    def on_start(self):

        #---------------------------------------------------------------------------------------------------
        # Get the event Repository
        #---------------------------------------------------------------------------------------------------

        self.event_repo = self.container.instance.event_repository

        self.smtp_client = setting_up_smtp_client()

        self.ION_NOTIFICATION_EMAIL_ADDRESS = 'ION_notifications-do-not-reply@oceanobservatories.org'

        #---------------------------------------------------------------------------------------------------
        # Create an event processor
        #---------------------------------------------------------------------------------------------------

        self.event_processor = EmailEventProcessor()

        #---------------------------------------------------------------------------------------------------
        # load event originators, types, and table
        #---------------------------------------------------------------------------------------------------

        self.notifications = {}

        #---------------------------------------------------------------------------------------------------
        # Dictionaries that maintain information about users and their subscribed notifications
        # The reverse_user_info is calculated from the user_info dictionary
        #---------------------------------------------------------------------------------------------------
        self.user_info = {}
        self.reverse_user_info = {}

        #---------------------------------------------------------------------------------------------------
        # Get the clients
        #---------------------------------------------------------------------------------------------------

        self.discovery = DiscoveryServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()
        self.event_publisher = EventPublisher()

        self.start_time = UserNotificationService.makeEpochTime(self.__now())

    def on_quit(self):
        """
        Handles stop/terminate.

        Cleans up subscribers spawned here, terminates any scheduled tasks to the scheduler.
        """
        for sub in self._subscribers:
            sub.stop()

        for sid in self._schedule_ids:
            try:
                self.clients.scheduler.cancel_timer(sid)
            except IonException as ex:
                log.info("Ignoring exception while cancelling schedule id (%s): %s: %s", sid, ex.__class__.__name__, ex)

        super(UserNotificationService, self).on_quit()

    def __now(self):
        """
        This method defines what the UNS uses as its "current" time
        """
        return datetime.utcnow()

    def set_process_batch_key(self, process_batch_key = ''):
        """
        This method allows an operator to set the process_batch_key, a string.
        Once this method is used by the operator, the UNS will start listening for timer events
        published by the scheduler with origin = process_batch_key.

        @param process_batch_key str
        """

        def process(event_msg, headers):
            assert event_msg.origin == process_batch_key

            self.end_time = UserNotificationService.makeEpochTime(self.__now())

            # run the process_batch() method
            self.process_batch(start_time=self.start_time, end_time=self.end_time)
            self.start_time = self.end_time

        # the subscriber for the batch processing
        """
        To trigger the batch notification, have the scheduler create a timer with event_origin = process_batch_key
        """
        self.batch_processing_subscriber = EventSubscriber(
            event_type="ResourceEvent",
            origin=process_batch_key,
            callback=process
        )
        self.batch_processing_subscriber.start()
        self._subscribers.append(self.batch_processing_subscriber)

    def create_notification(self, notification=None, user_id=''):
        """
        Persists the provided NotificationRequest object for the specified Origin id.
        Associate the Notification resource with the user_id string.
        returned id is the internal id by which NotificationRequest will be identified
        in the data store.

        @param notification        NotificationRequest
        @param user_id             str
        @retval notification_id    str
        @throws BadRequest    if object passed has _id or _rev attribute

        """

        if not user_id:
            raise BadRequest("User id not provided.")

        #---------------------------------------------------------------------------------------------------
        # Persist Notification object as a resource if it has already not been persisted
        #---------------------------------------------------------------------------------------------------

        # if the notification has already been registered, simply use the old id

        id = self._notification_in_notifications(notification, self.notifications)

        if id:
            log.debug("Notification object has already been created in resource registry before. No new id to be generated.")
            notification_id = id
        else:

            # since the notification has not been registered yet, register it and get the id
            notification.temporal_bounds = TemporalBounds()
            notification.temporal_bounds.start_datetime = self.makeEpochTime(self.__now())
            notification.temporal_bounds.end_datetime = ''

            notification_id, _ = self.clients.resource_registry.create(notification)
            self.notifications[notification_id] = notification

            # Link the user and the notification with a hasNotification association
            self.clients.resource_registry.create_association(user_id, PRED.hasNotification, notification_id)

        #-------------------------------------------------------------------------------------------------------------------
        # read the registered notification request object because this has an _id and is more useful
        #-------------------------------------------------------------------------------------------------------------------

        notification = self.clients.resource_registry.read(notification_id)

        #-----------------------------------------------------------------------------------------------------------
        # Create an event processor for user. This sets up callbacks etc.
        # As a side effect this updates the UserInfo object and also the user info and reverse user info dictionaries.
        #-----------------------------------------------------------------------------------------------------------

        user = self.event_processor.add_notification_for_user(notification_request=notification, user_id=user_id)

        # Update the user info object with the notification
        self.update_user_info_dictionary(user_id=user_id, new_notification=notification, old_notification=None)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        log.debug("(create notification) Publishing ReloadUserInfoEvent for notification_id: %s" % notification_id)

        self.event_publisher.publish_event( event_type= "ReloadUserInfoEvent",
            origin="UserNotificationService",
            description= "A notification has been created.",
            notification_id = notification_id)

        return notification_id

    def update_notification(self, notification=None, user_id = ''):
        """Updates the provided NotificationRequest object.  Throws NotFound exception if
        an existing version of NotificationRequest is not found.  Throws Conflict if
        the provided NotificationRequest object is not based on the latest persisted
        version of the object.

        @param notification     NotificationRequest
        @throws BadRequest      if object does not have _id or _rev attribute
        @throws NotFound        object with specified id does not exist
        @throws Conflict        object not based on latest persisted object version
        """

        #-------------------------------------------------------------------------------------------------------------------
        # Get the old notification
        #-------------------------------------------------------------------------------------------------------------------

        old_notification = self.clients.resource_registry.read(notification._id)

        #-------------------------------------------------------------------------------------------------------------------
        # Update the notification in the notifications dict
        #-------------------------------------------------------------------------------------------------------------------


        self._update_notification_in_notifications_dict(new_notification=notification,
                                                        old_notification=old_notification,
                                                        notifications=self.notifications)
        #-------------------------------------------------------------------------------------------------------------------
        # Update the notification in the registry
        #-------------------------------------------------------------------------------------------------------------------

        self.clients.resource_registry.update(notification)

        #-------------------------------------------------------------------------------------------------------------------
        # reading up the notification object to make sure we have the newly registered notification request object
        #-------------------------------------------------------------------------------------------------------------------

        notification_id = notification._id
        notification = self.clients.resource_registry.read(notification_id)

        #------------------------------------------------------------------------------------
        # Update the UserInfo object
        #------------------------------------------------------------------------------------

        user = self.update_user_info_object(user_id, notification, old_notification)

        #------------------------------------------------------------------------------------
        # Update the user_info dictionary maintained by UNS
        #------------------------------------------------------------------------------------

        self.update_user_info_dictionary(user_id, notification, old_notification)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by notification workers so that they can update their user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        log.info("(update notification) Publishing ReloadUserInfoEvent for updated notification")

        self.event_publisher.publish_event( event_type= "ReloadUserInfoEvent",
            origin="UserNotificationService",
            description= "A notification has been updated."
        )

    def read_notification(self, notification_id=''):
        """Returns the NotificationRequest object for the specified notification id.
        Throws exception if id does not match any persisted NotificationRequest
        objects.

        @param notification_id    str
        @retval notification    NotificationRequest
        @throws NotFound    object with specified id does not exist
        """
        notification = self.clients.resource_registry.read(notification_id)

        return notification

    def delete_notification(self, notification_id=''):
        """For now, permanently deletes NotificationRequest object with the specified
        id. Throws exception if id does not match any persisted NotificationRequest.

        @param notification_id    str
        @throws NotFound    object with specified id does not exist
        """

        #-------------------------------------------------------------------------------------------------------------------
        # Stop the event subscriber for the notification
        #-------------------------------------------------------------------------------------------------------------------
        notification_request = self.clients.resource_registry.read(notification_id)
        old_notification = notification_request

        #-------------------------------------------------------------------------------------------------------------------
        # Update the resource registry
        #-------------------------------------------------------------------------------------------------------------------

        notification_request.temporal_bounds.end_datetime = self.makeEpochTime(self.__now())

        self.clients.resource_registry.update(notification_request)

        #-------------------------------------------------------------------------------------------------------------------
        # Update the user info dictionaries
        #-------------------------------------------------------------------------------------------------------------------

        for user_id in self.user_info.iterkeys():
            self.update_user_info_dictionary(user_id, notification_request, old_notification)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        log.info("(delete notification) Publishing ReloadUserInfoEvent for notification_id: %s" % notification_id)

        self.event_publisher.publish_event( event_type= "ReloadUserInfoEvent",
            origin="UserNotificationService",
            description= "A notification has been deleted.",
            notification_id = notification_id)

    def delete_notification_from_user_info(self, notification_id):
        """
        Helper method to delete the notification from the user_info dictionary

        @param notification_id str
        """

        user_ids, assocs = self.clients.resource_registry.find_subjects(object=notification_id, predicate=PRED.hasNotification, id_only=True)

        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc)

        for user_id in user_ids:

            value = self.user_info[user_id]

            for notif in value['notifications']:
                if notification_id == notif._id:
                    # remove the notification
                    value['notifications'].remove(notif)

        self.reverse_user_info = calculate_reverse_user_info(self.user_info)

    def find_events(self, origin='', type='', min_datetime=0, max_datetime=0, limit= -1, descending=False):
        """
        This method leverages couchdb view and simple filters. It does not use elastic search.

        Returns a list of events that match the specified search criteria. Will throw a not NotFound exception
        if no events exist for the given parameters.

        @param origin         str
        @param event_type     str
        @param min_datetime   int  seconds
        @param max_datetime   int  seconds
        @param limit          int         (integer limiting the number of results (0 means unlimited))
        @param descending     boolean     (if True, reverse order (of production time) is applied, e.g. most recent first)
        @retval event_list    []
        @throws NotFound    object with specified parameters does not exist
        @throws NotFound    object with specified parameters does not exist
        """
        datastore = self.container.datastore_manager.get_datastore('events')


        # The reason for the if-else below is that couchdb query_view does not support passing in Null or -1 for limit
        # If the opreator does not want to set a limit for the search results in find_events, and does not therefore
        # provide a limit, one has to just omit it from the opts dictionary and pass that into the query_view() method.
        # Passing a null or negative for the limit to query view through opts results in a ServerError so we cannot do that.
        if limit > -1:
            opts = dict(
                startkey = [origin, type or 0, min_datetime or 0],
                endkey   = [origin, type or {}, max_datetime or {}],
                descending = descending,
                limit = limit,
                include_docs = True
            )

        else:
            opts = dict(
                startkey = [origin, type or 0, min_datetime or 0],
                endkey   = [origin, type or {}, max_datetime or {}],
                descending = descending,
                include_docs = True
            )
        if descending:
            t = opts['startkey']
            opts['startkey'] = opts['endkey']
            opts['endkey'] = t

        results = datastore.query_view('event/by_origintype',opts=opts)

        events = []
        for res in results:
            event_obj = res['doc']
            events.append(event_obj)

        log.debug("(find_events) UNS found the following relevant events: %s" % events)

        if -1 < limit < len(events):
            list = []
            for i in xrange(limit):
                list.append(events[i])
            return list

        return events


    #todo Uses Elastic Search. Later extend this to a larger search criteria
    def find_events_extended(self, origin='', type='', min_time= 0, max_time=0, limit=-1, descending=False):
        """Uses Elastic Search. Returns a list of events that match the specified search criteria. Will throw a not NotFound exception
        if no events exist for the given parameters.

        @param origin         str
        @param type           str
        @param min_time   int seconds
        @param max_time   int seconds
        @param limit          int         (integer limiting the number of results (0 means unlimited))
        @param descending     boolean     (if True, reverse order (of production time) is applied, e.g. most recent first)
        @retval event_list    []
        @throws NotFound    object with specified parameters does not exist
        @throws NotFound    object with specified parameters does not exist
        """

        if min_time and max_time:
            search_time = "SEARCH 'ts_created' VALUES FROM %s TO %s FROM 'events_index'" % (min_time, max_time)
        else:
            search_time = 'search "ts_created" is "*" from "events_index"'

        if origin:
            search_origin = 'search "origin" is "%s" from "events_index"' % origin
        else:
            search_origin = 'search "origin" is "*" from "events_index"'

        if type:
            search_type = 'search "type_" is "%s" from "events_index"' % type
        else:
            search_type = 'search "type_" is "*" from "events_index"'

        search_string = search_time + ' and ' + search_origin + ' and ' + search_type

        # get the list of ids corresponding to the events
        ret_vals = self.discovery.parse(search_string)
        log.debug("(find_events_extended) Discovery search returned the following event ids: %s" % ret_vals)

        events = []
        for event_id in ret_vals:
            datastore = self.container.datastore_manager.get_datastore('events')
            event_obj = datastore.read(event_id)
            events.append(event_obj)

        log.debug("(find_events_extended) UNS found the following relevant events: %s" % events)

        if limit > -1:
            list = []
            for i in xrange(limit):
                list.append(events[i])
            return list

        #todo implement time ordering: ascending or descending

        return events

    def publish_event(self, event=None):
        """
        Publish a general event at a certain time using the UNS

        @param event Event
        """

        self.event_publisher._publish_event( event_msg = event,
            origin=event.origin,
            event_type = event.type_)
        log.info("The publish_event() method of UNS was used to publish an event.")

    def get_recent_events(self, resource_id='', limit = 100):
        """
        Get recent events

        @param resource_id str
        @param limit int

        @retval events list of Event objects
        """

        now = self.makeEpochTime(datetime.utcnow())
        events = self.find_events(origin=resource_id,limit=limit, max_datetime=now, descending=True)

        ret = IonObject(OT.ComputedListValue)
        if events:
            ret.value = events
            ret.status = ComputedValueAvailability.PROVIDED
        else:
            ret.status = ComputedValueAvailability.NOTAVAILABLE

        return ret

    def get_user_notifications(self, user_id=''):
        """
        Get the notification request objects that are subscribed to by the user

        @param user_id str

        @retval notifications list of NotificationRequest objects
        """

        if self.user_info.has_key(user_id):
            notifications = self.user_info[user_id]['notifications']
            ret = IonObject(OT.ComputedListValue)

            if notifications:
                ret.value = notifications
                ret.status = ComputedValueAvailability.PROVIDED
            else:
                ret.status = ComputedValueAvailability.NOTAVAILABLE
            return ret
        else:
            return None

    def create_worker(self, number_of_workers=1):
        """
        Creates notification workers

        @param number_of_workers int
        @retval pids list

        """

        pids = []

        for n in xrange(number_of_workers):

            process_definition = ProcessDefinition( name='notification_worker_%s' % n)

            process_definition.executable = {
                'module': 'ion.processes.data.transforms.notification_worker',
                'class':'NotificationWorker'
            }
            process_definition_id = self.process_dispatcher.create_process_definition(process_definition=process_definition)

            # ------------------------------------------------------------------------------------
            # Process Spawning
            # ------------------------------------------------------------------------------------

            pid2 = self.process_dispatcher.create_process(process_definition_id)

            #@todo put in a configuration
            configuration = {}
            configuration['process'] = dict({
                'name': 'notification_worker_%s' % n,
                'type':'simple'
            })

            pid  = self.process_dispatcher.schedule_process(
                process_definition_id,
                configuration = configuration,
                process_id=pid2
            )

            pids.append(pid)

        return pids

    @staticmethod
    def makeEpochTime(date_time):
        """
        provides the seconds since epoch give a python datetime object.

        @param date_time Python datetime object
        @retval seconds_since_epoch int
        """
        date_time = date_time.isoformat().split('.')[0].replace('T',' ')
        #'2009-07-04 18:30:47'
        pattern = '%Y-%m-%d %H:%M:%S'
        seconds_since_epoch = int(time.mktime(time.strptime(date_time, pattern)))

        return seconds_since_epoch


    def process_batch(self, start_time = 0, end_time = 0):
        """
        This method is launched when an process_batch event is received. The user info dictionary maintained
        by the User Notification Service is used to query the event repository for all events for a particular
        user that have occurred in a provided time interval, and then an email is sent to the user containing
        the digest of all the events.

        @param start_time int
        @param end_time int
        """

        if end_time <= start_time:
            return

        for user_id, value in self.user_info.iteritems():

            notifications = value['notifications']

            events_for_message = []

            search_time = "SEARCH 'ts_created' VALUES FROM %s TO %s FROM 'events_index'" % (start_time, end_time)

            for notification in notifications:

                # If the notification request has expired, then do not use it in the search
                if notification.temporal_bounds.end_datetime:
                    continue

                if notification.origin:
                    search_origin = 'search "origin" is "%s" from "events_index"' % notification.origin
                else:
                    search_origin = 'search "origin" is "*" from "events_index"'

                if notification.origin_type:
                    search_origin_type= 'search "origin_type" is "%s" from "events_index"' % notification.origin_type
                else:
                    search_origin_type= 'search "origin_type" is "*" from "events_index"'

                if notification.event_type:
                    search_event_type = 'search "type_" is "%s" from "events_index"' % notification.event_type
                else:
                    search_event_type = 'search "type_" is "*" from "events_index"'

                search_string = search_time + ' and ' + search_origin + ' and ' + search_origin_type + ' and ' + search_event_type

                # get the list of ids corresponding to the events
                ret_vals = self.discovery.parse(search_string)

                for event_id in ret_vals:
                    datastore = self.container.datastore_manager.get_datastore('events')
                    event_obj = datastore.read(event_id)
                    events_for_message.append(event_obj)

            log.debug("Found following events of interest to user, %s: %s" % (user_id, events_for_message))

            # send a notification email to each user using a _send_email() method
            if events_for_message:
                self.format_and_send_email(events_for_message, user_id)

    def format_and_send_email(self, events_for_message, user_id):
        """
        Format the message for a particular user containing information about the events he is to be notified about

        @param events_for_message list
        @param user_id str
        """

        message = str(events_for_message)
        log.debug("The user, %s, will get the following events in his batch notification email: %s" % (user_id, message))

        msg_body = ''
        count = 1
        for event in events_for_message:
            # build the email from the event content
            msg_body += string.join(("\r\n",
                                     "Event %s: %s" %  (count, event),
                                     "",
                                     "Originator: %s" %  event.origin,
                                     "",
                                     "Description: %s" % event.description ,
                                     "",
                                     "Event time stamp: %s" %  event.ts_created,
                                     "\r\n",
                                     "------------------------"
                                     "\r\n"))
            count += 1

        msg_body += "You received this notification from ION because you asked to be " +\
                    "notified about this event from this source. " +\
                    "To modify or remove notifications about this event, " +\
                    "please access My Notifications Settings in the ION Web UI. " +\
                    "Do not reply to this email.  This email address is not monitored " +\
                    "and the emails will not be read. \r\n "


        log.debug("The email has the following message body: %s" % msg_body)

        msg_subject = "(SysName: " + get_sys_name() + ") ION event "

        self.send_batch_email(  msg_body = msg_body,
            msg_subject = msg_subject,
            msg_recipient=self.user_info[user_id]['user_contact'].email,
            smtp_client=self.smtp_client )

    def send_batch_email(self, msg_body, msg_subject, msg_recipient, smtp_client):
        """
        Send the email

        @param msg_body str
        @param msg_subject str
        @param msg_recipient str
        @param smtp_client object
        """

        msg = MIMEText(msg_body)
        msg['Subject'] = msg_subject
        msg['From'] = self.ION_NOTIFICATION_EMAIL_ADDRESS
        msg['To'] = msg_recipient
        log.debug("EventProcessor.subscription_callback(): sending email to %s"\
                  %msg_recipient)

        smtp_sender = CFG.get_safe('server.smtp.sender')

        smtp_client.sendmail(smtp_sender, msg_recipient, msg.as_string())

    def update_user_info_object(self, user_id, new_notification, old_notification):
        """
        Update the UserInfo object. If the passed in parameter, od_notification, is None, it does not need to remove the old notification

        @param user_id str
        @param new_notification NotificationRequest
        @param old_notification NotificationRequest
        """

        #------------------------------------------------------------------------------------
        # read the user
        #------------------------------------------------------------------------------------

        user = self.clients.resource_registry.read(user_id)

        if not user:
            raise BadRequest("No user with the provided user_id: %s" % user_id)

        notifications = []
        for item in user.variables:
            if item['name'] == 'notifications':
                if old_notification and old_notification in item['value']:

                    notifications = item['value']
                    # remove the old notification
                    notifications.remove(old_notification)

                # put in the new notification
                notifications.append(new_notification)

                item['value'] = notifications

                break

        #------------------------------------------------------------------------------------
        # update the resource registry
        #------------------------------------------------------------------------------------

        self.clients.resource_registry.update(user)

        return user

    def update_user_info_dictionary(self, user_id, new_notification, old_notification):

        notifications = []
        user = self.clients.resource_registry.read(user_id)

        #------------------------------------------------------------------------------------
        # If there was a previous notification which is being updated, check the dictionaries and update there
        #------------------------------------------------------------------------------------
        if old_notification:
            # Remove the old notifications
            if old_notification in self.user_info[user_id]['notifications']:

                # remove from notifications list
                self.user_info[user_id]['notifications'].remove(old_notification)

        # find the already existing notifications for the user
        if self.user_info.has_key(user_id):
            notifications = self.user_info[user_id]['notifications']

        #------------------------------------------------------------------------------------
        # update the user info - contact information, notifications
        #------------------------------------------------------------------------------------
        notifications.append(new_notification)

        self.user_info[user_id] = {'user_contact' : user.contact, 'notifications' : notifications}

        self.reverse_user_info = calculate_reverse_user_info(self.user_info)

    def get_subscriptions(self, resource_id='', include_nonactive=False):
        """
        This method is used to get the subscriptions to a data product. The method will return a list of NotificationRequest
        objects for whom the origin is set to this data product. This way all the users who were interested in listening to
        events with origin equal to this data product, will be known and all their subscriptions will be known.

        @param resource_id
        @param include_nonactive
        @return notification_requests []

        """

        search_origin = 'search "origin" is "%s" from "resources_index"' % resource_id
        ret_vals = self.discovery.parse(search_origin)
        log.debug("Returned results: %s" % ret_vals)

        notifications_all = set()
        notifications_active = set()

        for item in ret_vals:

            if item['_type'] == 'NotificationRequest':
                notif = self.clients.resource_registry.read(item['_id'])

                if include_nonactive:
                    # Add active or retired notification
                    notifications_all.add(notif)

                elif notif.temporal_bounds.end_datetime == '':
                    # Add the active notification
                    notifications_active.add(notif)

        if include_nonactive:
            return list(notifications_all)
        else:
            return list(notifications_active)


    def _notification_in_notifications(self, notification = None, notifications = None):

        for id, notif in notifications.iteritems():
            if notif.name == notification.name and \
            notif.origin == notification.origin and \
            notif.origin_type == notification.origin_type and \
            notif.event_type == notification.event_type:
                return id
        return None

    def _update_notification_in_notifications_dict(self, new_notification = None, old_notification = None, notifications = None ):

        for id, notif in notifications.iteritems():
            if notif.name == old_notification.name and\
               notif.origin == old_notification.origin and\
               notif.origin_type == old_notification.origin_type and\
               notif.event_type == old_notification.event_type:
                notifications.pop(id)
                notifications[id] = new_notification
                break

