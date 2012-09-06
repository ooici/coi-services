#!/usr/bin/env python
'''
@author Bill Bollenbacher
@author Swarbhanu Chatterjee
@author David Stuebe
@file ion/services/dm/presentation/user_notification_service.py
@description Implementation of the UserNotificationService
'''

from pyon.core.exception import BadRequest, IonException
from pyon.public import RT, PRED, get_sys_name, Container, CFG
from pyon.util.async import spawn
from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.datastore.datastore import DatastoreManager
from pyon.event.event import EventPublisher, EventSubscriber
from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.cei.ischeduler_service import SchedulerServiceClient

import string
import time
import gevent
from gevent.timeout import Timeout
from email.mime.text import MIMEText
from gevent import Greenlet
import elasticpy as ep
from datetime import datetime

from ion.services.dm.presentation.sms_providers import sms_providers
from interface.objects import ProcessDefinition

from interface.services.dm.iuser_notification_service import BaseUserNotificationService
from ion.services.dm.utility.uns_utility_methods import send_email, setting_up_smtp_client
from ion.services.dm.utility.uns_utility_methods import calculate_reverse_user_info
from ion.services.cei.scheduler_service import SchedulerService


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

class NotificationSubscription(object):
    """
    Ties a notification's info to it's event subscriber
    """

    def  __init__(self, notification_request=None, callback=None):
        self._res_obj = notification_request  # The Notification Request Resource Object
        self.subscriber = EventSubscriber(  origin=notification_request.origin,
            origin_type = notification_request.origin_type,
            event_type=notification_request.event_type,
            sub_type=notification_request.event_subtype,
            callback=callback)
        self.notification_subscription_id = None

    def set_notification_id(self, id_=None):
        """
        Set the notification id of the notification object
        @param notification id
        """
        self.notification_subscription_id = id_

    def activate(self):
        """
        Start subscribing
        """
        self.subscriber.start()

    def deactivate(self):
        """
        Stop subscribing
        """
        self.subscriber.stop()

class EventProcessor(object):
    """
    The Event Processor is the object that knows all about the user's subscriptions. There will be one event
    processor in the system.
    """

    def __init__(self):

        #---------------------------------------------------------------------------------------------------
        # Dictionaries that maintain information about users and their subscribed notifications
        # The user_info dictionary is loaded from the User Info Base (stored in couchdb)
        # The reverse_user_info is calculated from the user_info dictionary
        #---------------------------------------------------------------------------------------------------

        # user_info = {'user_name' : [list of notifications]}
        self.user_info = {}
        self.reverse_user_info = {}

        # the resource registry
        self.rr = ResourceRegistryServiceClient()

    def stop_notification_subscriber(self, notification_request):
        """
        Stops the subscriber of a notification
        """

        for val in self.user_info.itervalues():
            if notification_request in val['notifications']:
                notification_subscription = val['notification_subscriptions'][notification_request._id]
                notification_subscription.deactivate()
                # once the subscription is deactivated, exit, so as not to try deactivating an already deactivated subscription
                return

    def __str__(self):
        return str(self.__dict__)



class EmailEventProcessor(EventProcessor):
    '''
    Contains email related info.
    '''

    def __init__(self, smtp_client):
        '''
        Contain information about the smtp_client being used for that user
        '''
        super(EmailEventProcessor, self).__init__()
        self.smtp_client = smtp_client

    def add_notification_for_user(self, notification_request, user_id):
        '''
        Add a notification to the user's list of subscribed notifications
        '''

        #---------------------------------------------------------------------------------------------------
        # Make a callback function that is right for the user
        #---------------------------------------------------------------------------------------------------

        def callback(message, headers):
            """
            This callback is given to all the event subscribers that this user wants notifications for.
            If this callback gets called the user in this processor should get an email
            """

            # find the email address of the user
            user = self.rr.read(user_id)
            msg_recipient = user.contact.email

            # send email to the user
            send_email(message, msg_recipient, self.smtp_client)

        user = self.rr.read(user_id)

        #---------------------------------------------------------------------------------------------------
        # Add the notification into the user info object
        #---------------------------------------------------------------------------------------------------

        user = self.put_notification_in_user_object(user, notification_request)

        #---------------------------------------------------------------------------------------------------
        # Add a notification to the list of subscribed notifications for the user
        #---------------------------------------------------------------------------------------------------

        notification_subscription = self._add_callback_to_notification(notification_request, callback)

        #---------------------------------------------------------------------------------------------------
        # Update the user_info dictionary and also calculate the reverse user info dictionary
        #---------------------------------------------------------------------------------------------------

        self.update_user_info_dictionary(user, notification_subscription)

        # return the updated user object
        return user

    def put_notification_in_user_object(self, user, notification_request):
        '''
        Add the notification into the user info object.
        '''

        user_variables_has_notifications = False

        for item in user.variables:
            if item.has_key('name') and item['name']=='notifications':
                item['value'].append(notification_request)
                user_variables_has_notifications = True

        if not user_variables_has_notifications:
            user.variables.append({'name' : 'notifications', 'value' : [notification_request]})

        self.rr.update(user)

        return user


    def update_user_info_dictionary(self, user, notification_subscription):
        '''
        Update the user info and reverse user info dictionaries.
        '''

        if self.user_info.has_key(user.name):
            # the user is already known by the EventProcessor
            self.user_info[user.name]['notifications'].append(notification_subscription._res_obj)
            self.user_info[user.name]['notification_subscriptions'][notification_subscription._res_obj._id] = notification_subscription
        else:
            # it is a new user
            self.user_info[user.name] = {   'user_contact' : user.contact,
                                            'notifications' : [notification_subscription._res_obj],
                                            'notification_subscriptions' : { notification_subscription._res_obj._id :
                                                                                 notification_subscription}}
        self.reverse_user_info = calculate_reverse_user_info(self.user_info)

    def _add_callback_to_notification(self, notification_request=None, callback = None):
        """
        Adds a notification that this user then subscribes to.

        @param notification_request
        @retval notification object
        """

        #---------------------------------------------------------------------------------------------------
        # create and save notification in notifications list
        #---------------------------------------------------------------------------------------------------

        notification_subscription = NotificationSubscription(notification_request, callback)

        #---------------------------------------------------------------------------------------------------
        # start the event subscriber listening
        #---------------------------------------------------------------------------------------------------

        notification_subscription.activate()

        log.debug("EventProcessor.add_notification(): added notification " + str(notification_request))

        return notification_subscription


    def stop_notification_subscriber(self, notification_request):

        super(EmailEventProcessor, self).stop_notification_subscriber(notification_request)

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

        self.event_processor = EmailEventProcessor(self.smtp_client)

        #---------------------------------------------------------------------------------------------------
        # load event originators, types, and table
        #---------------------------------------------------------------------------------------------------

        self.event_types = CFG.event.types
        self.event_table = {}

        #---------------------------------------------------------------------------------------------------
        # Get the clients
        #---------------------------------------------------------------------------------------------------

        self.discovery = DiscoveryServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()
        self.datastore_manager = DatastoreManager()

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

    def __now(self):
        '''
        This method defines what the UNS uses as its "current" time
        '''
        return datetime.utcnow()

    def set_process_batch_key(self, process_batch_key = ''):
        '''
        This method allows an operator to set the process_batch_key, a string.
        Once this method is used by the operator, the UNS will start listening for timer events
        published by the scheduler with origin = process_batch_key.
        '''

        log.warning("process_batch_key= %s" % process_batch_key)

        def process(event_msg, headers):
            assert event_msg.origin == process_batch_key

            self.end_time = UserNotificationService.makeEpochTime(self.__now())

            log.warning("start_time : %s" % self.start_time)
            log.warning("end_time: %s" % self.end_time)

            # run the process_batch() method
            self.process_batch(start_time=self.start_time, end_time=self.end_time)
            self.start_time = self.end_time

        # the subscriber for the batch processing
        '''
        To trigger the batch notification, have the scheduler create a timer with event_origin = process_batch_key
        '''
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

        # find all notifications in the system
        notifs, _ = self.clients.resource_registry.find_resources(restype = RT.NotificationRequest)

        # if the notification has already been registered, simply use the old id
        if notification in notifs:
            log.warning("Notification object has already been created in resource registry before for another user. No new id to be generated.")
            notification_id = notification._id
        else:
            # since the notification has not been registered yet, register it and get the id
            notification_id, _ = self.clients.resource_registry.create(notification)

        #-------------------------------------------------------------------------------------------------------------------
        # read the registered notification request object because this has an _id and is more useful
        #-------------------------------------------------------------------------------------------------------------------

        notification = self.clients.resource_registry.read(notification_id)

        #-----------------------------------------------------------------------------------------------------------
        # Create an event processor for user. This sets up callbacks etc.
        # As a side effect this updates the UserInfo object and also the user info and reverse user info dictionaries.
        #-----------------------------------------------------------------------------------------------------------

        user = self.event_processor.add_notification_for_user(notification_request=notification, user_id=user_id)

        #-------------------------------------------------------------------------------------------------------------------
        # Allow the indexes to be updated for ElasticSearch
        # We publish event only after this so that the reload of the user info works by the
        # notification workers work properly
        #-------------------------------------------------------------------------------------------------------------------

        # todo: This is to allow time for the indexes to be created before publishing ReloadUserInfoEvent for notification workers.
        # todo: When things are more refined, it will be nice to have an event generated when the
        # indexes are updated so that a subscriber here when it received that event will publish
        # the reload user info event.
        time.sleep(4)

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
        # Update the notification
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

        self.update_user_info_dictionary(user, notification, old_notification)

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

        self.event_processor.stop_notification_subscriber(notification_request=notification_request)

        #-------------------------------------------------------------------------------------------------------------------
        # delete the notification from the user_info and reverse_user_info dictionaries
        #-------------------------------------------------------------------------------------------------------------------

        self.delete_notification_from_user_info(notification_id)

        #-------------------------------------------------------------------------------------------------------------------
        # delete from the resource registry
        #-------------------------------------------------------------------------------------------------------------------

        self.clients.resource_registry.delete(notification_id)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        log.info("(delete notification) Publishing ReloadUserInfoEvent for notification_id: %s" % notification_id)

        self.event_publisher.publish_event( event_type= "ReloadUserInfoEvent",
            origin="UserNotificationService",
            description= "A notification has been deleted.",
            notification_id = notification_id)

    def delete_notification_from_user_info(self, notification_id):
        '''
        Helper method to delete the notification from the user_info dictionary
        '''

        for user_name, value in self.event_processor.user_info.iteritems():
            for notif in value['notifications']:
                if notification_id == notif._id:
                    # remove the notification
                    value['notifications'].remove(notif)
                    # remove the notification_subscription
                    self.event_processor.user_info[user_name]['notification_subscriptions'].pop(notification_id)

        self.event_processor.reverse_user_info = calculate_reverse_user_info(self.event_processor.user_info)

    def find_events(self, origin='', type='', min_datetime='', max_datetime='', limit=-1, descending=False):
        """Returns a list of events that match the specified search criteria. Will throw a not NotFound exception
        if no events exist for the given parameters.

        @param origin         str
        @param type           str
        @param min_datetime   str
        @param max_datetime   str
        @param limit          int         (integer limiting the number of results (0 means unlimited))
        @param descending     boolean     (if True, reverse order (of production time) is applied, e.g. most recent first)
        @retval event_list    []
        @throws NotFound    object with specified parameters does not exist
        @throws NotFound    object with specified parameters does not exist
        """

        if min_datetime and max_datetime:
            search_time = "SEARCH 'ts_created' VALUES FROM %s TO %s FROM 'events_index'" % (min_datetime, max_datetime)
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
        log.debug("(find_events) Discovery search returned the following event ids: %s" % ret_vals)

        events = []
        for event_id in ret_vals:
            datastore = self.datastore_manager.get_datastore('events')
            event_obj = datastore.read(event_id)
            events.append(event_obj)

        log.debug("(find_events) UNS found the following relevant events: %s" % events)

        if limit > -1:
            list = []
            for i in xrange(limit):
                list.append(events[i])
            return list

        #todo implement time ordering: ascending or descending

        return events

    def publish_event(self, event=None, interval_timer_params= None):
        '''
        Publish a general event at a certain time using the UNS

        @param event Event
        @param interval_timer_params dict Ex: {'interval':3, 'number_of_intervals':4}
        '''

        #--------------------------------------------------------------------------------
        # Set up a subscriber to get the nod from the scheduler to publish the event
        #--------------------------------------------------------------------------------
        def publish(message, headers):
            self.event_publisher._publish_event( event_msg = event,
                origin=event.origin,
                event_type = event.type_)
            log.info("UNS published an event in response to a nod from the Scheduler Service.")

        event_subscriber = EventSubscriber( event_type = "ResourceEvent", callback=publish)
        event_subscriber.start()
        self._subscribers.append(event_subscriber)      # for cleanup later

        sid = self.clients.scheduler.create_interval_timer(start_time= time.time(),
                                                           interval=interval_timer_params['interval'],
                                                           number_of_intervals=interval_timer_params['number_of_intervals'],
                                                           event_origin=event.origin,
                                                           event_subtype='')
        self._schedule_ids.append(sid)

    def create_worker(self, number_of_workers=1):
        '''
        Creates notification workers

        @param number_of_workers int
        @ret_val pids list

        '''

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

        @param date_time: Python datetime object
        @return: seconds_since_epoch:: int
        """
        date_time = date_time.isoformat().split('.')[0].replace('T',' ')
        #'2009-07-04 18:30:47'
        pattern = '%Y-%m-%d %H:%M:%S'
        seconds_since_epoch = int(time.mktime(time.strptime(date_time, pattern)))

        return seconds_since_epoch


    def process_batch(self, start_time = 0, end_time = 0):
        '''
        This method is launched when an process_batch event is received. The user info dictionary maintained
        by the User Notification Service is used to query the event repository for all events for a particular
        user that have occurred in a provided time interval, and then an email is sent to the user containing
        the digest of all the events.
        '''

        log.warning("Processing notifications that arrived between %s seconds and %s seconds" % (start_time, end_time))

        log.warning("(In process batch) time now: %s" % UserNotificationService.makeEpochTime(self.__now()))

        if end_time <= start_time:
            return

        log.warning("self.event_processor.user_info: %s" % self.event_processor.user_info)

        for user_name, value in self.event_processor.user_info.iteritems():

            notifications = value['notifications']

            log.warning("notifications of interest: %s" % notifications)

            events_for_message = []

            search_time = "SEARCH 'ts_created' VALUES FROM %s TO %s FROM 'events_index'" % (start_time, end_time)

            for notification in notifications:

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

                log.warning("search_string: %s" % search_string)

                # get the list of ids corresponding to the events
                ret_vals = self.discovery.parse(search_string)

                log.warning ("ret_vals: %s" % ret_vals)

                for event_id in ret_vals:
                    datastore = self.datastore_manager.get_datastore('events')
                    event_obj = datastore.read(event_id)
                    events_for_message.append(event_obj)

            log.debug("Found following events of interest to user, %s: %s" % (user_name, events_for_message))
            log.warning("Found following events of interest to user, %s: %s" % (user_name, events_for_message))

            # send a notification email to each user using a _send_email() method
            if events_for_message:
                self.format_and_send_email(events_for_message, user_name)

    def format_and_send_email(self, events_for_message, user_name):
        '''
        Format the message for a particular user containing information about the events he is to be notified about
        '''

        message = str(events_for_message)
        log.info("The user, %s, will get the following events in his batch notification email: %s" % (user_name, message))

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
            msg_recipient=self.event_processor.user_info[user_name]['user_contact'].email,
            smtp_client=self.smtp_client )

    def send_batch_email(self, msg_body, msg_subject, msg_recipient, smtp_client):
        '''
        Send the email
        '''

        msg = MIMEText(msg_body)
        msg['Subject'] = msg_subject
        msg['From'] = self.ION_NOTIFICATION_EMAIL_ADDRESS
        msg['To'] = msg_recipient
        log.debug("EventProcessor.subscription_callback(): sending email to %s"\
        %msg_recipient)

        smtp_sender = CFG.get_safe('server.smtp.sender')

        smtp_client.sendmail(smtp_sender, msg_recipient, msg.as_string())

    def update_user_info_object(self, user_id, new_notification, old_notification):
        '''
        Update the UserInfo object. If the passed in parameter, od_notification, is None, it does not need to remove the old notification
        '''

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

    def update_user_info_dictionary(self, user, new_notification, old_notification):

        #------------------------------------------------------------------------------------
        # Remove the old notifications
        #------------------------------------------------------------------------------------

        if old_notification in self.event_processor.user_info[user.name]['notifications']:

            # remove from notifications list
            self.event_processor.user_info[user.name]['notifications'].remove(old_notification)

            #------------------------------------------------------------------------------------
            # update the notification subscription object
            #------------------------------------------------------------------------------------

            # get the old notification_subscription
            notification_subscription = self.event_processor.user_info[user.name]['notification_subscriptions'].pop(old_notification._id)

            # update that old notification subscription
            notification_subscription._res_obj = new_notification

            # feed the updated notification subscription back into the user info dictionary
            self.event_processor.user_info[user.name]['notification_subscriptions'][old_notification._id] = notification_subscription

        #------------------------------------------------------------------------------------
        # find the already existing notifications for the user
        #------------------------------------------------------------------------------------

        notifications = self.event_processor.user_info[user.name]['notifications']
        notifications.append(new_notification)

        #------------------------------------------------------------------------------------
        # update the user info - contact information, notifications
        #------------------------------------------------------------------------------------

        self.event_processor.user_info[user.name]['user_contact'] = user.contact
        self.event_processor.user_info[user.name]['notifications'] = notifications

        self.event_processor.reverse_user_info = calculate_reverse_user_info(self.event_processor.user_info)

