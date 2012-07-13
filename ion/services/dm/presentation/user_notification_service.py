#!/usr/bin/env python
'''
@author Bill Bollenbacher
@author Swarbhanu Chatterjee
@author David Stuebe
@file ion/services/dm/presentation/user_notification_service.py
@description Implementation of the UserNotificationService
'''

from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED, get_sys_name, Container, CFG
from pyon.util.async import spawn
from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.datastore.datastore import DatastoreManager
from pyon.event.event import EventPublisher, EventSubscriber
from ion.services.dm.utility.query_language import QueryLanguage
from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient

import string
import time, datetime
import gevent
from gevent.timeout import Timeout
from email.mime.text import MIMEText
from gevent import Greenlet
import elasticpy as ep

from ion.services.dm.presentation.sms_providers import sms_providers
from interface.objects import NotificationRequest, NotificationType, ProcessDefinition

from interface.services.dm.iuser_notification_service import BaseUserNotificationService
from ion.services.dm.utility.uns_utility_methods import send_email, setting_up_smtp_client
from ion.services.dm.utility.uns_utility_methods import calculate_reverse_user_info, FakeScheduler


"""
For every user that has existing notification requests (who has called
create_notification()) the UNS will contain a local UserEventProcessor
instance that contains the user's notification information (email address)
and all of the user's notifications (along with their event subscribers).
The UserEventProcessors are maintained local to the UNS in a dictionary
indexed by the user's resourceID.  When a notification is created the user's
UserEventProcessor will be created if it doesn't already exist , and it will
be deleted when the user deletes their last notification.

The user's UserEventProcessor will encapsulate a list of notification objects
that the user has requested, along with user information needed for send notifications
(email address for LCA). It will also encapsulate a subscriber callback method
that is passed to all event subscribers for each notification the user has created.

Each notification object will encapsulate the notification information and a
list of event subscribers (only one for LCA) that listen for the events in the notification.
"""

class SubscribedNotification(object):
    """
    Encapsulates a notification's info and it's event subscriber
    """

    def  __init__(self, notification_request=None, subscriber_callback=None):
        self._res_obj = notification_request  # The notification Request Resource Object
        self.subscriber = EventSubscriber(origin=notification_request.origin,
                                            origin_type = notification_request.origin_type,
                                            event_type=notification_request.event_type,
                                            sub_type=notification_request.event_subtype,
                                            callback=subscriber_callback)
        self.subscribed_notification_id = None

    def set_notification_id(self, id_=None):
        """
        Set the notification id of the notification object
        @param notification id
        """
        self.subscribed_notification_id = id_

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

class UserEventProcessor(object):
    """
    Encapsulates the user's info and a list of all the notifications they have.
    """

    def __init__(self, user_id = ''):
        self.user_id = user_id
        self.subscribed_notifications = []

    def add_notification_for_user(self, notification_request, user_id):
        self.subscribed_notifications.append(self._add_notification(notification_request=notification_request))

    def subscription_callback(self, message, headers):
        """
        This callback is given to all the event subscribers that this user wants notifications for.
        If this callback gets called the user in this processor should get an email
        """
        raise NotImplementedError("Subscription callback is not implemented in the base class")

    def _add_notification(self, notification_request=None):
        """
        Adds a notification that this user then subscribes to

        @param notification_request
        @retval notification object
        """

        # create and save notification in notifications list
        subscribed_notification = SubscribedNotification(notification_request, self.subscription_callback)

        # start the event subscriber listening
        subscribed_notification.activate()
        log.debug("UserEventProcessor.add_notification(): added notification " + str(notification_request) + " to user " + self.user_id)
        return subscribed_notification

    def remove_notification(self):
        """
        Removes a notification subscribed to by the user
        """
        for subscribed_notification in self.subscribed_notifications:
            subscribed_notification.deactivate()

    def __str__(self):
        return str(self.__dict__)



class EmailEventProcessor(UserEventProcessor):

    def __init__(self, user_id, smtp_client):

        super(EmailEventProcessor, self).__init__(user_id=user_id)
        self.smtp_client = smtp_client

    def subscription_callback(self, message, headers):
        """
        This callback is given to all the event subscribers that this user wants notifications for.
        If this callback gets called the user in this processor should get an email
        """

        # find the email address of the user
        resource_registry = ResourceRegistryServiceClient()
        user = resource_registry.read(self.user_id)
        msg_recipient = user.contact.email

        # send email to the user
        send_email(message, msg_recipient, self.smtp_client)

    def remove_notification(self):

        super(EmailEventProcessor, self).remove_notification()

#----------------------------------------------------------------------------------------------------------------
# Keep this note for the time when we need to also include sms delivery via email to sms providers
#        provider_email = sms_providers[provider] # self.notification.delivery_config.delivery['provider']
#        self.msg_recipient = notification_request.delivery_config.delivery['phone_number'] + provider_email
#----------------------------------------------------------------------------------------------------------------

class DetectionEventProcessor(UserEventProcessor):

    def __init__(self, user_id):

        super(DetectionEventProcessor, self).__init__(user_id=user_id)

        parser = QueryLanguage()

        for subscribed_notification in self.subscribed_notifications:
            search_string = subscribed_notification._res_obj.delivery_config.processing['search_string']
            self.query_dict = parser.parse(search_string)

    def generate_events(self, msg):
        '''
        Publish an event
        '''

        log.info('Detected an event')
        event_publisher = EventPublisher("DetectionEvent")

        for subscribed_notification in self.subscribed_notifications:

            message = str(subscribed_notification._res_obj.delivery_config.processing['search_string'])

            event_publisher.publish_event(origin='DetectionEventProcessor',
                message=msg,
                description="Event was detected by DetectionEventProcessor",
                condition = message,
                original_origin = subscribed_notification._res_obj.origin,
                original_type = subscribed_notification._res_obj.origin_type)

    def subscription_callback(self, message, headers):
        if QueryLanguage.evaluate_condition(message, self.query_dict):
            self.generate_events(message)

def create_event_processor(notification_request, user_id, smtp_client):

    if notification_request.type == NotificationType.EMAIL:
        event_processor = EmailEventProcessor(user_id=user_id, smtp_client = smtp_client)
        event_processor.add_notification_for_user(notification_request=notification_request, user_id=user_id)

    elif notification_request.type == NotificationType.FILTER:
        event_processor = DetectionEventProcessor(user_id=user_id)
        event_processor.add_notification_for_user(notification_request=notification_request, user_id=user_id)

    else:
        raise BadRequest('Invalid Notification Request Type!')

class UserNotificationService(BaseUserNotificationService):
    """
    A service that provides users with an API for CRUD methods for notifications.
    """

    def on_start(self):

        # Get the event Repository
        self.event_repo = self.container.instance.event_repository

        self.smtp_client = setting_up_smtp_client()

        self.ION_NOTIFICATION_EMAIL_ADDRESS = 'ION_notifications-do-not-reply@oceanobservatories.org'

        # load event originators, types, and table
        self.event_types = CFG.event.types
        self.event_table = {}

        self.user_info = {}
        self.reverse_user_info = {}

        # Get the discovery client for batch processing
        self.discovery = DiscoveryServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()
        self.datastore_manager = DatastoreManager()

    def on_quit(self):

        pass

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

        #@todo Write business logic to validate the subscription fields of the notification request object

        #---------------------------------------------------------------------------------------------------
        # Persist Notification object as a resource
        #---------------------------------------------------------------------------------------------------

        notification_id, _ = self.clients.resource_registry.create(notification)

        #-------------------------------------------------------------------------------------------------------------------
        # Update the UserInfo object and the user_info dictionary maintained by the UNS
        #-------------------------------------------------------------------------------------------------------------------

        self._update_user_with_notification(user_id, notification)

        #---------------------------------------------------------------------------------------------------
        # create event processor for user
        #---------------------------------------------------------------------------------------------------

        create_event_processor(notification_request=notification,user_id=user_id, smtp_client = self.smtp_client)

        #-------------------------------------------------------------------------------------------------------------------
        # Allow the indexes to be updated for ElasticSearch
        # We publish event only after this so that the reload of the user info works by the
        # notification workers work properly
        #-------------------------------------------------------------------------------------------------------------------

        # todo: This is to allow time for the indexes to be created before punlishing ReloadUserInfoEvent for notification workers.
        # todo: When things are more refined, it will be nice to have an event generated when the
        # indexes are updated so that a subscriber here when it received that event will publish
        # the reload user info event.
        time.sleep(4)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        log.debug("(create notification) Publishing ReloadUserInfoEvent for notification_id, notification origin: (%s, %s)" % (notification_id, notification.origin))

        event_publisher = EventPublisher("ReloadUserInfoEvent")
        event_publisher.publish_event(origin="UserNotificationService", description= "A notification has been created.", notification_id = notification_id)

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

        self.clients.resource_registry.update(notification)

        #-------------------------------------------------------------------------------------------------------------------
        # Update the UserInfo object and the user_info dictionary maintained by the UNS
        #-------------------------------------------------------------------------------------------------------------------

        #todo refine this so that the old notification gets removed from the user info dictionary
        self._update_user_with_notification(user_id, notification)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by notification workers so that they can update their user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        log.info("(update notification) Publishing ReloadUserInfoEvent for updated notification")

        event_publisher = EventPublisher("ReloadUserInfoEvent")
        event_publisher.publish_event(origin="UserNotificationService",
                                        description= "A notification has been updated.",
                                        )

    def read_notification(self, notification_id=''):
        """Returns the NotificationRequest object for the specified notification id.
        Throws exception if id does not match any persisted NotificationRequest
        objects.

        @param notification_id    str
        @retval notification    NotificationRequest
        @throws NotFound    object with specified id does not exist
        """
        # Read UserNotification object with _id matching passed notification_id
        notification = self.clients.resource_registry.read(notification_id)

        return notification

    def delete_notification(self, notification_id=''):
        """For now, permanently deletes NotificationRequest object with the specified
        id. Throws exception if id does not match any persisted NotificationRequest.

        @param notification_id    str
        @throws NotFound    object with specified id does not exist
        """

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

        event_publisher = EventPublisher("ReloadUserInfoEvent")
        event_publisher.publish_event(origin="UserNotificationService", description= "A notification has been deleted.", notification_id = notification_id)

    def delete_notification_from_user_info(self, notification_id):

        notification = self.clients.resource_registry.read(notification_id)

        for value in self.user_info.itervalues():
            print "value: ", value
            if notification in value['notifications']:
                value['notifications'].remove(notification)

        self.reverse_user_info = calculate_reverse_user_info(self.user_info)

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

        search_time = ''
        search_origin = ''
        search_type = ''

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


    def create_detection_filter(self, name='', description='', event_type='', event_subtype='', origin='', origin_type='', user_id='', filter_config=None):
        '''
         Creates a NotificationRequest object for the specified User Id. Associate the Notification
         resource with the user. Setup subscription and call back do a detection filter of some type...
         @todo - is the user email automatically selected from the user id?
        '''

        delivery_config = filter_config or {}

        #-------------------------------------------------------------------------------------
        # Create a notification object
        #-------------------------------------------------------------------------------------
        notification_request = NotificationRequest(
            name=name,
            description=description,
            type = NotificationType.FILTER,
            origin = origin,
            origin_type = origin_type,
            event_type=event_type,
            event_subtype = event_subtype ,
            delivery_config=delivery_config)

        #-------------------------------------------------------------------------------------
        # Set up things so that the user gets subscribed to receive this notification request
        #-------------------------------------------------------------------------------------

        notification_id = self.create_notification(notification=notification_request, user_id = user_id)


        return notification_id

    def publish_event(self, event=None, publish_time= None):
        '''
        Publish a general event at a certain time using the UNS

        @param event Event
        @param publish_time list Ex: [year, month, day, hour, minute, second]
        '''

        if not isinstance(publish_time, list) or not len(publish_time) == 6:
            raise AssertionError("Publish time must be a list of integers of length 6. \
                                    Ex: [year, month, day, hour, minute, second]")

        log.warning("event.origin: %s" % event.origin)
        log.warning("event.event_type: %s" % event.type_)

        def publish_immediately(message, headers):
            log.info("UNS received a SchedulerEvent")

            current_time = datetime.datetime.today()

            event_publisher = EventPublisher()
            event_publisher._publish_event( event_msg = event,
                                            origin=event.origin,
                                            event_type = event.type_)

        # Set up a subscriber to get the nod from the scheduler to publish the event
        event_subscriber = EventSubscriber( event_type = "SchedulerEvent",
            origin="Scheduler",
            callback=publish_immediately)

        event_subscriber.start()


        publish_time_object = datetime.datetime( publish_time[0], publish_time[1], publish_time[2], publish_time[3],
                                            publish_time[4], publish_time[5])

        #todo - When there is a real scheduler, we can publish events using that
        #todo - For now, creating a proxy for the scheduler

        log.info("UNS providing the scheduler the following time = %s" % publish_time_object)

        fake_scheduler = FakeScheduler()
        fake_scheduler.set_task(publish_time_object, "Schedule UserNotificationService.publish_event")


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


    def process_batch(self, start_time = 0, end_time = 10):
        '''
        This method is launched when an process_batch event is received. The user info dictionary maintained
        by the User Notification Service is used to query the event repository for all events for a particular
        user that have occurred in a provided time interval, and then an email is sent to the user containing
        the digest of all the events.
        '''
        for user_name, value in self.user_info.iteritems():

            notifications = value['notifications']

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

                # get the list of ids corresponding to the events
                ret_vals = self.discovery.parse(search_string)

                for event_id in ret_vals:
                    datastore = self.datastore_manager.get_datastore('events')
                    event_obj = datastore.read(event_id)
                    events_for_message.append(event_obj)

            log.debug("Found following events of interest to user, %s: %s" % (user_name, events_for_message))

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

        msg_body += "You received this notification from ION because you asked to be " + \
                    "notified about this event from this source. " + \
                    "To modify or remove notifications about this event, " + \
                    "please access My Notifications Settings in the ION Web UI. " + \
                    "Do not reply to this email.  This email address is not monitored " + \
                    "and the emails will not be read. \r\n "


        log.debug("The email has the following message body: %s" % msg_body)

        msg_subject = "(SysName: " + get_sys_name() + ") ION event "

        self.send_batch_email(  msg_body = msg_body,
                                msg_subject = msg_subject,
                                msg_recipient=self.user_info[user_name]['user_contact'].email,
                                smtp_client=self.smtp_client )

    def send_batch_email(self, msg_body, msg_subject, msg_recipient, smtp_client):

        msg = MIMEText(msg_body)
        msg['Subject'] = msg_subject
        msg['From'] = self.ION_NOTIFICATION_EMAIL_ADDRESS
        msg['To'] = msg_recipient
        log.debug("UserEventProcessor.subscription_callback(): sending email to %s"\
        %msg_recipient)

        smtp_sender = CFG.get_safe('server.smtp.sender')

        smtp_client.sendmail(smtp_sender, msg_recipient, msg.as_string())


    def _update_user_with_notification(self, user_id, notification):

        #------------------------------------------------------------------------------------
        # Update the UserInfo object
        #------------------------------------------------------------------------------------

        user = self.clients.resource_registry.read(user_id)

        if not user:
            raise BadRequest("No user with the provided user_id: %s" % user_id)

        notification_present = False

        for item in user.variables:
            if item['name'] == 'notification':
                notifications = item['value']
                if notifications and isinstance(notifications, list) :
                    notifications.append(notification)
                    item['value'] = notifications
                else:
                    item['value'] = [notification]
                notification_present = True

        if not notification_present:
            user.variables= [{'name' : 'notification', 'value' : [notification]}]

        # update the resource registry
        self.clients.resource_registry.update(user)

        #------------------------------------------------------------------------------------
        # Update the user_info dictionary maintained by UNS
        #------------------------------------------------------------------------------------

        notifications = []

        # find the already existing notifications for the user
        if self.user_info.has_key(user.name):
            notifications = self.user_info[user.name]['notifications']

        # append the new notification to the list of notifications for the user
        notifications.append(notification)

        # update the user info
        self.user_info[user.name] = { 'user_contact' : user.contact, 'notifications' : notifications}
        self.reverse_user_info = calculate_reverse_user_info(self.user_info)



#    def create_email(self, name='', description='', event_type='', event_subtype='', origin='', origin_type='', user_id='', email='', mode=None, frequency = Frequency.REAL_TIME, message_header='', parser=''):
#        '''
#         Creates a NotificationRequest object for the specified User Id. Associate the Notification
#         resource with the user. Setup subscription and call back to send email
#        '''
#
#        if not email:
#            raise BadRequest('Email missing.')
#
#        if not message_header:
#            message_header = "Default message header" #@todo this has to be decided
#
#        processing = {'message_header': message_header, 'parsing': parser}
#        delivery = {'email': email, 'mode' : mode, 'frequency' : frequency}
#        delivery_config = DeliveryConfig(processing=processing, delivery=delivery)
#
#        log.info("Delivery config: %s" % str(delivery_config))
#
#        #-------------------------------------------------------------------------------------
#        # Create a notification object
#        #-------------------------------------------------------------------------------------
#        notification_request = NotificationRequest(
#            name=name,
#            description=description,
#            type=NotificationType.EMAIL,
#            origin = origin,
#            origin_type = origin_type,
#            event_type=event_type,
#            event_subtype = event_subtype ,
#            delivery_config= delivery_config)
#
#        log.info("Notification Request: %s" % str(notification_request))
#
#        #-------------------------------------------------------------------------------------
#        # Set up things so that the user gets notified for the particular notification request
#        #-------------------------------------------------------------------------------------
#
#        notification_id =  self.create_notification(notification=notification_request, user_id = user_id)
#
#        return notification_id
