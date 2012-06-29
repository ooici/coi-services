#!/usr/bin/env python
'''
@author Bill Bollenbacher
@author Swarbhanu Chatterjee
@author David Stuebe
@file ion/services/dm/presentation/user_notification_service.py
@description Implementation of the UserNotificationService
'''

from pyon.core.exception import BadRequest, NotFound
from pyon.event.event import EventSubscriber
from pyon.public import RT, PRED, get_sys_name, Container, CFG
from pyon.util.async import spawn
from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.event.event import EventPublisher
from ion.services.dm.utility.query_language import QueryLanguage
from interface.services.dm.idiscovery_service import DiscoveryServiceClient

import string
import time
import gevent
from gevent.timeout import Timeout
from datetime import datetime
from email.mime.text import MIMEText
from gevent import Greenlet
import elasticpy as ep

from ion.services.dm.presentation.sms_providers import sms_providers
from interface.objects import NotificationRequest, DeliveryConfig, NotificationType, Frequency

from interface.services.dm.iuser_notification_service import BaseUserNotificationService
from ion.services.dm.utility.uns_utility_methods import send_email, load_user_info, setting_up_smtp_client
from ion.services.dm.utility.uns_utility_methods import calculate_reverse_user_info, fake_smtplib, check_user_notification_interest

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

"""
The class, NotificationEventSubscriber, has been replaced by a lower level way of
activating and deactivating the subscriber

"""


class SubscribedNotification(object):
    """
    Encapsulates a notification's info and it's event subscriber

    @David - is this class needed? It does not seem to serve any purpose?
    """

    def  __init__(self, notification_request=None, subscriber_callback=None):
        self._res_obj = notification_request  # The notification Request Resource Object
        # setup subscription using subscription_callback()
        # msg_recipientDO: make this walk the lists and set up a subscriber for every pair of
        # origin/event.  This will require a list to hold all the subscribers so they can
        # be started and killed

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
        self.subscriber.activate()

    def deactivate(self):
        """
        Stop subscribing
        """
        self.subscriber.deactivate()

class EventProcessor(object):
    """
    Encapsulates the user's info and a list of all the notifications they have.
    It also contains the callback that is passed to all event subscribers for this user's notifications.
    If the callback gets called, then this user had a notification for that event.

    @David - Make this more generic. Make user_id part of the notification request.
    What does it mean to make a notification on someone else's behalf?
    Is that what we want? All resources already have an owner association!
    """

    def __init__(self, notification_request, user_id):
        self.user_id = user_id
        self.subscribed_notification = self._add_notification(notification_request=notification_request)
        log.debug("UserEventProcessor.__init__():")

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

        @param notification_id
        @retval the number of notifications subscribed to by the user
        """
        self.subscribed_notification.deactivate()

    def __str__(self):
        return str(self.__dict__)



class EmailEventProcessor(EventProcessor):

    def __init__(self, notification_request, user_id):

        super(EmailEventProcessor, self).__init__(notification_request,user_id)

        self.smtp_client = setting_up_smtp_client()

        log.debug("UserEventProcessor.__init__(): email for user %s " %self.user_id)

    def subscription_callback(self, message, headers):
        """
        This callback is given to all the event subscribers that this user wants notifications for.
        If this callback gets called the user in this processor should get an email
        """
        log.debug("UserEventProcessor.subscription_callback(): message=" + str(message))
        log.debug("event type = " + str(message._get_type()))
        log.debug('type of message: %s' % type(message))

        msg_recipient = self.subscribed_notification._res_obj.delivery_config.delivery['email']

        send_email(message, msg_recipient, self.smtp_client)

    def remove_notification(self):

        super(EmailEventProcessor, self).remove_notification()

#        if CFG.get_safe('system.smtp',False):
#            self.smtp_client.close()

#---------------------
#        provider = notification_request.delivery_config.delivery['provider']
#
#        provider_email = sms_providers[provider] # self.notification.delivery_config.delivery['provider']
#        self.msg_recipient = notification_request.delivery_config.delivery['phone_number'] + provider_email

class DetectionEventProcessor(EventProcessor):

    def __init__(self, notification_request, user_id):

        super(DetectionEventProcessor, self).__init__(notification_request,user_id)

        parser = QueryLanguage()

        search_string = self.subscribed_notification._res_obj.delivery_config.processing['search_string']
        self.query_dict = parser.parse(search_string)

    def generate_event(self, msg):
        '''
        Publish an event
        '''

        log.info('Detected an event')
        event_publisher = EventPublisher("DetectionEvent")

        message = str(self.subscribed_notification._res_obj.delivery_config.processing['search_string'])

        #@David What should the origin and origin type be for Detection Events
        event_publisher.publish_event(origin='DetectionEventProcessor',
            message=msg,
            description="Event was detected by DetectionEventProcessor",
            condition = message, # Concatenate the filter and make it a message
            original_origin = self.subscribed_notification._res_obj.origin,
            original_type = self.subscribed_notification._res_obj.origin_type)

    def subscription_callback(self, message, headers):

        if QueryLanguage.evaluate_condition(message, self.query_dict):
            self.generate_event(message) # pass in the event message so we can put some of the content in the new event.

def create_event_processor(notification_request, user_id):
    if notification_request.type == NotificationType.EMAIL:
        return EmailEventProcessor(notification_request,user_id)

    elif notification_request.type == NotificationType.FILTER:
        return DetectionEventProcessor(notification_request,user_id)

    else:
        raise BadRequest('Invalid Notification Request Type!')

class UserNotificationService(BaseUserNotificationService):
    """
    A service that provides users with an API for CRUD methods for notifications.
    """

    def on_start(self):

        self.event_processors = {}

        # Get the event Repository
        self.event_repo = self.container.instance.event_repository

        # load event originators, types, and table
        self.event_originators = CFG.event.originators
        self.event_types = CFG.event.types
        self.event_table = {}

        self.user_info = {}
        self.reverse_user_info = {}

        # Get the discovery client for batch processing
        self.discovery = DiscoveryServiceClient()

        #-----------------------------------------------------------------
        # For discovery to work, we need elastic search
        #-----------------------------------------------------------------

        use_es = CFG.get_safe('system.elasticsearch',False)

        if use_es:
            self.es_host   = CFG.get_safe('server.elasticsearch.host', 'localhost')
            self.es_port   = CFG.get_safe('server.elasticsearch.port', '9200')
            CFG.server.elasticsearch.shards         = 1
            CFG.server.elasticsearch.replicas       = 0
            CFG.server.elasticsearch.river_shards   = 1
            CFG.server.elasticsearch.river_replicas = 0
            self.es = ep.ElasticSearch(
                host=self.es_host,
                port=self.es_port,
                timeout=10,
                verbose=True
            )

            op = DotDict(CFG)
            op.op = 'clean_bootstrap'
            self.container.spawn_process('index_bootstrap','ion.processes.bootstrap.index_bootstrap','IndexBootStrap', op)

        for originator in self.event_originators:
            try:
                self.event_table[originator] = CFG.event[originator]
            except NotFound:
                log.info("UserNotificationService.on_start(): event originator <%s> not found in configuration" %originator)

        #------------------------------------------------------------------------------------
        # start the event subscriber for listening to events which get generated when
        # notifications are updated.. this is required so that the UNS can update its user_info dict
        # that it needs for batch notifications
        #------------------------------------------------------------------------------------

        def reload_user_info(event_msg, headers):
            notification_id =  event_msg.notification_id
            log.warning("Received notification with id: %s" % notification_id)

            #------------------------------------------------------------------------------------------
            # reloads the user_info and reverse_user_info dictionaries
            #------------------------------------------------------------------------------------------

            self.user_info = load_user_info()
            if self.user_info:
                self.reverse_user_info =  calculate_reverse_user_info(self.user_info)

            log.warning("After reload, user_info: %s" % self.user_info)
            log.warning("After reload, reverse_user_info: %s" % self.reverse_user_info)

        self.event_subscriber = EventSubscriber(
            event_type="ReloadUserInfoEvent",
            callback=reload_user_info
        )
        self.event_subscriber.activate()

    def on_quit(self):

        for processor in self.event_processors.itervalues():

            processor.remove_notification()


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

        @todo Can we remove the user_id field? Not sure it makes sense for all notification requests and should be gotten
        from the context of the message - who sent it?
        """

        if not user_id:
            raise BadRequest("User id not provided.")

        #@todo Write business logic to validate the subscription fields of the notification request object

        #---------------------------------------------------------------------------------------------------
        # Persist Notification object as a resource
        #---------------------------------------------------------------------------------------------------

        notification_id, _ = self.clients.resource_registry.create(notification)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        event_publisher = EventPublisher("ReloadUserInfoEvent")
        event_publisher.publish_event(origin="UserNotificationService", description= "A notification has been created.", notification_id = notification_id)

        #---------------------------------------------------------------------------------------------------
        # create event processor for user
        #---------------------------------------------------------------------------------------------------

        self.event_processors[notification_id] = create_event_processor(notification_request=notification,user_id=user_id)
        log.debug("UserNotificationService.create_notification(): added event processor " +  str(self.event_processors[notification_id]))

        return notification_id

    def update_notification(self, notification=None):
        """Updates the provided NotificationRequest object.  Throws NotFound exception if
        an existing version of NotificationRequest is not found.  Throws Conflict if
        the provided NotificationRequest object is not based on the latest persisted
        version of the object.

        @param notification    NotificationRequest
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        # Read existing Notification object and see if it exists
        old_notification = self.event_processors[notification_id].notification._res_obj

        if not old_notification:
            raise NotFound("UserNotificationService.update_notification(): Notification %s does not exist" % notification_id)

        # check to see if the new notification is different than the old notification only in the delivery config fields
        if notification.origin != old_notification.origin or\
           notification.origin_type != old_notification.origin_type or\
           notification.event_type != old_notification.event_type or\
           notification.event_subtype != old_notification.event_subtype:


            log.info('Update unsuccessful. Only the delivery config is allowed to be modified!')
            raise BadRequest('Can not update the subscription for an event notification')

        else: # only the delivery_config is being modified, so we can go ahead with the update...
            _event_processor = self.event_processors[notification_id]
            _event_processor.notification = notification
            _event_processor.notification.set_notification_id(notification_id)
            # finally update the notification in the RR
            self.clients.resource_registry.update(notification)

            #-------------------------------------------------------------------------------------------------------------------
            # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
            #-------------------------------------------------------------------------------------------------------------------
            event_publisher = EventPublisher("ReloadUserInfoEvent")
            event_publisher.publish_event(origin="UserNotificationService", description= "A notification has been updated.", notification_id = notification_id)


            log.debug('Updated notification object with id: %s' % notification_id)

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

        _event_processor = self.event_processors[notification_id]
        del self.event_processors[notification_id]
        _event_processor.remove_notification(notification_id)
        self.clients.resource_registry.delete(notification_id)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        event_publisher = EventPublisher("ReloadUserInfoEvent")
        event_publisher.publish_event(origin="UserNotificationService", description= "A notification has been deleted.", notification_id = notification_id)

        #@todo clean up the association?

    def find_events(self, origin='', type='', min_datetime='', max_datetime='', limit=0, descending=False):
        """Returns a list of events that match the specified search criteria. Will throw a not NotFound exception
        if no events exist for the given parameters.

        @param origin         str
        @param type           str
        @param min_datetime   str
        @param max_datetime   str
        @param limit          int         (integer limiting the number of results (0 means unlimited))
        @param descending     boolean     (if True, reverse order (of production time) is applied, e.g. most recent first)
        @retval event_list    []
        @throws NotFound    object with specified paramteres does not exist
        @throws NotFound    object with specified paramteres does not exist
        """
        return self.event_repo.find_events(event_type=type,
            origin=origin,
            start_ts=min_datetime,
            end_ts=max_datetime,
            descending=descending,
            limit=limit)

    def create_email(self, name='', description='', event_type='', event_subtype='', origin='', origin_type='', user_id='', email='', mode=None, frequency = Frequency.REAL_TIME, message_header='', parser=''):
        '''
         Creates a NotificationRequest object for the specified User Id. Associate the Notification
         resource with the user. Setup subscription and call back to send email
         @todo - is the user email automatically selected from the user id?
        '''

        if not email:
            raise BadRequest('Email missing.')

        #-------------------------------------------------------------------------------------
        # Build the delivery config
        #-------------------------------------------------------------------------------------

        #@todo get the process_definition_id - Find it when the service starts... bootstrap
        #@todo Define a default for message header and parsing

        #--------------------------------------------------------------------------------------
        #@todo Decide on where to place this piece
        # Create an object to hold the delivery configs (which will allow one to specialize
        # if needed to email or sms)
        #--------------------------------------------------------------------------------------

        if not message_header:
            message_header = "Default message header" #@todo this has to be decided

        processing = {'message_header': message_header, 'parsing': parser}
        delivery = {'email': email, 'mode' : mode, 'frequency' : frequency}
        delivery_config = DeliveryConfig(processing=processing, delivery=delivery)

        log.info("Delivery config: %s" % str(delivery_config))

        #-------------------------------------------------------------------------------------
        # Create a notification object
        #-------------------------------------------------------------------------------------
        notification_request = NotificationRequest(
            name=name,
            description=description,
            type=NotificationType.EMAIL,
            origin = origin,
            origin_type = origin_type,
            event_type=event_type,
            event_subtype = event_subtype ,
            delivery_config= delivery_config)

        log.info("Notification Request: %s" % str(notification_request))

        #-------------------------------------------------------------------------------------
        # Set up things so that the user gets notified for the particular notification request
        #-------------------------------------------------------------------------------------

        notification_id =  self.create_notification(notification=notification_request, user_id = user_id)

        return notification_id

    def create_detection_filter(self, name='', description='', event_type='', event_subtype='', origin='', origin_type='', user_id='', filter_config=None):
        '''
         Creates a NotificationRequest object for the specified User Id. Associate the Notification
         resource with the user. Setup subscription and call back do a detection filter of some type...
         @todo - is the user email automatically selected from the user id?
        '''

        #@todo for now the delivery config is the filter config. Later there may be some smarts here to set up the config for the event processor
        delivery_config = filter_config or {}

        #-------------------------------------------------------------------------------------
        # Create a notification object
        #-------------------------------------------------------------------------------------
        #@todo get the process_definition_id - in this case, should it be added to the interface for this method?

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

    def publish_event(self, event=None, time=None):
        '''
        Publish a general event at a certain time using the UNS
        '''

        #todo - is there a concept of a general time in the system?
        # todo - publish an event at a particular time

        if event:
            type = event.type_
            origin = event.origin
            description = event.description
        else:
            type = "Event"
            origin = "User"
            description = "User defined event"

        #todo: fill this in with particulars as we come to understand the use cases
        event_publisher = EventPublisher(type)
        event_publisher.publish_event(origin=origin, description= description)

    def create_worker(self, number_of_workers=1):
        '''
        Creates notification workers
        '''

        # ------------------------------------------------------------------------------------
        # Use CEI (process_dispatcher) to create a new process definition
        # ------------------------------------------------------------------------------------

        process_definition = IonObject(RT.ProcessDefinition, name='notification_worker_definition')
        process_definition.executable = {
            'module': 'ion.processes.data.transforms.notification_worker',
            'class':'NotificationWorker'
        }
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)

        # ------------------------------------------------------------------------------------
        # Process Spawning
        # ------------------------------------------------------------------------------------

        #@todo put in a configuration
        configuration = {}

        pid = self.clients.process_dispatcher.schedule_process(
            process_definition_id=process_definition_id,
            configuration=configuration
        )

    def process_batch(self, start_time = 0, end_time = 10.0):
        '''
        This method is launched when an process_batch event is received. The user info dictionary maintained
        by the User Notification Service is used to query the event repository for all events for a particular
        user that have occurred in a certain time interval (right now the past 24 hours), and then an email
        is sent to the user containing the digest of all the events.
        '''

        # The UNS will use the flat dictionary (with user_ids as keys and notification_ids as values)
        # to query the Event Repository (using code in the event repository module) to see what
        # events corresponding to those notifications have been generated during the day.

        #        query_dict = {'and': [],
        #                      'or': [],
        #                      'query': {'field': '',
        #                               'index': 'events_index',
        #                               'range': {'from': 0.0, 'to': 100.0}}}


        log.warning("user_info: %s" % self.user_info)

        for user in self.user_info.iterkeys():
            notifications = self.user_info[user]['notifications']

            events_message = ''

            for notification in notifications:

                search_origin = 'search "origin" is "%s" from "users_index"' % notification.origin
                search_origin_type= 'search "origin_type" is "%s" from "users_index"' % notification.origin_type
                search_event_type = 'search "type_" is "%s" from "users_index"' % notification.event_type

                search_time = "SEARCH 'ts_created' VALUES FROM %s TO %s FROM 'events_index'"\
                % (start_time, end_time)

                search_string = search_origin + 'and' + search_origin_type + 'and' + search_event_type + 'and' +\
                                search_time

                ret_vals = self.discovery.parse(search_string)

                events_message += '\n' + str(ret_vals)

            log.warning("Each user gets the following message in email: %s" % events_message)
            # send a notification email to each user using a _send_email() method

            smtp_client = self.event_processors[notifications[0]].smtp_client

#            send_email( message = events_message,
#                        msg_recipient=self.user_info[user]['user_contact'].email,
#                        smtp_client=smtp_client )





