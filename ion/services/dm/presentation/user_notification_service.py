#!/usr/bin/env python
'''
@author Bill Bollenbacher
@file ion/services/dm/presentation/user_notification_service.py
@description Implementation of the UserNotificationService
'''
__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'


import string, smtplib, time
from datetime import datetime
from email.mime.text import MIMEText
from gevent import Greenlet

from pyon.core.exception import BadRequest, NotFound
from pyon.event.event import EventSubscriber
from pyon.public import RT, PRED, get_sys_name, Container, CFG
from pyon.util.async import spawn
from pyon.util.log import log

from interface.objects import NotificationRequest, SMSDeliveryConfig, EmailDeliveryConfig, NotificationType

from interface.services.dm.iuser_notification_service import BaseUserNotificationService

# the 'from' email address for notification emails
ION_NOTIFICATION_EMAIL_ADDRESS = 'ION_notifications-do-not-reply@oceanobservatories.org'
# the default smtp server
ION_SMTP_SERVER = 'mail.oceanobservatories.org'

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

class NotificationEventSubscriber(EventSubscriber):
    """
    Encapsulates the event subscriber and the event 'listen loop' greenlet implements methods to start/stop the listener
    """


    def __init__(self, origin=None, event_type=None, callback=None):
        self.listener_greenlet = None
        self.subscriber = EventSubscriber(origin=origin, event_type=event_type, callback=callback)

    def start_listening(self):
        """
        Spawns the listener greenlet
        """
        self.listener_greenlet = spawn(self.subscriber.listen)
        self.subscriber._ready_event.wait(timeout=5)     # not sure this is needed

    def stop_listening(self):
        """
        Kills the listener greenlet
        """
        if self.listener_greenlet:
            self.listener_greenlet.kill(exception=Greenlet.GreenletExit, block=False)


class Notification(object):
    """
    Encapsulates a notification's info and it's event subscriber
    """

    def  __init__(self, notification=None, subscriber_callback=None):
        self.notification = notification
        # setup subscription using subscription_callback()
        # msg_recipientDO: make this walk the lists and set up a subscriber for every pair of
        # origin/event.  This will require a list to hold all the subscribers so they can
        # be started and killed
        self.subscriber = NotificationEventSubscriber(origin=notification.origin,
                                                      event_type=notification.event_type,
                                                      callback=subscriber_callback)
        self.notification_id = None

    def set_notification_id(self, id_=None):
        """
        Set the notification id of the notification object
        @param notification id
        """
        self.notification_id = id_

    def start_subscriber(self):
        """
        Start subscribing
        """
        self.subscriber.start_listening()

    def kill_subscriber(self):
        """
        Stop subscribing
        """
        self.subscriber.stop_listening()
        del self.subscriber


class EventProcessor(object):
    """
    Encapsulates the user's info and a list of all the notifications they have.
    It also contains the callback that is passed to all event subscribers for this user's notifications.
    If the callback gets called, then this user had a notification for that event.
    """

    def __init__(self, notification_request, user_id):
        self.user_id = user_id
        self.notification = self._add_notification(notification_request=notification_request)
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
        notification_obj = Notification(notification_request, self.subscription_callback)

        # start the event subscriber listening
        notification_obj.start_subscriber()
        log.debug("UserEventProcessor.add_notification(): added notification " + str(notification_request) + " to user " + self.user_id)
        return notification_obj

    def remove_notification(self):
        """
        Removes a notification subscirbed to by the user

        @param notification_id
        @retval the number of notifications subscribed to by the user
        """
        self.notification.kill_subscriber()



    def __str__(self):
        return str(self.__dict__)

class EmailEventProcessor(EventProcessor):

    def __init__(self, notification_request, user_id):

        super(EmailEventProcessor, self).__init__(notification_request,user_id)

        self.smtp_server = CFG.get('smtp_server', ION_SMTP_SERVER)
        log.debug("UserEventProcessor.__init__(): email for user %s " %self.user_id)


    def subscription_callback(self, message, headers):
        """
        This callback is given to all the event subscribers that this user wants notifications for.
        If this callback gets called the user in this processor should get an email
        """

        log.debug("UserEventProcessor.subscription_callback(): args[0]=" + str(message))
        log.debug("event type = " + str(message._get_type()))

        '''
        time_stamp = str( datetime.fromtimestamp(time.mktime(time.gmtime(float(message.ts_created)/1000))))

        # build the email from the event content
        msg_body = string.join(("Event: %s" %  event,
                                "",
                                "Originator: %s" %  origin,
                                "",
                                "Description: %s" %  description,
                                "",
                                "Time stamp: %s" %  time_stamp,
                                "",
                                "You received this notification from ION because you asked to be "\
                                "notified about this event from this source. ",
                                "To modify or remove notifications about this event, "\
                                "please access My Notifications Settings in the ION Web UI.",
                                "Do not reply to this email.  This email address is not monitored "\
                                "and the emails will not be read."),
            "\r\n")
        msg_subject = "(SysName: " + get_sys_name() + ") ION event " + event + " from " + origin
        msg_sender = ION_NOTIFICATION_EMAIL_ADDRESS
        msg_recipient = self.user_email_addr
        msg = MIMEText(msg_body)
        msg['Subject'] = msg_subject
        msg['From'] = msg_sender
        msg['To'] = msg_recipient
        log.debug("UserEventProcessor.subscription_callback(): sending email to %s via %s"\
        %(msg_recipient, self.smtp_server))
        try:
            smtp_client = smtplib.SMTP(self.smtp_server)
        except Exception as ex:
            log.warning("UserEventProcessor.subscription_callback(): failed to connect to SMTP server %s <%s>"  %(ION_SMTP_SERVER, ex))
            return
        try:
            smtp_client.sendmail(msg_sender, msg_recipient, msg.as_string())
        except Exception as ex:
            log.warning("UserEventProcessor.subscription_callback(): failed to send email to %s <%s>" %(msg_recipient, ex))
        '''


class SMSEventProcessor(EmailEventProcessor):

    def __init__(self, notification_request, user_id):

        super(SMSEventProcessor, self).__init__(notification_request,user_id)

        #@todo Set the right email address for sending an SMS...


def create_event_processor(notification_request, user_id):

    if notification_request.type == NotificationType.EMAIL:
        return EmailEventProcessor(notification_request,user_id)

    elif notification_request.type == NotificationType.SMS:
        return SMSEventProcessor(notification_request,user_id)

    else:
        raise BadRequest('Invalid Notification Request Type!')


class UserNotificationService(BaseUserNotificationService):
    """
    A service that provides users with an API for CRUD methods for notifications.
    """

    def on_start(self):


        self._event_processors = {}

        # Get the event Repository
        self.event_repo = self.container.instance.event_repository

        # load event originators, types, and table
        self.event_originators = CFG.event.originators
        self.event_types = CFG.event.types
        self.event_table = {}
        for originator in self.event_originators:
            try:
                self.event_table[originator] = CFG.event[originator]
            except NotFound:
                log.warning("UserNotificationService.on_start(): event originator <%s> not found in configuration" %originator)
        log.debug("UserNotificationService.on_start(): event_originators=%s" %str(self.event_originators))
        log.debug("UserNotificationService.on_start(): event_types=%s" %str(self.event_types))
        log.debug("UserNotificationService.on_start(): event_table=%s" %str(self.event_table))


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

        # Persist Notification object as a resource
        notification_id, _ = self.clients.resource_registry.create(notification)

        # Retrieve the user's user_info object to get their email address
        objects, _ = self.clients.resource_registry.find_objects(user_id, PRED.hasInfo, RT.UserInfo)
        if not objects:
            raise NotFound("UserNotificationService.create_notification(): No user_info for user " + user_id)
        if len(objects) != 1:
            raise BadRequest("UserNotificationService.create_notification(): there should be only ONE user_info for " + user_id)
        user_info = objects[0]

        # create event processor for user
        self._event_processors[notification_id] = create_event_processor(notification_request=notification,user_id=user_id)
        log.debug("UserNotificationService.create_notification(): added event processor " +  str(self._event_processors[notification_id]))

        # associate the notification to the user
        self.clients.resource_registry.create_association(user_id, PRED.hasNotification, notification_id)

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
        #@todo - fix the update_notification implementation to only allow updates to the delivery config fields of a notification request and make sure the notification object is updated too.

        # Read existing Notification object and see if it exists
        notification_id = notification._id
        old_notification = self.clients.resource_registry.read(notification_id)

        if not old_notification:
            raise NotFound("UserNotificationService.update_notification(): Notification %s does not exist" % notification_id)

        # check to see if the new notification is different than the old notification only in the delivery config fields
        if notification.origin != old_notification.origin or \
            notification.origin_type != old_notification.origin_type or \
                notification.event_type != old_notification.event_type or \
                    notification.event_subtype != old_notification.event_subtype:

            log.warning('Update unsuccessful. Only the delivery config is allowed to be modified!')

        else: # only the delivery_config is being modified, so we can go ahead with the update
    #        # get the user that this notification is associated with
    #        subjects, _ = self.clients.resource_registry.find_subjects(RT.ActorIdentity, PRED.hasNotification, notification_id)
    #
    #        if not subjects:
    #            raise NotFound("UserNotificationService.delete_notification(): No user for notification " + notification_id)
    #        if len(subjects) != 1:
    #            raise BadRequest("UserNotificationService.delete_notification(): there should be only ONE user for " + notification_id)
            _event_processor = self._event_processors[notification_id]

            _event_processor.remove_notification(notification_id)

            # add updated notification to user's event processor
            notification_obj = _event_processor.add_notification(notification)

            notification_obj.set_notification_id(notification_id)

            # finally update the notification in the RR
            self.clients.resource_registry.update(notification)

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
        if not notification:
            raise NotFound("Notification %s does not exist" % notification_id)
        return notification

    def delete_notification(self, notification_id=''):
        """For now, permanently deletes NotificationRequest object with the specified
        id. Throws exception if id does not match any persisted NotificationRequest.

        @param notification_id    str
        @throws NotFound    object with specified id does not exist
        """
        #@todo - fix delete notification implementation to kill the subscriber and delete the event object

        notification_obj = self.clients.resource_registry.read(notification_id)

        notification_obj.kill_subscriber()

        _event_processor.remove_notification(notification_id)


        '''
        # Read specified Notification object and see if it exists
        notification = self.clients.resource_registry.read(notification_id)
        if not notification:
            raise NotFound("UserNotificationService.delete_notification(): Notification %s does not exist" % notification_id)

        #now get the user that this notification is associated with
        subjects, _ = self.clients.resource_registry.find_subjects(RT.ActorIdentity, PRED.hasNotification, notification_id)
        if not subjects:
            raise NotFound("UserNotificationService.delete_notification(): No user for notification " + notification_id)
        if len(subjects) != 1:
            raise BadRequest("UserNotificationService.delete_notification(): there should be only ONE user for " + notification_id)
        user_id = subjects[0]._id

        #remove the notification from the user's entry in the self.user_event_processors list
        #if it's the last notification for the user the delete the user from the self.user_event_processors list
        if user_id not in self.user_event_processors:
            log.warning("UserNotificationService.delete_notification(): user %s not found in user_event_processors list" % user_id)
        user_event_processor = self.user_event_processors[user_id]
        if user_event_processor.remove_notification(notification_id) == 0:
            del self.user_event_processors[user_id]
            log.debug("UserNotificationService.delete_notification(): removed user %s from user_event_processor list" % user_id)

        #now find and delete the association
        assocs = self.clients.resource_registry.find_associations(user_id, PRED.hasNotification, notification_id)
        if not assocs:
            raise NotFound("UserNotificationService.delete_notification(): notification association for user %s does not exist" % user_id)
        association_id = assocs[0]._id
        self.clients.resource_registry.delete_association(association_id)

        #finally delete the notification
        self.clients.resource_registry.delete(notification_id)
        '''

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

    def create_email(self, name='', description='', event_type='', event_subtype='', origin='', origin_type='', user_id='', email='', mode=None, message_header='', parser='', period=86400):
        '''
         Creates a NotificationRequest object for the specified User Id. Associate the Notification
         resource with the user. Setup subscription and call back to send email
         @todo - is the user email automatically selected from the user id?
        '''


        # assertions
        if not email:
            raise BadRequest("No email provided.")
        if not mode:
            raise BadRequest("No delivery mode provided.")

        #-------------------------------------------------------------------------------------
        # Build the email delivery config
        #-------------------------------------------------------------------------------------

        #@todo get the process_definition_id - Find it when the service starts... bootstrap
        #@todo Define a default for message header and parsing

        processing = {'message_header': message_header, 'parsing': parser}
        delivery = {'email': email, 'mode' : mode, 'period' : period}
        email_delivery_config = EmailDeliveryConfig(processing=processing, delivery=delivery)

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
            delivery_config= email_delivery_config)

        #-------------------------------------------------------------------------------------
        # Set up things so that the user gets notified for the particular notification request
        #-------------------------------------------------------------------------------------

        notification_id =  self.create_notification(notification=notification_request, user_id = user_id)

        return notification_id

    def create_sms(self, name='', description='', event_type='', event_subtype='', origin='', origin_type='', user_id='', phone='', provider='', message_header='', parser=''):
        '''
         Creates a NotificationRequest object for the specified User Id. Associate the Notification
         resource with the user. Setup subscription and call back to send an sms to their phone
         @todo - is the user email automatically selected from the user id?
        '''

        if not phone:
            raise BadRequest("No phone provided.")
        if not provider:
            raise BadRequest("No provider provided.")

        #-------------------------------------------------------------------------------------
        # Build the sms delivery config
        #-------------------------------------------------------------------------------------
        #@todo get the process_definition_id - Find it when the service starts... bootstrap

        processing = {'message_header': message_header, 'parsing': parser}
        delivery = {'phone_number': phone, 'provider': provider}

        sms_delivery_config = SMSDeliveryConfig(processing=processing, delivery=delivery)

        #-------------------------------------------------------------------------------------
        # Create a notification object
        #-------------------------------------------------------------------------------------
        #@todo need to get the deliver_config
        notification_request = NotificationRequest(
            name=name,
            description=description,
            type=NotificationType.SMS,
            origin = origin,
            origin_type = origin_type,
            event_type=event_type,
            event_subtype = event_subtype ,
            delivery_config=sms_delivery_config)

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
