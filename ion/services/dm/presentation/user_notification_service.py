#!/usr/bin/env python

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'


import string, smtplib, time
from datetime import datetime
from email.mime.text import MIMEText
from gevent import Greenlet

from pyon.core.exception import BadRequest, NotFound
from pyon.event.event import EventError, EventSubscriber, EventPublisher
from pyon.public import RT, PRED, get_sys_name, Container, CFG, IonObject
from pyon.util.async import spawn
from pyon.util.log import log

from interface.services.dm.iuser_notification_service import BaseUserNotificationService

# the 'from' email address for notification emails
ION_NOTIFICATION_EMAIL_ADDRESS = 'ION_notifications-do-not-reply@oceanobservatories.org'
# the default smtp server
ION_SMTP_SERVER = 'mail.oceanobservatories.org'

"""
For every user that has existing notification requests (who has called create_notification()) the UNS will contain a local 
UserEventProcessor instance that contains the user's notification information (email address) and all of the user's 
notifications (along with their event subscribers).  The UserEventProcessors are maintained local to the UNS in a dictionary 
indexed by the user's resourceID.  When a notification is created the user's UserEventProcessor will be created if it 
doesn't already exist , and it will be deleted when the user deletes their last notification.

The user's UserEventProcessor will encapsulate a list of notification objects that the user has requested, 
along with user information needed for send notifications (email address for LCA).  
It will also encapsulate a subscriber callback method that is passed to all event subscribers for each notification 
the user has created.

Each notification object will encapsulate the notification information and a list of event subscribers (only one for LCA) 
that listen for the events in the notification.
"""

class NotificationEventSubscriber(EventSubscriber):
    # encapsulates the event subscriber and the event 'listen loop' greenlet
    # implements methods to start/stop the listener
    
    def __init__(self, origin=None, event_type=None, callback=None):
        self.listener_greenlet = None
        self.subscriber = EventSubscriber(origin=origin, event_type=event_type, callback=callback)
        
    def start_listening(self):
        self.listener_greenlet = spawn(self.subscriber.listen)
        self.subscriber._ready_event.wait(timeout=5)     # not sure this is needed
        
    def stop_listening(self):
        if self.listener_greenlet:
            self.listener_greenlet.kill(exception=Greenlet.GreenletExit, block=False)
        

class Notification(object):
    # encapsulates a notification's info and it's event subscriber
    
    def  __init__(self, notification=None, subscriber_callback=None):
        self.notification = notification
        # setup subscription using subscription_callback()
        # TODO: make this walk the lists and set up a subscriber for every pair of
        # origin/event.  This will require a list to hold all the subscribers so they can
        # be started and killed
        self.subscriber = NotificationEventSubscriber(origin=notification.origin_list[0],
                                                      event_type=notification.events_list[0],
                                                      callback=subscriber_callback)
        self.notification_id = None
        
    def set_notification_id(self, id=None):
        self.notification_id = id
        
    def start_subscriber(self):
        self.subscriber.start_listening()

    def kill_subscriber(self):
        self.subscriber.stop_listening()
        del self.subscriber
        

class UserEventProcessor(object):
    # Encapsulates the user's info and a list of all the notifications they have
    # It also contains the callback that is passed to all event subscribers for this user's notifications
    # If the callback gets called, then this user had a notification for that event.
    
    def __init__(self, user_id=None, email_addr=None, smtp_server=None):
        self.user_id = user_id
        self.user_email_addr = email_addr
        self.smtp_server = smtp_server
        self.notifications = []
        log.debug("UserEventProcessor.__init__(): email for user %s set to %s" %(self.user_id, self.user_email_addr))
    
    def subscription_callback(self, *args, **kwargs):
        # this callback is given to all the event subscribers that this user wants notifications for
        # if this callback gets called the user in this processor should get an email
        log.debug("UserEventProcessor.subscription_callback(): args[0]=" + str(args[0]))
        log.debug("event type = " + str(args[0]._get_type()))
        
        origin = args[0].origin
        event = str(args[0]._get_type())
        description = args[0].description
        time_stamp = str( datetime.fromtimestamp(time.mktime(time.gmtime(float(args[0].ts_created)/1000))))

        # build the email from the event content
        BODY = string.join(("Event: %s" %  event,
                            "",
                            "Originator: %s" %  origin,
                            "",
                            "Description: %s" %  description,
                            "",
                            "Time stamp: %s" %  time_stamp,
                            "",
                            "You received this notification from ION because you asked to be notified about this event from this source. ",
                            "To modify or remove notifications about this event, please access My Notifications Settings in the ION Web UI.",
                            "Do not reply to this email.  This email address is not monitored and the emails will not be read."), 
                           "\r\n")
        SUBJECT = "(SysName: " + get_sys_name() + ") ION event " + event + " from " + origin
        FROM = ION_NOTIFICATION_EMAIL_ADDRESS
        TO = self.user_email_addr
        msg = MIMEText(BODY)
        msg['Subject'] = SUBJECT
        msg['From'] = FROM
        msg['To'] = TO
        log.debug("UserEventProcessor.subscription_callback(): sending email to %s via %s" %(TO, self.smtp_server))
        try:
            smtp_client = smtplib.SMTP(self.smtp_server)
        except Exception as ex:
            log.warning("UserEventProcessor.subscription_callback(): failed to connect to SMTP server %s <%s>" %(ION_SMTP_SERVER, ex))
            return
        try:
            smtp_client.sendmail(FROM, TO, msg.as_string())
        except Exception as ex:
            log.warning("UserEventProcessor.subscription_callback(): failed to send email to %s <%s>" %(TO, ex))           
   
    def add_notification(self, notification=None):
        for n in self.notifications:
            if n.notification == notification:
                raise BadRequest("UserEventProcessor.add_notification(): notification " + 
                                 str(notification) + " already exists for " + self.user_id)                
        # create and save notification in notifications list
        n = Notification(notification, self.subscription_callback)
        self.notifications.append(n)
        # start the event subscriber listening
        n.start_subscriber()
        log.debug("UserEventProcessor.add_notification(): added notification " + str(notification) + " to user " + self.user_id)
        return n
    
    def remove_notification(self, notification_id=None):
        found_notification = False
                
        for n in self.notifications:
            if n.notification_id == notification_id:
                self.notifications.remove(n)
                found_notification = True  
        if not found_notification:      
            raise BadRequest("UserEventProcessor.remove_notification(): notification " +
                             str(notification_id) + " does not exist for " + self.user_id)                
        # stop subscription
        n.kill_subscriber()
        log.debug("UserEventProcessor.remove_notification(): removed notification " + str(n.notification) + " from user " + self.user_id)
        # return the number of notifications left for this user
        return len(self.notifications)
    
    def __str__(self):
        return str(self.__dict__)
   

class UserNotificationService(BaseUserNotificationService):
    
    user_event_processors = {}
    
    def __init__(self):
        # get the event repository from the CC
        self.event_repo = Container.instance.event_repository
        BaseUserNotificationService.__init__(self)
    
    def on_start(self):
        # get the smtp server address if configured
        self.smtp_server = self.CFG.get('smtp_server', ION_SMTP_SERVER)        
        
        # load event originators, types, and table
        self.event_originators = CFG.event.originators        
        self.event_types = CFG.event.types
        self.event_table = {}
        for originator in self.event_originators:
            try:
                self.event_table[originator] = CFG.event[originator]
            except:
                log.warning("UserNotificationService.on_start(): event originator <%s> not found in configuration" %originator)
        log.debug("UserNotificationService.on_start(): event_originators=%s" %str(self.event_originators))        
        log.debug("UserNotificationService.on_start(): event_types=%s" %str(self.event_types)) 
        log.debug("UserNotificationService.on_start(): event_table=%s" %str(self.event_table)) 
        
        """
        # code to dump resource types, delete this from file for release
        rt = sorted(RT)
        for r in rt:
            print("RT.%s=%s" %(r, str(RT[r])))        
        """
        
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
        # check that user exists
        user = self.clients.resource_registry.read(user_id)
        if not user:
            raise NotFound("UserNotificationService.create_notification(): User %s does not exist" % user_id)

        if user_id not in self.user_event_processors:
            # user does not have an event processor, so create one
            # Retrieve the user's user_info object to get their email address
            objects, assocs = self.clients.resource_registry.find_objects(user_id, PRED.hasInfo, RT.UserInfo)
            if not objects:
                raise NotFound("UserNotificationService.create_notification(): No user_info for user " + user_id)
            if len(objects) != 1:
                raise BadRequest("UserNotificationService.create_notification(): there should be only ONE user_info for " + user_id)
            user_info = objects[0]
            if not user_info.contact.email or user_info.contact.email == '':
                raise NotFound("UserNotificationService.create_notification(): No email address in user_info for user " + user_id)
            # create event processor for user
            self.user_event_processors[user_id] = UserEventProcessor(user_id, user_info.contact.email, self.smtp_server)
            log.debug("UserNotificationService.create_notification(): added event processor " + str(self.user_event_processors[user_id]))
        
        # add notification to user's event_processor
        Notification = self.user_event_processors[user_id].add_notification(notification)

        # Persist Notification object 
        notification_id, version = self.clients.resource_registry.create(notification)

        # give the user's user_event_processor the id of the notification
        Notification.set_notification_id(notification_id)
        
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
        # Read existing Notification object and see if it exists
        old_notification = self.clients.resource_registry.read(notification._id)
        if not old_notification:
            raise NotFound("UserNotificationService.update_notification(): Notification %s does not exist" % notification._id)

        # get the user that this notification is associated with 
        subjects, assocs = self.clients.resource_registry.find_subjects(RT.UserIdentity, PRED.hasNotification, notification._id)
        if not subjects:
            raise NotFound("UserNotificationService.delete_notification(): No user for notification " + notification._id)
        if len(subjects) != 1:
            raise BadRequest("UserNotificationService.delete_notification(): there should be only ONE user for " + notification._id)
        user_id = subjects[0]._id
        
        #remove the old notification from the user's entry in the self.user_event_processors list
        if user_id not in self.user_event_processors:
            log.warning("UserNotificationService.delete_notification(): user %s not found in user_event_processors list" % user_id)
        user_event_processor = self.user_event_processors[user_id]
        user_event_processor.remove_notification(notification._id)
        
        # add updated notification to user's event processor
        Notification = user_event_processor.add_notification(notification)
        Notification.set_notification_id(notification._id)
        
        # finally update the notification in the RR
        self.clients.resource_registry.update(notification)

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
        # Read specified Notification object and see if it exists
        notification = self.clients.resource_registry.read(notification_id)
        if not notification:
            raise NotFound("UserNotificationService.delete_notification(): Notification %s does not exist" % notification_id)

        #now get the user that this notification is associated with 
        subjects, assocs = self.clients.resource_registry.find_subjects(RT.UserIdentity, PRED.hasNotification, notification_id)
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

    def find_notifications_by_user(self, user_id=''):
        """Returns a list of notifications for a specific user. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param user_id    str
        @retval notification_list    []
        @throws NotFound    object with specified id does not exist
        """
        objects, assocs = self.clients.resource_registry.find_objects(user_id, PRED.hasNotification, RT.NotificationRequest)
        # return the list
        return objects

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
        """
        return self.event_repo.find_events(event_type=type, 
                                           origin=origin, 
                                           start_ts=min_datetime, 
                                           end_ts=max_datetime,
                                           descending=descending,
                                           limit=limit)

    def find_event_types_for_resource(self, resource_id=''):
        resource_object = self.clients.resource_registry.read(resource_id)
        if not resource_object:
            raise NotFound("UserNotificationService.find_event_types_for_resource(): resource with id %s does not exist" % resource_id)
        resource_type = type(resource_object).__name__.lower()
        log.debug("UserNotificationService.find_event_types_for_resource(): resource type = " + resource_type)
        if resource_type in self.event_table:
            return self.event_table[resource_type]
        log.debug("UserNotificationService.find_event_types_for_resource(): resource type %s not an event originator" %resource_type)
        return []

    def generate_event(self, event_type='Event', origin='', origin_type='', event_fields=None, sub_type='', description=''):
        try:
            event_obj = IonObject(event_type)
        except Exception:
            raise NotFound("Event type %s unknown" % event_type)

        if not origin or type(origin) is not str:
            raise BadRequest("Argument value of origin illegal")
        if event_fields and type(event_fields) is not dict:
            raise BadRequest("Argument value of event_fields illegal")

        pub = EventPublisher()
        event_fields = event_fields or {}
        success = pub.publish_event(event_type=event_type, sub_type=sub_type, origin=origin, description=description, **event_fields)
        if not success:
            raise BadRequest("Cannot publish event")
