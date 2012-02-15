#!/usr/bin/env python

__author__ = 'Bill Bollenbacher'
__license__ = 'Apache 2.0'


from pyon.util.log import log
from interface.services.dm.iuser_notification_service import BaseUserNotificationService
from pyon.public import RT, PRED, sys_name
from pyon.core.exception import BadRequest, NotFound
from pyon.event.event import EventError, EventSubscriber, EventRepository
from pyon.util.async import spawn
from gevent import Greenlet
import string, smtplib, time
from datetime import datetime
from email.mime.text import MIMEText

#ION_DATA_ALERTS_EMAIL_ADDRESS = 'ION_notifications@oceanobservatories.org'
ION_DATA_ALERTS_EMAIL_ADDRESS = 'wbollenbacher@ucsd.edu'
#ION_SMTP_SERVER = 'mail.oceanobservatories.org'
ION_SMTP_SERVER = 'localhost'


class NotificationEventSubscriber(EventSubscriber):
    
    def __init__(self, origin=None, event_name=None, callback=None):
        self.listener_greenlet = None
        self.subscriber = EventSubscriber(origin=origin, event_name=event_name, callback=callback)
        
    def start_listening(self):
        self.listener_greenlet = spawn(self.subscriber.listen)
        self.subscriber._ready_event.wait(timeout=5)
        
    def stop_listening(self):
        self.listener_greenlet.kill(exception=Greenlet.GreenletExit, block=False)
        

class Notification(object):
    
    def  __init__(self, notification=None, subscriber=None):
        self.notification = notification
        self.subscriber = subscriber
        self.notification_id = None
        
    def set_notification_id(self, id=None):
        self.notification_id = id
        
    def kill_subscriber(self):
        self.subscriber.stop_listening()
        del self.subscriber
        

class UserEventProcessor(object):
    
    def __init__(self, user_id=None, email_addr=None):
        self.user_id = user_id
        self.user_email_addr = email_addr
        self.notifications = []
        log.debug("UserEventProcessor.__init__(): email for user %s set to %s" %(self.user_id, self.user_email_addr))
    
    def subscription_callback(self, *args, **kwargs):
        # TODO: send event notification to user's email address
        log.debug("UserEventProcessor.subscription_callback(): args=%s, kargs=%s" %(str(args), str(kwargs)))
        log.debug("event type = " + str(args[0]._get_type()))
        log.debug("args[0]=" + str(args[0]))
        log.debug("origin=%s, description=%s, ts=%s" %(args[0].origin, args[0].description, args[0].ts_created))
        
        origin = args[0].origin
        event = str(args[0]._get_type())
        description = args[0].description
        time_stamp = str( datetime.fromtimestamp(time.mktime(time.gmtime(args[0].ts_created))))

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
                            "To modify or remove notifications about this event, please access My Notifications Settings in the ION Web UI."), 
                           "\r\n")
        SUBJECT = "(SysName: " + sys_name + ") ION event " + event + " from " + origin
        FROM = ION_DATA_ALERTS_EMAIL_ADDRESS
        TO = self.user_email_addr
        msg = MIMEText(BODY)
        msg['Subject'] = SUBJECT
        msg['From'] = FROM
        msg['To'] = TO
        smtp_client = smtplib.SMTP(ION_SMTP_SERVER)
        #smtp_client.sendmail([TO], [FROM], msg.as_string())
        smtp_client.sendmail(TO, FROM, msg.as_string())

    
    def add_notification(self, notification=None, cc_node=None):
        for n in self.notifications:
            if n.notification == notification:
                raise BadRequest("UserEventProcessor.add_notification(): notification " + 
                                 str(notification) + " already exists for " + self.user_id)                
        # setup subscription using subscription_callback()
        subscription = NotificationEventSubscriber(origin=notification.origin_list[0],
                                                   event_name=notification.events_list[0], 
                                                   callback=self.subscription_callback)
        # save notification and subscription info in notifications list
        n = Notification(notification, subscription)
        self.notifications.append(n)
        # start the event listener
        subscription.start_listening()
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
            # Retrieve the user's user_info object
            objects, assocs = self.clients.resource_registry.find_objects(user_id, PRED.hasInfo, RT.UserInfo)
            if not objects:
                raise NotFound("UserNotificationService.create_notification(): No user_info for user " + user_id)
            if len(objects) != 1:
                raise BadRequest("UserNotificationService.create_notification(): there should be only ONE user_info for " + user_id)
            user_info = objects[0]
            if not user_info.contact.email or user_info.contact.email == '':
                raise NotFound("UserNotificationService.create_notification(): No email address in user_info for user " + user_id)
            # create event processor for user
            self.user_event_processors[user_id] = UserEventProcessor(user_id, user_info.contact.email)
            log.debug("UserNotificationService.create_notification(): added event processor " + str(self.user_event_processors[user_id]))
        
        # add notification to user's event_processor
        user_event_processor = self.user_event_processors[user_id]       
        Notification = user_event_processor.add_notification(notification, self.container.node)

        # Persist Notification object 
        notification_id, version = self.clients.resource_registry.create(notification)

        # give user_event_processor id of notification
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
        # get the user that this notification is associated with 
        subjects, assocs = self.clients.resource_registry.find_subjects(RT.UserIdentity, PRED.hasNotification, notification._id)
        if not subjects:
            raise NotFound("UserNotificationService.delete_notification(): No user for notification " + notification._id)
        if len(subjects) != 1:
            raise BadRequest("UserNotificationService.delete_notification(): there should be only ONE user for " + notification._id)
        user_id = subjects[0]._id
        
        # delete old notification
        self.delete_notification(notification._id)
        
        # add new notification after 'deleting' the id and rev
        del notification._id
        del notification._rev
        self.create_notification(notification, user_id)

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

    def find_events(self, origin='', type='', min_datetime='', max_datetime=''):
        """Returns a list of events that match the specified search criteria. Will throw a not NotFound exception
        if no events exist for the given parameters.

        @param origin    str
        @param type    str
        @param min_datetime    str
        @param max_datetime    str
        @retval event_list    []
        @throws NotFound    object with specified paramteres does not exist
        """
        pass


  