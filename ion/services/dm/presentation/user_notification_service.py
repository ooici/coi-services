#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from pyon.util.log import log
from interface.services.dm.iuser_notification_service import BaseUserNotificationService
from pyon.public import RT, PRED
from pyon.core.exception import BadRequest, NotFound

class Notification(object):
    notification = None
    subscription = None

class UserEventProcessor(object):
    user_id = None
    user_email_addr = None
    notifications = []
    
    def __init__(self, user_id=None, email_addr=None):
        self.user_id = user_id
        self.user_email_addr = email_addr
        log.debug("UserEventProcessor.__init__(): email for user %s set to %s" %(self.user_id, self.user_email_addr))
    
    def subscription_callback(self):
        # TODO: send event notification to user's email address
        pass
    
    def add_notification(self, notification=None):
        for n in self.notifications:
            if n == notification:
                raise BadRequest("UserEventProcessor.add_notification(): notification " + 
                                 str(notification) + " already exists for " + self.user_id)                
        self.notifications.append(notification)
        # setup subscription using subscription_callback()
        log.debug("UserEventProcessor.add_notification(): adding notification " + str(notification) + " to user " + self.user_id)
    
    def remove_notification(self, notification=None):
        try:
            self.notifications.remove(notification)  
        except:      
            raise BadRequest("UserEventProcessor.remove_notification(): notification " +
                             str(notification) + " does not exist for " + self.user_id)                
        # remove subscription
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

        # Persist Notification object 
        notification_id, version = self.clients.resource_registry.create(notification)

        # associate the notification to the user
        self.clients.resource_registry.create_association(user_id, PRED.hasNotification, notification_id)

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
            self.user_event_processors[user_id] = UserEventProcessor(user_id, user_info.contact.email)
            log.debug("UserNotificationService.create_notification(): added event processor " + str(self.user_event_processors[user_id]))
        
        user_event_processor = self.user_event_processors[user_id]
        
        user_event_processor.add_notification(notification)

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
        # TODO: get existing notification from RR
        #       get user_id from asso
        #       check for user in self.user_event_processors
        #       if user is found then remove the existing notification
        update_result = self.clients.resource_registry.update(notification)
        # TODO: if update was successful then update user's event processor
        #       add the new notification

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
        # Read and delete specified UserNotification object
        user_notification = self.clients.resource_registry.read(notification_id)
        if not user_notification:
            raise NotFound("UserNotification %s does not exist" % notification_id)

        #first get the user that this notification is associated with 
        #remove the notification from the user's entry in the self.user_event_processors list
        #if it's the last notification for the user the delete the user from the self.user_event_processors list
        #then delete the association
        #finally delete the notification

        self.clients.resource_registry.delete(user_notification)

    def find_notifications_by_user(self, user_id=''):
        """Returns a list of notifications for a specific user. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param user_id    str
        @retval notification_list    []
        @throws NotFound    object with specified id does not exist
        """
        # find all associations to this user of type hasNotification
        # ...

        # return the list
        pass

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


  