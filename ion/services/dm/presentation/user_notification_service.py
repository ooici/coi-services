#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.iuser_notification_service import BaseUserNotificationService


class UserNotificationService(BaseUserNotificationService):

    def create_notification(self, notification={}, user_id=''):
        """Persists the provided NotificationRequest object for the specified Org id. Associate the Notification resource with the use The id string returned
        is the internal id by which NotificationRequest will be identified in the data store.

        @param notification    NotificationRequest
        @param user_id    str
        @retval notification_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        # Read UserNotification object with _id matching passed user id
        user = self.clients.resource_registry.read(user_id)
        if not user:
            raise NotFound("User %s does not exist" % user_id)

        # Persist UserNotification object and return object _id as OOI id
        notification_id, version = self.clients.resource_registry.create(notification)

        #associate the notification to the user
        #self.clients.resource_registry.create_association(user_id, AT.hasNotification, notification_id)

        #store the notification in datastore for quick reference
        # TBD

        return notification_id


    def update_notification(self, notification={}):
        """Updates the provided NotificationRequest object.  Throws NotFound exception if
        an existing version of NotificationRequest is not found.  Throws Conflict if
        the provided NotificationRequest object is not based on the latest persisted
        version of the object.

        @param notification    NotificationRequest
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        return self.clients.resource_registry.update(notification)

    def read_notification(self, notification_id=''):
        """Returns the NotificationRequest object for the specified notification id.
        Throws exception if id does not match any persisted NotificationRequest
        objects.

        @param notification_id    str
        @retval notification    NotificationRequest
        @throws NotFound    object with specified id does not exist
        """
        # Read UserNotification object with _id matching passed user id
        user_notification = self.clients.resource_registry.read(notification_id)
        if not user_notification:
            raise NotFound("UserNotification %s does not exist" % notification_id)
        return user_notification

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
        self.clients.resource_registry.delete(user_notification)

    def find_notifications_by_user(self, user_id=''):
        """Returns a list of notifications for a specific user. Will throw a not NotFound exception
        if none of the specified ids do not exist.

        @param user_id    str
        @retval notification_list    []
        @throws NotFound    object with specified id does not exist
        """

        #
        # Retrieve usr resource

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


  