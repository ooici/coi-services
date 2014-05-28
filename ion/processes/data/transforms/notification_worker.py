#!/usr/bin/env python

'''
@package ion.processes.data.presentation
@file ion/processes/data/transforms/notification_worker.py
@author Swarbhanu Chatterjee
@brief NotificationWorker Class. An instance of this class acts as an notification worker.
'''

from pyon.public import log, RT, OT, PRED
from pyon.util.async import spawn
from pyon.core.exception import BadRequest, NotFound
from ion.core.process.transform import TransformEventListener
from pyon.event.event import EventSubscriber
from ion.services.dm.utility.uns_utility_methods import send_email, calculate_reverse_user_info
from ion.services.dm.utility.uns_utility_methods import setting_up_smtp_client, check_user_notification_interest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

import gevent, time
from gevent import queue

class NotificationWorker(TransformEventListener):
    """
    Instances of this class acts as a Notification Worker.
    """
    def on_init(self):

        # clients needed
        self.resource_registry = ResourceRegistryServiceClient() # TODO proper way to do this now?

        #TODO SMTP client
        self.smtp_client = None #TODO
        self.smtp_from = None # TODO CFG or hardcoded default?

        super(NotificationWorker, self).on_init()

    def on_start(self):

        super(NotificationWorker,self).on_start()

        _load_notifications()

        # reload notifications (new subscriber, subscription deleted, notifications changed, etc)
        # the subscriber for the ReloadUserInfoEvent
        self.reload_user_info_subscriber = EventSubscriber(
            event_type=OT.ReloadUserInfoEvent,
            origin='UserNotificationService',
            callback=_load_notifications
        )
        self.add_endpoint(self.reload_user_info_subscriber)

        # the subscriber for the UserInfo resource update events
        self.userinfo_rsc_mod_subscriber = EventSubscriber(
            event_type=OT.ResourceModifiedEvent,
            sub_type="UPDATE",
            origin_type="UserInfo",
            callback=_load_notifications
        )
        self.add_endpoint(self.userinfo_rsc_mod_subscriber)

    def process_event(self, msg, headers):
        """
        callback for the subscriber listening for all events
        """

        # create tuple key (origin,origin_type,event_type,event_subtype) to match against known notifications
        # TODO: make sure these are None if not present?
        origin = msg.origin
        origin_type = msg.origin_type
        event_type = msg.type_
        event_subtype = msg.sub_type
        key = (origin,origin_type,event_type,event_subtype)

        # users to notify with the notifications that have triggered the notification
        users = {} # users to be notified
        # loop the combinations of keys #TODO method for key_combinations (combinatorics)
        for k in key_combinations(key):
            for (notification, user) in self.notifications.get(k, []):
                if user not in users:
                    users[user] = []
                users[user].append(notification)
        # we now have a dict, keyed by users that will be notified, each has a list of notifications that match this event        

        # send email
        for user in users:

            # message from Jinja2 template (email or SMS) [we have the Event, the NotificationRequest and UserInfo]
            msg = _to_mimetext(event, notification, user_info) #TODO: are these params avail? TODO: notification could be a list (is a list?)

            # who's getting the message?
            smtp_to = user['user_contact'].email # TODO should we first look at or also send to the delivery_configurations.email?
            msg['To'] = smtp_to

            # TODO: the 2nd parameter to sendmail below is array of 'TO addresses', this should be array of user email plus delivery_configurations.email?
            try:
                self.smtp_client.sendmail(self.smtp_from, [smtp_to], msg.as_string())
            except: # Can be due to a broken connection... try to create a connection (TODO - is this a HACK?)
                self.smtp_client = setting_up_smtp_client() # reconnect
                log.debug("Connect again...message received after ehlo exchange: %s", str(smtp_client.ehlo()))
                self.smtp_client.sendmail(self.smtp_from, [smtp_to], msg.as_string())

            # TODO publish NotificationSentEvent (if successful?)

    def _to_mimetext(event, notification, user_info):

        # TODO subject
        subject = None

        # message from Jinja2 template (email or SMS) [we have the Event, the NotificationRequest and UserInfo]
        body = None # Jinja2 this!

        # TODO maybe move to utility
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = self.smtp_from
        msg['To'] = msg_recipient

        return msg

    def _load_notifications(self):
        """ local method so can be used as callback in EventSubscribers """
        self.notifications = load_notifications() # from uns_utility_methods

