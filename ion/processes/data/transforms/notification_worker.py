#!/usr/bin/env python

'''
@package ion.processes.data.transforms
@file ion/processes/data/transforms/notification_worker.py
@author Brian McKenna <bmckenna@asascience.com>
@brief NotificationWorker class processes real-time notifications
'''

from pyon.event.event import EventPublisher, EventSubscriber
from pyon.public import log, RT, OT, PRED
from ion.core.process.transform import TransformEventListener

from jinja2 import Environment, FileSystemLoader

class NotificationWorker(TransformEventListener):
    """
    Instances of this class acts as a Notification Worker.
    """
    def on_init(self):

        # clients
        self.resource_registry = self.container.resource_registry
        self.event_publisher = EventPublisher(OT.NotificationSentEvent)

        # SMTP client configurations
        self.smtp_from = CFG.get_safe('server.smtp.from', 'data_alerts@oceanobservatories.org')
        self.smtp_host = CFG.get_safe('server.smtp.host')
        self.smtp_port = CFG.get_safe('server.smtp.port', 25)

        # Jinja2 template environment
        self.jinja_env = Environment(loader=FileSystemLoader('res/templates'), trim_blocks=True, lstrip_blocks=True)

        super(NotificationWorker, self).on_init()

    def on_start(self):

        super(NotificationWorker,self).on_start()

        _load_notifications()

        # the subscriber for the ReloadUserInfoEvent (new subscriber, subscription deleted, notifications changed, etc)
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

    def process_event(self, event, headers):
        """
        callback for the subscriber listening for all events
        """

        # create tuple key (origin,origin_type,event_type,event_subtype) for incoming event
        # use key to match against known notifications, keyed by same tuple (or combination of this tuple)
        # TODO: make sure these are None if not present? or are they blank strings?
        origin = event.origin
        origin_type = event.origin_type
        event_type = event.type_
        event_subtype = event.sub_type
        key = (origin,origin_type,event_type,event_subtype)

        # users to notify with a list of the notifications that have been triggered by this Event
        users = {} # users to be notified
        # loop the combinations of keys (see _key_combinations below for explanation)
        for k in _key_combinations(key):
            for (notification, user) in self.notifications.get(k, []):
                # notification has been triggered
                if user not in users:
                    users[user] = []
                users[user].append(notification)
        # we now have a dict, keyed by users that will be notified, each user has a list of notifications triggered by this event
        
        # send email
        if users:

            # message content for Jinja2 template (these fields are based on Event and thus are the same for all users/notifications)
            context = {}
            context['event_label'] = event_type_to_label.get(event.type_, event.type_) # convert to UX label if known
            context['origin_type'] = event.origin_type
            context['origin'] = event.origin
            context['url'] = None # TODO get the current endpoint to ooinet?
            context['timestamp'] = event.ts_created # TODO format to ISO?

            # use one SMTP connection for all emails
            try:

                smtp = _initialize_smtp() #TODO move outside loop?

                # loop through list of users getting notified of this Event
                for user in users:

                    # list of NotificationRequests for this user triggered by this event
                    for notification in users[user]:

                        # name of NotificationRequest, defaults to...NotificationRequest? I don't think name gets set anywhere? TODO, what's default?
                        context['notification_name'] = notification.get('name', notification.type_)

                        # send message for each DeliveryConfiguration (this has mode and frequency to determine realtime, email or SMS)
                        for delivery_configuration in notification.delivery_configurations:

                            # default to UserInfo.contact.email if no email specified in DeliveryConfiguration
                            smtp_to = delivery_configuration.email if delivery_configuration.email else user.contact.email
                            context['smtp_to'] = smtp_to

                            # message from Jinja2 template (email or SMS)
                            smtp_msg = _mimetext(delivery_configuration, context)

                            # TODO: use NOOP to check connection first?
                            # TODO: determine if sendmail was successful?
                            smtp.sendmail(self.smtp_from, smtp_to, smtp_msg.as_string())

                        # publish NotificationSentEvent - one per NotificationRequest
                        self.event_publisher.publish_event(user_id = user._id, notification_id = notification._id, notification_max = notification.max) #TODO add max

            finally:

                smtp.quit() # TODO what happens if we _initialize_smtp() again, is there a dangling SMTP object? Should this be self.smtp and force quit on first line of _initialize_smtp()?

    def _mimetext(delivery_configuration, context):
        if 1:# TODO template depends on NotificationRequest.delivery_configurations.mode (need branch for email and SMS)
            body = self.jinja_env.get_template('notification_realtime_email.txt').render(context)
        else:
            body = self.jinja_env.get_template('notification_realtime_sms.txt').render(context)
        # create MIMEText message
        smtp_msg = MIMEText(body)
        smtp_msg['Subject'] = 'OOINet ION Event Notification - %s' % context['event_label']
        smtp_msg['From'] = self.smtp_from
        smtp_msg['To'] = context['smtp_to']
        return smtp_msg

    def _load_notifications(self):
        """ simple local method so this can be used as callback in EventSubscribers """
        self.notifications = load_notifications() # from uns_utility_methods

    def _initialize_smtp(self):
        return smtplib.SMTP(self.smtp_host, self.smtp_port)

    def _key_combinations(key):
        """
        creates a list of all possible combinations of the tuple elements, from 1 member to len(key) members

        only the elements of each combination are set, None elsewhere, all therefore have same length as key
        eg. ('a', 'b', 'c') -> (a) becomes ('a', None, None) and (b) becomes (None, 'b', None)

        extension of https://docs.python.org/2/library/itertools.html#itertools.combinations (Equivalent to section)
            differences:
            - loops all r from 1 to n
            - returns tuple of same length as n with None as filler
        """
        n = len(key)
        # want all combinations of 1 to n
        for r in range(1,n+1):
            indices = range(r)
            # first combination is the first r values
            combination = [None]*n # creates a list of n Nones
            for i in indices: 
                combination[i] = key[i]
            yield tuple(combination)
            # remaining combinations
            while True:
                for i in reversed(range(r)):
                    if indices[i] != i + n - r:
                        break
                else:
                    break
                indices[i] += 1
                for j in range(i+1, r):
                    indices[j] = indices[j-1] + 1
                combination = [None]*n
                for i in indices:
                    combination[i] = key[i]
                yield tuple(combination)

    # TODO: REMOVE AND REPLACE WITHIN NotificationRequest
    #       this is a temporary hack so we're using UX (ion-ux) defined labels in the email
    #       see https://github.com/ooici/ion-ux/blob/master/static/js/ux-views-notifications.js#L1-L70
    event_type_to_label = {
        'ResourceAgentConnectionLostErrorEvent': 'Communication Lost/Restored', 
        'ResourceAgentErrorEvent': 'Device Error', 
        'ResourceIssueReportedEvent': 'Issue reported', 
        'ResourceLifecycleEvent': 'Lifecycle state change', 
        'ResourceAgentStateEvent': 'Agent operational state change', 
        'ResourceAgentResourceStateEvent': 'Device operational state change', 
        'DeviceOperatorEvent': 'Operator event on device',
        'ResourceOperatorEvent': 'Operator event on resource', 
        'ParameterQCEvent': 'QC alert', 
        'OrgNegotiationInitiatedEvent': 'Request received', 
        'ResourceModifiedEvent': 'Resource modified', 
        'DeviceStatusAlertEvent': 'Status alert/change', 
        'DeviceAggregateStatusEvent': 'Aggregate Status alert/change', 
    }
