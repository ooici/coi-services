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

        # SMTP client configurations
        self.smtp_from = CFG.get_safe('server.smtp.from', 'data_alerts@oceanobservatories.org')
        self.smtp_host = CFG.get_safe('server.smtp.host')
        self.smtp_port = CFG.get_safe('server.smtp.port', 25)

        self.jinja_env = jinja2.Environment(loader=FileSystemLoader('res/templates'), trim_blocks=True, lstrip_blocks=True)

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

    def process_event(self, event, headers):
        """
        callback for the subscriber listening for all events
        """

        # create tuple key (origin,origin_type,event_type,event_subtype) to match against known notifications, keyed by same tuple
        # TODO: make sure these are None if not present? or are blank strings?
        origin = event.origin
        origin_type = event.origin_type
        event_type = event.type_
        event_subtype = event.sub_type
        key = (origin,origin_type,event_type,event_subtype)

        # users to notify with the notifications that have triggered the notification
        users = {} # users to be notified
        # loop the combinations of keys
        for k in _key_combinations(key):
            for (notification, user) in self.notifications.get(k, []):
                if user not in users:
                    users[user] = []
                users[user].append(notification)
        # we now have a dict, keyed by users that will be notified, each has a list of notifications that match this event        
        
        # send email
        if users:

            # message content for Jinja2 template (these fields are based on Event and same for all users/notifications)
            context = {}
            context['event_label'] = event_type_to_label.get(event.type_, event.type_) # convert to UX label if known
            context['origin_type'] = event.origin_type
            context['origin'] = event.origin
            context['url'] = None # TODO get the current endpoint to ooinet?
            context['timestamp'] = event.ts_created # TODO format to ISO?

            try:

                # use one SMTP connection for all emails
                smtp = _initialize_smtp() #TODO outside loop?

                # list of users getting notified of this Event
                for user in users:

                    # list of NotificationRequests for this user triggered by this event
                    for notification in users[user]:

                        # name of NotificationRequest, defaults to...NotificationRequest? I don't think name gets set anywhere? TODO
                        context['notification_name'] = notification.get('name', notification.type_)

                        # send message for each DeliveryConfiguration
                        for delivery_configuration in n.delivery_configurations:

                            # message from Jinja2 template (email or SMS)
                            smtp_msg = _to_mimetext(delivery_configuration, context)

                            # default to UserInfo.contact.email if no email specified in DeliveryConfiguration (not in _to_mimetext since may need user)
                            smtp_msg['To'] = delivery_configuration.email if delivery_configuration.email else user.contact.email

                            # TODO: use NOOP to check connection first?
                            smtp.sendmail(self.smtp_from, smtp_msg['To'], smtp_msg.as_string())

                        # TODO publish NotificationSentEvent (if successful?) - one per NotificationRequest

            finally:

                smtp.quit() # TODO what happens if we _initialize again, is there a dangling SMTP object? Should this be self.smtp and force quit on _initialize()?

    def _to_mimetext(delivery_configuration, context):
        if 1:# TODO template depends on NotificationRequest.delivery_configurations.mode (need branch for email and SMS)
            body = self.jinja_env.get_template('notification_realtime_email.txt').render(context)
        else:
            body = self.jinja_env.get_template('notification_realtime_sms.txt').render(context)
        # create MIMEText message
        smtp_msg = MIMEText(body)
        smtp_msg['Subject'] = 'OOINET ION Event Notification - %s' % context['event_label']
        smtp_msg['From'] = self.smtp_from
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
