#!/usr/bin/env python

'''
@package ion.processes.data.transforms
@file ion/processes/data/transforms/notification_worker.py
@author Brian McKenna <bmckenna@asascience.com>
@brief NotificationWorker class processes real-time notifications
'''

from datetime import datetime
from email.mime.text import MIMEText
import smtplib

from pyon.event.event import EventPublisher, EventSubscriber
from pyon.public import log, RT, OT, PRED, CFG

from interface.objects import DeliveryModeEnum, NotificationFrequencyEnum

from ion.core.process.transform import TransformEventListener
from ion.services.dm.utility.uns_utility_methods import load_notifications

from jinja2 import Environment, FileSystemLoader

import smtplib

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
        self.smtp_host = CFG.get_safe('server.smtp.host', 'localhost')
        self.smtp_port = CFG.get_safe('server.smtp.port', 25)

        # Jinja2 template environment
        self.jinja_env = Environment(loader=FileSystemLoader('res/templates'), trim_blocks=True, lstrip_blocks=True)

        super(NotificationWorker, self).on_init()

    def on_start(self):

        super(NotificationWorker,self).on_start()

        self.notifications = load_notifications()

        def _load_notifications_callback(msg, headers):
            """ local callback method so this can be used as callback in EventSubscribers """
            self.notifications = load_notifications() # from uns_utility_methods


        # the subscriber for the ReloadUserInfoEvent (new subscriber, subscription deleted, notifications changed, etc)
        self.reload_user_info_subscriber = EventSubscriber(
            event_type=OT.ReloadUserInfoEvent,
            #origin='UserNotificationService',
            callback=_load_notifications_callback
        )
        self.add_endpoint(self.reload_user_info_subscriber)

        # the subscriber for the UserInfo resource update events
        self.userinfo_rsc_mod_subscriber = EventSubscriber(
            event_type=OT.ResourceModifiedEvent,
            sub_type="UPDATE",
            origin_type="UserInfo",
            callback=_load_notifications_callback
        )
        self.add_endpoint(self.userinfo_rsc_mod_subscriber)

    def process_event(self, event, headers):
        """
        callback for the subscriber listening for all events
        """

        # create tuple key (origin,origin_type,event_type,event_subtype) for incoming event
        # use key to match against known notifications, keyed by same tuple (or combination of this tuple)
        origin = event.origin
        origin_type = event.origin_type
        event_type = event.type_
        event_subtype = event.sub_type
        key = (origin,origin_type,event_type,event_subtype)

        # users to notify with a list of the notifications that have been triggered by this Event
        users = {} # users to be notified
        # loop the combinations of keys (see _key_combinations below for explanation)
        # set() to eliminate duplicates when '' values exist in tuple
        for k in set(self._key_combinations(key)):
            if k in self.notifications:
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
            context['event_label'] = self.event_type_to_label(event.type_) # convert to UX label if known
            context['origin_type'] = event.origin_type
            context['origin'] = event.origin
            context['url'] = 'http://ooinet.oceanobservatories.org' # TODO get from CFG
            context['timestamp'] = datetime.utcfromtimestamp(float(event.ts_created)/1000.0).strftime('%Y-%m-%d %H:%M:%S (UTC)')

            # use one SMTP connection for all emails
            smtp = self._initialize_smtp()
            try:

                # loop through list of users getting notified of this Event
                for user in users:

                    # list of NotificationRequests for this user triggered by this event
                    for notification in users[user]:

                        # name of NotificationRequest, defaults to...NotificationRequest? I don't think name gets set anywhere? TODO, what's default?
                        context['notification_name'] = notification.name or notification.type_

                        # send message for each DeliveryConfiguration (this has mode and frequency to determine realtime, email or SMS)
                        for delivery_configuration in notification.delivery_configurations:

                            # skip if DeliveryConfiguration.frequency is DISABLED
                            if delivery_configuration.frequency == NotificationFrequencyEnum.DISABLED:
                                continue

                            # only process REAL_TIME
                            if delivery_configuration.frequency != NotificationFrequencyEnum.REAL_TIME:
                                continue

                            # default to UserInfo.contact.email if no email specified in DeliveryConfiguration
                            smtp_to = delivery_configuration.email if delivery_configuration.email else user.contact.email
                            context['smtp_to'] = smtp_to

                            # message from Jinja2 template (email or SMS)
                            try:

                                # email - MIMEText
                                if delivery_configuration.mode == DeliveryModeEnum.EMAIL:
                                    body = self.jinja_env.get_template('notification_realtime_email.txt').render(context)
                                    mime_text = MIMEText(body)
                                    mime_text['Subject'] = 'OOINet ION Event Notification - %s' % context['event_label']
                                    mime_text['From'] = self.smtp_from
                                    mime_text['To'] = context['smtp_to']
                                    smtp_msg = mime_text.as_string()

                                # SMS - just the template string
                                elif delivery_configuration.mode == DeliveryModeEnum.SMS:
                                    body = self.jinja_env.get_template('notification_realtime_sms.txt').render(context)
                                    smtp_msg = body

                                # unknown DeliveryMode
                                else:
                                    raise Exception #TODO specify unknown DeliveryModeEnum

                                smtp.sendmail(self.smtp_from, smtp_to, smtp_msg)

                            except Exception:
                                log.error('Failed to create message for notification %s', notification._id)
                                continue # skips this notification

                            # publish NotificationSentEvent - one per NotificationRequest (EventListener plugin NotificationSentScanner listens)
                            notification_max = int(CFG.get_safe("service.user_notification.max_daily_notifications", 1000))
                            self.event_publisher.publish_event(user_id=user._id, notification_id=notification._id, notification_max=notification_max)

            finally:
                smtp.quit()

    def _initialize_smtp(self):
        """ class method so user/pass/etc can be added """
        return smtplib.SMTP(self.smtp_host, self.smtp_port)

    def _key_combinations(self, key):
        """
        creates a list of all possible combinations of the tuple elements, from 1 member to len(key) members

        only the elements of each combination are set, '' elsewhere, all therefore have same length as key
        eg. ('a', 'b', 'c') -> (a) becomes ('a', '', '') and (b) becomes ('', 'b', '')

        extension of https://docs.python.org/2/library/itertools.html#itertools.combinations (Equivalent to section)
            differences:
            - loops all r from 1 to n
            - returns tuple of same length as n with '' as filler
        """
        n = len(key)
        # want all combinations of 1 to n
        for r in range(1,n+1):
            indices = range(r)
            # first combination is the first r values
            combination = ['']*n # creates a list of n ''s
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
                combination = ['']*n
                for i in indices:
                    combination[i] = key[i]
                yield tuple(combination)

    # TODO: REMOVE AND REPLACE WITHIN NotificationRequest
    #       this is a temporary hack so we're using UX (ion-ux) defined labels in the email
    #       see https://github.com/ooici/ion-ux/blob/master/static/js/ux-views-notifications.js#L1-L70
    def event_type_to_label(self, key):
        event_to_label = {
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
        # if not known, just return the event_type
        return event_to_label.get(key, key)
