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
        self.user_info = {}
        self.resource_registry = ResourceRegistryServiceClient()
        self.q = gevent.queue.Queue()

        super(NotificationWorker, self).on_init()

    def test_hook(self, user_info, reverse_user_info ):
        '''
        This method exists only to facilitate the testing of the reload of the user_info dictionary
        '''
        self.q.put((user_info, reverse_user_info))

    def on_start(self):
        super(NotificationWorker,self).on_start()

        self.reverse_user_info = None
        self.user_info = None

        #------------------------------------------------------------------------------------
        # Start by loading the user info and reverse user info dictionaries
        #------------------------------------------------------------------------------------

        try:
            self.user_info = self.load_user_info()
            self.reverse_user_info =  calculate_reverse_user_info(self.user_info)

            log.debug("On start up, notification workers loaded the following user_info dictionary: %s" % self.user_info)
            log.debug("The calculated reverse user info: %s" % self.reverse_user_info )

        except NotFound as exc:
            if exc.message.find('users_index') > -1:
                log.warning("Notification workers found on start up that users_index have not been loaded yet.")
            else:
                raise NotFound(exc.message)

        #------------------------------------------------------------------------------------
        # Create an event subscriber for Reload User Info events
        #------------------------------------------------------------------------------------

        def reload_user_info(event_msg, headers):
            '''
            Callback method for the subscriber to ReloadUserInfoEvent
            '''

            try:
                self.user_info = self.load_user_info()
            except NotFound:
                log.warning("ElasticSearch has not yet loaded the user_index.")

            self.reverse_user_info =  calculate_reverse_user_info(self.user_info)
            self.test_hook(self.user_info, self.reverse_user_info)

            #log.debug("After a reload, the user_info: %s" % self.user_info)
            #log.debug("The recalculated reverse_user_info: %s" % self.reverse_user_info)

        # the subscriber for the ReloadUSerInfoEvent
        self.reload_user_info_subscriber = EventSubscriber(
            event_type=OT.ReloadUserInfoEvent,
            origin='UserNotificationService',
            callback=reload_user_info
        )

        self.add_endpoint(self.reload_user_info_subscriber)


        # the subscriber for the UserInfo resource update events
        self.userinfo_rsc_mod_subscriber = EventSubscriber(
            event_type=OT.ResourceModifiedEvent,
            sub_type="UPDATE",
            origin_type="UserInfo",
            callback=reload_user_info
        )

        self.add_endpoint(self.userinfo_rsc_mod_subscriber)

    def process_event(self, msg, headers):
        """
        Callback method for the subscriber listening for all events
        """
        #------------------------------------------------------------------------------------
        # From the reverse user info dict find out which users have subscribed to that event
        #------------------------------------------------------------------------------------

        user_ids = []
        if self.reverse_user_info:
            user_ids = check_user_notification_interest(event = msg, reverse_user_info = self.reverse_user_info)

            #log.debug('process_event  user_ids: %s', user_ids)

            #log.debug("Notification worker found interested users %s" % user_ids)

        #------------------------------------------------------------------------------------
        # Send email to the users
        #------------------------------------------------------------------------------------

        for user_id in user_ids:
            msg_recipient = self.user_info[user_id]['user_contact'].email
            self.smtp_client = setting_up_smtp_client()
            send_email(event=msg,
                       msg_recipient=msg_recipient,
                       smtp_client=self.smtp_client,
                       rr_client=self.resource_registry)
            self.smtp_client.quit()

    def get_user_notifications(self, user_info_id=''):
        """
        Get the notification request objects that are subscribed to by the user

        @param user_info_id str

        @retval notifications list of NotificationRequest objects
        """
        notifications = []
        user_notif_req_objs, _ = self.resource_registry.find_objects(
            subject=user_info_id, predicate=PRED.hasNotification, object_type=RT.NotificationRequest, id_only=False)

        #log.debug("get_user_notifications Got %s notifications, for the user: %s", len(user_notif_req_objs), user_info_id)

        for notif in user_notif_req_objs:
            # do not include notifications that have expired
            if notif.temporal_bounds.end_datetime == '':
                notifications.append(notif)

        return notifications

    def load_user_info(self):
        '''
        Method to load the user info dictionary used by the notification workers and the UNS

        @retval user_info dict
        '''

        users, _ = self.resource_registry.find_resources(restype= RT.UserInfo)

        user_info = {}

        if not users:
            return {}

        for user in users:
            notifications_disabled = False
            notifications_daily_digest = False

            notifications = self.get_user_notifications(user_info_id=user)

            for variable in user.variables:
                if type(variable) is dict and variable.has_key('name'):

                    if variable['name'] == 'notifications_daily_digest':
                        notifications_daily_digest = variable['value']

                    if variable['name'] == 'notifications_disabled':
                        notifications_disabled = variable['value']
                else:
                    log.warning('Invalid variables attribute on UserInfo instance. UserInfo: %s', user)

            user_info[user._id] = { 'user_contact' : user.contact, 'notifications' : notifications,
                                    'notifications_daily_digest' : notifications_daily_digest, 'notifications_disabled' : notifications_disabled}


        return user_info
