#!/usr/bin/env python

'''
@package ion.processes.data.presentation
@file ion/processes/data/transforms/notification_worker.py
@author Swarbhanu Chatterjee
@brief NotificationWorker Class. An instance of this class acts as an notification worker.
'''

from pyon.public import log, RT
from pyon.ion.transform import TransformDataProcess
from pyon.util.async import spawn
from pyon.core.exception import BadRequest, NotFound
from pyon.ion.process import SimpleProcess
from pyon.event.event import EventSubscriber, EventPublisher
from ion.services.dm.utility.uns_utility_methods import send_email, calculate_reverse_user_info
from ion.services.dm.utility.uns_utility_methods import setting_up_smtp_client, check_user_notification_interest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

import gevent, time

class NotificationWorker(SimpleProcess):
    """
    Instances of this class acts as a Notification Worker.
    """
    def on_init(self):
        self.event_pub = EventPublisher()
        self.user_info = {}
        self.resource_registry = ResourceRegistryServiceClient()

    def test_hook(self, user_info, reverse_user_info ):
        '''
        This method exists only to facilitate the testing of the reload of the user_info dictionary
        '''
        pass

    def on_start(self):
        super(NotificationWorker,self).on_start()

        self.smtp_client = setting_up_smtp_client()

        #------------------------------------------------------------------------------------
        # Start by loading the user info and reverse user info dictionaries
        #------------------------------------------------------------------------------------

        try:
            self.user_info = self.load_user_info()
            self.reverse_user_info =  calculate_reverse_user_info(self.user_info)

            log.info("On start up, notification workers loaded the following user_info dictionary: %s" % self.user_info)
            log.info("The calculated reverse user info: %s" % self.reverse_user_info )

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

            notification_id =  event_msg.notification_id
            log.info("(Notification worker received a ReloadNotificationEvent. The relevant notification_id is %s" % notification_id)

            try:
                self.user_info = self.load_user_info()
            except NotFound:
                log.warning("ElasticSearch has not yet loaded the user_index.")

            self.reverse_user_info =  calculate_reverse_user_info(self.user_info)
            self.test_hook(self.user_info, self.reverse_user_info)

            log.debug("After a reload, the user_info: %s" % self.user_info)
            log.debug("The recalculated reverse_user_info: %s" % self.reverse_user_info)

        # the subscriber for the ReloadUSerInfoEvent
        self.reload_user_info_subscriber = EventSubscriber(
            event_type="ReloadUserInfoEvent",
            callback=reload_user_info
        )
        self.reload_user_info_subscriber.start()

        #------------------------------------------------------------------------------------
        # Create an event subscriber for all events that are of interest for notifications
        #------------------------------------------------------------------------------------

        self.event_subscriber = EventSubscriber(
            queue_name = 'uns_queue', # modify this to point at the right queue
            callback=self.process_event
        )
        self.event_subscriber.start()

    def process_event(self, msg, headers):
        """
        Callback method for the subscriber listening for all events

        Objective: From the user_info dict find out which users have subscribed to that event and
        send emails to them
        """
        #------------------------------------------------------------------------------------
        # From the reverse user info dict find out which users have subscribed to that event
        #------------------------------------------------------------------------------------

        users = []
        if self.reverse_user_info:
            users = check_user_notification_interest(event = msg, reverse_user_info = self.reverse_user_info)

        log.info("Notification worker received the event : %s" % msg)
        log.info("Notification worker deduced the following users were interested in the event: %s" % users )

        #------------------------------------------------------------------------------------
        # Send email to the users
        #------------------------------------------------------------------------------------

        for user_name in users:
            msg_recipient = self.user_info[user_name]['user_contact'].email
            send_email(message = msg, msg_recipient = msg_recipient, smtp_client = self.smtp_client )

    def on_stop(self):
        # close subscribers safely
        self.event_subscriber.stop()
        self.reload_user_info_subscriber.stop()

    def on_quit(self):
        # close subscribers safely
        self.event_subscriber.stop()
        self.reload_user_info_subscriber.stop()

    def poll(self, tries, callback, *args, **kwargs):
        '''
        Polling wrapper for queries
        Elasticsearch may not index and cache the changes right away so we may need
        a couple of tries and a little time to go by before the results show.
        '''
        for i in xrange(tries):
            retval = callback(*args, **kwargs)
            if retval:
                return retval
            time.sleep(0.2)
        return None


    def load_user_info(self):
        '''
        Method to load the user info dictionary... used by notification workers and the UNS

        @retval user_info dict
        '''

        users, _ = self.resource_registry.find_resources(restype= RT.UserInfo)

        user_info = {}

        if not users:
            return {}

        for user in users:
            user_name = user.name
            user_contact = user.contact

            notifications = []
            for variable in user.variables:
                if variable['name'] == 'notification':
                    notifications = variable['value']

            user_info[user_name] = { 'user_contact' : user_contact, 'notifications' : notifications}

        return user_info