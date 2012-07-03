#!/usr/bin/env python

'''
@package ion.processes.data.presentation
@file ion/processes/data/transforms/notification_worker.py
@author Swarbhanu Chatterjee
@brief NotificationWorker Class. An instance of this class acts as an notification worker.
'''

from pyon.public import log
from pyon.ion.transform import TransformDataProcess
from pyon.util.async import spawn
from pyon.core.exception import BadRequest
from pyon.ion.process import SimpleProcess
from pyon.event.event import EventSubscriber, EventPublisher
from ion.services.dm.utility.uns_utility_methods import send_email, load_user_info, calculate_reverse_user_info
from ion.services.dm.utility.uns_utility_methods import setting_up_smtp_client, check_user_notification_interest

class NotificationWorker(SimpleProcess):
    """
    Instances of this class acts as a Notification Worker.
    """

    def on_init(self):
        self.event_pub = EventPublisher()
        # the dictionary containing info for all the users
        self.user_info = {} #  dict = {'user_id' : notification}

        # the reverse  user info dictionary that maps event types etc to users
        self.reverse_user_info = {}

    def on_start(self):
        super(NotificationWorker,self).on_start()

        #------------------------------------------------------------------------------------
        # Start by loading the user info and reverse user info dictionaries
        #------------------------------------------------------------------------------------

        load_user_info()
        if self.user_info:
            calculate_reverse_user_info(self.user_info)

        def receive_update_notification_event(event_msg, headers):
            load_user_info()
            calculate_reverse_user_info()

        #------------------------------------------------------------------------------------
        # start the event subscriber for all events that are of interest for notifications
        #------------------------------------------------------------------------------------

        self.event_subscriber = EventSubscriber(
            event_type="Event",
            queue_name = 'uns_queue', # modify this to point at the right queue
            callback=self.process_event
        )

        #------------------------------------------------------------------------------------
        # start the event subscriber for reload user info
        #------------------------------------------------------------------------------------

        self.reload_user_info_subscriber = EventSubscriber(
            event_type="UpdateNotificationEvent",
            callback=receive_update_notification_event
        )
        self.reload_user_info_subscriber.start()

    def process_event(self, msg, headers):
        """
        From the user_info dict find out which user has subscribed to that event.
        Send email to the user
        """

        #------------------------------------------------------------------------------------
        # From the reverse user info dict find out which users have subscribed to that event
        #------------------------------------------------------------------------------------

        users = check_user_notification_interest(event = msg, reverse_user_info = self.reverse_user_info)

        #------------------------------------------------------------------------------------
        # Send email to the users
        #------------------------------------------------------------------------------------

        #todo format the message better instead of just converting the event_msg to a string
        message = str(msg)

        for user_name in users:
            smtp_client = setting_up_smtp_client()
            msg_recipient = user_info[user_name]['user_contact'].email
            send_email(message = message, msg_recipient = msg_recipient, smtp_client = smtp_client )

    def on_stop(self):
        # close subscribers safely
        self.event_subscriber.stop()
        self.reload_user_info_subscriber.stop()

    def on_quit(self):
        # close subscribers safely
        self.event_subscriber.stop()
        self.reload_user_info_subscriber.stop()

