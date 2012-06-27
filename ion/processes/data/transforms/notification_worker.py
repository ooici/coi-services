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
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from pyon.event.event import EventSubscriber, EventPublisher
from ion.services.dm.utility.uns_utility_methods import send_email, update_user_info, calculate_reverse_user_info

class NotificationWorker(TransformDataProcess):
    """
    Instances of this class acts as a Notification Worker.
    """

    def on_init(self):
        self.event_pub = EventPublisher()
        # the dictionary containing info for all the users
        self.user_info = {} #  dict = {'user_id' : notification}

        # the reverse dictionaries
        self.event_type_user = {}
        self.event_subtype_user = {}
        self.event_origin_user = {}
        self.event_origin_type_user = {}

    def on_start(self):
        super(NotificationWorker,self).on_start()

        self.update_user_info()

#        def receive_event(event_msg, headers):
#            # use the subscription call back of the email event processor to send an email
#            self.process_event(event_msg)

        def receive_update_notification_event(event_msg, headers):
            self.update_user_info()

#        #------------------------------------------------------------------------------------
#        # start the event subscriber for all events that are of interest for notifications
#        #------------------------------------------------------------------------------------
#
#        self.event_subscriber = EventSubscriber(
#            event_type="Event",
#            queue_name = 'uns_queue', # modify this to point at the right queue
#            callback=receive_event
#        )

        #------------------------------------------------------------------------------------
        # start the event subscriber for listening to events which get generated when
        # notifications are updated
        #------------------------------------------------------------------------------------

        self.event_subscriber = EventSubscriber(
            event_type="UpdateNotificationEvent",
            callback=receive_update_notification_event
        )

        self.gl = spawn(self.event_subscriber.listen)
        self.event_subscriber._ready_event.wait(timeout=5)

        # calculate the user info dictionary
        self.user_info = update_user_info()

        # calculate the reverse user info
        self.event_type_user, self.event_subtype_user, \
        self.event_origin_user, self.event_origin_type_user =  calculate_reverse_user_info(self.user_info)

    def process(self, packet):
        """Process incoming data!!!!

        From the user_info dict find out which user has subscribed to that event.
        Send email to the user

        """

        for key,value in packet.identifiables.iteritems():
            pass

    def on_stop(self):
        TransformDataProcess.on_stop(self)

        # close event subscriber safely
        self.event_subscriber.close()
        self.gl.join(timeout=5)
        self.gl.kill()

    def on_quit(self):
        TransformDataProcess.on_quit(self)

        # close event subscriber safely
        self.event_subscriber.close()
        self.gl.join(timeout=5)
        self.gl.kill()

