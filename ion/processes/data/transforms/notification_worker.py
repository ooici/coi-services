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
from pyon.event.event import EventSubscriber, EventPublisher

class NotificationWorker(TransformDataProcess):
    """
    Instances of this class acts as a Notification Worker.
    """

    # the dictionary containing info for all the users
    user_info = {} #  dict = {'user_id' : notification}

    def on_init(self):
        self.event_pub = EventPublisher()

    def on_start(self):
        super(NotificationWorker,self).on_start()


        def receive_event(event_msg, headers):
            pass


        # start the event subscriber
        self.event_subscriber = EventSubscriber(
            event_type="Event",
            queue_name = 'uns_queue', # modify this to point at the right queue
            callback=receive_event
        )

        self.gl = spawn(self.event_subscriber.listen)
        self.event_subscriber._ready_event.wait(timeout=5)

    def process(self, packet):
        """Process incoming data!!!!
        """
        # Process the packet

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

