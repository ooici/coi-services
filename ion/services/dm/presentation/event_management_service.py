#!/usr/bin/env python
'''
@author Swarbhanu Chatterjee
@file ion/services/dm/presentation/event_management_service.py
@description Implementation of the EventManagementService
'''

from pyon.public import log
from pyon.core.exception import BadRequest
from pyon.util.containers import create_unique_identifier, DotDict
from interface.services.dm.ievent_management_service import BaseEventManagementService
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ProcessDefinition
import time
from datetime import datetime

class EventManagementService(BaseEventManagementService):
    """
    A service that provides users with an API for CRUD methods for events.
    """

    def __init__(self, *args, **kwargs):
        super(EventManagementService, self).__init__()

    def on_start(self):
        super(EventManagementService, self).on_start()
        self.process_dispatcher = ProcessDispatcherServiceClient()


    def on_quit(self):
        """
        Handles stop/terminate.

        Cleans up ant subscribers that may be spawned through this service or
        terminate any scheduled tasks to the scheduler.
        """
        super(EventManagementService, self).on_quit()

    def create_event_type(self, event_type=None):
        """
        Persists the provided Event object for the specified Org id. Associates the
        Event resource with the use. The id string returned is the internal id
        by which Event will be identified in the data store.

        @param event            Event
        @retval event_id        str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        if hasattr(object, "_id") and hasattr(object, "_rev"):
            raise BadRequest("Object does not have required '_id' or '_rev' attribute")

        event_type_id, _ = self.clients.resource_registry.create(event_type)

        return event_type_id

    def update_event_type(self, event_type=None):
        """Updates the provided Event object.  Throws NotFound exception if
        an existing version of Event is not found.  Throws Conflict if
        the provided Event object is not based on the latest persisted
        version of the object.

        @param event     Event
        @throws BadRequest      if object does not have _id or _rev attribute
        @throws NotFound        object with specified id does not exist
        @throws Conflict        object not based on latest persisted object version
        """
        if not hasattr(object, "_id") or not hasattr(object, "_rev"):
            raise BadRequest("Object does not have required '_id' or '_rev' attribute")

        self.clients.resource_registry.update(event_type)

    def read_event_type(self, event_type_id=''):
        """Returns the Event object for the specified event id.
        Throws exception if id does not match any persisted Event
        objects.

        @param event_id     str
        @retval event       Event
        @throws NotFound    object with specified id does not exist
        """
        event_type = self.clients.resource_registry.read(event_type_id)
        return event_type

    def delete_event_type(self, event_type_id=''):
        """For now, permanently deletes Event object with the specified
        id. Throws exception if id does not match any persisted Event.

        @param event_id     str
        @throws NotFound    object with specified id does not exist
        """
        self.clients.resource_registry.delete(event_type_id)

    def create_event_process_definition(self, version='', module='', class_name='', uri='', arguments=None):
        '''
        Create a resource which defines the processing of events, from transform definition to scheduling
        '''

        config = DotDict()

        process_definition = ProcessDefinition(name=create_unique_identifier('event_process'))
        process_definition.executable = {
            'module':module,
            'class': class_name
        }
        process_definition.version = version
        process_definition.arguments = arguments

        procdef_id = self.process_dispatcher.create_process_definition(process_definition=process_definition)
        pid = self.process_dispatcher.schedule_process(process_definition_id= procdef_id, configuration=config)

        return pid

    def update_event_process_definition(self, event_process_definition_id='', version='', module='', class_name='', uri='', arguments=None):
        '''
        Update the process definition for the event process
        '''

        event_process_def = self.clients.resource_registry.read(event_process_definition_id)

        event_process_def.executable['module'] = module
        event_process_def.executable['class'] = class_name
        event_process_def.version = version
        event_process_def.arguments = arguments

        self.clients.resource_registry.update(event_process_def)

    def read_event_process_definition(self, event_process_definition_id=''):

        return self.clients.resource_registry.read(event_process_definition_id)

    def delete_event_process_definition(self, event_process_definition_id=''):
        self.clients.resource_registry.delete(event_process_definition_id)

    def create_event_process(self, process_definition_id='', event_types=None, sub_types=None, origins=None, origin_types=None):

        config = DotDict()

        config.event_types = event_types
        config.sub_types = sub_types
        config.origins = origins
        config.origin_types = origin_types

        pid = self.process_dispatcher.schedule_process(process_definition_id= process_definition_id, configuration=config)

        return pid

    def update_event_process(self):
        pass

    def update_event_process_inputs(self, event_process_id='', event_types=None, sub_types=None, origins=None, origin_types=None):
        '''
        Update the subscriptions of an event process
        '''

        event_process = self.clients.resource_registry.read(event_process_id)

        #todo how to update the subscriptions of the event process
#        self.event_types = event_types or []
#        self.origins = origins or []
#        self.process_id = process_id
#        self.configurat

        event_process.event_types = event_types
        event_process.sub_types = sub_types
        event_process.origins = origins
        event_process.origin_types = origin_types

        subscription_id = data_process_obj.input_subscription_id
        was_active = False
        if subscription_id:
            # get rid of all the current streams
            try:
                log.debug("Deactivating subscription '%s'", subscription_id)
                self.clients.pubsub_management.deactivate_subscription(subscription_id)
                was_active = True

            except BadRequest:
                log.info('Subscription was not active')

            self.clients.pu.delete_subscription(subscription_id)

        new_subscription_id = self.clients.pubsub_management.create_subscription(data_process_obj.name,
            stream_ids=in_stream_ids)
        data_process_obj.input_subscription_id = new_subscription_id

        if was_active:
            log.debug("Activating subscription '%s'", new_subscription_id)
            self.clients.pubsub_management.activate_subscription(new_subscription_id)


        self.clients.resource_registry.update(event_process)


    def read_event_process(self, event_process_id=''):
        return self.clients.resource_registry.read(event_process_id)

    def delete_event_process(self, event_process_id=''):
        self.clients.resource_registry.delete(event_process_id)

    def activate_event_process(self, event_process_id=''):
        pass








