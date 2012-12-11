#!/usr/bin/env python
'''
@author Swarbhanu Chatterjee
@file ion/services/dm/presentation/event_management_service.py
@description Implementation of the EventManagementService
'''

from pyon.public import log, PRED, RT
from pyon.core.exception import BadRequest
from pyon.util.arg_check import validate_is_not_none
from pyon.util.containers import create_unique_identifier, DotDict
from interface.services.dm.ievent_management_service import BaseEventManagementService
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.objects import ProcessDefinition, EventProcessDetail, EventProcessDefinitionDetail

class EventManagementService(BaseEventManagementService):
    """
    A service that provides users with an API for CRUD methods for events.
    """

    def on_start(self):
        super(EventManagementService, self).on_start()

    def on_quit(self):
        """
        Handles stop/terminate.

        Cleans up ant subscribers that may be spawned through this service
        """
        super(EventManagementService, self).on_quit()

    def create_event_type(self, event_type=None):
        """
        Persists the provided Event object for the specified Org id. Associates the
        Event resource with the use. The id string returned is the internal id
        by which Event will be identified in the data store.

        @param event            Event
        @return event_id        str
        @throws BadRequest    if object passed has _id or _rev attribute
        """

        event_type_id, _ = self.clients.resource_registry.create(event_type)
        return event_type_id

    def update_event_type(self, event_type=None):
        """
        Updates the provided Event object.  Throws NotFound exception if
        an existing version of Event is not found.  Throws Conflict if
        the provided Event object is not based on the latest persisted
        version of the object.

        @param event     Event
        @throws BadRequest      if object does not have _id or _rev attribute
        @throws NotFound        object with specified id does not exist
        @throws Conflict        object not based on latest persisted object version
        """
        self.clients.resource_registry.update(event_type)

    def read_event_type(self, event_type_id=''):
        """
        Returns the Event object for the specified event id.
        Throws exception if id does not match any persisted Event
        objects.

        @param event_id     str
        @return event       Event
        @throws NotFound    object with specified id does not exist
        """
        event_type = self.clients.resource_registry.read(event_type_id)
        return event_type

    def delete_event_type(self, event_type_id=''):
        """
        For now, permanently deletes Event object with the specified
        id. Throws exception if id does not match any persisted Event.

        @param event_id     str
        @throws NotFound    object with specified id does not exist
        """
        self.clients.resource_registry.delete(event_type_id)

    def create_event_process_definition(self, version='', module='', class_name='', uri='', arguments=None, event_types = None, sub_types = None, origin_types = None):
        """
        Create a resource which defines the processing of events.

        @param version str
        @param module str
        @param class_name str
        @param uri str
        @param arguments list

        @return procdef_id str
        """

        # Create the event process detail object
        event_process_definition_detail = EventProcessDefinitionDetail()
        event_process_definition_detail.event_types = event_types
        event_process_definition_detail.sub_types = sub_types
        event_process_definition_detail.origin_types = origin_types

        # Create the process definition
        process_definition = ProcessDefinition(name=create_unique_identifier('event_process'))
        process_definition.executable = {
            'module':module,
            'class': class_name,
            'url': uri
        }
        process_definition.version = version
        process_definition.arguments = arguments
        process_definition.definition = event_process_definition_detail

        procdef_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)

        return procdef_id

    def update_event_process_definition(self, event_process_definition_id='', version='', module='', class_name='', uri='', arguments=None, event_types=None, sub_types=None, origin_types=None):
        """
        Update the process definition for the event process.

        @param event_process_definition_id str
        @param version str
        @param module str
        @param class_name str
        @param uri str
        @arguments list
        """

        validate_is_not_none(event_process_definition_id)

        # The event_process_def is really only a process_def. Read up the process definition
        process_def = self.clients.resource_registry.read(event_process_definition_id)

        definition = process_def.definition

        # Fetch or make a new EventProcessDefinitionDetail object
        if definition:
            event_process_def_detail = EventProcessDefinitionDetail()
            event_process_def_detail.event_types = event_types or definition.event_types
            event_process_def_detail.sub_types = sub_types or definition.sub_types
            event_process_def_detail.origin_types = origin_types or definition.origin_types
        else:
            event_process_def_detail = EventProcessDefinitionDetail(event_types = event_types, sub_types = sub_types, origin_types = origin_types)

#        event_process_def_detail = process_def.definition or EventProcessDefinitionDetail()



        # Update the fields of the process definition
        process_def.executable['module'] = module
        process_def.executable['class'] = class_name
        process_def.executable['uri'] = uri
        process_def.version = version
        process_def.arguments = arguments
        process_def.definition = event_process_def_detail

        # Finally update the resource registry
        self.clients.resource_registry.update(process_def)

    def read_event_process_definition(self, event_process_definition_id=''):
        """
        Read the event process definition.

        @param event_process_definition_id str
        @return process_definition ProcessDefinition() object
        """
        return self.clients.resource_registry.read(event_process_definition_id)

    def delete_event_process_definition(self, event_process_definition_id=''):
        """
        Delete the event process definition.

        @param event_process_definition_id str
        """

        self.clients.resource_registry.delete(event_process_definition_id)

    def create_event_process(self, process_definition_id='', event_types=None, sub_types=None, origins=None, origin_types=None, out_data_products=None):
        """
        Create an event process using a process definition. Pass to the event process,
        the info about the events that the event process will subscribe to.

        @param process_definition_id str
        @param event_types list
        @param sub_types list
        @param origins list
        @param origin_types list

        @return process_id
        """

        # A process definition is required to be passed in
        validate_is_not_none(process_definition_id)

        #-------------------------------------------------------------------------
        # The output streams for the event process if any are provided
        #-------------------------------------------------------------------------

        output_streams = {}

        if out_data_products:
            for binding, output_data_product_id in out_data_products.iteritems():
                stream_ids, _ = self.clients.resource_registry.find_objects(output_data_product_id, PRED.hasStream, RT.Stream, True)
                if not stream_ids:
                    raise NotFound("No Stream created for output Data Product " + str(output_data_product_id))
                if len(stream_ids) != 1:
                    raise BadRequest("Data Product should only have ONE stream at this time" + str(output_data_product_id))
                output_streams[binding] = stream_ids[0]

        #-------------------------------------------------------------------------
        # The process definition
        #-------------------------------------------------------------------------

        # read the process definition object
        process_definition = self.clients.resource_registry.read(process_definition_id)

        #-------------------------------------------------------------------------
        # Get the event process detail object from the process definition
        #-------------------------------------------------------------------------
        event_process_def_detail = process_definition.definition or EventProcessDefinitionDetail()
        event_process_detail = EventProcessDetail()

        # But if event_types etc have been specified when the method is called, put them in the new
        # event process detail object, thus overwriting the ones that were transferred from the event process def detail object
        event_process_detail.event_types = event_types or event_process_def_detail.event_types
        event_process_detail.sub_types = sub_types or event_process_def_detail.sub_types
        event_process_detail.origins = origins
        event_process_detail.origin_types = origin_types or event_process_def_detail.origin_types
        event_process_detail.output_streams = output_streams

        #-------------------------------------------------------------------------
        # Launch the process
        #-------------------------------------------------------------------------

        # Create a config to pass the event_types, origins etc to the process, which is about to be created
        config = DotDict()
        config.process.event_types = event_types
        config.process.sub_types = sub_types
        config.process.origins = origins
        config.process.origin_types = origin_types
        config.process.publish_streams = output_streams


        # Schedule the process
        pid = self.clients.process_dispatcher.schedule_process(process_definition_id= process_definition_id,
                                                                configuration=config)

        event_process = self.clients.resource_registry.read(pid)
        event_process.detail = event_process_detail
        self.clients.resource_registry.update(event_process)

        #-------------------------------------------------------------------------
        # Associate the process with the process definition
        #-------------------------------------------------------------------------
        self.clients.resource_registry.create_association(  subject=pid,
                                                            predicate=PRED.hasProcessDefinition,
                                                            object=process_definition_id)

        #-------------------------------------------------------------------------
        # Register the process as a data producer
        #-------------------------------------------------------------------------
        self.clients.data_acquisition_management.register_event_process(process_id = pid)

        return pid

    def update_event_process(self):
        raise NotImplementedError('Not implemented for R2.0')

    def update_event_process_inputs(self, event_process_id='', event_types=None, sub_types=None, origins=None, origin_types=None):
        """
        Update the subscriptions of an event process.

        @param event_process_id str
        @param event_types list
        @param sub_types list
        @param origins list
        @param origin_types list
        """

        raise NotImplementedError('Not implemented for R2.0')

    def read_event_process(self, event_process_id=''):
        """
        Read the event process.

        @param event_process_id str
        @return event_process Process() object
        """

        return self.clients.resource_registry.read(event_process_id)

    def delete_event_process(self, event_process_id=''):
        """
        Delete the event process.

        @param event_process_id str
        """
        # unregister the process as a data producer
        self.clients.data_acquisition_management.unregister_event_process(process_id = event_process_id)

        # delete using the resource registry
        self.clients.resource_registry.delete(event_process_id)

    def activate_event_process(self, event_process_id=''):
        """
        Activate the event process.

        @param event_process_id
        """
        raise NotImplementedError('Not implemented for R2.0')

    def deactivate_event_process(self, event_process_id=''):
        """
        Deactivate the event process.

        @param event_process_id
        """

        raise NotImplementedError('Not implemented for R2.0')







