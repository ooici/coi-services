#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'
__license__ = 'Apache 2.0'

from interface.services.cei.iprocess_dispatcher_service import BaseProcessDispatcherService


class ProcessDispatcherService(BaseProcessDispatcherService):

    def create_process_definition(self, process_definition=None):
        """Creates a Process Definition based on given object.

        @param process_definition    ProcessDefinition
        @retval process_definition_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pd_id, version = self.clients.resource_registry.create(process_definition)
        return pd_id

    def update_process_definition(self, process_definition=None):
        """Updates a Process Definition based on given object.

        @param process_definition    ProcessDefinition
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.clients.resource_registry.update(process_definition)

    def read_process_definition(self, process_definition_id=''):
        """Returns a Process Definition as object.

        @param process_definition_id    str
        @retval process_definition    ProcessDefinition
        @throws NotFound    object with specified id does not exist
        """
        pdef = self.clients.resource_registry.read(process_definition_id)
        return pdef

    def delete_process_definition(self, process_definition_id=''):
        """Deletes/retires a Process Definition.

        @param process_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.clients.resource_registry.delete(process_definition_id)

    def find_process_definitions(self, filters=None):
        """Finds Process Definitions matching filter and returns a list of objects.

        @param filters    ResourceFilter
        @retval process_definition_list    []
        """
        pass

    def associate_execution_engine(self, process_definition_id='', execution_engine_definition_id=''):
        """Declare that the given process definition is compatible with the given execution engine.

        @param process_definition_id    str
        @param execution_engine_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.clients.resource_registry.create_association(process_definition_id,
                                                          PRED.supportsExecutionEngine,
                                                          execution_engine_definition_id)

    def dissociate_execution_engine(self, process_definition_id='', execution_engine_definition_id=''):
        """Remove the association of the process definition with an execution engine.

        @param process_definition_id    str
        @param execution_engine_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        assoc = self.clients.resource_registry.get_association(process_definition_id,
                                                          PRED.supportsExecutionEngine,
                                                          execution_engine_definition_id)
        self.clients.resource_registry.delete_association(assoc)

    def schedule_process(self, process_definition_id='', schedule=None):
        """Schedule a Process Definition for execution as process on an Execution Engine.

        @param process_definition_id    str
        @param schedule    ProcessSchedule
        @retval process_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        pass

    def cancel_process(self, process_id=''):
        """Cancels the execution of the given process id.

        @param process_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        pass
