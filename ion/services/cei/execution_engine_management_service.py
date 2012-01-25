#!/usr/bin/env python


__author__ = 'Stephen P. Henrie, Michael Meisinger'
__license__ = 'Apache 2.0'


from interface.services.cei.iexecution_engine_management_service import BaseExecutionEngineManagementService

class ExecutionEngineManagementService(BaseExecutionEngineManagementService):

    def create_execution_engine_definition(self, execution_engine_definition=None):
        """Creates an Execution Engine Definition based on given object.

        @param execution_engine_definition    ExecutionEngineDefinition
        @retval execution_engine_definition_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        ee_id, version = self.clients.resource_registry.create(execution_engine_definition)
        return ee_id

    def update_execution_engine_definition(self, execution_engine_definition=None):
        """Updates an Execution Engine Definition based on given object.

        @param execution_engine_definition    ExecutionEngineDefinition
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.clients.resource_registry.update(execution_engine_definition)

    def read_execution_engine_definition(self, execution_engine_definition_id=''):
        """Returns an Execution Engine Definition as object.

        @param execution_engine_definition_id    str
        @retval execution_engine_definition    ExecutionEngineDefinition
        @throws NotFound    object with specified id does not exist
        """
        eed = self.clients.resource_registry.read(execution_engine_definition_id)
        return eed

    def delete_execution_engine_definition(self, execution_engine_definition_id=''):
        """Deletes/retires an Execution Engine Definition.

        @param execution_engine_definition_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.clients.resource_registry.delete(execution_engine_definition_id)

    def create_execution_engine(self, execution_engine=None):
        """Creates an Execution Engine based on given object.

        @param execution_engine    ExecutionEngine
        @retval execution_engine_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        pass

    def update_execution_engine(self, execution_engine=None):
        """Updates an Execution Engine based on given object.

        @param execution_engine    ExecutionEngine
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        pass

    def read_execution_engine(self, execution_engine_id=''):
        """Returns an Execution Engine Definition as object.

        @param execution_engine_id    str
        @retval execution_engine    ExecutionEngine
        @throws NotFound    object with specified id does not exist
        """
        pass

    def delete_execution_engine(self, execution_engine_id=''):
        """Deletes/retires an Execution Engine.

        @param execution_engine_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass
