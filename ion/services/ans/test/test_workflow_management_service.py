#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT, IonObject
from ion.services.ans.workflow_management_service import WorkflowManagementService

@attr('UNIT', group='as')
class TestWorkflowManagementService(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('workflow_management')

        self.workflow_management_service = WorkflowManagementService()
        self.workflow_management_service.clients = mock_clients

        # Rename to save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_read = mock_clients.resource_registry.read
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_create_association = mock_clients.resource_registry.create_association
        self.mock_delete_association = mock_clients.resource_registry.delete_association
        self.mock_find_objects = mock_clients.resource_registry.find_objects
        self.mock_find_resources = mock_clients.resource_registry.find_resources
        self.mock_find_subjects = mock_clients.resource_registry.find_subjects
        self.mock_find_associations = mock_clients.resource_registry.find_associations

        # workflow definition
        self.workflow_definition = Mock()
        self.workflow_definition.name = "Foo"
        self.workflow_definition.description ="This is a test workflow definition"
        self.workflow_definition.workflow_steps = []

        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id='123')
        self.workflow_definition.workflow_steps.append(workflow_step_obj)

        workflow_step_obj = IonObject('DataProcessWorkflowStep', data_process_definition_id='456')
        self.workflow_definition.workflow_steps.append(workflow_step_obj)

        # WorkflowDefinition to DataProcessDefinition associations
        self.workflow_definition_to_dataprocess_definition_association = Mock()
        self.workflow_definition_to_dataprocess_definition_association._id = 'abc'
        self.workflow_definition_to_dataprocess_definition_association.s = "111"
        self.workflow_definition_to_dataprocess_definition_association.st = RT.WorkflowDefinition
        self.workflow_definition_to_dataprocess_definition_association.p = PRED.hasDataProcessDefinition
        self.workflow_definition_to_dataprocess_definition_association.o = "123"
        self.workflow_definition_to_dataprocess_definition_association.ot = RT.DataProcessDefinition

        self.workflow_definition_to_dataprocess_definition_association2 = Mock()
        self.workflow_definition_to_dataprocess_definition_association2._id = 'def'
        self.workflow_definition_to_dataprocess_definition_association2.s = "111"
        self.workflow_definition_to_dataprocess_definition_association2.st = RT.WorkflowDefinition
        self.workflow_definition_to_dataprocess_definition_association2.p = PRED.hasDataProcessDefinition
        self.workflow_definition_to_dataprocess_definition_association2.o = "456"
        self.workflow_definition_to_dataprocess_definition_association2.ot = RT.DataProcessDefinition


    def test_create_workflow_definition(self):
        self.mock_create.return_value = ['111', 1]
        self.mock_read.return_value = self.workflow_definition
        self.mock_find_associations.return_value = [self.workflow_definition_to_dataprocess_definition_association, self.workflow_definition_to_dataprocess_definition_association2]

        workflow_definition_id = self.workflow_management_service.create_workflow_definition(self.workflow_definition)

        assert workflow_definition_id == '111'
        self.mock_create.assert_called_once_with(self.workflow_definition)


    def test_read_and_update_workflow_definition(self):
        self.mock_read.return_value = self.workflow_definition
        self.mock_find_associations.return_value = [self.workflow_definition_to_dataprocess_definition_association, self.workflow_definition_to_dataprocess_definition_association2]

        workflow_definition = self.workflow_management_service.read_workflow_definition('111')

        assert workflow_definition is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')

        workflow_definition.name = 'Bar space'

        self.mock_update.return_value = ['111', 2]

        with self.assertRaises(BadRequest) as cm:
            self.workflow_management_service.update_workflow_definition(workflow_definition)

        ex = cm.exception
        self.assertEqual(ex.message, "The workflow definition name 'Bar space' can only contain alphanumeric and underscore characters")

        workflow_definition.name = 'Bar'

        self.workflow_management_service.update_workflow_definition(workflow_definition)

        self.mock_update.assert_called_once_with(workflow_definition)

    def test_delete_workflow_definition(self):
        self.mock_read.return_value = self.workflow_definition
        self.mock_find_associations.return_value = [self.workflow_definition_to_dataprocess_definition_association, self.workflow_definition_to_dataprocess_definition_association2]

        self.workflow_management_service.delete_workflow_definition('111')

        self.mock_delete.assert_called_once_with('111')

    def test_read_workflow_definition_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.workflow_management_service.read_workflow_definition('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'workflow_definition_id bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_workflow_definition_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.workflow_management_service.delete_workflow_definition('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'workflow_definition_id bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')


