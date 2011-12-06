#!/usr/bin/env python

__author__ = 'Thomas R. Lennan'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import AT, RT
from ion.services.coi.identity_management_service import IdentityManagementService
from interface.services.coi.iresource_registry_service import BaseResourceRegistryService

@attr('UNIT', group='coi')
class TestIdentityManagementService(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('examples.bank.trade_service.IonObject')
        self._create_service_mock('trade')

        self.identity_management_service = IdentityManagementService()
        self.identity_management_service.clients = self.clients

        # Rename to save some typing
        self.mock_create = self.resource_registry.create
        self.mock_read = self.resource_registry.read
        self.mock_update = self.resource_registry.update
        self.mock_delete = self.resource_registry.delete
        self.mock_create_association = self.resource_registry.create_association
        self.mock_delete_association = self.resource_registry.delete_association
        self.mock_find_objects = self.resource_registry.find_objects
        self.mock_find_resources = self.resource_registry.find_resources
        self.mock_find_subjects = self.resource_registry.find_subjects

        # UserIdentity
        self.user_identity = Mock()
        self.user_identity.name = "Foo"

        # UserCredential
        self.user_credentials = Mock()
        self.user_credentials._id = '222'
        self.user_credentials.name = "Bogus subject"

        # UserCredential
        self.user_credentials2 = Mock()
        self.user_credentials2._id = '777'
        self.user_credentials2.name = "Bogus subject"

        # UserIdentity to UserCredential association
        self.user_identity_to_credentials_association = Mock()
        self.user_identity_to_credentials_association._id = '333'
        self.user_identity_to_credentials_association.s = "111"
        self.user_identity_to_credentials_association.st = RT.UserIdentity
        self.user_identity_to_credentials_association.p = AT.hasCredentials
        self.user_identity_to_credentials_association.o = "222"
        self.user_identity_to_credentials_association.ot = RT.UserCredentials

        # UserIdentity to UserCredential association
        self.user_identity_to_credentials_association2 = Mock()
        self.user_identity_to_credentials_association2._id = '888'
        self.user_identity_to_credentials_association2.s = "111"
        self.user_identity_to_credentials_association2.st = RT.UserIdentity
        self.user_identity_to_credentials_association2.p = AT.hasCredentials
        self.user_identity_to_credentials_association2.o = "222"
        self.user_identity_to_credentials_association2.ot = RT.UserCredentials

        # UserInfo
        self.user_info = Mock()
        self.user_info.name = "John Doe"
        self.user_info.email = "John.Doe@devnull.com"
        self.user_info.phone = "555-555-5555"
        self.user_info.variables = [{"name": "subscribeToMailingList", "value": "False"}]

        self.user_info2 = Mock()
        self.user_info2.name = "John Doe"
        self.user_info2.email = "John.Doe2@devnull.com"
        self.user_info2.phone = "555-555-5556"
        self.user_info2.variables = [{"name": "subscribeToMailingList", "value": "True"}]

        # UserIdentity to UserInfo association
        self.user_identity_to_info_association = Mock()
        self.user_identity_to_info_association._id = '555'
        self.user_identity_to_info_association.s = "111"
        self.user_identity_to_info_association.st = RT.UserIdentity
        self.user_identity_to_info_association.p = AT.hasInfo
        self.user_identity_to_info_association.o = "444"
        self.user_identity_to_info_association.ot = RT.UserInfo

    def test_create_user_identity(self):
        self.mock_create.return_value = ['111', 1]
        
        user_id = self.identity_management_service.create_user_identity(self.user_identity)

        assert user_id == '111'
        self.mock_create.assert_called_once_with(self.user_identity)        

    def test_read_and_update_user_identity(self):
        self.mock_read.return_value = self.user_identity
        
        user_identity = self.identity_management_service.read_user_identity('111')

        assert user_identity is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')
        
        user_identity.name = 'Bar'

        self.mock_update.return_value = ['111', 2]
        
        result = self.identity_management_service.update_user_identity(user_identity)

        assert result == True
        self.mock_update.assert_called_once_with(user_identity)        

    def test_delete_user_identity(self):
        self.mock_read.return_value = self.user_identity
        
        result = self.identity_management_service.delete_user_identity('111')

        assert result == True
        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with(self.user_identity)        
 
    def test_read_user_identity_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.read_user_identity('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'UserIdentity bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')
 
    def test_delete_user_identity_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.delete_user_identity('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'UserIdentity bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_register_user_credentials(self):
        self.mock_create.return_value = ['222', 1]

        self.mock_create_association.return_value = ['333', 1]
        
        result = self.identity_management_service.register_user_credentials('111', self.user_credentials)

        assert result == True
        self.mock_create.assert_called_once_with(self.user_credentials)
        self.mock_create_association.assert_called_once_with('111', AT.hasCredentials, '222')

    def test_unregister_user_credentials(self):
        self.mock_find_resources.return_value = self.user_credentials

        self.mock_find_objects.return_value = ([self.user_credentials], [self.user_identity_to_credentials_association])
        
        result = self.identity_management_service.unregister_user_credentials('111', "Bogus subject")

        assert result == True
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, self.user_credentials.name, False)
        self.mock_find_objects.assert_called_once_with('111', AT.hasCredentials, RT.UserCredentials, False)
        self.mock_delete_association.assert_called_once_with('333')
        self.mock_delete.assert_called_once_with(self.user_credentials)

    def test_unregister_user_credentials_not_found(self):
        self.mock_find_resources.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_user_credentials('bad', 'bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'UserCredentials bad does not exist')
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, 'bad', False)

    def test_unregister_user_credentials_association_not_found(self):
        self.mock_find_resources.return_value = self.user_credentials

        self.mock_find_objects.return_value = (None, None)

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_user_credentials('111', "Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, 'UserIdentity to UserCredentials association for user id 111 to credential Bogus subject does not exist')
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, 'Bogus subject', False)
        self.mock_find_objects.assert_called_once_with('111', AT.hasCredentials, RT.UserCredentials, False)

    def test_create_user_info(self):
        self.mock_find_objects.return_value = (None, None)

        self.mock_create.return_value = ['444', 1]

        self.mock_create_association.return_value = ['555', 1]
        
        result = self.identity_management_service.create_user_info('111', self.user_info)

        assert result == '444'
        self.mock_create.assert_called_once_with(self.user_info)
        self.mock_create_association.assert_called_once_with('111', AT.hasInfo, '444')

    def test_create_user_info_fail_already_exists(self):
        self.mock_find_objects.return_value = ([self.user_info], [self.user_identity_to_info_association])

        # TEST: Execute the service operation call
        with self.assertRaises(Conflict) as cm:
            self.identity_management_service.create_user_info('111', self.user_info)

        ex = cm.exception
        self.assertEqual(ex.message, 'UserInfo already exists for user id 111')
        self.mock_find_objects.assert_called_once_with('111', AT.hasInfo, RT.UserInfo, False)

    def test_read_and_update_user_info(self):
        self.mock_read.return_value = self.user_info
        
        user_info = self.identity_management_service.read_user_info('444')

        assert user_info is self.mock_read.return_value
        self.mock_read.assert_called_once_with('444', '')
        
        user_info.name = 'Jane Doe'

        self.mock_update.return_value = ['444', 2]
        
        result = self.identity_management_service.update_user_info(user_info)

        assert result == True
        self.mock_update.assert_called_once_with(user_info)        

    def test_delete_user_info(self):
        self.mock_read.return_value = self.user_info

        self.mock_find_subjects.return_value = ([self.user_identity], [self.user_identity_to_info_association])
        
        result = self.identity_management_service.delete_user_info('444')

        assert result == True
        self.mock_find_subjects.assert_called_once_with('444', AT.hasInfo, RT.UserIdentity, False)
        self.mock_delete_association.assert_called_once_with('555')
        self.mock_delete.assert_called_once_with(self.user_info)
 
    def test_read_user_info_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.read_user_info('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'UserInfo bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')
 
    def test_delete_user_info_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.delete_user_info('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'UserInfo bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_delete_user_info_association_not_found(self):
        self.mock_read.return_value = self.user_info

        self.mock_find_subjects.return_value = (None, None)
        
        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.delete_user_info('444')

        ex = cm.exception
        self.assertEqual(ex.message, 'UserIdentity to UserInfo association for user info id 444 does not exist')
        self.mock_read.assert_called_once_with('444', '')
        self.mock_find_subjects.assert_called_once_with('444', AT.hasInfo, RT.UserIdentity, False)

    def test_find_user_info_by_id(self):
        self.mock_find_objects.return_value = ([self.user_info], [self.user_identity_to_info_association])
        
        result = self.identity_management_service.find_user_info_by_id('111')

        assert result == self.user_info
        self.mock_find_objects.assert_called_once_with('111', AT.hasInfo, RT.UserInfo, False)

    def test_find_user_info_by_id_association_not_found(self):
        self.mock_find_objects.return_value = (None, None)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_id('111')

        ex = cm.exception
        self.assertEqual(ex.message, 'UserInfo for user id 111 does not exist')
        self.mock_find_objects.assert_called_once_with('111', AT.hasInfo, RT.UserInfo, False)

    def test_find_user_info_by_name(self):
        self.mock_find_resources.return_value = ([self.user_info], [self.user_identity_to_info_association])
        
        result = self.identity_management_service.find_user_info_by_name("John Doe")

        assert result == self.user_info
        self.mock_find_resources.assert_called_once_with(RT.UserInfo, None, "John Doe", False)

    def test_find_user_info_by_name_not_found(self):
        self.mock_find_resources.return_value = (None, None)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_name("John Doe")

        ex = cm.exception
        self.assertEqual(ex.message, 'UserInfo with name John Doe does not exist')
        self.mock_find_resources.assert_called_once_with(RT.UserInfo, None, "John Doe", False)

    def test_find_user_info_by_name_multiple_found(self):
        self.mock_find_resources.return_value = ([self.user_info, self.user_info2], [self.user_identity_to_info_association])

        with self.assertRaises(Inconsistent) as cm:
            self.identity_management_service.find_user_info_by_name("John Doe")

        ex = cm.exception
        self.assertEqual(ex.message, 'Multiple UserInfos with name John Doe exist')
        self.mock_find_resources.assert_called_once_with(RT.UserInfo, None, "John Doe", False)

    def test_find_user_info_by_subject(self):
        self.mock_find_resources.return_value = ([self.user_credentials], [self.user_identity_to_credentials_association])

        self.mock_find_objects.return_value = ([self.user_info], [self.user_identity_to_info_association])
        
        result = self.identity_management_service.find_user_info_by_subject("Bogus subject")

        assert result == self.user_info
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)
        self.mock_find_objects.assert_called_once_with('111', AT.hasInfo, RT.UserInfo, False)

    def test_find_user_info_by_subject_identity_credentials_not_found(self):
        self.mock_find_resources.return_value = (None, None)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "UserCredentials with subject Bogus subject does not exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)

    def test_find_user_info_by_subject_identity_multiple_credentials_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials, self.user_credentials2], [self.user_identity_to_credentials_association, self.user_identity_to_credentials_association2])

        with self.assertRaises(Inconsistent) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "Multiple UserCredentials with subject Bogus subject exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)

    def test_find_user_info_by_subject_identity_association_not_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials], None)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "UserIdentity to UserCredentials association for subject Bogus subject does not exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)

    def test_find_user_info_by_subject_identity_multiple_associations_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials], [self.user_identity_to_credentials_association, self.user_identity_to_credentials_association2])

        with self.assertRaises(Inconsistent) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "Multiple UserIdentity to UserCredentials associations for subject Bogus subject exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)

    def test_find_user_info_by_subject_info_association_not_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials], [self.user_identity_to_credentials_association])

        self.mock_find_objects.return_value = (None, None)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "UserInfo for subject Bogus subject does not exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)
        self.mock_find_objects.assert_called_once_with('111', AT.hasInfo, RT.UserInfo, False)

    def test_find_user_info_by_subject_info_multiple_associations_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials], [self.user_identity_to_credentials_association])

        self.mock_find_objects.return_value = ([self.user_info, self.user_info2], [self.user_identity_to_info_association])

        with self.assertRaises(Inconsistent) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "Multiple UserInfos for subject Bogus subject exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)
        self.mock_find_objects.assert_called_once_with('111', AT.hasInfo, RT.UserInfo, False)
