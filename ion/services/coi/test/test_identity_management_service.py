#!/usr/bin/env python

__author__ = 'Thomas R. Lennan'
__license__ = 'Apache 2.0'

import unittest
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
from pyon.public import PRED, RT, IonObject
from ion.services.coi.identity_management_service import IdentityManagementService
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient, IdentityManagementServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceClient
from unittest.case import skip
from pyon.util.context import LocalContextMixin

@attr('UNIT', group='coi')
class TestIdentityManagementService(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('identity_management')

        self.identity_management_service = IdentityManagementService()
        self.identity_management_service.clients = mock_clients

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

        # ActorIdentity
        self.actor_identity = Mock()
        self.actor_identity._id = '111'
        self.actor_identity.name = "Foo"

        # UserCredential
        self.user_credentials = Mock()
        self.user_credentials._id = '222'
        self.user_credentials.name = "Bogus subject"

        # UserCredential
        self.user_credentials2 = Mock()
        self.user_credentials2._id = '777'
        self.user_credentials2.name = "Bogus subject"

        # ActorIdentity to UserCredential association
        self.actor_identity_to_credentials_association = Mock()
        self.actor_identity_to_credentials_association._id = '333'
        self.actor_identity_to_credentials_association.s = "111"
        self.actor_identity_to_credentials_association.st = RT.ActorIdentity
        self.actor_identity_to_credentials_association.p = PRED.hasCredentials
        self.actor_identity_to_credentials_association.o = "222"
        self.actor_identity_to_credentials_association.ot = RT.UserCredentials

        # ActorIdentity to UserCredential association
        self.actor_identity_to_credentials_association2 = Mock()
        self.actor_identity_to_credentials_association2._id = '888'
        self.actor_identity_to_credentials_association2.s = "111"
        self.actor_identity_to_credentials_association2.st = RT.ActorIdentity
        self.actor_identity_to_credentials_association2.p = PRED.hasCredentials
        self.actor_identity_to_credentials_association2.o = "222"
        self.actor_identity_to_credentials_association2.ot = RT.UserCredentials

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

        # ActorIdentity to UserInfo association
        self.actor_identity_to_info_association = Mock()
        self.actor_identity_to_info_association._id = '555'
        self.actor_identity_to_info_association.s = "111"
        self.actor_identity_to_info_association.st = RT.ActorIdentity
        self.actor_identity_to_info_association.p = PRED.hasInfo
        self.actor_identity_to_info_association.o = "444"
        self.actor_identity_to_info_association.ot = RT.UserInfo

    def test_create_actor_identity(self):
        self.mock_create.return_value = ['111', 1]
        
        user_id = self.identity_management_service.create_actor_identity(self.actor_identity)

        assert user_id == '111'
        self.mock_create.assert_called_once_with(self.actor_identity)        

    def test_read_and_update_actor_identity(self):
        self.mock_read.return_value = self.actor_identity
        
        actor_identity = self.identity_management_service.read_actor_identity('111')

        assert actor_identity is self.mock_read.return_value
        self.mock_read.assert_called_once_with('111', '')
        
        actor_identity.name = 'Bar'

        self.mock_update.return_value = ['111', 2]
        
        self.identity_management_service.update_actor_identity(actor_identity)

        self.mock_update.assert_called_once_with(actor_identity)        

    def test_delete_actor_identity(self):
        self.mock_read.return_value = self.actor_identity
        
        self.identity_management_service.delete_actor_identity('111')

        self.mock_read.assert_called_once_with('111', '')
        self.mock_delete.assert_called_once_with('111')
 
    def test_read_actor_identity_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.read_actor_identity('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'ActorIdentity bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')
 
    def test_delete_actor_identity_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.delete_actor_identity('bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'ActorIdentity bad does not exist')
        self.mock_read.assert_called_once_with('bad', '')

    def test_register_user_credentials(self):
        self.mock_create.return_value = ['222', 1]

        self.mock_create_association.return_value = ['333', 1]
        
        self.identity_management_service.register_user_credentials('111', self.user_credentials)

        self.mock_create.assert_called_once_with(self.user_credentials)
        self.mock_create_association.assert_called_once_with('111', PRED.hasCredentials, '222', "H2H")

    def test_unregister_user_credentials(self):
        self.mock_find_resources.return_value = ([self.user_credentials], [self.actor_identity_to_credentials_association])
        self.mock_find_associations.return_value = [self.actor_identity_to_credentials_association]
        
        self.identity_management_service.unregister_user_credentials('111', "Bogus subject")

        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, self.user_credentials.name, False)
        self.mock_find_associations.assert_called_once_with('111', PRED.hasCredentials, '222', None, False)
        self.mock_delete_association.assert_called_once_with('333')
        self.mock_delete.assert_called_once_with('222')

    def test_unregister_user_credentials_not_found(self):
        self.mock_find_resources.return_value = (None, None)

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_user_credentials('bad', 'bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'UserCredentials bad does not exist')
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, 'bad', False)

    def test_unregister_user_credentials_association_not_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials], [self.actor_identity_to_credentials_association])
        self.mock_find_associations.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_user_credentials('111', "Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, 'ActorIdentity to UserCredentials association for user id 111 to credential Bogus subject does not exist')
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, 'Bogus subject', False)
        self.mock_find_associations.assert_called_once_with('111', PRED.hasCredentials, '222', None, False)

    def test_create_user_info(self):
        self.mock_find_objects.return_value = (None, None)

        self.mock_create.return_value = ['444', 1]

        self.mock_create_association.return_value = ['555', 1]
        
        result = self.identity_management_service.create_user_info('111', self.user_info)

        assert result == '444'
        self.mock_create.assert_called_once_with(self.user_info)
        self.mock_create_association.assert_called_once_with('111', PRED.hasInfo, '444', "H2H")

    def test_create_user_info_fail_already_exists(self):
        self.mock_find_objects.return_value = ([self.user_info], [self.actor_identity_to_info_association])

        # TEST: Execute the service operation call
        with self.assertRaises(Conflict) as cm:
            self.identity_management_service.create_user_info('111', self.user_info)

        ex = cm.exception
        self.assertEqual(ex.message, 'UserInfo already exists for user id 111')
        self.mock_find_objects.assert_called_once_with('111', PRED.hasInfo, RT.UserInfo, False)

    def test_read_and_update_user_info(self):
        self.mock_read.return_value = self.user_info
        
        user_info = self.identity_management_service.read_user_info('444')

        assert user_info is self.mock_read.return_value
        self.mock_read.assert_called_once_with('444', '')
        
        user_info.name = 'Jane Doe'

        self.mock_update.return_value = ['444', 2]
        
        self.identity_management_service.update_user_info(user_info)

        self.mock_update.assert_called_once_with(user_info)        

    def test_delete_user_info(self):
        self.mock_read.return_value = self.user_info

        self.mock_find_subjects.return_value = ([self.actor_identity], [self.actor_identity_to_info_association])

        self.mock_find_associations.return_value = ([self.actor_identity_to_info_association])
        
        self.identity_management_service.delete_user_info('444')

        self.mock_find_subjects.assert_called_once_with(RT.ActorIdentity, PRED.hasInfo, '444', False)
        self.mock_find_associations.assert_called_once_with('111', PRED.hasInfo, '444', None, False)
        self.mock_delete_association.assert_called_once_with('555')
        self.mock_delete.assert_called_once_with('444')
 
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
        self.assertEqual(ex.message, 'ActorIdentity to UserInfo association for user info id 444 does not exist')
        self.mock_read.assert_called_once_with('444', '')
        self.mock_find_subjects.assert_called_once_with(RT.ActorIdentity, PRED.hasInfo, '444', False)

    def test_find_user_info_by_id(self):
        self.mock_find_objects.return_value = ([self.user_info], [self.actor_identity_to_info_association])
        
        result = self.identity_management_service.find_user_info_by_id('111')

        assert result == self.user_info
        self.mock_find_objects.assert_called_once_with('111', PRED.hasInfo, RT.UserInfo, False)

    def test_find_user_info_by_id_association_not_found(self):
        self.mock_find_objects.return_value = (None, None)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_id('111')

        ex = cm.exception
        self.assertEqual(ex.message, 'UserInfo for user id 111 does not exist')
        self.mock_find_objects.assert_called_once_with('111', PRED.hasInfo, RT.UserInfo, False)

    def test_find_user_info_by_name(self):
        self.mock_find_resources.return_value = ([self.user_info], [self.actor_identity_to_info_association])
        
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
        self.mock_find_resources.return_value = ([self.user_info, self.user_info2], [self.actor_identity_to_info_association])

        with self.assertRaises(Inconsistent) as cm:
            self.identity_management_service.find_user_info_by_name("John Doe")

        ex = cm.exception
        self.assertEqual(ex.message, 'Multiple UserInfo objects with name John Doe exist')
        self.mock_find_resources.assert_called_once_with(RT.UserInfo, None, "John Doe", False)

    def test_find_user_info_by_subject(self):
        self.mock_find_resources.return_value = ([self.user_credentials], [self.actor_identity_to_credentials_association])
        self.mock_find_subjects.return_value = ([self.actor_identity], [self.actor_identity_to_credentials_association])
        self.mock_find_objects.return_value = ([self.user_info], [self.actor_identity_to_info_association])
        
        result = self.identity_management_service.find_user_info_by_subject("Bogus subject")

        assert result == self.user_info
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)
        self.mock_find_objects.assert_called_once_with('111', PRED.hasInfo, RT.UserInfo, False)

    def test_find_user_info_by_subject_identity_credentials_not_found(self):
        self.mock_find_resources.return_value = (None, None)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "UserCredentials with subject Bogus subject does not exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)

    def test_find_user_info_by_subject_identity_multiple_credentials_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials, self.user_credentials2], [self.actor_identity_to_credentials_association, self.actor_identity_to_credentials_association2])

        with self.assertRaises(Inconsistent) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "Multiple UserCredentials with subject Bogus subject exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)

    def test_find_user_info_by_subject_identity_association_not_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials], None)
        self.mock_find_subjects.return_value = (None, None)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "ActorIdentity to UserCredentials association for subject Bogus subject does not exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)

    def test_find_user_info_by_subject_identity_multiple_associations_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials], [self.actor_identity_to_credentials_association, self.actor_identity_to_credentials_association2])
        self.mock_find_subjects.return_value = ([self.user_info, self.user_info2], [self.actor_identity_to_credentials_association, self.actor_identity_to_credentials_association2])

        with self.assertRaises(Inconsistent) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "Multiple ActorIdentity to UserCredentials associations for subject Bogus subject exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)

    def test_find_user_info_by_subject_info_association_not_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials], [self.actor_identity_to_credentials_association])
        self.mock_find_subjects.return_value = ([self.actor_identity], [self.actor_identity_to_credentials_association])
        self.mock_find_objects.return_value = (None, None)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "UserInfo for subject Bogus subject does not exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)
        self.mock_find_objects.assert_called_once_with('111', PRED.hasInfo, RT.UserInfo, False)

    def test_find_user_info_by_subject_info_multiple_associations_found(self):
        self.mock_find_resources.return_value = ([self.user_credentials], [self.actor_identity_to_credentials_association])
        self.mock_find_subjects.return_value = ([self.user_info], [self.actor_identity_to_credentials_association])
        self.mock_find_objects.return_value = ([self.user_info, self.user_info2], [self.actor_identity_to_info_association])

        with self.assertRaises(Inconsistent) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, "Multiple UserInfos for subject Bogus subject exist")
        self.mock_find_resources.assert_called_once_with(RT.UserCredentials, None, "Bogus subject", False)
        self.mock_find_subjects.assert_called_once_with(RT.ActorIdentity, PRED.hasCredentials, '222', False)

#    def test_signon_new_user(self):
#                user.certificate =  """-----BEGIN CERTIFICPREDE-----
#MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
#MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
#GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
#WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
#BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
#MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
#g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
#7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
#2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
#dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
#6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
#CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
#cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
#b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
#by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
#HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
#CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
#f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
#Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
#-----END CERTIFICPREDE-----"""
#
#    def test_signon_known_user(self):


@attr('INT', group='coi')
class TestIdentityManagementServiceInt(IonIntegrationTestCase):
    
    def setUp(self):
        self.subject = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254"

        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        self.identity_management_service = IdentityManagementServiceClient(node=self.container.node)
        self.org_client = OrgManagementServiceClient(node=self.container.node)

    def test_actor_identity(self):
        actor_identity_obj = IonObject("ActorIdentity", {"name": self.subject})        
        user_id = self.identity_management_service.create_actor_identity(actor_identity_obj)

        actor_identity = self.identity_management_service.read_actor_identity(user_id)

        actor_identity.name = 'Updated subject'
        self.identity_management_service.update_actor_identity(actor_identity)

        ai = self.identity_management_service.find_actor_identity_by_name(actor_identity.name)
        self._baseAssertEqual(ai.name, actor_identity.name)
        with self.assertRaises(NotFound):
            ai = self.identity_management_service.find_actor_identity_by_name("Yeah, well, you know, that's just, like, your opinion, man.")

        self._baseAssertEqual(ai.name, actor_identity.name)

        self.identity_management_service.delete_actor_identity(user_id)
 
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.read_actor_identity(user_id)
        self.assertTrue("does not exist" in cm.exception.message)
 
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.delete_actor_identity(user_id)
        self.assertTrue("does not exist" in cm.exception.message)

    def test_user_credentials(self):
        actor_identity_obj = IonObject("ActorIdentity", {"name": self.subject})        
        user_id = self.identity_management_service.create_actor_identity(actor_identity_obj)

        user_credentials_obj = IonObject("UserCredentials", {"name": self.subject})        
        self.identity_management_service.register_user_credentials(user_id, user_credentials_obj)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_user_credentials("bad", self.subject)
        self.assertTrue("does not exist" in cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_user_credentials(user_id, "bad")
        self.assertTrue("does not exist" in cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_user_credentials('bad', 'bad')
        self.assertTrue("does not exist" in cm.exception.message)

        self.identity_management_service.unregister_user_credentials(user_id, self.subject)

        self.identity_management_service.delete_actor_identity(user_id)




    def test_user_info(self):
        actor_identity_obj = IonObject("ActorIdentity", {"name": self.subject})
        user_id = self.identity_management_service.create_actor_identity(actor_identity_obj)

        user_credentials_obj = IonObject("UserCredentials", {"name": self.subject})
        self.identity_management_service.register_user_credentials(user_id, user_credentials_obj)

        user_info_obj = IonObject("UserInfo", {"name": "Foo"})
        user_info = self.identity_management_service.create_user_info(user_id, user_info_obj)

        with self.assertRaises(Conflict) as cm:
            self.identity_management_service.create_user_info(user_id, user_info_obj)
        self.assertTrue("UserInfo already exists for user id" in cm.exception.message)

        user_info_obj = self.identity_management_service.find_user_info_by_id(user_id)

        user_info_obj = self.identity_management_service.find_user_info_by_name("Foo")

        user_info_obj = self.identity_management_service.find_user_info_by_subject(self.subject)

        user_info_obj = self.identity_management_service.read_user_info(user_info)

        user_info_obj.name = 'Jane Doe'

        self.identity_management_service.update_user_info(user_info_obj)

        self.identity_management_service.delete_user_info(user_info)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.read_user_info(user_info)
        self.assertTrue('does not exist' in cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.delete_user_info(user_info)
        self.assertTrue('does not exist' in cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_name("John Doe")
        self.assertEqual(cm.exception.message, 'UserInfo with name John Doe does not exist')

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.find_user_info_by_subject("Bogus subject")
        self.assertEqual(cm.exception.message, "UserCredentials with subject Bogus subject does not exist")

        self.identity_management_service.unregister_user_credentials(user_id, self.subject)

        self.identity_management_service.delete_actor_identity(user_id)


    def test_signon(self):
        certificate =  """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""
        id, valid_until, registered = self.identity_management_service.signon(certificate, True)

        self.assertFalse(registered)

        id2, valid_until2, registered2 = self.identity_management_service.signon(certificate, True)

        self.assertFalse(registered2)
        self.assertTrue(id == id2)
        self.assertTrue(valid_until == valid_until2)

        user_info_obj = IonObject("UserInfo", {"name": "Foo"})        
        self.identity_management_service.create_user_info(id, user_info_obj)

        id3, valid_until3, registered3 = self.identity_management_service.signon(certificate, True)

        self.assertTrue(registered3)
        self.assertTrue(id == id3)
        self.assertTrue(valid_until == valid_until3)

    @attr('EXT')
    def test_get_extended_user_identity(self):

        actor_identity_obj = IonObject("ActorIdentity", {"name": self.subject})
        actor_id = self.identity_management_service.create_actor_identity(actor_identity_obj)

        user_credentials_obj = IonObject("UserCredentials", {"name": self.subject})
        self.identity_management_service.register_user_credentials(actor_id, user_credentials_obj)

        user_info_obj = IonObject("UserInfo", {"name": "Foo"})
        user_info_id = self.identity_management_service.create_user_info(actor_id, user_info_obj)

        ion_org = self.org_client.find_org()
        self.org_client.grant_role(ion_org._id, actor_id, 'ORG_MANAGER')

        with self.assertRaises(NotFound):
            self.identity_management_service.get_user_info_extension('That rug really tied the room together.')
        with self.assertRaises(BadRequest):
            self.identity_management_service.get_user_info_extension()

        extended_user = self.identity_management_service.get_user_info_extension(user_info_id)
        self.assertEqual(user_info_obj.type_,extended_user.resource.type_)
        self.assertEqual(len(extended_user.roles),2)

        self.identity_management_service.delete_user_info(user_info_id)

        self.org_client.revoke_role(ion_org._id, actor_id, 'ORG_MANAGER')

        self.identity_management_service.unregister_user_credentials(actor_id, self.subject)

        self.identity_management_service.delete_actor_identity(actor_id)



    def test_account_merge(self):
        certificate =  """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""
        subject = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254"
        certificate_2 = """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgIDAJ/lMA0GCSqGSIb3DQEBCwUAMGsxEzARBgoJkiaJk/IsZAEZFgNvcmcx
FzAVBgoJkiaJk/IsZAEZFgdjaWxvZ29uMQswCQYDVQQGEwJVUzEQMA4GA1UEChMHQ0lMb2dvbjEc
MBoGA1UEAxMTQ0lMb2dvbiBPcGVuSUQgQ0EgMTAeFw0xMjEwMTcwMDE2NDlaFw0xMjEwMTcxMjIx
NDlaMGkxEzARBgoJkiaJk/IsZAEZEwNvcmcxFzAVBgoJkiaJk/IsZAEZEwdjaWxvZ29uMQswCQYD
VQQGEwJVUzEPMA0GA1UEChMGR29vZ2xlMRswGQYDVQQDExJPd2VuIE93bmVycmVwIEE4OTMwggEi
MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDYLdpgg88sntivH+af4oamlp7blsUQcCQ5Yc/b
VDP/dwEKfxTcW36tMV3asLO7GcL7z4FESG761LAe86siT9rcwg2ttLkRjI9KeA3sFjC28N8XjKZ1
estCqG3odqw2pjo3VEFaU57219vIYMJhjmHKEgSnlMQeChMYun/sYIO5uNFba9BfiB6/PRS+bgee
cXRsIAm1vkB89AHdEjqdvH0uSN+jGjF6aAPXsESh70DUAHzs14lbFAomig7AZafT+weh0G5pnayC
lutVnhb9SyS3s1+A6kx8z9mkDUwY/NKXisuDeXa+WbRVq51D+Lc7ffOI+Ph+ynyfFGMcCBzbMADX
AgMBAAGjgeEwgd4wDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMCBLAwEwYDVR0lBAwwCgYIKwYB
BQUHAwIwGAYDVR0gBBEwDzANBgsrBgEEAYKRNgEDAzBsBgNVHR8EZTBjMC+gLaArhilodHRwOi8v
Y3JsLmNpbG9nb24ub3JnL2NpbG9nb24tb3BlbmlkLmNybDAwoC6gLIYqaHR0cDovL2NybC5kb2Vn
cmlkcy5vcmcvY2lsb2dvbi1vcGVuaWQuY3JsMCEGA1UdEQQaMBiBFm93ZW5vd25lcnJlcEBnbWFp
bC5jb20wDQYJKoZIhvcNAQELBQADggEBAHWd6ZOjSmJyOUyyLgZAPJpkSuk7DT5mFRhszJhfTGnu
gANHRIJZMs5e/LCMypE+ftxb8mnhAE+kURA2DmeucazHUDP5oYofU+8KMYqcNKnPpLnuiw+bCJPa
3BDxrYoi+vVislHb0U+QDjVYtUtQ2b1/Xhv8ShH89O9i65bbOq+sqez6z2AD9RWOEwRwpQLc9D65
9lkrsKGmJtuG8q3NTpZ1DSuaLOtn0QqttdmCg3pu5edRtgdpGadaSGR4s222JasV439bSTL8Z0Ug
HtjSclGqi8IBmvRkTZI61zTVbGdOKMP90LV1p8noJVLRkZpWRjLxI5xy9El8daAWMdjfrSc=
-----END CERTIFICATE-----"""
        subject_2 = "/DC=org/DC=cilogon/C=US/O=Google/CN=Owen Ownerrep A893"

        # Try to merge with nonexistent email account
        with self.assertRaises(NotFound):
            self.identity_management_service.initiate_account_merge("email3333_333@example.com")
        with self.assertRaises(BadRequest):
            self.identity_management_service.initiate_account_merge()

        # Create two users
        id, valid_until, registered = self.identity_management_service.signon(certificate, True)
        self.assertFalse(registered)
        id_2, valid_until_2, registered_2 = self.identity_management_service.signon(certificate_2, True)
        self.assertFalse(registered_2)

        # Validate the two accounts are different
        self.assertNotEqual(id, id_2, "The two accounts should have two different user id")

        # Create UserInfo
        contact_info_obj = IonObject("ContactInformation",{"email": "user_one@example.com"})
        user_info_obj = IonObject("UserInfo", {"name": "Dude", "contact": contact_info_obj})
        user_info_id = self.identity_management_service.create_user_info(id, user_info_obj)

        contact_info_obj_2 = IonObject("ContactInformation",{"email": "user_two@example.com"})
        user_info_obj_2 = IonObject("UserInfo", {"name": "theDude", "contact": contact_info_obj_2})
        user_info_id_2 = self.identity_management_service.create_user_info(id_2, user_info_obj_2)

        # Make sure the two users are registered
        id, valid_until, registered = self.identity_management_service.signon(certificate, True)
        self.assertTrue(registered)
        id_2, valid_until_2, registered_2 = self.identity_management_service.signon(certificate_2, True)
        self.assertTrue(registered_2)

        token = self.identity_management_service.initiate_account_merge("user_two@example.com",  headers={'ion-actor-id':id})

        # Try merging accounts with invalid token string
        with self.assertRaises(NotFound):
            self.identity_management_service.complete_account_merge(token_string="0xBeeF", headers={'ion-actor-id':id})
        with self.assertRaises(BadRequest):
            self.identity_management_service.complete_account_merge()

        # Try merging accounts with a different user
        # Since this user hasn't initiated account merge, the token doesn't exist in his/her UserInfo
        with self.assertRaises(NotFound):
            self.identity_management_service.complete_account_merge(token, headers={'ion-actor-id':id_2})

        self.identity_management_service.complete_account_merge(token, headers={'ion-actor-id':id})

        # Try merging the account again
        with self.assertRaises(BadRequest):
            self.identity_management_service.complete_account_merge(token, headers={'ion-actor-id':id})

        # Signon again and verify the two accounts have been merged
        id, valid_until, registered = self.identity_management_service.signon(certificate, True)
        self.assertTrue(registered)
        id_2, valid_until_2, registered_2 = self.identity_management_service.signon(certificate_2, True)
        self.assertTrue(registered_2)

        # Validate the two accounts are the same
        self.assertEqual(id, id_2, "The two accounts should have the same id")

        # Try to merge to your own account
        with self.assertRaises(BadRequest):
            token = self.identity_management_service.initiate_account_merge("user_one@example.com",  headers={'ion-actor-id':id})

        #  Done testing. Delete user
        self.identity_management_service.delete_user_info(user_info_id)
        self.identity_management_service.unregister_user_credentials(id, subject)
        self.identity_management_service.delete_actor_identity(id)



