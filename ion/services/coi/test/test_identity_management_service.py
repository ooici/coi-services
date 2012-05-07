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
        self.actor_identity.contact.name = "John Doe"
        self.actor_identity.contact.email = "John.Doe@devnull.com"
        self.actor_identity.contact.phone = "555-555-5555"
        self.actor_identity.contact.variables = [{"name": "subscribeToMailingList", "value": "False"}]

        self.contact_info = Mock()
        self.contact_info.name = "John Doe"
        self.contact_info.email = "John.Doe@gmail.com"
        self.contact_info.phone = "555-555-5555"
        self.contact_info.variables = [{"name": "subscribeToMailingList", "value": "True"}]

        # ActorIdentity post contact update
        self.actor_identity2 = Mock()
        self.actor_identity2._id = '111'
        self.actor_identity2.name = "Foo"
        self.actor_identity2.contact.name = "John Doe"
        self.actor_identity2.contact.email = "John.Doe@gmail.com"
        self.actor_identity2.contact.phone = "555-555-5555"
        self.actor_identity2.contact.variables = [{"name": "subscribeToMailingList", "value": "True"}]

        # ActorCredential
        self.actor_credentials = Mock()
        self.actor_credentials._id = '222'
        self.actor_credentials.name = "Bogus subject"

        # ActorCredential
        self.actor_credentials2 = Mock()
        self.actor_credentials2._id = '777'
        self.actor_credentials2.name = "Bogus subject"

        # ActorIdentity to ActorCredential association
        self.actor_identity_to_credentials_association = Mock()
        self.actor_identity_to_credentials_association._id = '333'
        self.actor_identity_to_credentials_association.s = "111"
        self.actor_identity_to_credentials_association.st = RT.ActorIdentity
        self.actor_identity_to_credentials_association.p = PRED.hasCredentials
        self.actor_identity_to_credentials_association.o = "222"
        self.actor_identity_to_credentials_association.ot = RT.ActorCredentials

        # ActorIdentity to ActorCredential association
        self.actor_identity_to_credentials_association2 = Mock()
        self.actor_identity_to_credentials_association2._id = '888'
        self.actor_identity_to_credentials_association2.s = "111"
        self.actor_identity_to_credentials_association2.st = RT.ActorIdentity
        self.actor_identity_to_credentials_association2.p = PRED.hasCredentials
        self.actor_identity_to_credentials_association2.o = "222"
        self.actor_identity_to_credentials_association2.ot = RT.ActorCredentials

    def test_create_actor_identity(self):
        self.mock_create.return_value = ['111', 1]
        
        actor_id = self.identity_management_service.create_actor_identity(self.actor_identity)

        assert actor_id == '111'
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

    def test_register_actor_credentials(self):
        self.mock_create.return_value = ['222', 1]

        self.mock_create_association.return_value = ['333', 1]
        
        self.identity_management_service.register_actor_credentials('111', self.actor_credentials)

        self.mock_create.assert_called_once_with(self.actor_credentials)
        self.mock_create_association.assert_called_once_with('111', PRED.hasCredentials, '222', None)

    def test_unregister_actor_credentials(self):
        self.mock_find_resources.return_value = ([self.actor_credentials], [self.actor_identity_to_credentials_association])
        self.mock_find_associations.return_value = [self.actor_identity_to_credentials_association]
        
        self.identity_management_service.unregister_actor_credentials('111', "Bogus subject")

        self.mock_find_resources.assert_called_once_with(RT.ActorCredentials, None, self.actor_credentials.name, False)
        self.mock_find_associations.assert_called_once_with('111', PRED.hasCredentials, '222', None, False)
        self.mock_delete_association.assert_called_once_with('333')
        self.mock_delete.assert_called_once_with('222')

    def test_unregister_actor_credentials_not_found(self):
        self.mock_find_resources.return_value = (None, None)

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_actor_credentials('bad', 'bad')

        ex = cm.exception
        self.assertEqual(ex.message, 'ActorCredentials bad does not exist')
        self.mock_find_resources.assert_called_once_with(RT.ActorCredentials, None, 'bad', False)

    def test_unregister_actor_credentials_association_not_found(self):
        self.mock_find_resources.return_value = ([self.actor_credentials], [self.actor_identity_to_credentials_association])
        self.mock_find_associations.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_actor_credentials('111', "Bogus subject")

        ex = cm.exception
        self.assertEqual(ex.message, 'ActorIdentity to ActorCredentials association for actor id 111 to credential Bogus subject does not exist')
        self.mock_find_resources.assert_called_once_with(RT.ActorCredentials, None, 'Bogus subject', False)
        self.mock_find_associations.assert_called_once_with('111', PRED.hasCredentials, '222', None, False)

    def test_update_actor_contact_info(self):
        self.identity_management_service.update_actor_contact_info('444', self.contact_info)

        self.mock_read.assert_called_once_with('444', '')


@attr('INT', group='coi')
class TestIdentityManagementServiceInt(IonIntegrationTestCase):
    
    def setUp(self):
        self.subject = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254"

        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        self.identity_management_service = IdentityManagementServiceClient(node=self.container.node)

    def test_actor_identity(self):
        actor_identity_obj = IonObject("ActorIdentity", {"name": self.subject})        
        actor_id = self.identity_management_service.create_actor_identity(actor_identity_obj)

        actor_identity = self.identity_management_service.read_actor_identity(actor_id)

        actor_identity.name = 'Updated subject'
        self.identity_management_service.update_actor_identity(actor_identity)

        contact_info = IonObject("ContactInformation", {"name": "Foo"})        
        self.identity_management_service.update_actor_contact_info(actor_id, contact_info)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.update_actor_contact_info("Bogus", contact_info)
        self.assertTrue('does not exist' in cm.exception.message)

        self.identity_management_service.delete_actor_identity(actor_id)
 
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.read_actor_identity(actor_id)
        self.assertTrue("does not exist" in cm.exception.message)
 
        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.delete_actor_identity(actor_id)
        self.assertTrue("does not exist" in cm.exception.message)

    def test_actor_credentials(self):
        actor_identity_obj = IonObject("ActorIdentity", {"name": self.subject})        
        actor_id = self.identity_management_service.create_actor_identity(actor_identity_obj)

        actor_credentials_obj = IonObject("ActorCredentials", {"name": self.subject})        
        self.identity_management_service.register_actor_credentials(actor_id, actor_credentials_obj)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_actor_credentials("bad", self.subject)
        self.assertTrue("does not exist" in cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_actor_credentials(actor_id, "bad")
        self.assertTrue("does not exist" in cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            self.identity_management_service.unregister_actor_credentials('bad', 'bad')
        self.assertTrue("does not exist" in cm.exception.message)

        self.identity_management_service.unregister_actor_credentials(actor_id, self.subject)

        self.identity_management_service.delete_actor_identity(actor_id)

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

        contact_info_obj = IonObject("ContactInformation", {"name": "Foo", "email": "foo@foo.com"})        
        self.identity_management_service.update_actor_contact_info(id, contact_info_obj)

        id3, valid_until3, registered3 = self.identity_management_service.signon(certificate, True)

        self.assertTrue(registered3)
        self.assertTrue(id == id3)
        self.assertTrue(valid_until == valid_until3)
