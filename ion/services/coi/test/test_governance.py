#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest, os, gevent
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.util.context import LocalContextMixin

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound, Unauthorized
from pyon.public import PRED, RT, IonObject, CFG, log, OT
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from interface.services.coi.iexchange_management_service import ExchangeManagementServiceProcessClient
from interface.services.coi.ipolicy_management_service import PolicyManagementServiceProcessClient

from ion.processes.bootstrap.load_system_policy import LoadSystemPolicy
from ion.services.coi.service_gateway_service import get_role_message_headers
from ion.services.coi.policy_management_service import MANAGER_ROLE, MEMBER_ROLE, ION_MANAGER
from pyon.core.governance.negotiate_request import REQUEST_DENIED
from ion.agents.instrument.test.test_instrument_agent import start_test_instrument_agent
from interface.objects import AgentCommand
from ion.agents.instrument.instrument_agent import InstrumentAgentState

ORG2 = 'Org2'
INSTRUMENT_OPERATOR = 'INSTRUMENT_OPERATOR'


USER1_CERTIFICATE =  """-----BEGIN CERTIFICATE-----
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


TEST_POLICY_TEXT = '''
        <Rule RuleId="urn:oasis:names:tc:xacml:2.0:example:ruleid:%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                            <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>


                <Actions>
                    <Action>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_exchange_space</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>


                    </Action>
                </Actions>

            </Target>

        </Rule>
        '''

class GovernanceTestProcess(LocalContextMixin):
    name = 'gov_test'
    id='gov_client'

@attr('INT', group='coi')
class TestGovernanceInt(IonIntegrationTestCase):



    def setUp(self):

        # Start container
        self._start_container()

        #Load a deploy file
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        #Instantiate a process to represent the test
        process=GovernanceTestProcess()


        #Load system policies after container has started all of the services
        LoadSystemPolicy.op_load_system_policies(process)

        gevent.sleep(1)  # Wait for events to be fired and policy updated

        self.rr_client = ResourceRegistryServiceProcessClient(node=self.container.node, process=process)

        self.id_client = IdentityManagementServiceProcessClient(node=self.container.node, process=process)

        self.pol_client = PolicyManagementServiceProcessClient(node=self.container.node, process=process)

        self.org_client = OrgManagementServiceProcessClient(node=self.container.node, process=process)

        self.ims_client = InstrumentManagementServiceProcessClient(node=self.container.node, process=process)

        self.ems_client = ExchangeManagementServiceProcessClient(node=self.container.node, process=process)

        self.ion_org = self.org_client.find_org()


        self.system_actor = self.id_client.find_actor_identity_by_name(name=CFG.system.system_actor)
        log.debug('system actor:' + self.system_actor._id)


        sa_header_roles = get_role_message_headers(self.org_client.find_all_roles_by_user(self.system_actor._id))
        self.sa_user_header = {'ion-actor-id': self.system_actor._id, 'ion-actor-roles': sa_header_roles }

    def tearDown(self):
        policy_list, _ = self.rr_client.find_resources(restype=RT.Policy)

        #Must remove the policies in the reverse order they were added
        for policy in sorted(policy_list,key=lambda p: p.ts_created, reverse=True):
            self.pol_client.delete_policy(policy._id, headers=self.sa_user_header)

        gevent.sleep(1)  # Wait for events to be fired and policy updated

    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_basic_policy(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")


        #Attempt to access an operation in service which does not have specific policies set
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #Add a new policy to allow the the above service call.
        test_policy_id = self.pol_client.create_service_access_policy('exchange_management', 'Exchange_Management_Test_Policy',
            'Allow specific operations in the Exchange Management Service for anonymous user',
            TEST_POLICY_TEXT, headers=self.sa_user_header)

        gevent.sleep(1)  # Wait for events to be fired and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj)
        self.assertIn( 'Arguments not set',cm.exception.message)

        #disable the test policy to try again
        self.pol_client.disable_policy(test_policy_id, headers=self.sa_user_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #now enable the test policy to try again
        self.pol_client.enable_policy(test_policy_id, headers=self.sa_user_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj)
        self.assertIn( 'Arguments not set',cm.exception.message)

        #self.pol_client.remove_service_policy('exchange_management', test_policy_id, headers=self.sa_user_header)
        self.pol_client.delete_policy(test_policy_id, headers=self.sa_user_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_org_policy(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")


        with self.assertRaises(BadRequest) as cm:
            myorg = self.org_client.read_org()
        self.assertTrue(cm.exception.message == 'The org_id parameter is missing')


        user_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True)
        log.debug( "user id=" + user_id)

        user_roles = get_role_message_headers(self.org_client.find_all_roles_by_user(user_id))
        user_header = {'ion-actor-id': user_id, 'ion-actor-roles': user_roles }

        #Attempt to enroll a user anonymously - should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.enroll_member(self.ion_org._id,user_id)
        self.assertIn( 'org_management(enroll_member) has been denied',cm.exception.message)

        #Attempt to let a user enroll themselves - should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.enroll_member(self.ion_org._id,user_id, headers=user_header)
        self.assertIn( 'org_management(enroll_member) has been denied',cm.exception.message)

        #Attept to enroll the user in the ION Root org as a manager - should not be allowed since
        #registration with the system implies membership in the ROOT Org.
        with self.assertRaises(BadRequest) as cm:
            self.org_client.enroll_member(self.ion_org._id,user_id, headers=self.sa_user_header)
        self.assertTrue(cm.exception.message == 'A request to enroll in the root ION Org is not allowed')

        with self.assertRaises(Unauthorized) as cm:
            users = self.org_client.find_enrolled_users(self.ion_org._id)
        self.assertIn('org_management(find_enrolled_users) has been denied',cm.exception.message)


        with self.assertRaises(Unauthorized) as cm:
            users = self.org_client.find_enrolled_users(self.ion_org._id, headers=user_header)
        self.assertIn( 'org_management(find_enrolled_users) has been denied',cm.exception.message)

        users = self.org_client.find_enrolled_users(self.ion_org._id, headers=self.sa_user_header)
        self.assertEqual(len(users),2)

        ## test_org_roles and policies

        roles = self.org_client.find_org_roles(self.ion_org._id)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.name for r in roles], [MANAGER_ROLE, MEMBER_ROLE, ION_MANAGER])

        roles = self.org_client.find_org_roles_by_user(self.ion_org._id, self.system_actor._id, headers=self.sa_user_header)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.name for r in roles], [MEMBER_ROLE, MANAGER_ROLE, ION_MANAGER])

        roles = self.org_client.find_org_roles_by_user(self.ion_org._id, user_id, headers=self.sa_user_header)
        self.assertEqual(len(roles),1)
        self.assertItemsEqual([r.name for r in roles], [MEMBER_ROLE])


        with self.assertRaises(NotFound) as nf:
            org2 = self.org_client.find_org(ORG2)
        self.assertIn('The Org with name Org2 does not exist',nf.exception.message)

        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.sa_user_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),2)
        self.assertItemsEqual([r.name for r in roles], [MANAGER_ROLE, MEMBER_ROLE])


        operator_role = IonObject(RT.UserRole, name=INSTRUMENT_OPERATOR,label='Instrument Operator', description='Instrument Operator')

        #First try to add the user role anonymously
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.add_user_role(org2_id, operator_role)
        self.assertIn('org_management(add_user_role) has been denied',cm.exception.message)

        self.org_client.add_user_role(org2_id, operator_role, headers=self.sa_user_header)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.name for r in roles], [MANAGER_ROLE, MEMBER_ROLE,  INSTRUMENT_OPERATOR])


        # test requests for enrollments and roles.

        #First try to find user requests anonymously
        with self.assertRaises(Unauthorized) as cm:
            requests = self.org_client.find_requests(org2_id)
        self.assertIn('org_management(find_requests) has been denied',cm.exception.message)


        #Next try to find user requests as as a basic member
        with self.assertRaises(Unauthorized) as cm:
            requests = self.org_client.find_requests(org2_id, headers=user_header)
        self.assertIn('org_management(find_requests) has been denied',cm.exception.message)

        requests = self.org_client.find_requests(org2_id, headers=self.sa_user_header)
        self.assertEqual(len(requests),0)

        # First try to request a role without being a member
        with self.assertRaises(BadRequest) as cm:
            req_id = self.org_client.request_role(org2_id, user_id, INSTRUMENT_OPERATOR, headers=user_header )
        self.assertIn('A precondition for this request has not been satisfied: is_enrolled(org_id,user_id)',cm.exception.message)

        requests = self.org_client.find_requests(org2_id, headers=self.sa_user_header)
        self.assertEqual(len(requests),0)

        req_id = self.org_client.request_enroll(org2_id, user_id, headers=user_header )

        requests = self.org_client.find_requests(org2_id, headers=self.sa_user_header)
        self.assertEqual(len(requests),1)

        requests = self.org_client.find_user_requests(user_id, org2_id, headers=self.sa_user_header)
        self.assertEqual(len(requests),1)

        #User tried requesting enrollment again - this should fail
        with self.assertRaises(BadRequest) as cm:
            req_id = self.org_client.request_enroll(org2_id, user_id, headers=user_header )
        self.assertIn('A precondition for this request has not been satisfied: enroll_req_not_exist(org_id,user_id)',cm.exception.message)

        #Manager denies the request
        self.org_client.deny_request(org2_id,req_id,'To test the deny process', headers=self.sa_user_header)

        requests = self.org_client.find_requests(org2_id, headers=self.sa_user_header)
        self.assertEqual(len(requests),1)

        self.assertEqual(requests[0].status, REQUEST_DENIED)

        #Manager approves request
        self.org_client.approve_request(org2_id,req_id, headers=self.sa_user_header)

        users = self.org_client.find_enrolled_users(org2_id, headers=self.sa_user_header)
        self.assertEqual(len(users),0)

        #User Accepts request
        self.org_client.accept_request(org2_id,req_id,  headers=user_header)

        users = self.org_client.find_enrolled_users(org2_id, headers=self.sa_user_header)
        self.assertEqual(len(users),1)

        #User tried requesting enrollment again - this should fail
        with self.assertRaises(BadRequest) as cm:
            req_id = self.org_client.request_enroll(org2_id, user_id, headers=user_header )
        self.assertIn('A precondition for this request has not been satisfied: is_not_enrolled(org_id,user_id)',cm.exception.message)


        req_id = self.org_client.request_role(org2_id, user_id, INSTRUMENT_OPERATOR, headers=user_header )

        requests = self.org_client.find_requests(org2_id, headers=self.sa_user_header)
        self.assertEqual(len(requests),2)

        requests = self.org_client.find_requests(org2_id,request_status='Open', headers=self.sa_user_header)
        self.assertEqual(len(requests),1)

        requests = self.org_client.find_user_requests(user_id, org2_id, headers=user_header)
        self.assertEqual(len(requests),2)

        requests = self.org_client.find_user_requests(user_id, org2_id, request_type=RT.RoleRequest, headers=user_header)
        self.assertEqual(len(requests),1)

        requests = self.org_client.find_user_requests(user_id, org2_id, request_status="Open", headers=user_header)
        self.assertEqual(len(requests),1)

        ia_list,_ = self.rr_client.find_resources(restype=RT.InstrumentAgent)

        self.assertEqual(len(ia_list),0)

        ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent1', description='The first Instrument Agent')

        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.create_instrument_agent(ia_obj)
        self.assertIn('instrument_management(create_instrument_agent) has been denied',cm.exception.message)

        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.create_instrument_agent(ia_obj, headers=user_header)
        self.assertIn('instrument_management(create_instrument_agent) has been denied',cm.exception.message)

        #Manager approves request
        self.org_client.approve_request(org2_id,req_id, headers=self.sa_user_header)

        requests = self.org_client.find_user_requests(user_id, org2_id, request_status="Open", headers=user_header)
        self.assertEqual(len(requests),0)

        #User accepts request
        self.org_client.accept_request(org2_id, req_id, headers=user_header)

        #Refresh headers with new role
        user_roles = get_role_message_headers(self.org_client.find_all_roles_by_user(user_id))
        user_header = {'ion-actor-id': user_id, 'ion-actor-roles': user_roles }

        self.ims_client.create_instrument_agent(ia_obj, headers=user_header)

        ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent2', description='The second Instrument Agent')
        self.ims_client.create_instrument_agent(ia_obj, headers=user_header)

        ia_list,_ = self.rr_client.find_resources(restype=RT.InstrumentAgent)
        self.assertEqual(len(ia_list),2)

        #First make a acquire resource request with an non-enrolled user.
        with self.assertRaises(BadRequest) as cm:
            req_id = self.org_client.request_acquire_resource(org2_id,self.system_actor._id,ia_list[0]._id , headers=self.sa_user_header)
        self.assertIn('A precondition for this request has not been satisfied: is_enrolled(org_id,user_id)',cm.exception.message)

        req_id = self.org_client.request_acquire_resource(org2_id,user_id,ia_list[0]._id , headers=user_header)

        requests = self.org_client.find_requests(org2_id, headers=self.sa_user_header)
        self.assertEqual(len(requests),3)

        requests = self.org_client.find_user_requests(user_id, org2_id, headers=user_header)
        self.assertEqual(len(requests),3)

        requests = self.org_client.find_user_requests(user_id, org2_id, request_type=RT.ResourceRequest, headers=user_header)
        self.assertEqual(len(requests),1)

        requests = self.org_client.find_user_requests(user_id, org2_id, request_status="Open", headers=user_header)
        self.assertEqual(len(requests),1)

        self.assertEqual(requests[0]._id, req_id)

        #Manager approves Instrument request
        self.org_client.approve_request(org2_id,req_id, headers=self.sa_user_header)

        requests = self.org_client.find_user_requests(user_id, org2_id, request_status="Open", headers=user_header)
        self.assertEqual(len(requests),0)

        #User accepts request
        self.org_client.accept_request(org2_id,req_id, headers=user_header)

        #Check commitments
        commitments, _ = self.rr_client.find_objects(ia_list[0]._id,PRED.hasCommitment, RT.ResourceCommitment)
        self.assertEqual(len(commitments),1)

        commitments, _ = self.rr_client.find_objects(user_id,PRED.hasCommitment, RT.ResourceCommitment)
        self.assertEqual(len(commitments),1)

        #Release the resource
        self.org_client.release_resource(org2_id,user_id ,ia_list[0]._id, headers=self.sa_user_header,timeout=15)  #TODO - Refactor release_resource

        #Check commitments
        commitments, _ = self.rr_client.find_objects(ia_list[0]._id,PRED.hasCommitment, RT.ResourceCommitment)
        self.assertEqual(len(commitments),0)

        commitments, _ = self.rr_client.find_objects(user_id,PRED.hasCommitment, RT.ResourceCommitment)
        self.assertEqual(len(commitments),0)

    @unittest.skip('not ready')
    def test_agent_policy(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")


        #Startup an agent - TODO: will fail with Unauthorized to spawn process if not right user level - test this
        ia_client = start_test_instrument_agent(self.container, message_headers=self.sa_user_header)

        cmd = AgentCommand(command='get_current_state')
        retval = ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='get_current_state')
        retval = ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='reset')
        retval = ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='get_current_state')
        retval = ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

