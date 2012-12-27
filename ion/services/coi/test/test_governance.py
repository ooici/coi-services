#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest, os, gevent, time, datetime
from mock import Mock, patch
from pyon.util.containers import get_ion_ts
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.util.context import LocalContextMixin

from pyon.agent.agent import ResourceAgentState, ResourceAgentEvent

from pyon.datastore.datastore import DatastoreManager
from pyon.event.event import EventRepository

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound, Unauthorized, InstStateError
from pyon.public import PRED, RT, IonObject, CFG, log, OT, LCS, LCE
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from interface.services.coi.iexchange_management_service import ExchangeManagementServiceProcessClient
from interface.services.coi.ipolicy_management_service import PolicyManagementServiceProcessClient
from interface.services.cei.ischeduler_service import SchedulerServiceProcessClient


from interface.objects import AgentCommand, ProposalOriginatorEnum, ProposalStatusEnum, NegotiationStatusEnum
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter
from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
from mi.core.instrument.instrument_driver import DriverConnectionState
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessTypes
from pyon.core.governance.negotiation import Negotiation
from ion.processes.bootstrap.load_system_policy import LoadSystemPolicy
from pyon.core.governance.governance_controller import ORG_MANAGER_ROLE, ORG_MEMBER_ROLE, ION_MANAGER
from ion.services.sa.observatory.observatory_management_service import INSTRUMENT_OPERATOR_ROLE

ORG2 = 'Org2'


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


TEST_BOUNDARY_POLICY_TEXT = '''
        <Rule RuleId="urn:oasis:names:tc:xacml:2.0:example:ruleid:%s" Effect="Deny">
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


            </Target>

        </Rule>
        '''


class GovernanceTestProcess(LocalContextMixin):
    name = 'gov_test'
    id='gov_client'
    process_type = 'simple'

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
        policy_loaded = CFG.get_safe('system.load_policy', False)

        if not policy_loaded:
            LoadSystemPolicy.op_load_system_policies(process)

        gevent.sleep(1)  # Wait for events to be fired and policy updated

        self.rr_client = ResourceRegistryServiceProcessClient(node=self.container.node, process=process)

        self.id_client = IdentityManagementServiceProcessClient(node=self.container.node, process=process)

        self.pol_client = PolicyManagementServiceProcessClient(node=self.container.node, process=process)

        self.org_client = OrgManagementServiceProcessClient(node=self.container.node, process=process)

        self.ims_client = InstrumentManagementServiceProcessClient(node=self.container.node, process=process)

        self.ems_client = ExchangeManagementServiceProcessClient(node=self.container.node, process=process)

        self.ssclient = SchedulerServiceProcessClient(node=self.container.node, process=process)


        #Get info on the ION System Actor
        self.system_actor = self.container.governance_controller.get_system_actor()
        log.info('system actor:' + self.system_actor._id)

        self.system_actor_header = self.container.governance_controller.get_system_actor_header()

        #Create a Actor which represents an originator like a web server.
        apache_obj = IonObject(RT.ActorIdentity, name='ApacheWebServer', description='Represents a non user actor like an apache web server')
        apache_actor_id,_ = self.rr_client.create(apache_obj, headers=self.system_actor_header)
        self.apache_actor = self.rr_client.read(apache_actor_id)

        self.apache_actor_header = self.container.governance_controller.get_actor_header(self.apache_actor._id)

        self.anonymous_user_headers = {'ion-actor-id':'anonymous'}

        self.ion_org = self.org_client.find_org()


        #Setup access to event repository
        dsm = DatastoreManager()
        ds = dsm.get_datastore("events")

        self.event_repo = EventRepository(dsm)


    def tearDown(self):
        policy_list, _ = self.rr_client.find_resources(restype=RT.Policy, headers=self.system_actor_header)

        #Must remove the policies in the reverse order they were added
        for policy in sorted(policy_list,key=lambda p: p.ts_created, reverse=True):
            self.pol_client.delete_policy(policy._id, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be fired and policy updated

        #Clean up the non user actor
        self.rr_client.delete(self.apache_actor._id, headers=self.system_actor_header)


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_basic_policy_operations(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")


        #Attempt to access an operation in service which does not have specific policies set
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_user_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #Add a new policy to allow the the above service call.
        test_policy_id = self.pol_client.create_service_access_policy('exchange_management', 'Exchange_Management_Test_Policy',
            'Allow specific operations in the Exchange Management Service for anonymous user',
            TEST_POLICY_TEXT, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be fired and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_user_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)

        #disable the test policy to try again
        self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_user_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #now enable the test policy to try again
        self.pol_client.enable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_user_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        #Now test service operation specific policies

        pol1_id = self.pol_client.add_process_operation_precondition_policy(process_name='policy_management', op='disable_policy', policy_content='func1_pass', headers=self.system_actor_header )

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #try to disable the test policy  again
        self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_user_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #now enable the test policy to try again
        self.pol_client.enable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_user_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        pol2_id = self.pol_client.add_process_operation_precondition_policy(process_name='policy_management', op='disable_policy', policy_content='func2_deny', headers=self.system_actor_header )

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #try to disable the test policy again
        with self.assertRaises(Unauthorized) as cm:
            self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)
        self.assertIn( 'Denied for no reason',cm.exception.message)


        self.pol_client.delete_policy(pol2_id,  headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #try to disable the test policy  again
        self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_user_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        pre_func1 =\
        """def precondition_func(process, msg, headers):
            if headers['op'] == 'disable_policy':
                return False, 'Denied for no reason again'
            else:
                return True, ''

        """

        pol2_id = self.pol_client.add_process_operation_precondition_policy(process_name='policy_management', op='disable_policy', policy_content=pre_func1, headers=self.system_actor_header )

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #try to disable the test policy again
        with self.assertRaises(Unauthorized) as cm:
            self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)
        self.assertIn( 'Denied for no reason again',cm.exception.message)


        self.pol_client.delete_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_user_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        ###########
        ### Now test access to service create* operations based on roles...

        #Anonymous users should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            id = self.ssclient.create_interval_timer(start_time="now", event_origin="Interval_Timer_233", headers=self.anonymous_user_headers)
        self.assertIn( 'scheduler(create_interval_timer) has been denied',cm.exception.message)

        #now try creating a new user with a valid actor
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "user id=" + actor_id)
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #User without OPERATOR or MANAGER role should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            id = self.ssclient.create_interval_timer(start_time="now", event_origin="Interval_Timer_233", headers=actor_header)
        self.assertIn( 'scheduler(create_interval_timer) has been denied',cm.exception.message)

        #Remove the INSTRUMENT_OPERATOR_ROLE from the user.
        self.org_client.grant_role(self.ion_org._id, actor_id, ORG_MANAGER_ROLE,  headers=self.system_actor_header)

        #Refresh headers with new role
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #User with proper role should now be allowed to access this service operation.
        id = self.ssclient.create_interval_timer(start_time="now", event_origin="Interval_Timer_233", headers=actor_header)



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    @patch.dict(CFG, {'container':{'org_boundary':True}})
    def test_org_boundary(self):

        with self.assertRaises(NotFound) as nf:
            org2 = self.org_client.find_org(ORG2)
        self.assertIn('The Org with name Org2 does not exist',nf.exception.message)

        #Create a second Org
        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        #First try to get a list of Users by hitting the RR anonymously - should be allowed.
        users,_ = self.rr_client.find_resources(restype=RT.ActorIdentity)
        self.assertEqual(len(users),2) #Should include the ION System Actor and non-user actor from setup as well.

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_user_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)


        #now try creating a new user with a valid actor
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "user id=" + actor_id)
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #First try to get a list of Users by hitting the RR anonymously - should be allowed.
        users,_ = self.rr_client.find_resources(restype=RT.ActorIdentity)
        self.assertEqual(len(users),3) #Should include the ION System Actor and non-user actor from setup as well.

        #Now enroll the user as a member of the Second Org
        self.org_client.enroll_member(org2_id,actor_id, headers=self.system_actor_header)
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #Add a new Org boundary policy which deny's all anonymous access
        test_policy_id = self.pol_client.create_resource_access_policy( org2_id, 'Org_Test_Policy',
            'Deny all access for anonymous user',
            TEST_BOUNDARY_POLICY_TEXT, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be fired and policy updated

        #Hack to force container into an Org Boundary for second Org
        self.container.governance_controller._container_org_name = org2.name
        self.container.governance_controller._is_container_org_boundary = True

        #First try to get a list of Users by hitting the RR anonymously - should be denied.
        with self.assertRaises(Unauthorized) as cm:
            users,_ = self.rr_client.find_resources(restype=RT.ActorIdentity, headers=self.anonymous_user_headers)
        self.assertIn( 'resource_registry(find_resources) has been denied',cm.exception.message)


        #Now try to hit the RR with a real user and should noe bw allowed
        users,_ = self.rr_client.find_resources(restype=RT.ActorIdentity, headers=actor_header)
        self.assertEqual(len(users),3) #Should include the ION System Actor and non-user actor from setup as well.

        #TODO - figure out how to right a XACML rule to be a member of the specific Org as well

        #Hack to force container back to default values
        self.container.governance_controller._container_org_name = 'ION'
        self.container.governance_controller._is_container_org_boundary = False
        self.container.governance_controller._container_org_id = None

        self.pol_client.delete_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_org_enroll_negotiation(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")


        with self.assertRaises(BadRequest) as cm:
            myorg = self.org_client.read_org()
        self.assertTrue(cm.exception.message == 'The org_id parameter is missing')

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_user_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Now create user with proper credentials
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "user id=" + actor_id)

        #Build the message headers used with this user
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #Attempt to enroll a user anonymously - should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.enroll_member(self.ion_org._id,actor_id, headers=self.anonymous_user_headers)
        self.assertIn( 'org_management(enroll_member) has been denied',cm.exception.message)

        #Attempt to let a user enroll themselves - should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.enroll_member(self.ion_org._id,actor_id, headers=actor_header)
        self.assertIn( 'org_management(enroll_member) has been denied',cm.exception.message)

        #Attept to enroll the user in the ION Root org as a manager - should not be allowed since
        #registration with the system implies membership in the ROOT Org.
        with self.assertRaises(BadRequest) as cm:
            self.org_client.enroll_member(self.ion_org._id,actor_id, headers=self.system_actor_header)
        self.assertTrue(cm.exception.message == 'A request to enroll in the root ION Org is not allowed')

        #Verify that anonymous user cannot find a list of enrolled users in an Org
        with self.assertRaises(Unauthorized) as cm:
            users = self.org_client.find_enrolled_users(self.ion_org._id, headers=self.anonymous_user_headers)
        self.assertIn('org_management(find_enrolled_users) has been denied',cm.exception.message)

        #Verify that a user without the proper Org Manager cannot find a list of enrolled users in an Org
        with self.assertRaises(Unauthorized) as cm:
            users = self.org_client.find_enrolled_users(self.ion_org._id, headers=actor_header)
        self.assertIn( 'org_management(find_enrolled_users) has been denied',cm.exception.message)

        users = self.org_client.find_enrolled_users(self.ion_org._id, headers=self.system_actor_header)
        self.assertEqual(len(users),3)  # WIll include the ION system actor and the non user actor from setup

        #Create a second Org
        with self.assertRaises(NotFound) as nf:
            org2 = self.org_client.find_org(ORG2)
        self.assertIn('The Org with name Org2 does not exist',nf.exception.message)

        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),0)

        #Build the Service Agreement Proposal for enrollment request
        sap = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id )

        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        #User tried proposing an enrollment again - this should fail
        with self.assertRaises(BadRequest) as cm:
            self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: not is_enroll_negotiation_open',cm.exception.message)

        #Manager trys to reject the proposal but incorrectly
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.EnrollmentProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.REJECTED, ProposalOriginatorEnum.PROVIDER)
        sap_response.sequence_num -= 1

        #Should fail because the proposal sequence was not incremented
        with self.assertRaises(Inconsistent) as cm:
            self.org_client.negotiate(sap_response, headers=actor_header )
        self.assertIn('The Service Agreement Proposal does not have the correct sequence_num value (0) for this negotiation (1)',cm.exception.message)

        #Manager now trys to reject the proposal but with the correct proposal sequence
        sap_response.sequence_num += 1

        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        self.assertEqual(negotiations[0].negotiation_status, NegotiationStatusEnum.REJECTED)

        gevent.sleep(2)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.EnrollmentNegotiationStatusEvent)
        self.assertEquals(len(events_r), 2)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.REJECTED])

        #Create a new enrollment proposal

        #Build the Service Agreement Proposal to enroll
        sap = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id )

        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),2)

        users = self.org_client.find_enrolled_users(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(users),0)

        #Manager approves proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.EnrollmentProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        users = self.org_client.find_enrolled_users(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(users),1)

        #User tried requesting enrollment again - this should fail
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id )
            neg_id = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: not is_enrolled',cm.exception.message)

        gevent.sleep(2)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.EnrollmentNegotiationStatusEvent)
        self.assertEquals(len(events_r), 4)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_org_role_negotiation(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")


        with self.assertRaises(BadRequest) as cm:
            myorg = self.org_client.read_org()
        self.assertTrue(cm.exception.message == 'The org_id parameter is missing')

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_user_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Now create user with proper credentials
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "user id=" + actor_id)

        #Build the message headers used with this user
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        users = self.org_client.find_enrolled_users(self.ion_org._id, headers=self.system_actor_header)
        self.assertEqual(len(users),3)  # WIll include the ION system actor and the non user actor from setup

        ## test_org_roles and policies

        roles = self.org_client.find_org_roles(self.ion_org._id)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE, ION_MANAGER])

        roles = self.org_client.find_org_roles_by_user(self.ion_org._id, self.system_actor._id, headers=self.system_actor_header)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.name for r in roles], [ORG_MEMBER_ROLE, ORG_MANAGER_ROLE, ION_MANAGER])

        roles = self.org_client.find_org_roles_by_user(self.ion_org._id, actor_id, headers=self.system_actor_header)
        self.assertEqual(len(roles),1)
        self.assertItemsEqual([r.name for r in roles], [ORG_MEMBER_ROLE])


        #Create a second Org
        with self.assertRaises(NotFound) as nf:
            org2 = self.org_client.find_org(ORG2)
        self.assertIn('The Org with name Org2 does not exist',nf.exception.message)

        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),2)
        self.assertItemsEqual([r.name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE])

        #Create the Instrument Operator Role
        operator_role = IonObject(RT.UserRole, name=INSTRUMENT_OPERATOR_ROLE,label='Instrument Operator', description='Instrument Operator')

        #First try to add the user role anonymously
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.add_user_role(org2_id, operator_role, headers=self.anonymous_user_headers)
        self.assertIn('org_management(add_user_role) has been denied',cm.exception.message)

        self.org_client.add_user_role(org2_id, operator_role, headers=self.system_actor_header)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE,  INSTRUMENT_OPERATOR_ROLE])

        #Add the same role to the first Org as well
        self.org_client.add_user_role(self.ion_org._id, operator_role, headers=self.system_actor_header)

        # test proposals roles.

        #First try to find user requests anonymously
        with self.assertRaises(Unauthorized) as cm:
            requests = self.org_client.find_org_negotiations(org2_id, headers=self.anonymous_user_headers)
        self.assertIn('org_management(find_org_negotiations) has been denied',cm.exception.message)

        #Next try to find user requests as as a basic member
        with self.assertRaises(Unauthorized) as cm:
            requests = self.org_client.find_org_negotiations(org2_id, headers=actor_header)
        self.assertIn('org_management(find_org_negotiations) has been denied',cm.exception.message)

        #Should not be denied for user with Org Manager role or ION System manager role
        requests = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(requests),0)

        #Build the Service Agreement Proposal for assigning  a role to a user
        sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=INSTRUMENT_OPERATOR_ROLE )

        # First try to request a role anonymously
        with self.assertRaises(Unauthorized) as cm:
            sap_response = self.org_client.negotiate(sap, headers=self.anonymous_user_headers)
        self.assertIn('org_management(negotiate) has been denied',cm.exception.message)

        # Next try to propose to assign a role without being a member
        with self.assertRaises(BadRequest) as cm:
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_enrolled',cm.exception.message)

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),0)

        #Build the Service Agreement Proposal to enroll
        sap = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id )

        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        users = self.org_client.find_enrolled_users(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(users),0)

        #Manager approves proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.EnrollmentProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        users = self.org_client.find_enrolled_users(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(users),1)

        #Create a proposal to add a role to a user
        sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=INSTRUMENT_OPERATOR_ROLE )
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        ret = self.org_client.has_role(org2_id, actor_id,INSTRUMENT_OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, False)

        #Run through a series of differet finds to ensure the various parameter filters are working.
        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_org_negotiations(org2_id,negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.RequestRoleProposal, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        #Manager  rejects the initial role proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.RequestRoleProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.REJECTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_org_negotiations(org2_id,negotiation_status=NegotiationStatusEnum.REJECTED, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),1)

        self.assertEqual(negotiations[0].negotiation_status, NegotiationStatusEnum.REJECTED)

        #Make sure the user still does not have the requested role
        ret = self.org_client.has_role(org2_id, actor_id,INSTRUMENT_OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, False)


        gevent.sleep(2)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.RequestRoleNegotiationStatusEvent)
        self.assertEquals(len(events_r), 2)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.REJECTED])


        #Create a second proposal to add a role to a user
        sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=INSTRUMENT_OPERATOR_ROLE )
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),3)

        #Create an instrument resource
        ia_list,_ = self.rr_client.find_resources(restype=RT.InstrumentAgent)

        self.assertEqual(len(ia_list),0)

        ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent1', description='The first Instrument Agent')

        #Intruments should not be able to be created by anoymous users
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.create_instrument_agent(ia_obj, headers=self.anonymous_user_headers)
        self.assertIn('instrument_management(create_instrument_agent) has been denied',cm.exception.message)

        #Intruments should not be able to be created by users that are not Instrument Operators
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.create_instrument_agent(ia_obj, headers=actor_header)
        self.assertIn('instrument_management(create_instrument_agent) has been denied',cm.exception.message)

        #Manager approves proposal for role request
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.RequestRoleProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        #mke sure there are no more open negotiations
        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)
        self.assertEqual(len(negotiations),0)

        #Verify the user has been assigned the requested role in the second Org
        ret = self.org_client.has_role(org2_id, actor_id,INSTRUMENT_OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, True)

        #Verify the user has only been assigned the requested role in the second Org and not in the first Org
        ret = self.org_client.has_role(self.ion_org._id, actor_id,INSTRUMENT_OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, False)

        #Refresh headers with new role
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #Now the user with the proper role should be able to create an instrument.
        self.ims_client.create_instrument_agent(ia_obj, headers=actor_header)

        gevent.sleep(2)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.RequestRoleNegotiationStatusEvent)
        self.assertEquals(len(events_r), 4)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])
        self.assertEqual(events_r[-1][2].role_name, sap_response2.role_name)



    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_org_acquire_resource_negotiation(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")

        with self.assertRaises(BadRequest) as cm:
            myorg = self.org_client.read_org()
        self.assertTrue(cm.exception.message == 'The org_id parameter is missing')

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_user_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Now create user with proper credentials
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "user id=" + actor_id)

        #Create a second Org
        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),2)
        self.assertItemsEqual([r.name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE])

        #Create the Instrument Operator Role
        operator_role = IonObject(RT.UserRole, name=INSTRUMENT_OPERATOR_ROLE,label='Instrument Operator', description='Instrument Operator')

        #And add it to all Orgs
        self.org_client.add_user_role(self.ion_org._id, operator_role, headers=self.system_actor_header)
        self.org_client.add_user_role(org2_id, operator_role, headers=self.system_actor_header)

        #Add the INSTRUMENT_OPERATOR_ROLE to the User for the ION Org
        self.org_client.grant_role(self.ion_org._id, actor_id, INSTRUMENT_OPERATOR_ROLE, headers=self.system_actor_header)

        #Enroll the user in the second Org - do without Negotiation for test
        self.org_client.enroll_member(org2_id, actor_id,headers=self.system_actor_header )

        #Build the message headers used with this user
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #Test the invitation process

        #Create a invitation proposal to add a role to a user
        sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=INSTRUMENT_OPERATOR_ROLE,
            originator=ProposalOriginatorEnum.PROVIDER )
        sap_response = self.org_client.negotiate(sap, headers=self.system_actor_header )

        ret = self.org_client.has_role(org2_id, actor_id,INSTRUMENT_OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, False)

        #User creates proposal to approve
        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.RequestRoleProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED)
        sap_response2 = self.org_client.negotiate(sap_response, headers=actor_header )

        #Verify the user has been assigned the requested role in the second Org
        ret = self.org_client.has_role(org2_id, actor_id,INSTRUMENT_OPERATOR_ROLE, headers=actor_header )
        self.assertEqual(ret, True)

        #Build the message headers used with this user
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        gevent.sleep(2)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.RequestRoleNegotiationStatusEvent)
        self.assertEquals(len(events_r), 4)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])

        #Create the instrument agent with the user that has the proper role
        ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent1', description='The Instrument Agent')
        self.ims_client.create_instrument_agent(ia_obj, headers=actor_header)

        #Ensure the instrument agent has been created
        ia_list,_ = self.rr_client.find_resources(restype=RT.InstrumentAgent)
        self.assertEqual(len(ia_list),1)
        self.assertEquals(ia_list[0].lcstate, LCS.DRAFT_PRIVATE)

        #Advance the Life cycle to planned. Must be INSTRUMENT_OPERATOR so anonymous user should fail
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_agent_lifecycle(ia_list[0]._id, LCE.PLAN, headers=self.anonymous_user_headers)
        self.assertIn( 'instrument_management(execute_instrument_agent_lifecycle) has been denied',cm.exception.message)

        #Advance the Life cycle to planned. Must be INSTRUMENT_OPERATOR
        self.ims_client.execute_instrument_agent_lifecycle(ia_list[0]._id, LCE.PLAN, headers=actor_header)
        ia = self.rr_client.read(ia_list[0]._id)
        self.assertEquals(ia.lcstate, LCS.PLANNED_PRIVATE)


        #First make a acquire resource request with an non-enrolled user.
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=self.system_actor._id, provider=org2_id, resource=ia_list[0]._id )
            sap_response = self.org_client.negotiate(sap, headers=self.system_actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_enrolled',cm.exception.message)


        #Make a proposal to acquire a resource with an enrolled user that has the right role but the resource is not shared the Org
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_resource_shared',cm.exception.message)

        #So share the resource
        self.org_client.share_resource(org_id=org2_id, resource_id=ia_list[0]._id, headers=self.system_actor_header  )

        #Verify the resource is shared
        res_list,_ = self.rr_client.find_objects(org2,PRED.hasResource)
        self.assertEqual(len(res_list), 1)
        self.assertEqual(res_list[0]._id, ia_list[0]._id)


        #First try to acquire the resource exclusively but it should fail since the user cannot do this without first
        #having had acquired the resource
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_resource_acquired',cm.exception.message)


        #Make a proposal to acquire a resource with an enrolled user that has the right role and is now shared
        sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource=ia_list[0]._id)
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.AcquireResourceProposal, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        self.assertEqual(negotiations[0]._id, sap_response.negotiation_id)


        #Manager Creates a counter proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        #Counter proposals for demonstration only
        #Calculate one week from now in milliseconds
        cur_time = int(get_ion_ts())
        week_expiration = cur_time +  ( 7 * 24 * 60 * 60 * 1000 )

        sap_response = Negotiation.create_counter_proposal(negotiations[0], originator=ProposalOriginatorEnum.PROVIDER)
        sap_response.expiration = week_expiration
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        #User Creates a counter proposal
        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)

        cur_time = int(get_ion_ts())
        month_expiration = cur_time +  ( 30 * 24 * 60 * 60 * 1000 )

        sap_response = Negotiation.create_counter_proposal(negotiations[0])
        sap_response.expiration = month_expiration
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )


        gevent.sleep(2)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.AcquireResourceNegotiationStatusEvent)
        self.assertEquals(len(events_r), 3)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.COUNTER])
        self.assertEqual(events_r[-1][2].resource, ia_list[0]._id)


        #Manager approves Instrument resource proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        #User accepts proposal in return
        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED)
        sap_response2 = self.org_client.negotiate(sap_response, headers=actor_header )

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)
        self.assertEqual(len(negotiations),0)

        #Check commitment to be active
        commitments, _ = self.rr_client.find_objects(ia_list[0]._id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),1)

        resource_commitment, _ = self.rr_client.find_objects(actor_id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(resource_commitment),1)
        self.assertNotEqual(resource_commitment[0].lcstate, LCS.RETIRED)


        subjects, _ = self.rr_client.find_subjects(None,PRED.hasCommitment, commitments[0]._id)
        self.assertEqual(len(subjects),3)

        contracts, _ = self.rr_client.find_subjects(RT.Negotiation,PRED.hasContract, commitments[0]._id)
        self.assertEqual(len(contracts),1)

        cur_time = int(get_ion_ts())
        invalid_expiration = cur_time +  ( 13 * 60 * 60 * 1000 ) # 12 hours from now

        #Now try to acquire the resource exclusively for longer than 12 hours
        sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource=ia_list[0]._id,
                    expiration=invalid_expiration)
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        #make sure the negotiation was rejected for being too long.
        negotiation = self.rr_client.read(sap_response.negotiation_id)
        self.assertEqual(negotiation.negotiation_status, NegotiationStatusEnum.REJECTED)

        #Now try to acquire the resource exclusively for 20 minutes
        cur_time = int(get_ion_ts())
        valid_expiration = cur_time +  ( 20 * 60 * 1000 ) # 12 hours from now

        sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource=ia_list[0]._id,
                    expiration=valid_expiration)
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        #Check commitment to be active
        commitments, _ = self.rr_client.find_objects(ia_list[0]._id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),2)

        exclusive_contract, _ = self.rr_client.find_objects(sap_response.negotiation_id,PRED.hasContract, RT.Commitment)
        self.assertEqual(len(contracts),1)

        #Now try to acquire the resource exclusively again - should fail
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: not is_resource_acquired_exclusively',cm.exception.message)

        #Release the exclusive commitment to the resource
        self.org_client.release_commitment(exclusive_contract[0]._id, headers=actor_header)

        #Check commitment to be inactive
        commitments, _ = self.rr_client.find_objects(ia_list[0]._id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),2)
        for com in commitments:
            if com.commitment.exclusive:
                self.assertEqual(com.lcstate, LCS.RETIRED)
            else:
                self.assertNotEqual(com.lcstate, LCS.RETIRED)

        #Release the resource
        self.org_client.release_commitment(resource_commitment[0]._id, headers=actor_header)

        #Check commitment to be inactive
        commitments, _ = self.rr_client.find_objects(ia_list[0]._id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),2)
        for com in commitments:
            self.assertEqual(com.lcstate, LCS.RETIRED)

        commitments, _ = self.rr_client.find_objects(actor_id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),2)
        for com in commitments:
            self.assertEqual(com.lcstate, LCS.RETIRED)


        #Now check some negative cases...

        #Attempt to acquire the same resource from the ION Org which is not sharing it - should fail
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=self.ion_org._id, resource=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_resource_shared',cm.exception.message)


        #Remove the INSTRUMENT_OPERATOR_ROLE from the user.
        self.org_client.revoke_role(org2_id, actor_id, INSTRUMENT_OPERATOR_ROLE,  headers=self.system_actor_header)

        #Refresh headers with new role
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #Make a proposal to acquire a resource with an enrolled user that does not have the right role
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource=ia_list[0]._id )
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: has_role',cm.exception.message)

        gevent.sleep(2)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.AcquireResourceNegotiationStatusEvent)
        self.assertEquals(len(events_r), 6)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])
        self.assertEqual(events_r[-1][2].resource, ia_list[0]._id)


    @attr('LOCOINT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    @patch.dict(CFG, {'system':{'load_policy':True}})
    def test_instrument_agent_policy(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")



        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_user_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Create user
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.debug( "user id=" + actor_id)

        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #Create a second Org
        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),2)
        self.assertItemsEqual([r.name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE])

        #Create Instrument Operator Role and add it to the second Org
        operator_role = IonObject(RT.UserRole, name=INSTRUMENT_OPERATOR_ROLE, label='Instrument Operator', description='Instrument Operator')
        self.org_client.add_user_role(org2_id, operator_role, headers=self.system_actor_header)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE, INSTRUMENT_OPERATOR_ROLE])


        #Create Test InstrumentDevice - use the system admin for now
        inst_obj = IonObject(RT.InstrumentDevice, name='Test_Instrument_123')
        inst_obj_id,_ = self.rr_client.create(inst_obj, headers=self.system_actor_header)

        #Startup an agent - TODO: will fail with Unauthorized to spawn process if not right user role
        from ion.agents.instrument.test.test_instrument_agent import start_instrument_agent_process
        ia_client = start_instrument_agent_process(self.container, resource_id=inst_obj_id, resource_name=inst_obj.name,
            org_name=org2.name, message_headers=self.system_actor_header)

        #First try a basic agent operation anonymously - it should be denied
        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_capabilities(headers=self.anonymous_user_headers )
        self.assertIn('(get_capabilities) has been denied',cm.exception.message)

        #However the ION Manager should be allowed
        retval = ia_client.get_capabilities(headers=self.system_actor_header)

        #Next try a basic agent operation with a user that is not an Instrument Operator or Member of Org - it should be denied
        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_capabilities(headers=actor_header)
        self.assertIn('(get_capabilities) has been denied',cm.exception.message)

        #Attempt to grant role to user but not as a Member of the Org so it will fail
        with self.assertRaises(BadRequest) as cm:
            self.org_client.grant_role(org2_id,actor_id, INSTRUMENT_OPERATOR_ROLE, headers=self.system_actor_header)
        self.assertIn('The user is not a member of the specified Org',cm.exception.message)

        #Enroll user in second Org
        self.org_client.enroll_member(org2_id,actor_id, headers=self.system_actor_header)

        #Refresh header with updated roles
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #Next try a basic agent operation with a user that is a member of the Org but not an Instrument Operator - it should be allowed
        retval = ia_client.get_capabilities(headers=actor_header)

        #This agent operation should not be allowed for anonymous user
        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_resource_state(headers=self.anonymous_user_headers)
        self.assertIn('(get_resource_state) has been denied',cm.exception.message)

        #This agent operation should not be allowed for a user that is not an Instrument Operator
        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_resource_state(headers=actor_header)
        self.assertIn('(get_resource_state) has been denied',cm.exception.message)

        #This agent operation should not be allowed for a user that is not an Instrument Operator
        with self.assertRaises(Unauthorized) as cm:
            params = SBE37Parameter.ALL
            retval = ia_client.get_resource(params, headers=actor_header)
        self.assertIn('(get_resource) has been denied',cm.exception.message)

        #This agent operation should not be allowed for a user that is not an Instrument Operator
        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_agent_state(headers=actor_header)
            self.assertEqual(retval, ResourceAgentState.UNINITIALIZED)
        self.assertIn('(get_agent_state) has been denied',cm.exception.message)

        #Grant the role of Instrument Operator to the user
        self.org_client.grant_role(org2_id,actor_id, INSTRUMENT_OPERATOR_ROLE, headers=self.system_actor_header)

        #Refresh header with updated roles
        actor_header = self.container.governance_controller.get_actor_header(actor_id)

        #This operation should now be allowed with the Instrument Operator role - just checking policy not agent functionality
        with self.assertRaises(Conflict) as cm:
            res_state = ia_client.get_resource_state(headers=actor_header)

        #This operation should now be allowed with the Instrument Operator role
        with self.assertRaises(Conflict) as cm:
            params = SBE37Parameter.ALL
            retval = ia_client.get_resource(params, headers=actor_header)


        #This agent operation should now be allowed for a user that is an Instrument Operator
        retval = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(retval, ResourceAgentState.UNINITIALIZED)


        #The execute commnand should fail if the user has not acquired the resource
        with self.assertRaises(Unauthorized) as cm:
            cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
            retval = ia_client.execute_resource(cmd, headers=actor_header)
        self.assertIn('(execute_resource) has been denied',cm.exception.message)


        #Going to try access to other operations on the agent, don't care if they actually work - just
        #do they get denied or not
        new_params = {
            SBE37Parameter.TA0 : 2,
            SBE37Parameter.INTERVAL : 1,
        }

        #First try anonymously - should be denied
        with self.assertRaises(Unauthorized) as cm:
            ia_client.set_resource(new_params, headers=self.anonymous_user_headers)
        self.assertIn('(set_resource) has been denied',cm.exception.message)

        #THey try with user with Instrument Operator role, but should fail with out acquiring a resource
        with self.assertRaises(Unauthorized) as cm:
            ia_client.set_resource(new_params, headers=actor_header)
        self.assertIn('(set_resource) has been denied',cm.exception.message)

        #Make a proposal to acquire a resource with an enrolled user that has the right role - but the resource is not shared
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource=inst_obj_id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_resource_shared',cm.exception.message)

        #So share the resource
        self.org_client.share_resource(org_id=org2_id, resource_id=inst_obj_id, headers=self.system_actor_header  )

        #Renegotiate the proposal
        sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource=inst_obj_id)
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        #Have the Org accept the proposal
        negotiation = self.rr_client.read(sap_response.negotiation_id)
        sap_response2 = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response3 = self.org_client.negotiate(sap_response2, headers=self.system_actor_header )

        #Have the User counter-accept the proposal
        negotiation = self.rr_client.read(sap_response3.negotiation_id)
        sap_response4 = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED)
        sap_response5 = self.org_client.negotiate(sap_response4, headers=actor_header )

        #This operation should now be allowed since the resource has been acquired
        with self.assertRaises(Conflict) as cm:
            cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
            retval = ia_client.execute_resource(cmd, headers=actor_header)

        #This operation should now be allowed since the resource has been acquired
        with self.assertRaises(Conflict) as cm:
           ia_client.set_resource(new_params, headers=actor_header)

        resource_commitment, _ = self.rr_client.find_objects(actor_id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(resource_commitment),1)
        self.assertNotEqual(resource_commitment[0].lcstate, LCS.RETIRED)


        #Request for the instrument to be put into Direct Access mode - should be denied since user does not have exclusive  access
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.request_direct_access(inst_obj_id, headers=actor_header)
        self.assertIn('Direct Access Mode has been denied',cm.exception.message)

        #Request to access the resource exclusively for two hours
        cur_time = int(get_ion_ts())
        two_hour_expiration = cur_time +  ( 2 * 60 * 60 * 1000 ) # 2 hours from now

        sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource=inst_obj_id,
                    expiration=two_hour_expiration)
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        #Request Direct Access again
        with self.assertRaises(Exception) as cm:
            self.ims_client.request_direct_access(inst_obj_id, headers=actor_header)

        #Stop DA
        with self.assertRaises(Exception) as cm:
            self.ims_client.stop_direct_access(inst_obj_id, headers=actor_header)

        #Release the exclusive commitment to the resource
        exclusive_contract, _ = self.rr_client.find_objects(sap_response.negotiation_id,PRED.hasContract, RT.Commitment)
        self.org_client.release_commitment(exclusive_contract[0]._id, headers=actor_header)

        #Try to Request for the instrument to be put into Direct Access mode - should be denied since user does not have exclusive  access
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.request_direct_access(inst_obj_id, headers=actor_header)
        self.assertIn('Direct Access Mode has been denied',cm.exception.message)

        #The agent related functions should not be allowed for a user that is not an Org Manager
        with self.assertRaises(Unauthorized) as cm:
            cmd = AgentCommand(command=ResourceAgentEvent.RESET)
            retval = ia_client.execute_agent(cmd, headers=actor_header)
        self.assertIn('(execute_agent) has been denied',cm.exception.message)

        #Now release the shared commitment
        #Check commitment to be inactive
        commitments, _ = self.rr_client.find_objects(inst_obj_id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),2)
        for com in commitments:
            if com.commitment.exclusive:
                self.assertEqual(com.lcstate, LCS.RETIRED)
            else:
                self.assertNotEqual(com.lcstate, LCS.RETIRED)
                #Release the resource
                self.org_client.release_commitment(resource_commitment[0]._id, headers=actor_header)


        #Check commitment to be inactive
        commitments, _ = self.rr_client.find_objects(inst_obj_id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),2)
        for com in commitments:
            self.assertEqual(com.lcstate, LCS.RETIRED)


        #Try again with user with only Instrument Operator role, but should fail with out acquiring a resource
        with self.assertRaises(Unauthorized) as cm:
            ia_client.set_resource(new_params, headers=actor_header)
        self.assertIn('(set_resource) has been denied',cm.exception.message)


        #Grant the role of Org Manager to the user
        self.org_client.grant_role(org2_id,actor_id, ORG_MANAGER_ROLE, headers=self.system_actor_header)

        #Refresh header with updated roles
        actor_header = self.container.governance_controller.get_actor_header(actor_id)


        #Try again with user with also Org Manager role and should pass even with out acquiring a resource
        with self.assertRaises(Conflict) as cm:
            ia_client.set_resource(new_params, headers=actor_header)


        #Now reset the agent for checking operation based policy
        #The reset command should now be allowed for the Org Manager

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = ia_client.execute_agent(cmd, headers=actor_header)
        state = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = ia_client.execute_agent(cmd, headers=actor_header)
        retval = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(retval, ResourceAgentState.UNINITIALIZED)


        #Test access precondition to deny get_current_state commands but allow all others
        pre_func1 =\
        """def precondition_func(process, msg, headers):
            from pyon.agent.agent import ResourceAgentEvent
            if msg['command'].command == ResourceAgentEvent.RESET:
                return False, 'ResourceAgentEvent.RESET is being denied'
            else:
                return True, ''

        """

        #Add an example of a operation specific policy that checks internal values to decide on access
        pol_id = self.pol_client.add_process_operation_precondition_policy(process_name=RT.InstrumentDevice, op='execute_agent', policy_content=pre_func1, headers=self.system_actor_header )

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The initialize command should be allowed
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = ia_client.execute_agent(cmd, headers=actor_header)
        retval = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(retval, ResourceAgentState.INACTIVE)


        #The reset command should be denied
        with self.assertRaises(Unauthorized) as cm:
            cmd = AgentCommand(command=ResourceAgentEvent.RESET)
            retval = ia_client.execute_agent(cmd, headers=actor_header)
        self.assertIn( 'ResourceAgentEvent.RESET is being denied',cm.exception.message)


        #Now delete the get_current_state policy and try again
        self.pol_client.delete_policy(pol_id, headers=self.system_actor_header)

        gevent.sleep(1)  # Wait for events to be published and policy updated

        #The reset command should now be allowed
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = ia_client.execute_agent(cmd, headers=actor_header)
        retval = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(retval, ResourceAgentState.UNINITIALIZED)

        #The initialize command should be allowed
        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = ia_client.execute_agent(cmd, headers=actor_header)
        retval = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(retval, ResourceAgentState.INACTIVE)

        #Now try to go into Direct Access Mode directly through the agent as an Org Manager
        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
            #kwargs={'session_type': DirectAccessTypes.telnet,
            kwargs={'session_type':DirectAccessTypes.vsp,
                    'session_timeout':600,
                    'inactivity_timeout':600})

        with self.assertRaises(Conflict) as cm:
            retval = ia_client.execute_agent(cmd, headers=actor_header)
            state = ia_client.get_agent_state(headers=actor_header)
            self.assertEqual(state, ResourceAgentState.DIRECT_ACCESS)

        #Exit DA Mode
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = ia_client.execute_agent(cmd, headers=actor_header)
        retval = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(retval, ResourceAgentState.UNINITIALIZED)
