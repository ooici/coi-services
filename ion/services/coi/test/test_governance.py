#!/usr/bin/env python

__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import unittest, os, gevent, platform, simplejson
from mock import Mock, patch
from pyon.util.containers import get_ion_ts
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.util.context import LocalContextMixin

from pyon.agent.agent import ResourceAgentState, ResourceAgentEvent

from pyon.datastore.datastore import DatastoreManager
from pyon.event.event import EventRepository

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound, Unauthorized, InstStateError
from pyon.public import PRED, RT, IonObject, CFG, log, OT, LCS, LCE, AS
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from interface.services.coi.iexchange_management_service import ExchangeManagementServiceProcessClient
from interface.services.coi.ipolicy_management_service import PolicyManagementServiceProcessClient
from interface.services.cei.ischeduler_service import SchedulerServiceProcessClient
from interface.services.coi.isystem_management_service import SystemManagementServiceProcessClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceProcessClient
from pyon.ion.resregistry import ResourceRegistryServiceWrapper
from interface.objects import AgentCommand, ProposalOriginatorEnum, ProposalStatusEnum, NegotiationStatusEnum, ComputedValueAvailability
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessTypes
from pyon.core.governance.negotiation import Negotiation
from ion.processes.bootstrap.load_system_policy import LoadSystemPolicy
from pyon.core.governance import ORG_MANAGER_ROLE, ORG_MEMBER_ROLE, ION_MANAGER, get_system_actor, get_system_actor_header
from pyon.core.governance import get_actor_header, get_web_authentication_actor
from ion.services.sa.observatory.observatory_management_service import INSTRUMENT_OPERATOR_ROLE, OBSERVATORY_OPERATOR_ROLE
from pyon.net.endpoint import RPCClient, BidirClientChannel

from ion.services.sa.test.test_find_related_resources import ResourceHelper


ORG2 = 'Org 2'


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

DENY_EXCHANGE_TEXT = '''
        <Rule RuleId="%s" Effect="Deny">
            <Description>
                %s
            </Description>

            <Target>
                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">exchange_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

            </Target>

        </Rule>
        '''


TEST_POLICY_TEXT = '''
        <Rule RuleId="%s" Effect="Permit">
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
        <Rule RuleId="%s" Effect="Deny">
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

###########


DENY_PARAM_50_RULE = '''
            <Rule RuleId="%s:" Effect="Permit">
                <Description>
                    %s
                </Description>

                <Target>
                    <Resources>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">.*$</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">agent</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                    </Resources>

                    <Actions>
                        <Action>
                            <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">set_resource</AttributeValue>
                                <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ActionMatch>
                        </Action>
                    </Actions>

                </Target>

              <Condition>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-code">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string"><![CDATA[def policy_func(process, message, headers):
                            params = message['params']
                            if params['INTERVAL'] <= 50:
                                return True, ''
                            return False, 'The value for SBE37Parameter.INTERVAL cannot be greater than 50'
                        ]]>
                        </AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                    </Apply>
                </Condition>

            </Rule>
            '''


DENY_PARAM_30_RULE = '''
            <Rule RuleId="%s:" Effect="Permit">
                <Description>
                    %s
                </Description>

                <Target>
                    <Resources>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">.*$</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">agent</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                    </Resources>

                    <Actions>
                        <Action>
                            <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">set_resource</AttributeValue>
                                <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ActionMatch>
                        </Action>
                    </Actions>

                </Target>

              <Condition>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-code">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string"><![CDATA[def policy_func(process, message, headers):
                            params = message['params']
                            if params['INTERVAL'] <= 30:
                                return True, ''
                            return False, 'The value for SBE37Parameter.INTERVAL cannot be greater than 30'
                        ]]>
                        </AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                    </Apply>
                </Condition>

            </Rule>
            '''

DENY_PARAM_10_RULE = '''
            <Rule RuleId="%s:" Effect="Permit">
                <Description>
                    %s
                </Description>

                <Target>
                    <Resources>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">.*$</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                        <Resource>
                            <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">agent</AttributeValue>
                                <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ResourceMatch>
                        </Resource>
                    </Resources>

                    <Actions>
                        <Action>
                            <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">set_resource</AttributeValue>
                                <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            </ActionMatch>
                        </Action>
                    </Actions>

                </Target>

              <Condition>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-code">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string"><![CDATA[def policy_func(process, message, headers):
                            params = message['params']
                            if params['INTERVAL'] <= 10:
                                return True, ''
                            return False, 'The value for SBE37Parameter.INTERVAL cannot be greater than 10'
                        ]]>
                        </AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                    </Apply>
                </Condition>

            </Rule>
            '''



@attr('INT', group='coi')
class TestGovernanceHeaders(IonIntegrationTestCase):
    def setUp(self):

        # Start container
        self._start_container()

        #Load a deploy file
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        #Instantiate a process to represent the test
        process=GovernanceTestProcess()

        self.rr_client = ResourceRegistryServiceProcessClient(node=self.container.node, process=process)

        #Get info on the ION System Actor
        self.system_actor = get_system_actor()
        log.info('system actor:' + self.system_actor._id)

        self.system_actor_header = get_system_actor_header()

        self.resource_id_header_value = ''

    @attr('LOCOINT')
    @attr('HEADERS')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_governance_message_headers(self):
        '''
        This test is used to make sure the ION endpoint code is properly setting the
        '''

        #Get function pointer to send function
        old_send = BidirClientChannel._send

        # make new send to patch on that duplicates send
        def patched_send(*args, **kwargs):

            #Only duplicate the message send from the initial client call
            msg_headers = kwargs['headers']

            self.resource_id_header_value = ''

            if msg_headers.has_key('resource-id'):
                self.resource_id_header_value = msg_headers['resource-id']

            return old_send(*args, **kwargs)

        # patch it into place with auto-cleanup to try to interogate the message headers
        patcher = patch('pyon.net.endpoint.BidirClientChannel._send', patched_send)
        patcher.start()
        self.addCleanup(patcher.stop)

        # Instantiate an object
        obj = IonObject("UserInfo", name="name")

        # Can't call update with object that hasn't been persisted
        with self.assertRaises(BadRequest) as cm:
            self.rr_client.update(obj)
       # self.assertTrue(cm.exception.message.startswith("Object does not have required '_id' or '_rev' attribute"))

        self.resource_id_header_value = ''

        # Persist object and read it back
        obj_id, obj_rev = self.rr_client.create(obj)
        log.debug('The id of the created object is %s', obj_id)
        self.assertEqual(self.resource_id_header_value, '' )

        self.resource_id_header_value = ''
        read_obj = self.rr_client.read(obj_id)
        self.assertEqual(self.resource_id_header_value, obj_id )

        # Cannot create object with _id and _rev fields pre-set
        self.resource_id_header_value = ''
        with self.assertRaises(BadRequest) as cm:
            self.rr_client.create(read_obj)
        #self.assertTrue(cm.exception.message.startswith("Doc must not have '_id'"))
        self.assertEqual(self.resource_id_header_value, '' )

        # Update object
        read_obj.name = "John Doe"
        self.resource_id_header_value = ''
        self.rr_client.update(read_obj)
        self.assertEqual(self.resource_id_header_value, obj_id )

        # Update should fail with revision mismatch
        self.resource_id_header_value = ''
        with self.assertRaises(Conflict) as cm:
            self.rr_client.update(read_obj)
        #self.assertTrue(cm.exception.message.startswith("Object not based on most current version"))
        self.assertEqual(self.resource_id_header_value, obj_id )

        # Re-read and update object
        self.resource_id_header_value = ''
        read_obj = self.rr_client.read(obj_id)
        self.assertEqual(self.resource_id_header_value, obj_id )

        self.resource_id_header_value = ''
        self.rr_client.update(read_obj)
        self.assertEqual(self.resource_id_header_value, obj_id )

        #Create second object
        obj = IonObject("UserInfo", name="Babs Smith")

        self.resource_id_header_value = ''

        # Persist object and read it back
        obj2_id, obj2_rev = self.rr_client.create(obj)
        log.debug('The id of the created object is %s', obj_id)
        self.assertEqual(self.resource_id_header_value, '' )

        #Test for multi-read
        self.resource_id_header_value = ''
        objs = self.rr_client.read_mult([obj_id, obj2_id])

        self.assertAlmostEquals(self.resource_id_header_value, [obj_id, obj2_id])
        self.assertEqual(len(objs),2)

        # Delete object
        self.resource_id_header_value = ''
        self.rr_client.delete(obj_id)
        self.assertEqual(self.resource_id_header_value, obj_id )


        # Delete object
        self.resource_id_header_value = ''
        self.rr_client.delete(obj2_id)
        self.assertEqual(self.resource_id_header_value, obj2_id )


class GovernanceTestProcess(LocalContextMixin):
    name = 'gov_test'
    id='gov_client'
    process_type = 'simple'

@attr('INT', group='coi')
class TestGovernanceInt(IonIntegrationTestCase):


    def __init__(self, *args, **kwargs):

        #Hack for running tests on CentOS which is significantly slower than a Mac
        self.SLEEP_TIME = 1
        ver = platform.mac_ver()
        if ver[0] == '':
            self.SLEEP_TIME = 3  # Increase for non Macs
            log.info('Not running on a Mac')
        else:
            log.info('Running on a Mac)')

        IonIntegrationTestCase.__init__(self, *args, **kwargs)

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
            log.debug('Loading policy')
            LoadSystemPolicy.op_load_system_policies(process)

        gevent.sleep(self.SLEEP_TIME*2)  # Wait for events to be fired and policy updated

        self.rr_msg_client = ResourceRegistryServiceProcessClient(node=self.container.node, process=process)
        self.rr_client = ResourceRegistryServiceWrapper(self.container.resource_registry, process)

        self.id_client = IdentityManagementServiceProcessClient(node=self.container.node, process=process)

        self.pol_client = PolicyManagementServiceProcessClient(node=self.container.node, process=process)

        self.org_client = OrgManagementServiceProcessClient(node=self.container.node, process=process)

        self.ims_client = InstrumentManagementServiceProcessClient(node=self.container.node, process=process)

        self.ems_client = ExchangeManagementServiceProcessClient(node=self.container.node, process=process)

        self.ssclient = SchedulerServiceProcessClient(node=self.container.node, process=process)

        self.sys_management = SystemManagementServiceProcessClient(node=self.container.node, process=process)

        self.obs_client = ObservatoryManagementServiceProcessClient(node=self.container.node, process=process)


        #Get info on the ION System Actor
        self.system_actor = get_system_actor()
        log.info('system actor:' + self.system_actor._id)

        self.system_actor_header = get_system_actor_header()

        #Get info on the Web Authentication Actor
        self.apache_actor = get_web_authentication_actor()
        if not self.apache_actor:
            #Can't find the apache actor so just use the system actor
            self.apache_actor = self.system_actor

        self.apache_actor_header = get_actor_header(self.apache_actor._id)

        self.anonymous_actor_headers = {'ion-actor-id':'anonymous'}

        self.ion_org = self.org_client.find_org()


        #Setup access to event repository
        dsm = DatastoreManager()
        ds = dsm.get_datastore("events")

        self.event_repo = EventRepository(dsm)


    def tearDown(self):
        policy_list, _ = self.rr_client.find_resources(restype=RT.Policy)

        #Must remove the policies in the reverse order they were added
        for policy in sorted(policy_list,key=lambda p: p.ts_created, reverse=True):
            self.pol_client.delete_policy(policy._id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and policy updated


    @attr('LOCOINT')
    @attr('BASIC')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_basic_policy_operations(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy, id_only=True)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")

        log.debug('Begin testing with policies')

        #First check existing policies to see if they are in place to keep an anonymous user from creating things
        with self.assertRaises(Unauthorized) as cm:
            test_org_id = self.org_client.create_org(org=IonObject(RT.Org, name='test_org', description='A test Org'))
        self.assertIn( 'org_management(create_org) has been denied',cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            test_org = self.org_client.find_org(name='test_org')

        #Add a new policy to deny all operations to the exchange_management by default .
        test_policy_id = self.pol_client.create_service_access_policy('exchange_management', 'Exchange_Management_Deny_Policy',
            'Deny all operations in  Exchange Management Service by default',
            DENY_EXCHANGE_TEXT, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and policy updated

        #Attempt to access an operation in service which does not have specific policies set
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #Add a new policy to allow the the above service call.
        test_policy_id = self.pol_client.create_service_access_policy('exchange_management', 'Exchange_Management_Test_Policy',
            'Allow specific operations in the Exchange Management Service for anonymous user',
            TEST_POLICY_TEXT, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)

        #disable the test policy to try again
        self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #now enable the test policy to try again
        self.pol_client.enable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        #Now test service operation specific policies - specifically that there can be more than one on the same operation.

        pol1_id = self.pol_client.add_process_operation_precondition_policy(process_name='policy_management', op='disable_policy', policy_content='func1_pass', headers=self.system_actor_header )

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #try to disable the test policy  again
        self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        #now enable the test policy to try again
        self.pol_client.enable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        pol2_id = self.pol_client.add_process_operation_precondition_policy(process_name='policy_management', op='disable_policy', policy_content='func2_deny', headers=self.system_actor_header )

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #try to disable the test policy again
        with self.assertRaises(Unauthorized) as cm:
            self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)
        self.assertIn( 'Denied for no reason',cm.exception.message)


        self.pol_client.delete_policy(pol2_id,  headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #try to disable the test policy  again
        self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)


        #try to enable the test policy  again
        self.pol_client.enable_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated


        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        pre_func1 =\
        """def precondition_func(process, msg, headers):
            if headers['op'] == 'disable_policy':
                return False, 'Denied for no reason again'
            else:
                return True, ''

        """
        #Create a dynamic precondition function to deny calls to disable policy
        pre_func1_id = self.pol_client.add_process_operation_precondition_policy(process_name='policy_management', op='disable_policy', policy_content=pre_func1, headers=self.system_actor_header )

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #try to disable the test policy again
        with self.assertRaises(Unauthorized) as cm:
            self.pol_client.disable_policy(test_policy_id, headers=self.system_actor_header)
        self.assertIn( 'Denied for no reason again',cm.exception.message)

        #Now delete the most recent precondition policy
        self.pol_client.delete_policy(pre_func1_id,  headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated


        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        #Now test that a precondition function can be enabled and disabled
        pre_func2 =\
        """def precondition_func(process, msg, headers):
            if headers['op'] == 'create_exchange_space':
                return False, 'Denied for from a operation precondition function'
            else:
                return True, ''

        """
        #Create a dynamic precondition function to deny calls to disable policy
        pre_func2_id = self.pol_client.add_process_operation_precondition_policy(process_name='exchange_management', op='create_exchange_space', policy_content=pre_func2, headers=self.system_actor_header )

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Denied for from a operation precondition function',cm.exception.message)


        #try to enable the precondition policy
        self.pol_client.disable_policy(pre_func2_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated


        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)

        #try to enable the precondition policy
        self.pol_client.enable_policy(pre_func2_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Denied for from a operation precondition function',cm.exception.message)

        #Delete the precondition policy
        self.pol_client.delete_policy(pre_func2_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The previous attempt at this operations should now be allowed.
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(BadRequest) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'Arguments not set',cm.exception.message)


        self.pol_client.delete_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #The same request that previously was allowed should now be denied
        es_obj = IonObject(RT.ExchangeSpace, description= 'ION test XS', name='ioncore2' )
        with self.assertRaises(Unauthorized) as cm:
            self.ems_client.create_exchange_space(es_obj, headers=self.anonymous_actor_headers)
        self.assertIn( 'exchange_management(create_exchange_space) has been denied',cm.exception.message)

        ###########
        ### Now test access to service create* operations based on roles...

        #Anonymous users should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            id = self.ssclient.create_interval_timer(start_time="now", event_origin="Interval_Timer_233", headers=self.anonymous_actor_headers)
        self.assertIn( 'scheduler(create_interval_timer) has been denied',cm.exception.message)

        #now try creating a new user with a valid actor
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "actor id=" + actor_id)
        actor_header = get_actor_header(actor_id)

        #User without OPERATOR or MANAGER role should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            id = self.ssclient.create_interval_timer(start_time="now", event_origin="Interval_Timer_233", headers=actor_header)
        self.assertIn( 'scheduler(create_interval_timer) has been denied',cm.exception.message)

        #Remove the INSTRUMENT_OPERATOR_ROLE from the user.
        self.org_client.grant_role(self.ion_org._id, actor_id, ORG_MANAGER_ROLE,  headers=self.system_actor_header)

        #Refresh headers with new role
        actor_header = get_actor_header(actor_id)

        #User with proper role should now be allowed to access this service operation.
        id = self.ssclient.create_interval_timer(start_time="now", event_origin="Interval_Timer_233", headers=actor_header)


    @attr('LOCOINT')
    @attr('RESET')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    @patch.dict(CFG, {'container':{'org_boundary':True}})
    def test_policy_cache_reset(self):

        before_policy_set = self.container.governance_controller.get_active_policies()


        #First clear all of the policies to test that failures will be caught due to missing policies
        self.container.governance_controller._reset_container_policy_caches()

        empty_policy_set = self.container.governance_controller.get_active_policies()
        self.assertEqual(len(empty_policy_set['service_access'].keys()), 0)
        self.assertEqual(len(empty_policy_set['resource_access'].keys()), 0)

        #With policies gone, an anonymous user should be able to create an object
        test_org_id = self.org_client.create_org(org=IonObject(RT.Org, name='test_org1', description='A test Org'))
        test_org = self.org_client.find_org(name='test_org1')
        self.assertEqual(test_org._id, test_org_id)

        #Trigger the event to reset the policy caches
        self.sys_management.reset_policy_cache()

        gevent.sleep(20)  # Wait for events to be published and policy reloaded for all running processes

        after_policy_set = self.container.governance_controller.get_active_policies()

        #With policies refreshed, an anonymous user should NOT be able to create an object
        with self.assertRaises(Unauthorized) as cm:
            test_org_id = self.org_client.create_org(org=IonObject(RT.Org, name='test_org2', description='A test Org'))
        self.assertIn( 'org_management(create_org) has been denied',cm.exception.message)

        with self.assertRaises(NotFound) as cm:
            test_org = self.org_client.find_org(name='test_org2')

        self.assertEqual(len(before_policy_set.keys()), len(after_policy_set.keys()))
        self.assertEqual(len(before_policy_set['service_access'].keys()), len(after_policy_set['service_access'].keys()))
        self.assertEqual(len(before_policy_set['resource_access'].keys()), len(after_policy_set['resource_access'].keys()))
        self.assertEqual(len(before_policy_set['service_operation'].keys()), len(after_policy_set['service_operation'].keys()))

        #If the number of keys for service operations were equal, then check each set of operation precondition functions
        for key in before_policy_set['service_operation']:
            self.assertEqual(len(before_policy_set['service_operation'][key]), len(after_policy_set['service_operation'][key]))


    @attr('LOCOINT')
    @attr('BOUNDARY')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    @patch.dict(CFG, {'container':{'org_boundary':True}})
    def test_org_boundary(self):

        with self.assertRaises(NotFound) as nf:
            org2 = self.org_client.find_org(ORG2)
        self.assertIn('The Org with name Org 2 does not exist',nf.exception.message)

        #Create a second Org
        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        #First try to get a list of Users by hitting the RR anonymously - should be allowed.
        actors,_ = self.rr_msg_client.find_resources(restype=RT.ActorIdentity)
        self.assertEqual(len(actors),2) #Should include the ION System Actor, Web auth actor.

        log.debug('Begin testing with policies')

        #Create a new actor - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_actor_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)


        #now try creating a new actors with a valid actor
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "actor id=" + actor_id)
        actor_header = get_actor_header(actor_id)

        #First try to get a list of Users by hitting the RR anonymously - should be allowed.
        actors,_ = self.rr_msg_client.find_resources(restype=RT.ActorIdentity)
        self.assertEqual(len(actors),3) #Should include the ION System Actor and web auth actor as well.

        #Now enroll the actor as a member of the Second Org
        self.org_client.enroll_member(org2_id,actor_id, headers=self.system_actor_header)
        actor_header = get_actor_header(actor_id)

        #Add a new Org boundary policy which deny's all anonymous access
        test_policy_id = self.pol_client.create_resource_access_policy( org2_id, 'Org_Test_Policy',
            'Deny all access for anonymous actor',
            TEST_BOUNDARY_POLICY_TEXT, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and policy updated

        #Hack to force container into an Org Boundary for second Org
        self.container.governance_controller._container_org_name = org2.org_governance_name
        self.container.governance_controller._is_container_org_boundary = True

        #First try to get a list of Users by hitting the RR anonymously - should be denied.
        with self.assertRaises(Unauthorized) as cm:
            actors,_ = self.rr_msg_client.find_resources(restype=RT.ActorIdentity, headers=self.anonymous_actor_headers)
        self.assertIn( 'resource_registry(find_resources) has been denied',cm.exception.message)


        #Now try to hit the RR with a real user and should now be allowed
        actors,_ = self.rr_msg_client.find_resources(restype=RT.ActorIdentity, headers=actor_header)
        self.assertEqual(len(actors),3) #Should include the ION System Actor and web auth actor as well.

        #TODO - figure out how to right a XACML rule to be a member of the specific Org as well

        #Hack to force container back to default values
        self.container.governance_controller._container_org_name = 'ION'
        self.container.governance_controller._is_container_org_boundary = False
        self.container.governance_controller._container_org_id = None

        self.pol_client.delete_policy(test_policy_id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated



    @attr('LOCOINT')
    @attr('ENROLL')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_org_enroll_negotiation(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")


        with self.assertRaises(BadRequest) as cm:
            myorg = self.org_client.read_org()
        self.assertTrue(cm.exception.message == 'The org_id parameter is missing')

        log.debug('Begin testing with policies')

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_actor_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Now create user with proper credentials
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "actor id=" + actor_id)

        #Build the message headers used with this user
        actor_header = get_actor_header(actor_id)

        #Get the associated user id
        user_info = IonObject(RT.UserInfo, name='Test User')
        actor_user_id = self.id_client.create_user_info(actor_id=actor_id, user_info=user_info, headers=actor_header)

        #Attempt to enroll a user anonymously - should not be allowed
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.enroll_member(self.ion_org._id,actor_id, headers=self.anonymous_actor_headers)
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
            actors = self.org_client.find_enrolled_users(self.ion_org._id, headers=self.anonymous_actor_headers)
        self.assertIn('org_management(find_enrolled_users) has been denied',cm.exception.message)

        #Verify that a user without the proper Org Manager cannot find a list of enrolled users in an Org
        with self.assertRaises(Unauthorized) as cm:
            actors = self.org_client.find_enrolled_users(self.ion_org._id, headers=actor_header)
        self.assertIn( 'org_management(find_enrolled_users) has been denied',cm.exception.message)

        actors = self.org_client.find_enrolled_users(self.ion_org._id, headers=self.system_actor_header)
        self.assertEqual(len(actors),3)  # WIll include the ION system actor

        #Create a second Org
        with self.assertRaises(NotFound) as nf:
            org2 = self.org_client.find_org(ORG2)
        self.assertIn('The Org with name Org 2 does not exist',nf.exception.message)

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

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        #Build the Service Agreement Proposal for enrollment request
        sap2 = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id )

        #User tried proposing an enrollment again - this should fail
        with self.assertRaises(BadRequest) as cm:
            self.org_client.negotiate(sap2, headers=actor_header )
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

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.EnrollmentNegotiationStatusEvent)
        self.assertEquals(len(events_r), 2)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.REJECTED])

        #Create a new enrollment proposal

        #Build the Service Agreement Proposal to enroll
        sap = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id, description='Enrollment request for test user' )

        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),2)

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=actor_header)
        self.assertEqual(len(negotiations),2)

        actors = self.org_client.find_enrolled_users(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(actors),0)

        #Check the get extended marine facility to check on the open and closed negotiations when called by normal user
        ext_mf = self.obs_client.get_marine_facility_extension(org_id=org2_id,user_id=actor_user_id, headers=actor_header)
        self.assertEqual(len(ext_mf.closed_requests), 0)
        self.assertEqual(len(ext_mf.open_requests), 0)

        #Check the get extended marine facility to check on the open and closed negotiations when called by privledged user
        ext_mf = self.obs_client.get_marine_facility_extension(org_id=org2_id,user_id=self.system_actor._id, headers=self.system_actor_header)
        self.assertEqual(len(ext_mf.closed_requests), 1)
        self.assertEqual(len(ext_mf.open_requests), 1)


        #Manager approves proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.EnrollmentProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        #Make sure the Negotiation object has the proper description set from the initial SAP
        self.assertEqual(negotiations[0].description, sap.description)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        actors = self.org_client.find_enrolled_users(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(actors),1)

        #User tried requesting enrollment again - this should fail
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.EnrollmentProposal,consumer=actor_id, provider=org2_id )
            neg_id = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: not is_enrolled',cm.exception.message)


        #Check the get extended marine facility to check on the open and closed negotiations when called by normal user
        ext_mf = self.obs_client.get_marine_facility_extension(org_id=org2_id,user_id=actor_user_id, headers=actor_header)
        self.assertEqual(len(ext_mf.closed_requests), 0)
        self.assertEqual(len(ext_mf.open_requests), 0)

        #Check the get extended marine facility to check on the open and closed negotiations when called by privledged user
        ext_mf = self.obs_client.get_marine_facility_extension(org_id=org2_id,user_id=self.system_actor._id, headers=self.system_actor_header)
        self.assertEqual(len(ext_mf.closed_requests), 2)
        self.assertEqual(len(ext_mf.open_requests), 0)


        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.EnrollmentNegotiationStatusEvent)
        self.assertEquals(len(events_r), 4)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])

        events_c = self.event_repo.find_events(origin=org2_id, event_type=OT.OrgMembershipGrantedEvent)
        self.assertEquals(len(events_c), 1)

        events_i = self.event_repo.find_events(origin=org2_id, event_type=OT.OrgNegotiationInitiatedEvent)
        self.assertEquals(len(events_i), 2)

        ret = self.org_client.is_enrolled(org_id=org2_id, actor_id=actor_id, headers=self.system_actor_header)
        self.assertEquals(ret, True)

        self.org_client.cancel_member_enrollment(org_id=org2_id, actor_id=actor_id, headers=self.system_actor_header)

        ret = self.org_client.is_enrolled(org_id=org2_id, actor_id=actor_id, headers=self.system_actor_header)
        self.assertEquals(ret, False)

    @attr('LOCOINT')
    @attr('ROLE')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_org_role_negotiation(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")


        with self.assertRaises(BadRequest) as cm:
            myorg = self.org_client.read_org()
        self.assertTrue(cm.exception.message == 'The org_id parameter is missing')

        log.debug('Begin testing with policies')

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_actor_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Now create user with proper credentials
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "actor id=" + actor_id)

        #Build the message headers used with this user
        actor_header = get_actor_header(actor_id)

        actors = self.org_client.find_enrolled_users(self.ion_org._id, headers=self.system_actor_header)
        self.assertEqual(len(actors),3)  # WIll include the ION system actor and the non user actor from setup

        ## test_org_roles and policies

        roles = self.org_client.find_org_roles(self.ion_org._id)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE, ION_MANAGER])

        roles = self.org_client.find_org_roles_by_user(self.ion_org._id, self.system_actor._id, headers=self.system_actor_header)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MEMBER_ROLE, ORG_MANAGER_ROLE, ION_MANAGER])

        roles = self.org_client.find_org_roles_by_user(self.ion_org._id, actor_id, headers=self.system_actor_header)
        self.assertEqual(len(roles),1)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MEMBER_ROLE])


        #Create a second Org
        with self.assertRaises(NotFound) as nf:
            org2 = self.org_client.find_org(ORG2)
        self.assertIn('The Org with name Org 2 does not exist',nf.exception.message)

        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),2)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE])

        #Create the Instrument Operator Role
        operator_role = IonObject(RT.UserRole, governance_name=INSTRUMENT_OPERATOR_ROLE,name='Instrument Operator', description='Instrument Operator')

        #First try to add the user role anonymously
        with self.assertRaises(Unauthorized) as cm:
            self.org_client.add_user_role(org2_id, operator_role, headers=self.anonymous_actor_headers)
        self.assertIn('org_management(add_user_role) has been denied',cm.exception.message)

        self.org_client.add_user_role(org2_id, operator_role, headers=self.system_actor_header)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE,  INSTRUMENT_OPERATOR_ROLE])

        #Add the same role to the first Org as well
        self.org_client.add_user_role(self.ion_org._id, operator_role, headers=self.system_actor_header)

        # test proposals roles.

        #First try to find user requests anonymously
        with self.assertRaises(Unauthorized) as cm:
            requests = self.org_client.find_org_negotiations(org2_id, headers=self.anonymous_actor_headers)
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
            sap_response = self.org_client.negotiate(sap, headers=self.anonymous_actor_headers)
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

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, headers=actor_header)
        self.assertEqual(len(negotiations),1)

        actors = self.org_client.find_enrolled_users(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(actors),0)

        #Manager approves proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.EnrollmentProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        actors = self.org_client.find_enrolled_users(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(actors),1)

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


        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.RequestRoleNegotiationStatusEvent)
        self.assertEquals(len(events_r), 2)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.REJECTED])


        #Create a second proposal to add a role to a user
        sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=INSTRUMENT_OPERATOR_ROLE )
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        negotiations = self.org_client.find_org_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(negotiations),3)

        closed_negotiations = self.org_client.find_org_closed_negotiations(org2_id, headers=self.system_actor_header)
        self.assertEqual(len(closed_negotiations),2)

        #Create an instrument resource
        ia_list,_ = self.rr_client.find_resources(restype=RT.InstrumentAgent)

        self.assertEqual(len(ia_list),0)

        ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent1', description='The first Instrument Agent')

        #Intruments should not be able to be created by anoymous users
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.create_instrument_agent(ia_obj, headers=self.anonymous_actor_headers)
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
        actor_header = get_actor_header(actor_id)

        #now try to request the same role for the same user - should be denied
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.RequestRoleProposal,consumer=actor_id, provider=org2_id, role_name=INSTRUMENT_OPERATOR_ROLE )
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: not has_role',cm.exception.message)

        #Now the user with the proper role should be able to create an instrument.
        self.ims_client.create_instrument_agent(ia_obj, headers=actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.RequestRoleNegotiationStatusEvent)
        self.assertEquals(len(events_r), 4)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])
        self.assertEqual(events_r[-1][2].role_name, sap_response2.role_name)

        events_c = self.event_repo.find_events(origin=org2_id, event_type=OT.UserRoleGrantedEvent)
        self.assertEquals(len(events_c), 2)

        events_i = self.event_repo.find_events(origin=org2_id, event_type=OT.OrgNegotiationInitiatedEvent)
        self.assertEquals(len(events_i), 3)

    @attr('LOCOINT')
    @attr('ACQUIRE')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    def test_org_acquire_resource_negotiation(self):

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")

        with self.assertRaises(BadRequest) as cm:
            myorg = self.org_client.read_org()
        self.assertTrue(cm.exception.message == 'The org_id parameter is missing')

        log.debug('Begin testing with policies')

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_actor_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Now create user with proper credentials
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.info( "actor id=" + actor_id)

        #Create a second Org
        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),2)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE])

        #Create the Instrument Operator Role
        operator_role = IonObject(RT.UserRole, governance_name=INSTRUMENT_OPERATOR_ROLE,name='Instrument Operator', description='Instrument Operator')

        #And add it to all Orgs
        self.org_client.add_user_role(self.ion_org._id, operator_role, headers=self.system_actor_header)
        self.org_client.add_user_role(org2_id, operator_role, headers=self.system_actor_header)

        #Add the INSTRUMENT_OPERATOR_ROLE to the User for the ION Org
        self.org_client.grant_role(self.ion_org._id, actor_id, INSTRUMENT_OPERATOR_ROLE, headers=self.system_actor_header)

        #Enroll the user in the second Org - do without Negotiation for test
        self.org_client.enroll_member(org2_id, actor_id,headers=self.system_actor_header )

        #Build the message headers used with this user
        actor_header = get_actor_header(actor_id)

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
        actor_header = get_actor_header(actor_id)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published

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
        self.assertEquals(ia_list[0].lcstate, LCS.DRAFT)
        self.assertEquals(ia_list[0].availability, AS.PRIVATE)

        #Advance the Life cycle to planned. Must be INSTRUMENT_OPERATOR so anonymous user should fail
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_agent_lifecycle(ia_list[0]._id, LCE.PLAN, headers=self.anonymous_actor_headers)
        self.assertIn( 'instrument_management(execute_instrument_agent_lifecycle) has been denied',cm.exception.message)

        #Advance the Life cycle to planned. Must be INSTRUMENT_OPERATOR
        self.ims_client.execute_instrument_agent_lifecycle(ia_list[0]._id, LCE.PLAN, headers=actor_header)
        ia = self.rr_client.read(ia_list[0]._id)
        self.assertEquals(ia.lcstate, LCS.PLANNED)


        #First make a acquire resource request with an non-enrolled user.
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=self.system_actor._id, provider=org2_id, resource_id=ia_list[0]._id )
            sap_response = self.org_client.negotiate(sap, headers=self.system_actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_enrolled',cm.exception.message)


        #Make a proposal to acquire a resource with an enrolled user that has the right role but the resource is not shared the Org
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id)
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
            sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_resource_acquired',cm.exception.message)


        #Make a proposal to acquire a resource with an enrolled user that has the right role and is now shared
        sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id)
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
        sap_response.expiration = str(week_expiration)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        #User Creates a counter proposal
        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)

        cur_time = int(get_ion_ts())
        month_expiration = cur_time +  ( 30 * 24 * 60 * 60 * 1000 )

        sap_response = Negotiation.create_counter_proposal(negotiations[0])
        sap_response.expiration = str(month_expiration)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )


        gevent.sleep(self.SLEEP_TIME+1)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.AcquireResourceNegotiationStatusEvent)
        self.assertEquals(len(events_r), 3)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.COUNTER])
        self.assertEqual(events_r[-1][2].resource_id, ia_list[0]._id)


        #Manager approves Instrument resource proposal
        negotiations = self.org_client.find_org_negotiations(org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=self.system_actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response2 = self.org_client.negotiate(sap_response, headers=self.system_actor_header )

        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)

        self.assertEqual(len(negotiations),0) #Should be no more open negotiations for a user because auto-accept is enabled

        #The following are no longer needed with auto-accept enabled for acquiring a resource
        '''
        self.assertEqual(len(negotiations),1)

        #User accepts proposal in return
        negotiations = self.org_client.find_user_negotiations(actor_id, org2_id, proposal_type=OT.AcquireResourceProposal,
            negotiation_status=NegotiationStatusEnum.OPEN, headers=actor_header)

        sap_response = Negotiation.create_counter_proposal(negotiations[0], ProposalStatusEnum.ACCEPTED)
        sap_response2 = self.org_client.negotiate(sap_response, headers=actor_header )

        '''

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
        sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id,
                    expiration=str(invalid_expiration))
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        #make sure the negotiation was rejected for being too long.
        negotiation = self.rr_client.read(sap_response.negotiation_id)
        self.assertEqual(negotiation.negotiation_status, NegotiationStatusEnum.REJECTED)

        #Now try to acquire the resource exclusively for 20 minutes
        cur_time = int(get_ion_ts())
        valid_expiration = cur_time +  ( 20 * 60 * 1000 ) # 12 hours from now

        sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id,
                    expiration=str(valid_expiration))
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        #Check commitment to be active
        commitments, _ = self.rr_client.find_objects(ia_list[0]._id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),2)

        exclusive_contract, _ = self.rr_client.find_objects(sap_response.negotiation_id,PRED.hasContract, RT.Commitment)
        self.assertEqual(len(contracts),1)

        #Now try to acquire the resource exclusively again - should fail
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: not is_resource_acquired_exclusively',cm.exception.message)

        #Release the exclusive commitment to the resource
        self.org_client.release_commitment(exclusive_contract[0]._id, headers=actor_header)


        #Check exclusive commitment to be inactive
        commitments, _ = self.rr_client.find_resources(restype=RT.Commitment, lcstate=LCS.RETIRED)
        self.assertEqual(len(commitments),1)
        self.assertEqual(commitments[0].commitment.exclusive, True)

        #Shared commitment is still actove
        commitments, _ = self.rr_client.find_objects(ia_list[0],PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),1)
        self.assertNotEqual(commitments[0].lcstate, LCS.RETIRED)

        #Now release the shared commitment
        self.org_client.release_commitment(resource_commitment[0]._id, headers=actor_header)

        #Check for both commitments to be inactive
        commitments, _ = self.rr_client.find_resources(restype=RT.Commitment, lcstate=LCS.RETIRED)
        self.assertEqual(len(commitments),2)

        commitments, _ = self.rr_client.find_objects(ia_list[0],PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),0)


        #Now check some negative cases...

        #Attempt to acquire the same resource from the ION Org which is not sharing it - should fail
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=self.ion_org._id, resource_id=ia_list[0]._id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_resource_shared',cm.exception.message)


        #Remove the INSTRUMENT_OPERATOR_ROLE from the user.
        self.org_client.revoke_role(org2_id, actor_id, INSTRUMENT_OPERATOR_ROLE,  headers=self.system_actor_header)

        #Refresh headers with new role
        actor_header = get_actor_header(actor_id)

        #Make a proposal to acquire a resource with an enrolled user that does not have the right role
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource_id=ia_list[0]._id )
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: has_role',cm.exception.message)

        gevent.sleep(self.SLEEP_TIME+1)  # Wait for events to be published

        #Check that there are the correct number of events
        events_r = self.event_repo.find_events(origin=sap_response2.negotiation_id, event_type=OT.AcquireResourceNegotiationStatusEvent)
        self.assertEquals(len(events_r), 6)
        self.assertEqual(events_r[-1][2].description, ProposalStatusEnum._str_map[ProposalStatusEnum.GRANTED])
        self.assertEqual(events_r[-1][2].resource_id, ia_list[0]._id)

        events_c = self.event_repo.find_events(origin=org2_id, event_type=OT.ResourceCommitmentCreatedEvent)
        self.assertEquals(len(events_c), 2)

        events_i = self.event_repo.find_events(origin=org2_id, event_type=OT.OrgNegotiationInitiatedEvent)
        self.assertEquals(len(events_i), 4)

        ret = self.org_client.is_resource_shared(org_id=org2_id, resource_id=ia_list[0]._id, headers=self.system_actor_header )
        self.assertEquals(ret, True)

        #So unshare the resource
        self.org_client.unshare_resource(org_id=org2_id, resource_id=ia_list[0]._id, headers=self.system_actor_header  )

        ret = self.org_client.is_resource_shared(org_id=org2_id, resource_id=ia_list[0]._id, headers=self.system_actor_header )
        self.assertEquals(ret, False)


    def start_instrument_direct_access(self, ia_client, actor_header):

        #The reset command should now be allowed
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = ia_client.execute_agent(cmd, headers=actor_header)
        retval = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(retval, ResourceAgentState.UNINITIALIZED)


        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        retval = ia_client.execute_agent(cmd, headers=actor_header)
        state = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(state, ResourceAgentState.INACTIVE)

        cmd = AgentCommand(command=ResourceAgentEvent.GO_DIRECT_ACCESS,
            #kwargs={'session_type': DirectAccessTypes.telnet,
            kwargs={'session_type':DirectAccessTypes.vsp,
                    'session_timeout':600,
                    'inactivity_timeout':600})

        retval = ia_client.execute_agent(cmd, headers=actor_header)
        state = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(state, ResourceAgentState.DIRECT_ACCESS)



    def stop_instrument_direct_access(self, ia_client, actor_header):

        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        retval = ia_client.execute_agent(cmd, headers=actor_header)
        state = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(state, ResourceAgentState.UNINITIALIZED)


    @attr('LOCOINT')
    @attr('AGENT')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    @patch.dict(CFG, {'system':{'load_policy':True}})
    def test_instrument_agent_policy(self):

        # This import will dynamically load the driver egg.  It is needed for the MI includes below
        import ion.agents.instrument.test.test_instrument_agent
        from mi.core.instrument.instrument_driver import DriverProtocolState
        from mi.core.instrument.instrument_driver import DriverConnectionState
        from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
        from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter

        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")

        log.debug('Begin testing with policies')

        #Create a new user - should be denied for anonymous access
        with self.assertRaises(Unauthorized) as cm:
            actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.anonymous_actor_headers)
        self.assertIn( 'identity_management(signon) has been denied',cm.exception.message)

        #Create user
        actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.debug( "actor id=" + actor_id)

        actor_header = get_actor_header(actor_id)

        #Create a third user to be used as observatory operator
        obs_operator_actor_obj = IonObject(RT.ActorIdentity, name='observatory operator actor')
        obs_operator_actor_id,_ = self.rr_client.create(obs_operator_actor_obj)
        assert(obs_operator_actor_id)

        #Create a second Org
        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),2)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE])

        #Create Instrument Operator Role and add it to the second Org
        operator_role = IonObject(RT.UserRole, governance_name=INSTRUMENT_OPERATOR_ROLE, name='Instrument Operator', description='Instrument Operator')
        self.org_client.add_user_role(org2_id, operator_role, headers=self.system_actor_header)

        #Create Instrument Operator Role and add it to the second Org
        obs_operator_role = IonObject(RT.UserRole, governance_name=OBSERVATORY_OPERATOR_ROLE, name='Observatory Operator', description='Observatory Operator')
        self.org_client.add_user_role(org2_id, obs_operator_role, headers=self.system_actor_header)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),4)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE, INSTRUMENT_OPERATOR_ROLE, OBSERVATORY_OPERATOR_ROLE])

        #Grant the role of Observatory Operator to the user
        self.org_client.enroll_member(org2_id,obs_operator_actor_id, headers=self.system_actor_header)
        self.org_client.grant_role(org2_id, obs_operator_actor_id, OBSERVATORY_OPERATOR_ROLE, headers=self.system_actor_header)
        obs_operator_actor_header = get_actor_header(obs_operator_actor_id)

        #Create Test InstrumentDevice - use the system admin for now
        inst_obj = IonObject(RT.InstrumentDevice, name='Test_Instrument_123')
        inst_obj_id,_ = self.rr_client.create(inst_obj )

        #Startup an agent - TODO: will fail with Unauthorized to spawn process if not right user role
        from ion.agents.instrument.test.test_instrument_agent import start_instrument_agent_process
        ia_client = start_instrument_agent_process(self.container, resource_id=inst_obj_id, resource_name=inst_obj.name,
            org_governance_name=org2.org_governance_name, message_headers=self.system_actor_header)

        #First try a basic agent operation anonymously - it should be denied
        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_capabilities(headers=self.anonymous_actor_headers )
        self.assertIn('InstrumentDevice(get_capabilities) has been denied',cm.exception.message)

        #However the ION Manager should be allowed
        retval = ia_client.get_capabilities(headers=self.system_actor_header)

        #Next try a basic agent operation with a user that is not an Instrument Operator or Member of Org - it should be denied
        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_capabilities(headers=actor_header)
        self.assertIn('InstrumentDevice(get_capabilities) has been denied',cm.exception.message)

        #Attempt to grant role to user but not as a Member of the Org so it will fail
        with self.assertRaises(BadRequest) as cm:
            self.org_client.grant_role(org2_id,actor_id, INSTRUMENT_OPERATOR_ROLE, headers=self.system_actor_header)
        self.assertIn('The actor is not a member of the specified Org',cm.exception.message)

        #Enroll user in second Org
        self.org_client.enroll_member(org2_id,actor_id, headers=self.system_actor_header)

        #Refresh header with updated roles
        actor_header = get_actor_header(actor_id)

        #Next try a basic agent operation with a user that is a member of the Org but not an Instrument Operator - it should be allowed
        retval = ia_client.get_capabilities(headers=actor_header)

        #This agent operation should not be allowed for anonymous user
        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_resource_state(headers=self.anonymous_actor_headers)
        self.assertIn('InstrumentDevice(get_resource_state) has been denied',cm.exception.message)

        #This agent operation should not be allowed for a user that is not an Instrument Operator
        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_resource_state(headers=actor_header)
        self.assertIn('InstrumentDevice(get_resource_state) has been denied',cm.exception.message)

        #This agent operation should not be allowed for a user that is not an Instrument Operator
        with self.assertRaises(Unauthorized) as cm:
            params = [SBE37Parameter.ALL]
            retval = ia_client.get_resource(params, headers=actor_header)
        self.assertIn('InstrumentDevice(get_resource) has been denied',cm.exception.message)

        #This agent operation should not be allowed for a user that is not an Instrument Operator
        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_agent_state(headers=actor_header)
        self.assertIn('InstrumentDevice(get_agent_state) has been denied',cm.exception.message)

        with self.assertRaises(Unauthorized) as cm:
            retval = ia_client.get_agent(headers=actor_header)
        self.assertIn('InstrumentDevice(get_agent) has been denied',cm.exception.message)

        #Check the availability of the get_instrument_device_extension operation for various user types - some of the
        #agent related status should not be allowed for users without the proper role

        #Anonymous get extended is allowed, but internal agent status should be unavailable
        extended_inst = self.ims_client.get_instrument_device_extension(inst_obj_id, headers=self.anonymous_actor_headers)
        self.assertEqual(extended_inst._id, inst_obj_id)
        self.assertEqual(extended_inst.computed.communications_status_roll_up.status, ComputedValueAvailability.NOTAVAILABLE)
        self.assertIn('InstrumentDevice(get_agent) has been denied',extended_inst.computed.communications_status_roll_up.reason)

        #Org member get extended is allowed, but internal agent status should be unavailable
        extended_inst = self.ims_client.get_instrument_device_extension(inst_obj_id, headers=actor_header)
        self.assertEqual(extended_inst._id, inst_obj_id)
        self.assertEqual(extended_inst.computed.communications_status_roll_up.status, ComputedValueAvailability.NOTAVAILABLE)
        self.assertIn('InstrumentDevice(get_agent) has been denied',extended_inst.computed.communications_status_roll_up.reason)


        #Grant the role of Instrument Operator to the user
        self.org_client.grant_role(org2_id,actor_id, INSTRUMENT_OPERATOR_ROLE, headers=self.system_actor_header)

        #Refresh header with updated roles
        actor_header = get_actor_header(actor_id)

        #This operation should now be allowed with the Instrument Operator role - just checking policy not agent functionality
        with self.assertRaises(Conflict) as cm:
            res_state = ia_client.get_resource_state(headers=actor_header)

        #This operation should now be allowed with the Instrument Operator role
        with self.assertRaises(Conflict) as cm:
            params = [SBE37Parameter.ALL]
            retval = ia_client.get_resource(params, headers=actor_header)

        #Instrument Operator role get extended is allowed and contain agent status information
        extended_inst = self.ims_client.get_instrument_device_extension(inst_obj_id, headers=actor_header)
        self.assertEqual(extended_inst._id, inst_obj_id)
        self.assertEqual(extended_inst.computed.communications_status_roll_up.status, ComputedValueAvailability.PROVIDED)
        self.assertEqual(extended_inst.computed.communications_status_roll_up.reason, None)

        #This agent operation should now be allowed for a user that is an Instrument Operator
        retval = ia_client.get_agent_state(headers=actor_header)
        self.assertEqual(retval, ResourceAgentState.UNINITIALIZED)

        #This agent operation should now be allowed for a user that is an Instrument Operator
        with self.assertRaises(BadRequest) as cm:
            retval = ia_client.get_agent(params=[123], headers=actor_header)

        #The execute commnand should fail if the user has not acquired the resource
        with self.assertRaises(Unauthorized) as cm:
            cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
            retval = ia_client.execute_resource(cmd, headers=actor_header)
        self.assertIn('InstrumentDevice(execute_resource) has been denied',cm.exception.message)


        #Going to try access to other operations on the agent, don't care if they actually work - just
        #do they get denied or not
        new_params = {
            SBE37Parameter.TA0 : 2,
            SBE37Parameter.INTERVAL : 1,
        }

        #First try anonymously - should be denied
        with self.assertRaises(Unauthorized) as cm:
            ia_client.set_resource(new_params, headers=self.anonymous_actor_headers)
        self.assertIn('InstrumentDevice(set_resource) has been denied',cm.exception.message)

        #THey try with user with Instrument Operator role, but should fail with out acquiring a resource
        with self.assertRaises(Unauthorized) as cm:
            ia_client.set_resource(new_params, headers=actor_header)
        self.assertIn('InstrumentDevice(set_resource) has been denied',cm.exception.message)

        #Make a proposal to acquire a resource with an enrolled user that has the right role - but the resource is not shared
        with self.assertRaises(BadRequest) as cm:
            sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource_id=inst_obj_id)
            sap_response = self.org_client.negotiate(sap, headers=actor_header )
        self.assertIn('A precondition for this request has not been satisfied: is_resource_shared',cm.exception.message)

        #So share the resource
        self.org_client.share_resource(org_id=org2_id, resource_id=inst_obj_id, headers=self.system_actor_header  )

        #Renegotiate the proposal
        sap = IonObject(OT.AcquireResourceProposal,consumer=actor_id, provider=org2_id, resource_id=inst_obj_id)
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        #Have the Org accept the proposal
        negotiation = self.rr_client.read(sap_response.negotiation_id)
        sap_response2 = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED, ProposalOriginatorEnum.PROVIDER)
        sap_response3 = self.org_client.negotiate(sap_response2, headers=self.system_actor_header )

        #Have the User counter-accept the proposal
        '''
        negotiation = self.rr_client.read(sap_response3.negotiation_id)
        sap_response4 = Negotiation.create_counter_proposal(negotiation, ProposalStatusEnum.ACCEPTED)
        sap_response5 = self.org_client.negotiate(sap_response4, headers=actor_header )
        '''

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and commitments recorded

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

        #Request for the instrument to be put into Direct Access mode - should be denied for anonymous users
        with self.assertRaises(Unauthorized) as cm:
            self.start_instrument_direct_access(ia_client, actor_header=self.anonymous_actor_headers )
        self.assertIn('InstrumentDevice(execute_agent) has been denied',cm.exception.message)

        #Request for the instrument to be put into Direct Access mode - should be denied since user does not have exclusive  access
        with self.assertRaises(Unauthorized) as cm:
            self.start_instrument_direct_access(ia_client, actor_header=actor_header )
        self.assertIn('InstrumentDevice(execute_agent) has been denied',cm.exception.message)

        #Request to access the resource exclusively for two hours
        cur_time = int(get_ion_ts())
        two_hour_expiration = cur_time +  ( 2 * 60 * 60 * 1000 ) # 2 hours from now

        sap = IonObject(OT.AcquireResourceExclusiveProposal,consumer=actor_id, provider=org2_id, resource_id=inst_obj_id,
                    expiration=str(two_hour_expiration))
        sap_response = self.org_client.negotiate(sap, headers=actor_header )

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and commitments recorded


        #Should fail if another user has acquired the resource exclusively
        with self.assertRaises(Unauthorized) as cm:
            ia_client.set_resource(new_params, headers=obs_operator_actor_header)
        self.assertIn('InstrumentDevice(set_resource) has been denied since another user',cm.exception.message)

        #Should fail if another user has acquired the resource exclusively
        with self.assertRaises(Unauthorized) as cm:
            cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_SAMPLE)
            retval = ia_client.execute_resource(cmd, headers=obs_operator_actor_header)
        self.assertIn('InstrumentDevice(execute_resource) has been denied since another user',cm.exception.message)


        #Request Direct Access again - with a different user and it should fail since other user has exclusive access
        with self.assertRaises(Unauthorized) as cm:
            self.start_instrument_direct_access(ia_client, actor_header=obs_operator_actor_header )
        self.assertIn('InstrumentDevice(execute_agent) has been denied since another user',cm.exception.message)

        #Request Direct Access again with user that has exclusive access - and it should pass this time.
        with self.assertRaises(Conflict) as cm:
            self.start_instrument_direct_access(ia_client, actor_header=actor_header )

        #Try stopping the direct access by other user - should fail
        with self.assertRaises(Unauthorized) as cm:
            self.stop_instrument_direct_access(ia_client, actor_header=obs_operator_actor_header)
        self.assertIn('InstrumentDevice(execute_agent) has been denied since another user',cm.exception.message)

        #Stop Direct Access by user with exclusive access - and it should pass this time.
        with self.assertRaises(Conflict) as cm:
            self.stop_instrument_direct_access(ia_client, actor_header=actor_header)

        #Release the exclusive commitment to the resource
        exclusive_contract, _ = self.rr_client.find_objects(sap_response.negotiation_id,PRED.hasContract, RT.Commitment)
        self.org_client.release_commitment(exclusive_contract[0]._id, headers=actor_header)

        #Try to Request for the instrument to be put into Direct Access mode - should be denied since user does not have exclusive  access
        with self.assertRaises(Unauthorized) as cm:
            self.start_instrument_direct_access(ia_client, actor_header=actor_header )
        self.assertIn('InstrumentDevice(execute_agent) has been denied',cm.exception.message)

        #Request Direct Access again - with obs manager role that does not need a commitment.
        with self.assertRaises(Conflict) as cm:
            self.start_instrument_direct_access(ia_client, actor_header=obs_operator_actor_header )

        #Stop Direct Access with obs manager role that does not need a commitment.
        with self.assertRaises(Conflict) as cm:
            self.stop_instrument_direct_access(ia_client, actor_header=obs_operator_actor_header)

        #The agent related functions should not be allowed for a user that is not an Org Manager
        with self.assertRaises(Unauthorized) as cm:
            cmd = AgentCommand(command=ResourceAgentEvent.RESET)
            retval = ia_client.execute_agent(cmd, headers=actor_header)
        self.assertIn('InstrumentDevice(execute_agent) has been denied',cm.exception.message)

        #Check exclusive commitment to be inactive
        commitments, _ = self.rr_client.find_resources(restype=RT.Commitment, lcstate=LCS.RETIRED)
        self.assertEqual(len(commitments),1)
        self.assertEqual(commitments[0].commitment.exclusive, True)

        #Shared commitment is still active
        commitments, _ = self.rr_client.find_objects(inst_obj_id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),1)
        self.assertNotEqual(commitments[0].lcstate, LCS.RETIRED)

        #Now release the shared commitment
        self.org_client.release_commitment(resource_commitment[0]._id, headers=actor_header)

        #Check for both commitments to be inactive
        commitments, _ = self.rr_client.find_resources(restype=RT.Commitment, lcstate=LCS.RETIRED)
        self.assertEqual(len(commitments),2)

        commitments, _ = self.rr_client.find_objects(inst_obj_id,PRED.hasCommitment, RT.Commitment)
        self.assertEqual(len(commitments),0)


        #Try again with user with only Instrument Operator role, but should fail with out acquiring a resource
        with self.assertRaises(Unauthorized) as cm:
            ia_client.set_resource(new_params, headers=actor_header)
        self.assertIn('InstrumentDevice(set_resource) has been denied',cm.exception.message)


        #Revoke the role of Inst Operator to the user
        self.org_client.revoke_role(org2_id,actor_id, INSTRUMENT_OPERATOR_ROLE, headers=self.system_actor_header)


        #Grant the role of Org Manager to the user
        self.org_client.grant_role(org2_id,actor_id, ORG_MANAGER_ROLE, headers=self.system_actor_header)

        #Refresh header with updated roles
        actor_header = get_actor_header(actor_id)

        #Try again with user with also Org Manager role and should pass even with out acquiring a resource
        with self.assertRaises(Conflict) as cm:
            ia_client.set_resource(new_params, headers=actor_header)

        #This agent operation should now be allowed for a user that is an Org Manager
        with self.assertRaises(BadRequest) as cm:
            retval = ia_client.get_agent(params=[123], headers=actor_header)


        #Org Manager role get extended is allowed and contain agent status information
        extended_inst = self.ims_client.get_instrument_device_extension(inst_obj_id, headers=actor_header)
        self.assertEqual(extended_inst._id, inst_obj_id)
        self.assertEqual(extended_inst.computed.communications_status_roll_up.status, ComputedValueAvailability.PROVIDED)
        self.assertEqual(extended_inst.computed.communications_status_roll_up.reason, None)

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
        """def precondition_func(process, message, headers):
            from pyon.agent.agent import ResourceAgentEvent
            if message['command'].command == ResourceAgentEvent.RESET:
                return False, 'ResourceAgentEvent.RESET is being denied'
            else:
                return True, ''

        """

        #Add an example of a operation specific policy that checks internal values to decide on access
        pol_id = self.pol_client.add_process_operation_precondition_policy(process_name=RT.InstrumentDevice, op='execute_agent', policy_content=pre_func1, headers=self.system_actor_header )

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

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

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be published and policy updated

        #Now try to go into Direct Access Mode directly through the agent as an Org Manager
        with self.assertRaises(Conflict) as cm:
            self.start_instrument_direct_access(ia_client, actor_header=actor_header )

        #Exit DA Mode
        self.stop_instrument_direct_access(ia_client, actor_header=actor_header)

        events_i = self.event_repo.find_events(origin=org2_id, event_type=OT.OrgNegotiationInitiatedEvent)
        self.assertEquals(len(events_i), 2)


    @attr('LOCOINT')
    @attr('LCS')
    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
    @patch.dict(CFG, {'system':{'load_policy':True}})
    def test_instrument_lifecycle_policy(self):


        #Make sure that the system policies have been loaded
        policy_list,_ = self.rr_client.find_resources(restype=RT.Policy)
        self.assertNotEqual(len(policy_list),0,"The system policies have not been loaded into the Resource Registry")

        log.debug('Begin testing with policies')

        #Create user
        inst_operator_actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.debug( "actor id=" + inst_operator_actor_id)

        inst_operator_actor_header = get_actor_header(inst_operator_actor_id)

        #Create a second user to be used as regular member
        member_actor_obj = IonObject(RT.ActorIdentity, name='org member actor')
        member_actor_id,_ = self.rr_client.create(member_actor_obj)
        assert(member_actor_id)

        #Create a third user to be used as observatory operator
        obs_operator_actor_obj = IonObject(RT.ActorIdentity, name='observatory operator actor')
        obs_operator_actor_id,_ = self.rr_client.create(obs_operator_actor_obj)
        assert(obs_operator_actor_id)

        #Create a second Org
        org2 = IonObject(RT.Org, name=ORG2, description='A second Org')
        org2_id = self.org_client.create_org(org2, headers=self.system_actor_header)

        org2 = self.org_client.find_org(ORG2)
        self.assertEqual(org2_id, org2._id)

        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),2)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE])

        #Create Instrument Operator Role and add it to the second Org
        inst_operator_role = IonObject(RT.UserRole, governance_name=INSTRUMENT_OPERATOR_ROLE, name='Instrument Operator', description='Instrument Operator')
        self.org_client.add_user_role(org2_id, inst_operator_role, headers=self.system_actor_header)

        #Create Instrument Operator Role and add it to the second Org
        obs_operator_role = IonObject(RT.UserRole, governance_name=OBSERVATORY_OPERATOR_ROLE, name='Observatory Operator', description='Observatory Operator')
        self.org_client.add_user_role(org2_id, obs_operator_role, headers=self.system_actor_header)


        roles = self.org_client.find_org_roles(org2_id)
        self.assertEqual(len(roles),4)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MANAGER_ROLE, ORG_MEMBER_ROLE, INSTRUMENT_OPERATOR_ROLE, OBSERVATORY_OPERATOR_ROLE])

        self.org_client.enroll_member(org2_id,inst_operator_actor_id, headers=self.system_actor_header)
        self.org_client.enroll_member(org2_id,member_actor_id, headers=self.system_actor_header)
        self.org_client.enroll_member(org2_id,obs_operator_actor_id, headers=self.system_actor_header)


        #Grant the role of Instrument Operator to the user
        self.org_client.grant_role(org2_id, inst_operator_actor_id, INSTRUMENT_OPERATOR_ROLE, headers=self.system_actor_header)

        #Grant the role of Observatory Operator to the user
        self.org_client.grant_role(org2_id, obs_operator_actor_id, OBSERVATORY_OPERATOR_ROLE, headers=self.system_actor_header)


        #Refresh header with updated roles
        inst_operator_actor_header = get_actor_header(inst_operator_actor_id)
        member_actor_header = get_actor_header(member_actor_id)
        obs_operator_actor_header = get_actor_header(obs_operator_actor_id)

        #Attempt to Create Test InstrumentDevice as Org Member
        inst_dev = IonObject(RT.InstrumentDevice, name='Test_Instrument_123')

        #Should be denied without being the proper role
        with self.assertRaises(Unauthorized) as cm:
            inst_dev_id = self.ims_client.create_instrument_device(inst_dev, headers=member_actor_header)
        self.assertIn('instrument_management(create_instrument_device) has been denied',cm.exception.message)

        #Should be allowed
        inst_dev_id = self.ims_client.create_instrument_device(inst_dev, headers=inst_operator_actor_header)

        #Reads are always allowed anonymously
        inst_dev_obj = self.ims_client.read_instrument_device(inst_dev_id)
        self.assertEqual(inst_dev_obj.lcstate, LCS.DRAFT)

        #Advance the Life cycle to planned. Must be INSTRUMENT_OPERATOR so anonymous user should fail
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.PLAN, headers=self.anonymous_actor_headers)
        self.assertIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)

        #Advance the Life cycle to planned. Must be INSTRUMENT_OPERATOR so member user should fail
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.PLAN, headers=member_actor_header)
        self.assertIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)

        #Advance the Life cycle to planned. Must be INSTRUMENT_OPERATOR - will fail since resource is not shared by an Org
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.PLAN, headers=inst_operator_actor_header)
        #self.assertIn( 'has not been shared with any Org',cm.exception.message)

        #Ensure the resource is shareable
        self.org_client.share_resource(org2_id, inst_dev_id, headers=self.system_actor_header)


        #Successfully advance the Life cycle to planned - this user is owner and Inst operator
        self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.PLAN, headers=inst_operator_actor_header)

        inst_dev_obj = self.ims_client.read_instrument_device(inst_dev_id)
        self.assertEquals(inst_dev_obj.lcstate, LCS.PLANNED)

        #Advance the Life cycle to DEVELOP - should fail since not owner or acquried resource
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.DEVELOP, headers=obs_operator_actor_header)
        self.assertIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)

        inst_dev_obj = self.ims_client.read_instrument_device(inst_dev_id)
        self.assertEquals(inst_dev_obj.lcstate, LCS.PLANNED)

        #Have the Obs Operator acquire the resource since not the owner
        commitment_id = self.org_client.create_resource_commitment(org2_id, obs_operator_actor_id, inst_dev_id)

        #Advance the Life cycle to DEVELOP - should pass governance but fail because of other hard wired preconditions.
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.DEVELOP, headers=obs_operator_actor_header)
        self.assertNotIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)


        #These states are only ever allowed for the Observatory Operator ( or Org Manager)
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.INTEGRATE, headers=inst_operator_actor_header)
        self.assertIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)

        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.DEPLOY, headers=inst_operator_actor_header)
        self.assertIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)

        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.RETIRE, headers=inst_operator_actor_header)
        self.assertIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)

        #These states are only ever allowed for the Observatory Operator ( or Org Manager)
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.INTEGRATE, headers=obs_operator_actor_header)
        self.assertNotIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)

        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.DEPLOY, headers=obs_operator_actor_header)
        self.assertNotIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)


        #Back to testing a few other states with different actors
        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.ANNOUNCE, headers=member_actor_header)
        self.assertIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)

        with self.assertRaises(Unauthorized) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.ENABLE, headers=member_actor_header)
        self.assertIn( 'instrument_management(execute_instrument_device_lifecycle) has been denied',cm.exception.message)

        inst_dev_obj = self.ims_client.read_instrument_device(inst_dev_id)
        self.assertEquals(inst_dev_obj.lcstate, LCS.PLANNED)
        self.assertEquals(inst_dev_obj.availability, AS.PRIVATE)

        #print "old state: " + inst_dev_obj.lcstate

        self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.ANNOUNCE, headers=inst_operator_actor_header)
        inst_dev_obj = self.ims_client.read_instrument_device(inst_dev_id)
        self.assertEquals(inst_dev_obj.lcstate, LCS.PLANNED)
        self.assertEquals(inst_dev_obj.availability, AS.DISCOVERABLE)
        #print "new state: " + inst_dev_obj.lcstate


        self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.ENABLE, headers=inst_operator_actor_header)

        with self.assertRaises(BadRequest) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.ANNOUNCE, headers=obs_operator_actor_header)
        self.assertIn( 'PLANNED_AVAILABLE has no transition for event',cm.exception.message)

        with self.assertRaises(BadRequest) as cm:
            self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.ENABLE, headers=obs_operator_actor_header)
        self.assertIn( 'PLANNED_AVAILABLE has no transition for event',cm.exception.message)

        #Should be able to retire a device anytime
        self.ims_client.execute_instrument_device_lifecycle(inst_dev_id, LCE.RETIRE, headers=obs_operator_actor_header)
        inst_dev_obj = self.ims_client.read_instrument_device(inst_dev_id)
        self.assertEquals(inst_dev_obj.lcstate, LCS.RETIRED)


        #Clean up
        self.org_client.release_commitment(commitment_id, headers=inst_operator_actor_header)
        self.ims_client.force_delete_instrument_device(inst_dev_id, headers=self.system_actor_header)

        self.id_client.delete_actor_identity(inst_operator_actor_id,headers=self.system_actor_header )
        self.rr_client.delete(member_actor_id)
        self.rr_client.delete(obs_operator_actor_id)


@attr('INT', group='coi')
class TestResourcePolicyInt(IonIntegrationTestCase, ResourceHelper):


    def __init__(self, *args, **kwargs):

        #Hack for running tests on CentOS which is significantly slower than a Mac
        self.SLEEP_TIME = 1
        ver = platform.mac_ver()
        if ver[0] == '':
            self.SLEEP_TIME = 3  # Increase for non Macs
            log.info('Not running on a Mac')
        else:
            log.info('Running on a Mac)')

        self.care = {}
        self.dontcare = {}
        self.realtype = {}

        IonIntegrationTestCase.__init__(self, *args, **kwargs)

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
            log.debug('Loading policy')
            LoadSystemPolicy.op_load_system_policies(process)

        gevent.sleep(self.SLEEP_TIME*2)  # Wait for events to be fired and policy updated

        #self.rr_client = ResourceRegistryServiceProcessClient(node=self.container.node, process=process)
        self.rr_client = ResourceRegistryServiceWrapper(self.container.resource_registry, process)
        self.RR = self.rr_client  # support for the parent SA test class of this class

        self.id_client = IdentityManagementServiceProcessClient(node=self.container.node, process=process)

        self.pol_client = PolicyManagementServiceProcessClient(node=self.container.node, process=process)

        self.org_client = OrgManagementServiceProcessClient(node=self.container.node, process=process)

        self.ims_client = InstrumentManagementServiceProcessClient(node=self.container.node, process=process)

        self.ems_client = ExchangeManagementServiceProcessClient(node=self.container.node, process=process)

        self.ssclient = SchedulerServiceProcessClient(node=self.container.node, process=process)

        self.sys_management = SystemManagementServiceProcessClient(node=self.container.node, process=process)



        #Get info on the ION System Actor
        self.system_actor = get_system_actor()
        log.info('system actor:' + self.system_actor._id)

        self.system_actor_header = get_system_actor_header()

        #Get info on the Web Authentication Actor
        self.apache_actor = get_web_authentication_actor()
        if not self.apache_actor:
            #Can't find the apache actor so just use the system actor
            self.apache_actor = self.system_actor

        self.apache_actor_header = get_actor_header(self.apache_actor._id)

        self.anonymous_actor_headers = {'ion-actor-id':'anonymous'}

        self.ion_org = self.org_client.find_org()


        #Setup access to event repository
        dsm = DatastoreManager()
        ds = dsm.get_datastore("events")

        self.event_repo = EventRepository(dsm)

        self.care = {}
        self.dontcare = {}
        self.realtype = {}

    def tearDown(self):
        policy_list, _ = self.rr_client.find_resources(restype=RT.Policy )

        #Must remove the policies in the reverse order they were added
        for policy in sorted(policy_list,key=lambda p: p.ts_created, reverse=True):
            self.pol_client.delete_policy(policy._id, headers=self.system_actor_header)

        gevent.sleep(self.SLEEP_TIME)  # Wait for events to be fired and policy updated

    @attr('RESOURCE')
    def test_related_resource_policies(self):
        """
        This test is used to verify that policies of related resources can be setup and invoked in the "tree" of resources.
        @return:
        """

        # This import will dynamically load the driver egg.  It is needed for the MI includes below
        import ion.agents.instrument.test.test_instrument_agent
        from mi.core.instrument.instrument_driver import DriverProtocolState
        from mi.core.instrument.instrument_driver import DriverConnectionState
        from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
        from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter

        obs_id = self.create_observatory(True, create_with_marine_facility=True)

        orgs,_ = self.rr_client.find_subjects(RT.Org ,PRED.hasResource, obs_id)
        assert orgs
        obs_org = orgs[0]

        #Create user
        inst_operator_actor_id, valid_until, registered = self.id_client.signon(USER1_CERTIFICATE, True, headers=self.apache_actor_header)
        log.debug( "actor id=" + inst_operator_actor_id)

        inst_operator_actor_header = get_actor_header(inst_operator_actor_id)

        #Create a second user to be used as regular member
        member_actor_obj = IonObject(RT.ActorIdentity, name='org member actor')
        member_actor_id,_ = self.rr_client.create(member_actor_obj)
        assert(member_actor_id)

        #Create a third user to be used as observatory operator
        obs_operator_actor_obj = IonObject(RT.ActorIdentity, name='observatory operator actor')
        obs_operator_actor_id,_ = self.rr_client.create(obs_operator_actor_obj)
        assert(obs_operator_actor_id)

        #Create Instrument Operator Role and add it to the second Org
        org_member_role = IonObject(RT.UserRole, governance_name=ORG_MEMBER_ROLE, name='Org Member', description='Org Member')
        self.org_client.add_user_role(obs_org._id, org_member_role, headers=self.system_actor_header)

        inst_operator_role = IonObject(RT.UserRole, governance_name=INSTRUMENT_OPERATOR_ROLE, name='Instrument Operator', description='Instrument Operator')
        self.org_client.add_user_role(obs_org._id, inst_operator_role, headers=self.system_actor_header)

        #Create Instrument Operator Role and add it to the second Org
        obs_operator_role = IonObject(RT.UserRole, governance_name=OBSERVATORY_OPERATOR_ROLE, name='Observatory Operator', description='Observatory Operator')
        self.org_client.add_user_role(obs_org._id, obs_operator_role, headers=self.system_actor_header)


        roles = self.org_client.find_org_roles(obs_org._id)
        self.assertEqual(len(roles),3)
        self.assertItemsEqual([r.governance_name for r in roles], [ORG_MEMBER_ROLE, INSTRUMENT_OPERATOR_ROLE, OBSERVATORY_OPERATOR_ROLE])

        self.org_client.enroll_member(obs_org._id,inst_operator_actor_id, headers=self.system_actor_header)
        self.org_client.enroll_member(obs_org._id,member_actor_id, headers=self.system_actor_header)
        self.org_client.enroll_member(obs_org._id,obs_operator_actor_id, headers=self.system_actor_header)


        #Grant the role of Instrument Operator to the user
        self.org_client.grant_role(obs_org._id, inst_operator_actor_id, INSTRUMENT_OPERATOR_ROLE, headers=self.system_actor_header)

        #Grant the role of Observatory Operator to the user
        self.org_client.grant_role(obs_org._id, obs_operator_actor_id, OBSERVATORY_OPERATOR_ROLE, headers=self.system_actor_header)


        #Refresh header with updated roles
        inst_operator_actor_header = get_actor_header(inst_operator_actor_id)
        member_actor_header = get_actor_header(member_actor_id)
        obs_operator_actor_header = get_actor_header(obs_operator_actor_id)


        inst_devices,_ = self.rr_client.find_resources(restype=RT.InstrumentDevice)
        assert len(inst_devices) > 0

        instrument_id = inst_devices[1]._id   #just pick one
        instrument_name = inst_devices[1].name   #just pick one


        #Setup commitment to acquire the resource
        self.org_client.create_resource_commitment(org_id=obs_org._id, actor_id=inst_operator_actor_id, resource_id=instrument_id, headers=self.system_actor_header)

        #Creating second set of resources just to make it a more filled RR
        self.create_observatory(False, create_with_marine_facility=True)

        #Startup an agent - TODO: will fail with Unauthorized to spawn process if not right user role
        from ion.agents.instrument.test.test_instrument_agent import start_instrument_agent_process
        ia_client = start_instrument_agent_process(self.container, resource_id=instrument_id, resource_name=instrument_name,
            org_governance_name=obs_org.org_governance_name, message_headers=self.system_actor_header)

        #Going to try access to other operations on the agent, don't care if they actually work - just
        #do they get denied or not
        new_params = {
            SBE37Parameter.TA0 : 2,
            SBE37Parameter.INTERVAL : 100,
            }


        with self.assertRaises(Conflict) as cm:
            ret_val = ia_client.set_resource(new_params, headers=inst_operator_actor_header)

        print 'Creating policy for inst: ' + instrument_id
        policy_id = self.pol_client.create_resource_access_policy(instrument_id, 'restrict_50_policy',
            'Check the value of the SBE37Parameter.INTERVAL parameter',
            DENY_PARAM_50_RULE, headers=self.system_actor_header)


        gevent.sleep(5)  # Wait for events to be published and policy updated

        #The policy should now deny the same command as above since the SBE37Parameter.INTERVAL value is greater than 50
        with self.assertRaises(Unauthorized) as cm:
            ret_val = ia_client.set_resource(new_params, headers=inst_operator_actor_header)
        self.assertIn( 'The value for SBE37Parameter.INTERVAL cannot be greater than 50',cm.exception.message)


        new_params = {
            SBE37Parameter.TA0 : 2,
            SBE37Parameter.INTERVAL : 35,
            }

        #The policy should not deny this since the  SBE37Parameter.INTERVAL value is less than 50
        with self.assertRaises(Conflict) as cm:
            ret_val = ia_client.set_resource(new_params, headers=inst_operator_actor_header)

        #The policy should now deny the same command as above since the actor does not have the right role
        with self.assertRaises(Unauthorized) as cm:
            ret_val = ia_client.set_resource(new_params, headers=member_actor_header)

        #The policy should not deny this since the  SBE37Parameter.INTERVAL value is less than 50 and the user has the right role
        with self.assertRaises(Conflict) as cm:
            ret_val = ia_client.set_resource(new_params, headers=obs_operator_actor_header)


        #FROM HERE ON DOWN THE TESTS ARE FOR POLICIES ON RELATED RESOURCES
        '''
        #Find Model
        models,_ = self.rr_client.find_objects(instrument_id ,PRED.hasModel, RT.InstrumentModel)
        assert models
        inst_model = models[0]

        #Add a more restrictive policy for instruments models associated to this instrument
        print 'Creating policy for model: ' + inst_model._id
        policy_id = self.pol_client.create_resource_access_policy(inst_model._id, 'restrict_30_policy',
            'Check the value of the SBE37Parameter.INTERVAL parameter',
            DENY_PARAM_30_RULE, headers=self.system_actor_header)


        gevent.sleep(5)  # Wait for events to be published and policy updated

        #The policy should now deny the same command as above since the policy checks for a lower value for all models associated with this instrument
        with self.assertRaises(Unauthorized) as cm:
            ret_val = ia_client.set_resource(new_params, headers=inst_operator_actor_header)
        self.assertIn( 'The value for SBE37Parameter.INTERVAL cannot be greater than 30',cm.exception.message)



        new_params = {
            SBE37Parameter.TA0 : 2,
            SBE37Parameter.INTERVAL : 25,
            }

        #The policy should not deny this since the  SBE37Parameter.INTERVAL value is less than 30
        with self.assertRaises(Conflict) as cm:
            ret_val = ia_client.set_resource(new_params, headers=inst_operator_actor_header)


        #Add a more restrictive policy for the observatory that holds all of these resources

        print 'Creating policy for obs: ' + obs_id

        policy_id = self.pol_client.create_resource_access_policy(obs_id, 'restrict_10_policy',
            'Check the value of the SBE37Parameter.INTERVAL parameter',
            DENY_PARAM_10_RULE, headers=self.system_actor_header)


        gevent.sleep(5)  # Wait for events to be published and policy updated

        #The policy should now deny the same command as above since the policy checks for a lower value for all instruments in thie observatory
        with self.assertRaises(Unauthorized) as cm:
            ret_val = ia_client.set_resource(new_params, headers=inst_operator_actor_header)
        self.assertIn( 'The value for SBE37Parameter.INTERVAL cannot be greater than 10',cm.exception.message)



        new_params = {
            SBE37Parameter.TA0 : 2,
            SBE37Parameter.INTERVAL : 11,
            }

        #The policy should still deny the same command as above since the policy checks for a lower value for all instruments in thie observatory
        with self.assertRaises(Unauthorized) as cm:
            ret_val = ia_client.set_resource(new_params, headers=inst_operator_actor_header)
        self.assertIn( 'The value for SBE37Parameter.INTERVAL cannot be greater than 10',cm.exception.message)


        new_params = {
            SBE37Parameter.TA0 : 2,
            SBE37Parameter.INTERVAL : 8,
            }

        #The policy should not deny this since the  SBE37Parameter.INTERVAL value is less than 10
        with self.assertRaises(Conflict) as cm:
            ret_val = ia_client.set_resource(new_params, headers=inst_operator_actor_header)

        '''

