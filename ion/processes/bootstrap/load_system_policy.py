#!/usr/bin/env python

"""Process that loads the system policy"""
__author__ = 'Stephen P. Henrie'

"""
Process that loads the system policy
"""

from pyon.core.governance import get_system_actor, get_system_actor_header
from pyon.public import CFG, log, ImmediateProcess, iex, Container, IonObject, RT, OT

from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.ipolicy_management_service import PolicyManagementServiceProcessClient


class LoadSystemPolicy(ImmediateProcess):
    """
    bin/pycc -x ion.processes.bootstrap.load_system_policy.LoadSystemPolicy op=load
    """
    def on_init(self):
        pass

    def on_start(self):
        op = self.CFG.get("op", None)
        log.info("LoadSystemPolicy: {op=%s}" % op)
        if op:
            if op == "load":
                self.op_load_system_policies(self)
            else:
                raise iex.BadRequest("Operation unknown")
        else:
            raise iex.BadRequest("No operation specified")

    def on_quit(self):
        pass

    #
    # Create the initial set of policy rules for the ION system. To make the rules easier to write, start by
    # denying all anonymous access to Org services and then add rules which Permit access to specific operations
    # based on conditions.
    #
    @classmethod
    def op_load_system_policies(cls, calling_process):

        org_client = OrgManagementServiceProcessClient(node=Container.instance.node, process=calling_process)
        ion_org = org_client.find_org()

        id_client = IdentityManagementServiceProcessClient(node=Container.instance.node, process=calling_process )

        system_actor = get_system_actor()
        log.info('system actor:' + system_actor._id)

        sa_user_header = get_system_actor_header()

        policy_client = PolicyManagementServiceProcessClient(node=Container.instance.node, process=calling_process)


        timeout = 20



##############
        '''
        This rule must be loaded before the Deny_Everything rule
        '''

        policy_client = PolicyManagementServiceProcessClient(node=Container.instance.node, process=calling_process)

        policy_text = '''
        <Rule RuleId="%s:" Effect="Permit">
            <Description>
                %s
            </Description>


        <Target>
            <Subjects>
                <Subject>
                    <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ION_MANAGER</AttributeValue>
                        <SubjectAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </SubjectMatch>
                </Subject>
            </Subjects>
        </Target>


        </Rule>
        '''

        policy_id = policy_client.create_common_service_access_policy( 'ION_Manager_Permit_Everything',
            'A global policy rule that permits access to everything with the ION Manager role',
            policy_text, headers=sa_user_header)




##############


        '''
        This rule must be loaded before the Deny_Everything rule
        '''

        policy_text = '''
        <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">service</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">read_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">READ</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">FIND</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">get_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">GET</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">

                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_enrolled_users</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_enrolled_orgs</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_org_negotiations</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_org_closed_negotiations</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">initiate_realtime_visualization_data</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">get_realtime_visualization_data</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">terminate_realtime_visualization_data</AttributeValue>
                        </Apply>
                        <ActionAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                </Apply>
            </Condition>

        </Rule>
        '''

        policy_id = policy_client.create_common_service_access_policy( 'Allowed_Anonymous_Service_Operations',
            'A global policy rule which specifies operations that are allowed with anonymous access',
            policy_text, headers=sa_user_header)

  ##############

        #This rule has been modified specifically for 2.0 to Deny for only specific services and agents. Everything else will be allowed.

        policy_text = '''
        <Rule RuleId="%s:" Effect="Deny">
            <Description>
                %s
            </Description>

            <Target>
                    <!-- REMOVE THE RESOURCE TARGETS BELOW AFTER 2.0 TO TIGHTEN POLICY -->

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">org_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">policy_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">instrument_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">observatory_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">InstrumentDevice</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">PlatformDevice</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">identity_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">scheduler</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">resource_registry</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">visualization</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">user_notification</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">data_product_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

            </Target>

        </Rule>
        '''

        policy_id = policy_client.create_common_service_access_policy( 'Deny_Everything', 'A global policy rule that denies access to everything by default',
            policy_text, headers=sa_user_header)


        ##############

        ###THIS POLICY HAS BEEN COMMENTED OUT FOR THE INITIAL R2 RELEASE

        """
        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

               <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">service</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">CREATE</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">update_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">UPDATE</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">delete_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">RETIRE</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DATA_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

        </Rule> '''

        policy_id = policy_client.create_common_service_access_policy( 'Allowed_CUD_Service_Operations_for_Roles',
            'A global policy rule which specifies operations that are allowed with for OPERATOR AND MANAGER roles',
            policy_text, headers=sa_user_header)
        """

        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">resource_registry</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>


                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DATA_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:and">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DELETE</AttributeValue>
                            </Apply>
                            <ActionAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">UPDATE</AttributeValue>
                            </Apply>
                            <ActionAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_attachment</AttributeValue>
                            </Apply>
                            <ActionAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('resource_registry', 'RR_Permitted_Operations',
            'Permit these operations in the Resource Registry Service for the proper roles',
            policy_text, headers=sa_user_header)


        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">resource_registry</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>
                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_attachment</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DATA_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MEMBER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_attachment_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>
        </Rule> '''

        policy_id = policy_client.create_service_access_policy('resource_registry', 'RR_create_attachment_Operation',
            'Permit create attachment operation only in the context of the org to which the user belongs',
            policy_text, headers=sa_user_header)

        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">resource_registry</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>
                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">UPDATE</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>
            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_edit_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>
        </Rule> '''

        policy_id = policy_client.create_service_access_policy('resource_registry', 'RR_edit_Operation',
            'Permit edit operation only in the context of the org to which the user belongs',
            policy_text, headers=sa_user_header)

        ##############
        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">identity_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">prepare_user_info_support</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">signon</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_user_info</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">update_user_info</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">delete_user_info</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">initiate_account_merge</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">complete_account_merge</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>


            </Target>


            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-one-and-only">
                        <SubjectAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('identity_management', 'IDS_Permitted_Non_Anonymous',
            'Permit these operations in the Identity Management Service if the user is not anonymous',
            policy_text, headers=sa_user_header)



        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">org_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>



                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">

                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DELETE</AttributeValue>
                        </Apply>
                        <ActionAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                </Apply>
            </Condition>



        </Rule> '''

        policy_id = policy_client.create_service_access_policy('org_management', 'OrgMS_Org_Manager_Role_Permitted',
            'Permit these operations in the Org Management Service for the role of Org Manager',
            policy_text, headers=sa_user_header)


        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">policy_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>



                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">

                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DELETE</AttributeValue>
                        </Apply>
                        <ActionAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                </Apply>
            </Condition>



        </Rule> '''

        policy_id = policy_client.create_service_access_policy('policy_management', 'PolicyMS_Org_Manager_Role_Permitted',
            'Permit these operations in the Policy Management Service for the role of Org Manager',
            policy_text, headers=sa_user_header)


        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">org_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>


                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">release_commitment</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DATA_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('org_management', 'OrgMS_Role_Permitted',
            'Permit these operations in the Org Management Service for the operator roles',
            policy_text, headers=sa_user_header)

##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">org_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">negotiate</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>


                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MEMBER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>


        </Rule> '''

        policy_id = policy_client.create_service_access_policy('org_management', 'OMS_Org_Member_Role_Permitted',
            'Permit these operations in the Org Management Service for any user that is a simple Member of the Org',
            policy_text, headers=sa_user_header)



        ##############


        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">observatory_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:and">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DELETE</AttributeValue>
                            </Apply>
                            <ActionAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">activate_deployment</AttributeValue>
                            </Apply>
                            <ActionAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">deactivate_deployment</AttributeValue>
                            </Apply>
                            <ActionAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('observatory_management', 'OBS_Role_Permitted_Operations',
            'Permit these operations in the Observatory Management Service for role of Observatory Operator or Org Manager',
            policy_text, headers=sa_user_header)

        ##############


        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">observatory_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>
                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">activate_deployment</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">deactivate_deployment</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>
                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_deployment_activation_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>
        </Rule> '''

        policy_id = policy_client.create_service_access_policy('observatory_management', 'OBS_Permitted_Deployment_Operations',
            'Permit deployment activation/deactivation in the Observatory Management Service for role of Observatory Operator or Org Manager',
            policy_text, headers=sa_user_header)


        ##############


        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

               <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">instrument_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DELETE</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">LCS-CHANGE</AttributeValue>
                        </Apply>
                        <ActionAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                 </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('instrument_management', 'IMS_Role_Permitted_Operations',
            'Permit these operations in the Instrument Management Service for role of Instrument Operator',
            policy_text, headers=sa_user_header)


        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">instrument_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DELETE</AttributeValue>
                        </Apply>
                        <ActionAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                 </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('instrument_management', 'IMS_Higher_Role_Permitted_Operations',
            'Permit these operations in the Instrument Management Service for role of Observatory Operator or Org Manager',
            policy_text, headers=sa_user_header)


        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

               <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">data_product_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DATA_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DELETE</AttributeValue>
                        </Apply>
                        <ActionAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                 </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('data_product_management', 'DPMS_Role_Permitted_Operations',
            'Permit these operations in the Data Product Management Service for role of Instrument Operator, Data Operator, Observatory Operator or Org Manager',
            policy_text, headers=sa_user_header)




        ##############


        policy_text = '''
        <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

               <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">instrument_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">LCS-CHANGE</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-verb" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_lifecycle_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('instrument_management', 'IMS_Check_Lifecycle_Operations',
            'Call the check_lifecycle_policy operation in the IMS',
            policy_text, headers=sa_user_header)


        ##############

        policy_text = '''
        <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">scheduler</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">user_notification</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">DATA_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">publish_event</AttributeValue>
                        </Apply>
                        <ActionAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                 </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('scheduler', 'Scheduler_Permitted_Operations',
            'Permit these operations in the Scheduler Service for the proper roles',
            policy_text, headers=sa_user_header)

        policy_id = policy_client.create_service_access_policy('user_notification', 'User_Notification_Permitted_Operations',
            'Permit these operations in the User Notification Service for the proper roles',
            policy_text, headers=sa_user_header)


        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>
                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">user_notification</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MEMBER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>
                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_notification</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">delete_notification</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_subscription_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('user_notification', 'Subscription_For_Members',
            'Permit subscriptions for org members',
            policy_text, headers=sa_user_header)

        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>
                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">user_notification</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MEMBER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>
                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">publish_event</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

            </Target>
            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_publish_event_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('user_notification', 'Publish_issue_event_for_ION_members',
            'Permit registered users to publish issue event',
            policy_text, headers=sa_user_header)

        ##############

        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">visualization</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">initiate_realtime_visualization_data</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">get_realtime_visualization_data</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">terminate_realtime_visualization_data</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>


            </Target>


            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-one-and-only">
                        <SubjectAttributeDesignator
                             AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id"
                             DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </Apply>
                    </Apply>
                </Apply>
            </Condition>

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('visualization', 'Vis_Service_Permitted_Non_Anonymous',
            'Permit these operations in the Visualization Service if the user is not anonymous',
            policy_text, headers=sa_user_header)


        ##############


        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

               <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">InstrumentDevice</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">PlatformDevice</AttributeValue>
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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">negotiate</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">get_capabilities</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MEMBER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>
            </Target>

        </Rule> '''

        #All resource_agents are kind of handled the same - but the resource-id in the rule is set to the specific type
        policy_id = policy_client.create_service_access_policy('InstrumentDevice', 'Instrument_Agent_Org_Member_Permitted',
        'Permit these operations in an instrument agent for a Member of the Org',
        policy_text, headers=sa_user_header)


        #All resource_agents are kind of handled the same - but the resource-id in the rule is set to the specific type
        policy_id = policy_client.create_service_access_policy('PlatformDevice', 'Platform_Agent_Org_Member_Permitted',
            'Permit these operations in an platform agent for a Member of the Org',
            policy_text, headers=sa_user_header)


#############


        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

               <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">InstrumentDevice</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">PlatformDevice</AttributeValue>
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
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">get_.*$</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>
            </Target>

        </Rule> '''

        #All resource_agents are kind of handled the same - but the resource-id in the rule is set to the specific type
        policy_id = policy_client.create_service_access_policy('InstrumentDevice', 'Instrument_Agent_Operator_Manager_Permitted',
            'Permit these operations in an instrument agent any user with the proper Operator or Manager role.',
            policy_text, headers=sa_user_header)

        #All resource_agents are kind of handled the same - but the resource-id in the rule is set to the specific type
        policy_id = policy_client.create_service_access_policy('PlatformDevice', 'Platform_Agent_Operator_Manager_Permitted',
            'Permit these operations in an platform agent any user with the proper Operator or Manager role.',
            policy_text, headers=sa_user_header)



        #############


        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

               <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">InstrumentDevice</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">PlatformDevice</AttributeValue>
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
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">execute_resource</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ping_resource</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>


            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_resource_operation_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>

        </Rule> '''

        #All resource_agents are kind of handled the same - but the resource-id in the rule is set to the specific type
        policy_id = policy_client.create_service_access_policy('InstrumentDevice', 'Instrument_Agent_Check_Resource_Operation_Policy',
            'Call the check_resource_operation_policy operation in the Instrument Agent',
            policy_text, headers=sa_user_header)

        #All resource_agents are kind of handled the same - but the resource-id in the rule is set to the specific type
        policy_id = policy_client.create_service_access_policy('PlatformDevice', 'Platform_Agent_Check_Resource_Operation_Policy',
            'Call the check_resource_operation_policy operation in the Platform Agent',
            policy_text, headers=sa_user_header)


        #############


        policy_text = '''
            <Rule RuleId="%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

               <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">InstrumentDevice</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">PlatformDevice</AttributeValue>
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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">set_agent</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">execute_agent</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ping_agent</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">INSTRUMENT_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">OBSERVATORY_OPERATOR</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>


            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:evaluate-function">
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">check_agent_operation_policy</AttributeValue>
                    <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:param-dict" DataType="http://www.w3.org/2001/XMLSchema#dict"/>
                </Apply>
            </Condition>

        </Rule> '''

        #All resource_agents are kind of handled the same - but the resource-id in the rule is set to the specific type
        policy_id = policy_client.create_service_access_policy('InstrumentDevice', 'Instrument_Agent_Check_Agent_Operation_Policy',
            'Call the check_agent_operation_policy operation in the Instrument Agent',
            policy_text, headers=sa_user_header)

        #All resource_agents are kind of handled the same - but the resource-id in the rule is set to the specific type
        policy_id = policy_client.create_service_access_policy('PlatformDevice', 'Platform_Agent_Check_Agent_Operation_Policy',
            'Call the check_agent_operation_policy operation in the Platform Agent',
            policy_text, headers=sa_user_header)


        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.InstrumentDevice, op='get_resource',
            policy_content='check_if_direct_access_mode', headers=sa_user_header )

        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.InstrumentDevice, op='set_resource',
            policy_content='check_if_direct_access_mode', headers=sa_user_header )

        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.InstrumentDevice, op='execute_resource',
            policy_content='check_if_direct_access_mode', headers=sa_user_header )

        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.PlatformDevice, op='get_resource',
            policy_content='check_if_direct_access_mode', headers=sa_user_header )

        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.PlatformDevice, op='set_resource',
            policy_content='check_if_direct_access_mode', headers=sa_user_header )

        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.PlatformDevice, op='execute_resource',
            policy_content='check_if_direct_access_mode', headers=sa_user_header )

