#!/usr/bin/env python

"""Process that loads the system policy"""
__author__ = 'Stephen P. Henrie'

"""
Process that loads the system policy
"""
from pyon.public import CFG, log, ImmediateProcess, iex, Container, IonObject, RT, OT
from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.ipolicy_management_service import PolicyManagementServiceProcessClient

from pyon.public import CFG, log, ImmediateProcess, iex, Container

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

        system_actor = Container.instance.governance_controller.get_system_actor()
        log.info('system actor:' + system_actor._id)

        sa_user_header = Container.instance.governance_controller.get_system_actor_header()

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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">read*</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find*</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">get*</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

            </Target>

            <Condition>
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:not">

                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-at-least-one-member-of">
                        <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-bag">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_org_negotiations</AttributeValue>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_enrolled_users</AttributeValue>
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

        policy_text = '''
        <Rule RuleId="%s:" Effect="Deny">
            <Description>
                %s
            </Description>
        </Rule>
        '''

        policy_id = policy_client.create_common_service_access_policy( 'Deny_Everything',
            'A global policy rule that denies access to everything by default',
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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">service</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">update</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">delete</AttributeValue>
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

        </Rule> '''

        policy_id = policy_client.create_common_service_access_policy( 'Allowed_CUD_Service_Operations_for_Roles',
            'A global policy rule which specifies operations that are allowed with for OPERATOR AND MANAGER roles',
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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">signon</AttributeValue>
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
            'Permit these operations in the Identity Management Service is the user is not anonymous',
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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_org_negotiations</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">find_enrolled_users</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">approve_negotiation</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">deny_negotiation</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">enroll_member</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">cancel_member_enrollment</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">grant_role</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">revoke_role</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">add_user_role</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">remove_user_role</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">acquire_resource</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">ORG_MANAGER</AttributeValue>
                            <SubjectAttributeDesignator
                                 AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-id"
                                 DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

            </Target>


        </Rule> '''

        policy_id = policy_client.create_service_access_policy('org_management', 'OMS_Org_Manager_Role_Permitted',
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
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">has_role</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">instrument_management</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">update</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">delete</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">execute_.*_lifecycle</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">request_direct_access</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">stop_direct_access</AttributeValue>
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

        </Rule> '''

        policy_id = policy_client.create_service_access_policy('instrument_management', 'IMS_Role_Permitted_Operations',
            'Permit these operations in the Instrument Management Service for role of Instrument Operator, Observatory Operator or Org Manager',
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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">agent</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:receiver-type" DataType="http://www.w3.org/2001/XMLSchema#string"/>
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

        </Rule> '''

        #All resource_agents are kind of handled the same - but the resource-id in the rule is set to the specific type
        policy_id = policy_client.create_service_access_policy('InstrumentDevice', 'Instrument_Agent_Org_Manager_Role_Permitted',
            'Permit all instrument agent operations for the role of Org Manager',
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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">get_resource_state</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">get_resource</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
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
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">get_agent_state</AttributeValue>
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
                </Subjects>
            </Target>

        </Rule> '''

        #All resource_agents are kind of handled the same - but the resource-id in the rule is set to the specific type
        policy_id = policy_client.create_service_access_policy('InstrumentDevice', 'Instrument_Agent_Instrument_Operator_Permitted',
            'Permit these operations in an instrument agent for an Instrument Operator',
            policy_text, headers=sa_user_header)


#########  Load Operation Specific Preconditions #############


        #Add precondition policies for the Instrument Agents

        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.InstrumentDevice, op='execute_resource',
                policy_content='check_execute_resource', headers=sa_user_header )


        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.InstrumentDevice, op='set_resource',
            policy_content='check_set_resource', headers=sa_user_header )


        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.InstrumentDevice, op='ping_resource',
            policy_content='check_ping_resource', headers=sa_user_header )


        #Add precondition policies for the Platform Agents

        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.PlatformDevice, op='execute_resource',
            policy_content='check_execute_resource', headers=sa_user_header )


        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.PlatformDevice, op='set_resource',
            policy_content='check_set_resource', headers=sa_user_header )


        pol_id = policy_client.add_process_operation_precondition_policy(process_name=RT.PlatformDevice, op='ping_resource',
            policy_content='check_ping_resource', headers=sa_user_header )

        #Add precondition policies for IMS Direct Access operations
        pol_id = policy_client.add_process_operation_precondition_policy(process_name='instrument_management', op='request_direct_access',
            policy_content='check_exclusive_commitment', headers=sa_user_header )

        pol_id = policy_client.add_process_operation_precondition_policy(process_name='instrument_management', op='stop_direct_access',
            policy_content='check_exclusive_commitment', headers=sa_user_header )

