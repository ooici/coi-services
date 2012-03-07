#!/usr/bin/env python

"""Process that loads the system policy"""

__author__ = 'Stephen P. Henrie'

"""
Process that loads the system policy
"""
from pyon.public import CFG, log, ImmediateProcess, iex, Container, IonObject, RT
from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.ipolicy_management_service import PolicyManagementServiceProcessClient

from pyon.public import CFG, log, ImmediateProcess, iex, Container

class LoadSystemPolicy(ImmediateProcess):
    """
    bin/pycc -x ion.processes.bootstrap.load_system_policy.LoadSystemPolicy op=load system.force_clean=False
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

    @classmethod
    def op_load_system_policies(cls, calling_process):

        org_client = OrgManagementServiceProcessClient(node=Container.instance.node, process=calling_process)
        ion_org = org_client.find_org()

        id_client = IdentityManagementServiceProcessClient(node=Container.instance.node, process=calling_process )

        system_actor = id_client.find_user_identity_by_name(name=CFG.system.system_actor)
        log.info('system actor:' + system_actor._id)

        policy_client = PolicyManagementServiceProcessClient(node=Container.instance.node, process=calling_process)

        policy_text = '''
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


                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">update</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">delete</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">activate</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">deactivate</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">add</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">remove</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">assign</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">unassign</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">register</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">unregister</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">execute</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">set</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>

                    </Action>
                </Actions>

            </Target>


        </Rule>
        '''


        policy_obj = IonObject(RT.Policy, name='Anonymous_No_Create_Update', definition_type="global", rule=policy_text,
            description='A global rule which specifies that an anonymous user can access most services except create/update operations')

        policy_id = policy_client.create_policy(policy_obj, headers={'ion-actor-id': system_actor._id})
        policy_client.add_resource_policy(ion_org._id, policy_id, headers={'ion-actor-id': system_actor._id})
        log.debug('Policy created: ' + policy_obj.name)

        policy_text = '''
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

                <Resources>
                    <Resource>
                        <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">datastore</AttributeValue>
                            <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ResourceMatch>
                    </Resource>
                </Resources>

                <Actions>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_doc</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

            </Target>

            <Condition>

                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-one-and-only">
                        <SubjectAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id-sender"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">bootstrap</AttributeValue>
                </Apply>

            </Condition>

        </Rule>
        '''

        policy_obj = IonObject(RT.Policy, name='DataStore_Anonymous_Bootstrap', definition_type="service", rule=policy_text,
            description='An anonymous user can only access datastore.create_doc during bootstrap')

        policy_id = policy_client.create_policy(policy_obj)
        policy_client.add_service_policy('datastore', policy_id)
        log.debug('Policy created: ' + policy_obj.name)


        policy_text = '''
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
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_user_identity</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

            </Target>

            <Condition>

                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-one-and-only">
                        <SubjectAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id-sender"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">bootstrap</AttributeValue>
                </Apply>

            </Condition>
        </Rule>
        '''

        policy_obj = IonObject(RT.Policy, name='Identity_Management_Anonymous_Bootstrap', definition_type="service", rule=policy_text,
            description='An anonymous user can onbly access identity_management.create_user_identity during bootstrap')

        policy_id = policy_client.create_policy(policy_obj)
        policy_client.add_service_policy('identity_management', policy_id)
        log.debug('Policy created: ' + policy_obj.name)

        policy_text = '''
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
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create</AttributeValue>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_association</AttributeValue>
                        </ActionMatch>
                    </Action>
                </Actions>

            </Target>
            <Condition>

                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                    <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-one-and-only">
                        <SubjectAttributeDesignator
                                AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id-sender"
                                DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </Apply>
                    <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">identity_management</AttributeValue>
                </Apply>

            </Condition>

        </Rule>
        '''


        policy_obj = IonObject(RT.Policy, name='Resource_Registry_Anonymous_Bootstrap', definition_type="service", rule=policy_text,
            description='An anonymous user can only access resource_registry.create and resource_registry.create_association from the identity_management service during bootstrap')

        policy_id = policy_client.create_policy(policy_obj)
        policy_client.add_service_policy('resource_registry', policy_id)
        log.debug('Policy created: ' + policy_obj.name)

        policy_text = '''
            <Rule RuleId="urn:oasis:names:tc:xacml:2.0:example:ruleid:%s" Effect="Permit">
            <Description>
                %s
            </Description>

            <Target>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">True</AttributeValue>
                            <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-Manager" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

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
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">grant_role</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">revoke_role</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

            </Target>


        </Rule> '''

        policy_obj = IonObject(RT.Policy, name='Org_Management_Manager_Role_Permitted', definition_type="service", rule=policy_text,
            description='Specific operations in the Org Management are only allowed for users that are assigned the User Role of Manager')

        policy_id = policy_client.create_policy(policy_obj)
        policy_client.add_service_policy('org_management', policy_id)
        log.debug('Policy created: ' + policy_obj.name)

        policy_text = '''
            <Rule RuleId="urn:oasis:names:tc:xacml:2.0:example:ruleid:%s" Effect="Deny">
            <Description>
                %s
            </Description>

            <Target>

                <Subjects>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">True</AttributeValue>
                            <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-Member" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                    <Subject>
                        <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                            <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </SubjectMatch>
                    </Subject>
                </Subjects>

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
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">grant_role</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                    <Action>
                        <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                            <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">revoke_role</AttributeValue>
                            <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        </ActionMatch>
                    </Action>
                </Actions>

            </Target>


        </Rule>
        '''
        policy_obj = IonObject(RT.Policy, name='Org_Management_Member_Role_Denied', definition_type="service", rule=policy_text,
            description='Specific operations in the Org Management are not allowed for users that are only assigned the User Role of Member')

        policy_id = policy_client.create_policy(policy_obj)
        policy_client.add_service_policy('org_management', policy_id)
        log.debug('Policy created: ' + policy_obj.name)


        #TODO - replace with Event update framework
        from pyon.core.bootstrap import service_registry
        for service_name in service_registry.services:
            policy_rules = policy_client.get_active_service_policy_rules(ion_org._id, service_name)
            Container.instance.governance_controller.load_policy_for_service(service_name, policy_rules)

