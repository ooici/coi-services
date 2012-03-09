from pyon.core.exception import Inconsistent, NotFound, BadRequest
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.coi.ipolicy_management_service import PolicyManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from pyon.public import Container, RT, IonObject, CFG, log
from pyon.util.context import LocalContextMixin
from interface.services.sa.idata_product_management_service import DataProductManagementServiceProcessClient
from interface.services.sa.imarine_facility_management_service import MarineFacilityManagementServiceProcessClient
import simplejson, urllib
from ion.services.coi.service_gateway_service import GATEWAY_RESPONSE, GATEWAY_ERROR, GATEWAY_ERROR_MESSAGE, GATEWAY_ERROR_EXCEPTION, get_role_message_headers
from ion.services.coi.policy_management_service import MANAGER_ROLE, MEMBER_ROLE

class FakeProcess(LocalContextMixin):
    name = 'gov_client'
    id='gov_client'

def seed_gov(container, process=FakeProcess()):

    id_client = IdentityManagementServiceProcessClient(node=container.node, process=process)

    system_actor = id_client.find_user_identity_by_name(name=CFG.system.system_actor)
    log.info('system actor:' + system_actor._id)



    org_client = OrgManagementServiceProcessClient(node=container.node, process=process)
    ion_org = org_client.find_org()



    operator_role = IonObject(RT.UserRole, name='INSTRUMENT_OPERATOR',label='Instrument Operator', description='Instrument Operator')
    org_client.add_user_role(ion_org._id, operator_role, headers={'ion-actor-id': system_actor._id})

    try:
        org_client.add_user_role(ion_org._id, operator_role)
    except Exception, e:
        log.info("This should fail")
        log.info(e.message)

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


    user_id, valid_until, registered = id_client.signon(certificate, True)

    log.info( "user id=" + user_id)

    try:
        org_client.enroll_member(ion_org._id,user_id)
    except Exception, e:
        log.info("This should fail:")
        log.info(e.message)


    roles = org_client.find_org_roles(ion_org._id)
    for r in roles:
        log.info('Org UserRole: ' + str(r))

    users = org_client.find_enrolled_users(ion_org._id)
    for u in users:
        log.info( str(u))

    roles = org_client.find_roles_by_user(ion_org._id, user_id)
    for r in roles:
        log.info('User UserRole: ' + str(r))


    results = find_data_products(user_id)
    log.info(results)

    results = find_instrument_agents(user_id)
    log.info(results)

    header_roles = get_role_message_headers(org_client.find_all_roles_by_user(system_actor._id))

    org_client.grant_role(ion_org._id, user_id, 'INSTRUMENT_OPERATOR', headers={'ion-actor-id': system_actor._id, 'ion-actor-roles': header_roles })
    roles = org_client.find_roles_by_user(ion_org._id, user_id)
    for r in roles:
        log.info('User UserRole: ' +str(r))

    roles = org_client.find_roles_by_user(ion_org._id, system_actor._id)
    for r in roles:
        log.info('ION System UserRole: ' +str(r))


#    log.info("Adding Instrument Operator Role")

#    role = org_client.find_org_role_by_name(ion_org._id, 'Instrument Operator')
#    org_client.grant_role(ion_org._id, user_id, role._id)
#    roles = org_client.find_roles_by_user(ion_org._id, user_id)
#    for r in roles:
#        log.info('User UserRole: ' +str(r))


#    requests = org_client.find_requests(ion_org._id)
#    log.info("Request count: %d" % len(requests))
##    for r in requests:
#        log.info('Org Request: ' +str(r))



def test_policy(container, process=FakeProcess()):

    org_client = OrgManagementServiceProcessClient(node=container.node, process=process)
    ion_org = org_client.find_org()

    id_client = IdentityManagementServiceProcessClient(node=container.node, process=process)

    system_actor = id_client.find_user_identity_by_name(name=CFG.system.system_actor)
    log.info('system actor:' + system_actor._id)

    policy_client = PolicyManagementServiceProcessClient(node=container.node, process=process)

    users = org_client.find_enrolled_users(ion_org._id)
    for u in users:
        log.info( str(u))

    user = id_client.find_user_identity_by_name('/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254')
    log.debug('user_id: ' + user._id)

    roles = org_client.find_roles_by_user(ion_org._id, user._id)
    for r in roles:
        log.info('User UserRole: ' +str(r))

    header_roles = get_role_message_headers(org_client.find_all_roles_by_user(user._id))

    try:
        org_client.grant_role(ion_org._id, user._id, 'INSTRUMENT_OPERATOR', headers={'ion-actor-id': user._id, 'ion-actor-roles': header_roles })
    except Exception, e:
        log.info('This grant role should be denied:' + e.message)

    roles = org_client.find_roles_by_user(ion_org._id, user._id)
    for r in roles:
        log.info('User UserRole: ' +str(r))


    header_roles = get_role_message_headers(org_client.find_all_roles_by_user(system_actor._id))

    try:
        org_client.grant_role(ion_org._id, user._id, 'INSTRUMENT_OPERATOR', headers={'ion-actor-id': system_actor._id, 'ion-actor-roles': header_roles })
    except Exception, e:
        log.info('This grant role should be fail as duplicate:' + e.message)

    header_roles = get_role_message_headers(org_client.find_all_roles_by_user(user._id))

    try:
        org_client.revoke_role(ion_org._id, user._id, 'INSTRUMENT_OPERATOR', headers={'ion-actor-id': user._id, 'ion-actor-roles': header_roles })
    except Exception, e:
        log.info('This revoke role should be denied:' + e.message)

    header_roles = get_role_message_headers(org_client.find_all_roles_by_user(system_actor._id))

    org_client.revoke_role(ion_org._id, user._id, 'INSTRUMENT_OPERATOR', headers={'ion-actor-id': system_actor._id, 'ion-actor-roles': header_roles })

    roles = org_client.find_roles_by_user(ion_org._id, user._id)
    for r in roles:
        log.info('User UserRole: ' +str(r))


    dp_client = DataProductManagementServiceProcessClient(node=container.node, process=process)

    dp_obj = IonObject(RT.DataProduct, name='DataProd1', description='some new dp')

    dp_client.create_data_product(dp_obj, headers={'ion-actor-id': system_actor._id})


    dp_obj = IonObject(RT.DataProduct,
        name='DataProd2',
        description='and of course another new dp')

    dp_client.create_data_product(dp_obj, headers={'ion-actor-id': system_actor._id})

    dp_obj = IonObject(RT.DataProduct,
        name='DataProd3',
        description='yet another new dp')

    dp_client.create_data_product(dp_obj, headers={'ion-actor-id': system_actor._id})

    log.info('Data Products')
    dp_list = dp_client.find_data_products()
    for dp_obj in dp_list:
        log.debug( str(dp_obj))


    results = find_data_products()
    log.info(results)

    ims_client = InstrumentManagementServiceProcessClient(node=container.node, process=process)

    try:
        ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent1', description='The first Instrument Agent')

        ims_client.create_instrument_agent(ia_obj, headers={'ion-actor-id': system_actor._id})
    except Exception, e:
        log.info('This operation should be denied: ' + e.message)


    header_roles = get_role_message_headers(org_client.find_all_roles_by_user(system_actor._id))

    org_client.grant_role(ion_org._id, user._id, 'INSTRUMENT_OPERATOR', headers={'ion-actor-id': system_actor._id, 'ion-actor-roles': header_roles })

    header_roles = get_role_message_headers(org_client.find_all_roles_by_user(user._id))

    ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent1', description='The first Instrument Agent')

    ims_client.create_instrument_agent(ia_obj,  headers={'ion-actor-id': user._id, 'ion-actor-roles': header_roles })

    ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent2', description='The second Instrument Agent')

    ims_client.create_instrument_agent(ia_obj, headers={'ion-actor-id': user._id, 'ion-actor-roles': header_roles })

    log.info( 'Instrument Agents')
    ia_list = ims_client.find_instrument_agents()
    for ia_obj in ia_list:
        log.info( str(ia_obj))

    results = find_instrument_agents()
    log.info(results)

def test_enrollment_request(container, process=FakeProcess()):

    org_client = OrgManagementServiceProcessClient(node=container.node, process=process)
    ion_org = org_client.find_org()

    id_client = IdentityManagementServiceProcessClient(node=container.node, process=process)

    system_actor = id_client.find_user_identity_by_name(name=CFG.system.system_actor)
    log.info('system actor:' + system_actor._id)

    sa_header_roles = get_role_message_headers(org_client.find_all_roles_by_user(system_actor._id))


    try:
        user = id_client.find_user_identity_by_name('/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254')
    except:
        raise Inconsistent("The test user is not found; did you seed the data?")

    log.debug('user_id: ' + user._id)
    user_header_roles = get_role_message_headers(org_client.find_all_roles_by_user(user._id))


    try:
        org2 = org_client.find_org('Org2')
        org2_id = org2._id
    except NotFound, e:

        org2 = IonObject(RT.Org, name='Org2', description='A second Org')
        org2_id = org_client.create_org(org2, headers={'ion-actor-id': system_actor._id, 'ion-actor-roles': sa_header_roles })


    roles = org_client.find_org_roles(org2_id)
    for r in roles:
        log.info('Org2 UserRole: ' + str(r))


    log.info(user._id)
    org_roles = org_client.find_all_roles_by_user(user._id)
    log.info("All Org Roles: " + str(org_roles))


    requests = org_client.find_requests(org2_id)
    log.info("Org2 Request count: %d" % len(requests))
    for r in requests:
        log.info('Org2 Request: ' +str(r))


    req_id = None
    try:
        log.info("User requesting enrollment")
        req_id = org_client.request_enroll(org2_id,user._id, headers={'ion-actor-id': user._id, 'ion-actor-roles': user_header_roles } )
    except BadRequest, e:
        pass

    requests = org_client.find_requests(org2_id)
    log.info("Org2 Request count: %d" % len(requests))
    for r in requests:
        log.info('Org Request: ' +str(r))

    if req_id is not None:
        org_client.deny_request(org2_id,req_id,'To test the deny process', headers={'ion-actor-id': system_actor._id, 'ion-actor-roles': sa_header_roles })

    requests = org_client.find_requests(org2_id)
    log.info("Org2 Request count: %d" % len(requests))
    for r in requests:
        log.info('Org Request: ' +str(r))


   # req_id = None
  #  try:
   #     log.info("User requesting enrollment")
   #     req_id = org_client.request_enroll(org2_id,user._id, headers={'ion-actor-id': user._id, 'ion-actor-roles': user_header_roles } )
    #except BadRequest, e:
#        log.info(e.message)


    if req_id is not None:
        org_client.approve_request(org2_id,req_id, headers={'ion-actor-id': system_actor._id, 'ion-actor-roles': sa_header_roles })

    requests = org_client.find_requests(org2_id)
    log.info("Org2 Request count: %d" % len(requests))
    for r in requests:
        log.info('Org Request: ' +str(r))

#    org_client.approve_request(org2_id, req_id)

#    requests = org_client.find_user_requests(user_id)
#    log.info("User Request count: %d" % len(requests))
#    for r in requests:
#        log.info('User Request: ' +str(r))

#    role = org_client.find_org_role_by_name(org2_id, MANAGER_ROLE)
#    req_id = org_client.request_role(org2_id,user_id, role._id)
#    requests = org_client.find_requests(org2_id)
#    log.info("Org2 Request count: %d" % len(requests))
#    for r in requests:
#        log.info('Org Request: ' +str(r))

#    marine_client = MarineFacilityManagementServiceProcessClient(node=container.node, process=process)
#    mf_obj = IonObject(RT.MarineFacility, name='Marine Facility Org', description='a new marine facility')
#    mf_id = marine_client.create_marine_facility(mf_obj)

#    roles = org_client.find_org_roles(mf_id)
#    for r in roles:
#        log.info(str(r))


#    role_obj = policy_client.find_role(MEMBER_ROLE)  # Not available anymore

#   org_client.grant_role(ion_org._id,user_id,role_obj._id)

#    members = org_client.find_enrolled_users(ion_org._id)
#    log.info("Number of Org members: %d" % len(members))

#    try:
#        org_client.remove_user_role(ion_org._id,role_obj._id)
#    except Exception, e:
#        log.info("This should fail:")
#        log.info(e.message)

#    org_client.remove_user_role(ion_org._id,role_obj._id,True)


def gateway_request(uri, payload):

    server_hostname = 'localhost'
    server_port = 5000
    web_server_cfg = None
    try:
        web_server_cfg = CFG['container']['service_gateway']['web_server']
    except Exception, e:
        web_server_cfg = None

    if web_server_cfg is not None:
        if 'hostname' in web_server_cfg:
            server_hostname = web_server_cfg['hostname']
        if 'port' in web_server_cfg:
            server_port = web_server_cfg['port']



    SEARCH_BASE = 'http://' + server_hostname + ':' + str(server_port) + '/ion-service/' + uri


    args = {}
    args.update({
        #'format': "unix",
        #'output': 'json'
    })
    url = SEARCH_BASE + '?' + urllib.urlencode(args)
    log.debug(url)
    log.debug(payload)

    result = simplejson.load(urllib.urlopen(url, 'payload=' + str(payload ) ))
    if not result.has_key('data'):
        log.error('Not a correct JSON response: %s' & result)

    return result

def find_data_products(requester=None):

    """
    data_product_find_request = {  "serviceRequest": {
        "serviceName": "resource_registry",
        "serviceOp": "find_resources",
        "requester": requester,
        "expiry": 0,
        "params": {
            "restype": 'DataProduct',
            "id_only": False
        }
    }
    }

    response = gateway_request('resource_registry/find_resources',  simplejson.dumps(data_product_find_request) )
    """

    data_product_find_request = {  "serviceRequest": {
        "serviceName": "data_product_management",
        "serviceOp": "find_data_products",
        "expiry": 0,
        "params": {
        }
    }
    }

    if requester is not None:
        data_product_find_request["serviceRequest"]["requester"] = requester

    response = gateway_request('data_product_management/find_data_products',  simplejson.dumps(data_product_find_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]

    log.info('Number of DataProduct objects: %s' % (str(len(response_data))))
    for res in response_data:
        log.debug(res)

    return response_data

def find_instrument_agents( requester=None):


    instrument_agent_find_request = {  "serviceRequest": {
        "serviceName": "instrument_management",
        "serviceOp": "find_instrument_agents",
        "expiry": 0,
        "params": {
        }
    }
    }

    if requester is not None:
        instrument_agent_find_request["serviceRequest"]["requester"] = requester

    response = gateway_request('instrument_management/find_instrument_agents',  simplejson.dumps(instrument_agent_find_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]

    log.info('Number of Instrument Agent objects: %s' % (str(len(response_data))))
    for res in response_data:
        log.debug(res)

    return response_data

def find_org_roles(requester=''):

    find_org_request = {  "serviceRequest": {
        "serviceName": "org_management",
        "serviceOp": "find_org",
        "expiry": 0,
        "params": {
        }
    }
    }

    if requester is not None:
        find_org_request["serviceRequest"]["requester"] = requester

    response = gateway_request('org_management/find_org',  simplejson.dumps(find_org_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]
    ion_org_id = response_data['_id']


    find_org_roles_request = {  "serviceRequest": {
        "serviceName": "org_management",
        "serviceOp": "find_org_roles",
        "expiry": 0,
        "params": {
            "org_id": ion_org_id
        }
    }
    }

    if requester is not None:
        find_org_roles_request["serviceRequest"]["requester"] = requester

    response = gateway_request('org_management/find_org_roles',  simplejson.dumps(find_org_roles_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]

    log.info('Number of UserRole objects in the ION Org: %s' % (str(len(response_data))))
    for res in response_data:
        log.debug(res)

    return response_data



if __name__ == '__main__':

    container = Container()
    container.start() # :(
    seed_gov(container)
    container.stop()
