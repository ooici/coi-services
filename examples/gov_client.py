from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.coi.ipolicy_management_service import PolicyManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from pyon.public import Container, RT, IonObject, CFG, log
from pyon.util.context import LocalContextMixin
from interface.services.sa.idata_product_management_service import DataProductManagementServiceProcessClient
from interface.services.sa.imarine_facility_management_service import MarineFacilityManagementServiceProcessClient
import simplejson, urllib
from ion.services.coi.service_gateway_service import GATEWAY_RESPONSE, GATEWAY_ERROR, GATEWAY_ERROR_MESSAGE, GATEWAY_ERROR_EXCEPTION
from ion.services.coi.policy_management_service import MANAGER_ROLE, MEMBER_ROLE

class FakeProcess(LocalContextMixin):
    name = 'gov_client'
    id='gov_client'

def seed_gov(container, process=FakeProcess()):

    dp_client = DataProductManagementServiceProcessClient(node=container.node, process=process)

    dp_obj = IonObject(RT.DataProduct, name='DataProd1', description='some new dp')

    dp_client.create_data_product(dp_obj)


    dp_obj = IonObject(RT.DataProduct,
        name='DataProd2',
        description='and of course another new dp')

    dp_client.create_data_product(dp_obj,)

    dp_obj = IonObject(RT.DataProduct,
        name='DataProd3',
        description='yet another new dp')

    dp_client.create_data_product(dp_obj,)

    log.debug('Data Products')
    dp_list = dp_client.find_data_products()
    for dp_obj in dp_list:
        log.debug( str(dp_obj))

    ims_client = InstrumentManagementServiceProcessClient(node=container.node, process=process)

    ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent1', description='The first Instrument Agent')

    ims_client.create_instrument_agent(ia_obj)

    ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent2', description='The second Instrument Agent')

    ims_client.create_instrument_agent(ia_obj)

    log.debug( 'Instrument Agents')
    ia_list = ims_client.find_instrument_agents()
    for ia_obj in ia_list:
        log.debug( str(ia_obj))


    org_client = OrgManagementServiceProcessClient(node=container.node, process=process)
    ion_org = org_client.find_org()


    policy_client = PolicyManagementServiceProcessClient(node=container.node, process=process)

    org_client.add_user_role(ion_org._id, name='Operator', description='Instrument Operator')

    try:
        org_client.add_user_role(ion_org._id, name='Operator', description='Instrument Operator')
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

    id_client = IdentityManagementServiceProcessClient(node=container.node, process=process)

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
        log.info('User is enrolled in Root ION Org: %s' % str(org_client.is_enrolled(ion_org._id, user_id)))

    roles = org_client.find_roles_by_user(ion_org._id, user_id)
    for r in roles:
        log.info('User UserRole: ' + str(r))


    results = find_data_products(ion_org._id,user_id)
    log.info(results)

    results = find_instrument_agents(ion_org._id,user_id)
    log.info(results)


    org_client.grant_role(ion_org._id, user_id, 'Operator')
    roles = org_client.find_roles_by_user(ion_org._id, user_id)
    for r in roles:
        log.info('User UserRole: ' +str(r))


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

    org2 = IonObject(RT.Org, name='Org2', description='A second Org')
    org2_id = org_client.create_org(org2)

    requests = org_client.find_requests(org2_id)
    log.info("Org2 Request count: %d" % len(requests))
    for r in requests:
        log.info('Org2 Request: ' +str(r))

    roles = org_client.find_org_roles(org2_id)
    for r in roles:
        log.info('Org2 UserRole: ' + str(r))


    org_roles = org_client.find_all_roles_by_user(user_id)
    log.info("All Org Roles: " + str(org_roles))

    org_client.enroll_member(org2_id, user_id)


    roles = org_client.find_roles_by_user(org2_id, user_id)

    org_roles = org_client.find_all_roles_by_user(user_id)
    log.info("All Org Roles: " + str(org_roles))

#    req_id = org_client.request_enroll(org2_id,user_id )

#    requests = org_client.find_requests(org2_id)
#    log.info("Org2 Request count: %d" % len(requests))
#    for r in requests:
#        log.info('Org Request: ' +str(r))

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


#    role_obj = policy_client.find_role(MEMBER_ROLE)

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
    if 'web_server' in CFG:
        web_server_cfg = CFG['web_server']
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

def find_data_products(org_id = '', requester=''):

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
        "serviceOrg": org_id,
        "requester": requester,
        "expiry": 0,
        "params": {
        }
    }
    }

    response = gateway_request('data_product_management/find_data_products',  simplejson.dumps(data_product_find_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]

    log.info('Number of DataProduct objects: %s' % (str(len(response_data))))
    for res in response_data:
        log.debug(res)

    return response_data

def find_instrument_agents(org_id = '', requester=''):


    instrument_agent_find_request = {  "serviceRequest": {
        "serviceName": "instrument_management",
        "serviceOp": "find_instrument_agents",
        "serviceOrg": org_id,
        "requester": requester,
        "expiry": 0,
        "params": {
        }
    }
    }

    response = gateway_request('instrument_management/find_instrument_agents',  simplejson.dumps(instrument_agent_find_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]

    log.info('Number of Instrument Agent objects: %s' % (str(len(response_data))))
    for res in response_data:
        log.debug(res)

    return response_data

def find_org_roles(org_id = '', requester=''):

    find_org_request = {  "serviceRequest": {
        "serviceName": "org_management",
        "serviceOp": "find_org",
        "serviceOrg": org_id,
        "requester": requester,
        "expiry": 0,
        "params": {
        }
    }
    }

    response = gateway_request('org_management/find_org',  simplejson.dumps(find_org_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]
    ion_org_id = response_data['_id']


    find_org_roles_request = {  "serviceRequest": {
        "serviceName": "org_management",
        "serviceOp": "find_org_roles",
        "serviceOrg": org_id,
        "requester": requester,
        "expiry": 0,
        "params": {
            "org_id": ion_org_id
        }
    }
    }

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
