#!/usr/bin/env python
# coding: utf-8




__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import simplejson, collections
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from webtest import TestApp

from pyon.core.registry import getextends
from ion.services.coi.service_gateway_service import service_gateway_app, GATEWAY_RESPONSE, \
            GATEWAY_ERROR, GATEWAY_ERROR_MESSAGE, GATEWAY_ERROR_EXCEPTION, GATEWAY_ERROR_TRACE

from interface.services.coi.iservice_gateway_service import ServiceGatewayServiceClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.coi.iorg_management_service import OrgManagementServiceClient
from pyon.event.event import EventPublisher
from pyon.util.containers import DictDiffer
from pyon.util.log import log
from pyon.public import OT

import unittest
import os
import gevent

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

#These are supposed to be unicode fields that contain unicode characters.
DATA_PRODUCT_NAME = u"♣ TestDataProduct ♥"
DATA_PRODUCT_DESCRIPTION = u"A test data product Ĕ ∆"


def convert_unicode(data):
    if isinstance(data, unicode):
        return str(data.encode('utf8'))
    elif isinstance(data, collections.Mapping):
        return dict(map(convert_unicode, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert_unicode, data))
    else:
        return data


@attr('LOCOINT', 'INT', group='coi-sgs')
@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
class TestServiceGatewayServiceInt(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2coi.yml')

        # Now create client to service
        self.service_gateway_service = ServiceGatewayServiceClient(node=self.container.node)

        log.debug('stopping Gateway web server')
        # Stop the web server as it is not needed.
        self.service_gateway_service.stop_service()

        self.test_app = TestApp(service_gateway_app)

    def tearDown(self):
        self._stop_container()

    #Common SGS Response header check
    def check_response_headers(self, response):
        self.assertEqual(response.content_type, "application/json")
        self.assertEqual(len(response.json), 1)
        self.assertIn('data',response.json)

    def test_list_resource_types(self):

        response = self.test_app.get('/ion-service/list_resource_types')

        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        expected_type_list = getextends('Resource')

        self.assertEqual(len(response.json['data'][GATEWAY_RESPONSE]), len(expected_type_list))

        result_set = set(response.json['data'][GATEWAY_RESPONSE])
        expected_type_set = set(expected_type_list)

        intersect_result = expected_type_set.intersection(result_set)
        self.assertEqual(len(intersect_result), len(expected_type_list))

        response = self.test_app.get('/ion-service/list_resource_types?type=InformationResource')

        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        expected_type_list = getextends('InformationResource')

        self.assertEqual(len(response.json['data'][GATEWAY_RESPONSE]), len(expected_type_list))

        result_set = set(response.json['data'][GATEWAY_RESPONSE])
        expected_type_set = set(expected_type_list)

        intersect_result = expected_type_set.intersection(result_set)
        self.assertEqual(len(intersect_result), len(expected_type_list))

        response = self.test_app.get('/ion-service/list_resource_types?type=TaskableResource')

        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        expected_type_list = getextends('TaskableResource')

        self.assertEqual(len(response.json['data'][GATEWAY_RESPONSE]), len(expected_type_list))

        result_set = set(response.json['data'][GATEWAY_RESPONSE])
        expected_type_set = set(expected_type_list)

        intersect_result = expected_type_set.intersection(result_set)
        self.assertEqual(len(intersect_result), len(expected_type_list))

        response = self.test_app.get('/ion-service/list_resource_types?type=MyFakeResource')

        self.check_response_headers(response)
        self.assertIn(GATEWAY_ERROR, response.json['data'])
        self.assertIn('KeyError', response.json['data'][GATEWAY_ERROR][GATEWAY_ERROR_EXCEPTION])
        self.assertIn('MyFakeResource', response.json['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        self.assertIsNotNone(response.json['data'][GATEWAY_ERROR][GATEWAY_ERROR_TRACE])

    def create_data_product_resource(self):

        data_product_create_request = {  "serviceRequest": {
            "serviceName": "resource_registry",
            "serviceOp": "create",
            "params": {
                "object": {
                    "type_": "DataProduct",
                    "lcstate": "DRAFT",
                    "description": DATA_PRODUCT_DESCRIPTION,
                    "name": DATA_PRODUCT_NAME
                }
            }
        }
        }

        response = self.test_app.post('/ion-service/resource_registry/create', {'payload': simplejson.dumps(data_product_create_request) })
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        response_data = response.json['data'][GATEWAY_RESPONSE]
        self.assertEqual(len(response_data), 2 )
        data_product_id = convert_unicode(response_data[0])
        return data_product_id


    def delete_data_product_resource(self, data_product_id):

        data_product_delete_request = {  "serviceRequest": {
            "serviceName": "resource_registry",
            "serviceOp": "delete",
            "params": {
                "object_id": data_product_id
            }
        }
        }
        response = self.test_app.post('/ion-service/resource_registry/delete', {'payload': simplejson.dumps(data_product_delete_request) })
        self.check_response_headers(response)
        return response


    @attr('SMOKE')
    def test_anonymous_resource_registry_operations_through_gateway(self):
        """
        This test ensures that requests make through the service gateway through messaging to the Resource Registry and
        back; including the support of unicode characters.
        @return:
        """

        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=' + convert_unicode(DATA_PRODUCT_NAME) + '&id_only=True')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        response_data = response.json['data'][GATEWAY_RESPONSE]
        self.assertEqual(len(response_data),2 )
        self.assertEqual(len(response_data[0]), 0 )

        data_product_id = self.create_data_product_resource()

        data_product_read_request = {  "serviceRequest": {
                                    "serviceName": "resource_registry",
                                    "serviceOp": "read",
                                    "params": {
                                        "object_id": data_product_id
                                            }
                                        }
                                    }

        response = self.test_app.post('/ion-service/resource_registry/read', {'payload': simplejson.dumps(data_product_read_request) })
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        data_product_obj = convert_unicode(response.json['data'][GATEWAY_RESPONSE])


        #Verify the the name and description fields containing unicode characters match all the way through couch and the messaging
        self.assertEqual(data_product_obj['name'], convert_unicode(DATA_PRODUCT_NAME))
        self.assertEqual(data_product_obj['description'], convert_unicode(DATA_PRODUCT_DESCRIPTION))

        updated_description_text = data_product_obj['description'] + '---Updated!!'

        #modify some fields in the data for testing update
        data_product_obj['description'] = updated_description_text

        data_product_update_request = {
            "serviceRequest": {
                "serviceName": "resource_registry",
                "serviceOp": "update",
                "params": {
                    "object": data_product_obj
                }
            }
        }

        response = self.test_app.post('/ion-service/resource_registry/update', {'payload': simplejson.dumps(data_product_update_request) })
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        response = self.test_app.post('/ion-service/resource_registry/read', {'payload': simplejson.dumps(data_product_read_request) })
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        updated_data_product_obj = convert_unicode(response.json['data'][GATEWAY_RESPONSE])
        self.assertEqual(updated_data_product_obj['description'], updated_description_text )

        differ = DictDiffer(updated_data_product_obj, data_product_obj)
        self.assertEqual(len(differ.changed()), 2)  # Only the _rev and ts_updated fields should be different after an update

        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=' + convert_unicode(DATA_PRODUCT_NAME) + '&id_only=True')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        response_data = response.json['data'][GATEWAY_RESPONSE]
        self.assertEqual(len(response_data),2 )
        self.assertEqual(len(response_data[0]), 1 )

        response = self.delete_data_product_resource(data_product_id)
        self.assertIsNone(response.json['data'][GATEWAY_RESPONSE])

        response = self.test_app.post('/ion-service/resource_registry/read', {'payload': simplejson.dumps(data_product_read_request) })
        self.check_response_headers(response)
        self.assertIn(GATEWAY_ERROR, response.json['data'])
        self.assertIn('does not exist', response.json['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])

        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=' + convert_unicode(DATA_PRODUCT_NAME) + '&id_only=True')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        response_data = response.json['data'][GATEWAY_RESPONSE]
        self.assertEqual(len(response_data),2 )
        self.assertEqual(len(response_data[0]), 0 )

        response = self.delete_data_product_resource(data_product_id)
        self.assertIn(GATEWAY_ERROR, response.json['data'])
        self.assertIn('does not exist', response.json['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        self.assertIsNotNone(response.json['data'][GATEWAY_ERROR][GATEWAY_ERROR_TRACE])

    def test_non_anonymous_resource_registry_operations_through_gateway(self):

        id_client = IdentityManagementServiceClient(node=self.container.node)

        actor_id, valid_until, registered = id_client.signon(USER1_CERTIFICATE, True)

        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=TestDataProduct&id_only=True&requester=' + actor_id)
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        response_data = response.json['data'][GATEWAY_RESPONSE]
        self.assertEqual(len(response_data),2 )
        self.assertEqual(len(response_data[0]), 0 )

        id_client.delete_actor_identity(actor_id)


    def test_get_resource_schema(self):

        response = self.test_app.get('/ion-service/resource_type_schema/Org')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        org_obj = convert_unicode(response.json['data'][GATEWAY_RESPONSE])
        self.assertTrue(isinstance(org_obj, dict))
        self.assertIn('object', org_obj)
        self.assertIn('schemas', org_obj)
        self.assertIn('Org', org_obj['schemas'])
        self.assertIn('OrgTypeEnum', org_obj['schemas'])
        self.assertIn('Institution', org_obj['schemas'])

        response = self.test_app.get('/ion-service/resource_type_schema/InstrumentModel')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        model_obj = convert_unicode(response.json['data'][GATEWAY_RESPONSE])
        self.assertTrue(isinstance(org_obj, dict))
        self.assertIn('object', model_obj)
        self.assertIn('schemas', model_obj)
        self.assertIn('InstrumentModel', model_obj['schemas'])
        self.assertIn('PrimaryInterface', model_obj['schemas'])


        response = self.test_app.get('/ion-service/resource_type_schema/DataProduct')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        data_product_obj = convert_unicode(response.json['data'][GATEWAY_RESPONSE])
        self.assertTrue(isinstance(data_product_obj, dict))
        self.assertIn('object', data_product_obj)
        self.assertIn('schemas', data_product_obj)
        self.assertIn('DataProduct', data_product_obj['schemas'])
        self.assertIn('GeospatialBounds', data_product_obj['schemas'])
        self.assertIn('GeospatialCoordinateReferenceSystem', data_product_obj['schemas'])

        response = self.test_app.get('/ion-service/resource_type_schema/DataProduct123')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_ERROR, response.json['data'])
        self.assertIn('No matching class found', response.json['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        self.assertIsNotNone(response.json['data'][GATEWAY_ERROR][GATEWAY_ERROR_TRACE])



    def test_get_resource(self):

        data_product_id = self.create_data_product_resource()

        response = self.test_app.get('/ion-resources/resource/' + data_product_id)
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        response_data = response.json['data'][GATEWAY_RESPONSE]
        self.assertNotIn('does not exist', response_data)

        data_product_obj = convert_unicode(response_data)
        self.assertEqual(data_product_id, data_product_obj['_id'])

        self.delete_data_product_resource(data_product_id)

    def test_list_resources_by_type(self):

        data_product_id = self.create_data_product_resource()

        response = self.test_app.get('/ion-resources/find_resources/DataProduct')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        response_data = response.json['data'][GATEWAY_RESPONSE]
        self.assertEqual(len(response_data),1 )

        data_product_obj = convert_unicode(response_data[0])
        self.assertEqual(data_product_id, data_product_obj['_id'])

        self.delete_data_product_resource(data_product_id)

    def test_list_org_roles(self):

        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=ionsystem')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        response_data = response.json['data'][GATEWAY_RESPONSE]

        actor_id = response_data[0][0]['_id']
        response = self.test_app.get('/ion-service/org_roles/' + actor_id)
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        response_data = response.json['data'][GATEWAY_RESPONSE]

        self.assertIn('ION', response_data)
        self.assertEqual(len(response_data['ION']), 3)
        self.assertIn('ION_MANAGER', response_data['ION'])
        self.assertIn('ORG_MANAGER', response_data['ION'])
        self.assertIn('ORG_MEMBER', response_data['ION'])

    def test_user_role_cache(self):


        #Create a user
        id_client = IdentityManagementServiceClient(node=self.container.node)

        actor_id, valid_until, registered = id_client.signon(USER1_CERTIFICATE, True)

        #Make a request with this new user  to get it into the cache
        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=TestDataProduct&id_only=True&requester=' + actor_id)
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        #Check the contents of the user role cache for this user
        service_gateway_user_cache = self.container.proc_manager.procs_by_name['service_gateway'].user_data_cache
        self.assertEqual(service_gateway_user_cache.has_key(actor_id), True)

        role_header = service_gateway_user_cache.get(actor_id)
        self.assertIn('ION', role_header)
        self.assertEqual(len(role_header['ION']), 1)
        self.assertIn('ORG_MEMBER', role_header['ION'])

        org_client = OrgManagementServiceClient(node=self.container.node)

        ion_org = org_client.find_org()
        manager_role = org_client.find_org_role_by_name(org_id=ion_org._id, role_name='ORG_MANAGER')

        org_client.grant_role(org_id=ion_org._id, user_id=actor_id, role_name='ORG_MANAGER')

        #Just allow some time for event processing on slower platforms
        gevent.sleep(2)

        #The user should be evicted from the cache due to a change in roles
        self.assertEqual(service_gateway_user_cache.has_key(actor_id), False)

        #Do it again to check for new roles
        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=TestDataProduct&id_only=True&requester=' + actor_id)
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        #Check the contents of the user role cache for this user
        self.assertEqual(service_gateway_user_cache.has_key(actor_id), True)

        role_header = service_gateway_user_cache.get(actor_id)
        self.assertIn('ION', role_header)
        self.assertEqual(len(role_header['ION']), 2)
        self.assertIn('ORG_MEMBER', role_header['ION'])
        self.assertIn('ORG_MANAGER', role_header['ION'])

        #Now flush the user_role_cache and make sure it was flushed
        event_publisher = EventPublisher()
        event_publisher.publish_event(event_type=OT.UserRoleCacheResetEvent)

        #Just allow some time for event processing on slower platforms
        gevent.sleep(2)

        self.assertEqual(service_gateway_user_cache.has_key(actor_id), False)
        self.assertEqual(service_gateway_user_cache.size(), 0)

        #Change the role once again and see if it is there again
        org_client.revoke_role(org_id=ion_org._id, user_id=actor_id, role_name='ORG_MANAGER')

        #Just allow some time for event processing on slower platforms
        gevent.sleep(2)

        #The user should still not be there
        self.assertEqual(service_gateway_user_cache.has_key(actor_id), False)

        #Do it again to check for new roles
        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=TestDataProduct&id_only=True&requester=' + actor_id)
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        #Check the contents of the user role cache for this user
        self.assertEqual(service_gateway_user_cache.has_key(actor_id), True)

        role_header = service_gateway_user_cache.get(actor_id)
        self.assertIn('ION', role_header)
        self.assertEqual(len(role_header['ION']), 1)
        self.assertIn('ORG_MEMBER', role_header['ION'])

        id_client.delete_actor_identity(actor_id)

