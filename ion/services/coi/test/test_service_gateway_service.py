#!/usr/bin/env python




__author__ = 'Stephen P. Henrie'
__license__ = 'Apache 2.0'

import simplejson, json
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from webtest import TestApp

from pyon.core.registry import get_message_class_in_parm_type, getextends
from ion.services.coi.service_gateway_service import ServiceGatewayService, app, convert_unicode, GATEWAY_RESPONSE, \
            GATEWAY_ERROR, GATEWAY_ERROR_MESSAGE, GATEWAY_ERROR_EXCEPTION

from interface.services.coi.iservice_gateway_service import ServiceGatewayServiceClient
from pyon.util.containers import DictDiffer
from pyon.util.log import log

import unittest
import os

@attr('INT', group='coi')
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

        self.test_app = TestApp(app)

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


    def create_data_product_resource(self):

        data_product_create_request = {  "serviceRequest": {
            "serviceName": "resource_registry",
            "serviceOp": "create",
            "params": {
                "object": {
                    "type_": "DataProduct",
                    "provider_project": "Integration Test",
                    "lcstate": "DRAFT",
                    "description": "A test data product",
                    "name": "TestDataProduct",
                    "contact": {
                        "name": "Test User",
                        "phone": "858-555-1212",
                        "city": "San Diego",
                        "postalcode": "92093"
                    }
                }
            }
        }
        }

        response = self.test_app.post('/ion-service/resource_registry/create', {'payload': json.dumps(data_product_create_request) })
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


    def test_resource_registry_operations_through_gateway(self):


        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=TestDataProduct&id_only=True')
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

        #modify some fields in the data for testing update
        data_product_obj['description'] = 'An updated description for test data'
        data_product_obj['contact']['postalcode'] = '12345'

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
        self.assertEqual(updated_data_product_obj['description'], 'An updated description for test data', )
        self.assertEqual(updated_data_product_obj['contact']['postalcode'], '12345')

        differ = DictDiffer(updated_data_product_obj, data_product_obj)
        self.assertEqual(len(differ.changed()), 2)  # Only the _rev and ts_updated fields should be different after an update

        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=TestDataProduct&id_only=True')
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

        response = self.test_app.get('/ion-service/resource_registry/find_resources?name=TestDataProduct&id_only=True')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        response_data = response.json['data'][GATEWAY_RESPONSE]
        self.assertEqual(len(response_data),2 )
        self.assertEqual(len(response_data[0]), 0 )

        response = self.delete_data_product_resource(data_product_id)
        self.assertIn(GATEWAY_ERROR, response.json['data'])
        self.assertIn('does not exist', response.json['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])

    def test_get_resource_schema(self):

        response = self.test_app.get('/ion-service/resource_type_schema/DataProduct')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        data_product_obj = convert_unicode(response.json['data'][GATEWAY_RESPONSE])
        self.assertTrue(isinstance(data_product_obj, dict))

        response = self.test_app.get('/ion-service/resource_type_schema/DataProduct123')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_ERROR, response.json['data'])
        self.assertIn('No matching class found', response.json['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])




    def test_get_resource(self):

        data_product_id = self.create_data_product_resource()

        response = self.test_app.get('/ion-service/rest/resource/' + data_product_id)
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])

        response_data = response.json['data'][GATEWAY_RESPONSE]
        self.assertNotIn('does not exist', response_data)

        data_product_obj = convert_unicode(response_data)
        self.assertEqual(data_product_id, data_product_obj['_id'])

        self.delete_data_product_resource(data_product_id)

    def test_list_resources_by_type(self):

        data_product_id = self.create_data_product_resource()

        response = self.test_app.get('/ion-service/rest/find_resources/DataProduct')
        self.check_response_headers(response)
        self.assertIn(GATEWAY_RESPONSE, response.json['data'])
        response_data = response.json['data'][GATEWAY_RESPONSE]
        self.assertEqual(len(response_data),1 )

        data_product_obj = convert_unicode(response_data[0])
        self.assertEqual(data_product_id, data_product_obj['_id'])

        self.delete_data_product_resource(data_product_id)
