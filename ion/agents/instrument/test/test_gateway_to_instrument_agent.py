#!/usr/bin/env python

"""
@package ion.agents.instrument.test.test_gateway_to_instrument_agent
@file ion/agents.instrument/test_gateway_to_instrument_agent.py
@author Stephen Henrie
@brief Test cases for R2 instrument agent through the Service Gateway
"""

__author__ = 'Stephen Henrie'
__license__ = 'Apache 2.0'

import simplejson, urllib, os, unittest
from mock import patch

from pyon.public import log, CFG
from nose.plugins.attrib import attr

import pyon.core.exception as pyex
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest
from pyon.core.object import IonObjectSerializer
from pyon.agent.agent import ResourceAgentClient
from ion.agents.instrument.test.test_instrument_agent import TestInstrumentAgent, IA_RESOURCE_ID

from ion.services.coi.service_gateway_service import GATEWAY_RESPONSE, GATEWAY_ERROR, GATEWAY_ERROR_MESSAGE, GATEWAY_ERROR_EXCEPTION

# bin/nosetests -s -v ion/agents/instrument/test/test_gateway_to_instrument_agent.py:TestInstrumentAgentViaGateway.test_initialize


@attr('HARDWARE', group='mi')
@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False),'Not integrated for CEI')
class TestInstrumentAgentViaGateway(TestInstrumentAgent):
    """
    Test cases for accessing the instrument agent class through the service gateway. This class is an extension of the
    class which tests the instrument agent in general. Essentially uses everything in the parent class, except accesses
    the agent using the service gateway.
    """

    def setUp(self):

        super(TestInstrumentAgentViaGateway, self).setUp()

        # Override a resource agent client to talk with the instrument agent through the Service Gateway.
        self._ia_client = None
        self._ia_client = ResourceAgentViaServiceGateway(IA_RESOURCE_ID)
        log.info('Accessing resource agent client through gateway')


    @attr('SMOKE')
    def test_autosample(self):
        super(TestInstrumentAgentViaGateway, self).test_autosample()


class ResourceAgentViaServiceGateway(ResourceAgentClient):
    """
    A test fixture for routing resource agent client requests through the service gateway.
    """
    
    # The resource agent interface replicated in this test fixture, redirected
    # to gateway calls.
    def negotiate(self, *args, **kwargs):
        return self.gw_negotiate(IA_RESOURCE_ID, *args, **kwargs)

    def get_capabilities(self, *args, **kwargs):
        return self.gw_get_capabilities(IA_RESOURCE_ID, *args, **kwargs)
        
    def execute_agent(self, *args, **kwargs):
        return self.gw_execute_agent(IA_RESOURCE_ID, *args, **kwargs)

    def get_agent(self, *args, **kwargs):
        return self.gw_get_agent(IA_RESOURCE_ID, *args, **kwargs)

    def set_agent(self, *args, **kwargs):
        return self.gw_set_agent(IA_RESOURCE_ID, *args, **kwargs)

    def get_agent_state(self, *args, **kwargs):
        return self.gw_get_agent_state(IA_RESOURCE_ID, *args, **kwargs)

    def ping_agent(self, *args, **kwargs):
        return self.gw_ping_agent(IA_RESOURCE_ID, *args, **kwargs)

    def execute_resource(self, *args, **kwargs):
        return self.gw_execute_resource(IA_RESOURCE_ID, *args, **kwargs)

    def get_resource(self, *args, **kwargs):
        return self.gw_get_resource(IA_RESOURCE_ID, *args, **kwargs)

    def set_resource(self, *args, **kwargs):
        return self.gw_set_resource(IA_RESOURCE_ID, *args, **kwargs)

    def get_resource_state(self, *args, **kwargs):
        return self.gw_get_resource_state(IA_RESOURCE_ID, *args, **kwargs)

    def ping_resource(self, *args, **kwargs):
        return self.gw_ping_resource(IA_RESOURCE_ID, *args, **kwargs)

    def emit(self, *args, **kwargs):
        return self.gw_emit(IA_RESOURCE_ID, *args, **kwargs)
    
    
    # Interface gateway calls.
    def gw_negotiate(self, resource_id="", sap_in=None, requester=None):
        pass

    # XXX    
    def gw_get_capabilities(self, resource_id="", current_state=True, requester=None):
        agent_get_capabilities_request = {  "agentRequest": {
            "agentId": resource_id,
            "agentOp": "get_capabilities",
            "expiry": 0,
            "params": {
                "current_state": current_state}
        }}

        return _process_gateway_request(resource_id, "get_capabilities", agent_get_capabilities_request, requester)
        
    # XXX            
    def gw_execute_agent(self, resource_id="", command=None, requester=None):
        return self._gw_execute( 'execute_agent', resource_id, command, requester)
    
    # XXX    
    def gw_get_agent(self, resource_id='', params=[], requester=None):
        return self._gw_get('get_agent', resource_id, params, requester)
    
    def gw_set_agent(self, resource_id='', params={}, requester=None):
        return self._gw_set('set_agent', resource_id, params, requester)
    
    def gw_get_agent_state(self, resource_id='', requester=None):
        return self._gw_get_state('get_agent_state', resource_id, requester)
    
    def gw_ping_agent(self, resource_id="", requester=None):
        return self._gw_ping('ping_agent', resource_id, requester)
    
    def gw_execute_resource(self, resource_id='', command=None, requester=None):
        return self._gw_execute('execute_resource', resource_id, command, requester)
    
    def gw_get_resource(self, resource_id='', params=[], requester=None):
        return self._gw_get('get_resource', resource_id, params, requester)
    
    def gw_set_resource(self, resource_id='', params={}, requester=None):
        return self._gw_set('set_resource', resource_id, params, requester)
    
    def gw_get_resource_state(self, resource_id='', requester=None):
        return self._gw_get_state('get_resource_state', resource_id, requester)
    
    def gw_ping_resource(self, resource_id='', requester=None):
        return self._gw_ping('ping_resource', resource_id, requester)
    
    def gw_emit(self, requester=None):
        pass
    
    def _gw_execute(self, op, resource_id, cmd, requester=None):

        agent_cmd_params = IonObjectSerializer().serialize(cmd)

        agent_execute_request = {  "agentRequest": {
            "agentId": resource_id,
            "agentOp": op,
            "expiry": 0,
            "params": {
                "command": agent_cmd_params
            }
        }}

        ret_values = _process_gateway_request(resource_id, op, agent_execute_request, requester)

        ret_obj = IonObject('AgentCommandResult',ret_values)
        return ret_obj


    def _gw_get(self, op, resource_id, params, requester=None):

        agent_get_param_request = {  "agentRequest": {
            "agentId": resource_id,
            "agentOp": op,
            "expiry": 0,
            "params": {
                "params" : params
            }
        }}

        return _process_gateway_request(resource_id, op, agent_get_param_request, requester)

    def _gw_set(self, op, resource_id,  params, requester=None):

        agent_set_param_request = {  "agentRequest": {
            "agentId": resource_id,
            "agentOp": op,
            "expiry": 0,
            "params": {
                'params' : params
            }
        }}

        return _process_gateway_request(resource_id, op, agent_set_param_request, requester)

    def _gw_get_state(self, op, resource_id, current_state, requester=None):

        agent_get_state_request = {  "agentRequest": {
            "agentId": resource_id,
            "agentOp": op,
            "expiry": 0,
            "params": {
                'current_state' : current_state
            }
        }}

        return _process_gateway_request(resource_id, op, agent_get_state_request, requester)

    def _gw_ping(self, op, resource_id, requester=None):

        agent_ping_request = {  "agentRequest": {
            "agentId": resource_id,
            "agentOp": op,
            "expiry": 0,
            "params": {}
        }}

        return _process_gateway_request(resource_id, op, agent_ping_request, requester)


def _agent_gateway_request(uri, payload):

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



    SEARCH_BASE = 'http://' + server_hostname + ':' + str(server_port) + '/ion-agent/' + uri


    args = {}
    args.update({
        #'format': "unix",
        #'output': 'json'
    })
    url = SEARCH_BASE + '?' + urllib.urlencode(args)
    log.debug(url)
    log.debug(payload)

    log.info("Service Gateway Request: %s %s" % (uri, payload))

    result = simplejson.load(urllib.urlopen(url, 'payload=' + str(payload ) ))
    if not result.has_key('data'):
        log.error('Not a correct JSON response: %s' & result)

    log.info("Service Gateway Response: %s" % result)

    return result

def _process_gateway_request(resource_id, operation, json_request, requester):

    if requester is not None:
        json_request["agentRequest"]["requester"] = requester

    payload = simplejson.dumps(json_request)

    response = _agent_gateway_request(resource_id + '/' + operation,   payload)

    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        #raise BadRequest(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        ex_cls = response['data'][GATEWAY_ERROR][GATEWAY_ERROR_EXCEPTION]
        ex_msg = response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]
        if hasattr(pyex, ex_cls):
            raise getattr(pyex, ex_cls)(ex_msg)
        
        else:
            raise Exception(ex_msg)

    try:
        if "type_" in response['data'][GATEWAY_RESPONSE]:
            del response['data'][GATEWAY_RESPONSE]["type_"]
    except Exception, e:
        pass

    return response['data'][GATEWAY_RESPONSE]


