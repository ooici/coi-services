import os
import unittest
from pyon.public import IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.ion.resource import LCS

from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from pyon.core.exception import NotFound, Unauthorized #, Conflict
from pyon.public import RT, LCS, LCE, PRED, CFG
from nose.plugins.attrib import attr

from ion.services.sa.test.helpers import any_old

from interface.services.dm.idataset_management_service import DatasetManagementServiceClient

import string, simplejson, urllib
import pyon.core.exception as pyex

from pyon.core.bootstrap import IonObject
from pyon.core.object import IonObjectSerializer

from ion.services.coi.service_gateway_service import GATEWAY_RESPONSE, GATEWAY_ERROR, GATEWAY_ERROR_MESSAGE, GATEWAY_ERROR_EXCEPTION


# some stuff for logging info to the console
log = DotDict()

def mk_logger(level):
    def logger(fmt, *args):
        print "%s %s" % (string.ljust("%s:" % level, 8), (fmt % args))

    return logger

log.debug = mk_logger("DEBUG")
log.info  = mk_logger("INFO")
log.warn  = mk_logger("WARNING")


@attr('INT', group='sa')
class TestAssembly(IonIntegrationTestCase):
    """
    assembly integration tests at the service level
    """

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.client = DotDict()
        self.client.IMS  = InstrumentManagementServiceClient(node=self.container.node)

        self.client.RR   = ResourceRegistryServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()


    def test_policy_exception_ims(self):

        def fail_execute_via_ims(platform_agent_id, lc_event):
            log.debug("attemting to trigger exception through IMS")
            with self.assertRaises(Unauthorized) as cm:
                self.client.IMS.execute_platform_agent_lifecycle(platform_agent_id, lc_event)
            self.assertIn('No model associated with agent', cm.exception.message)

        self.base_policy_exception(fail_execute_via_ims)



    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Assumes localhost gateway, not supportd in CEI LAUNCH mode')
    def test_policy_exception_sgw(self):
        """

        """
        def fail_execute_via_sgw(platform_agent_id, lc_event):
            log.debug("attempting to trigger exception through service gateway")

            response = _gw_call('instrument_management',
                                "execute_platform_agent_lifecycle",
                                platform_agent_id,
                                lc_event)

            self.assertIn(GATEWAY_ERROR, response['data'])
            self.assertEqual(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_EXCEPTION], 'Unauthorized')
            self.assertEqual(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE], 'No model associated with agent')
            log.debug("Service Gateway Response: %s" % response['data'][GATEWAY_ERROR])

        self.base_policy_exception(fail_execute_via_sgw)




    def base_policy_exception(self, fail_fn):
        log.info("Create platform agent")
        platform_agent_obj = any_old(RT.PlatformAgent)
        platform_agent_id = self.client.IMS.create_platform_agent(platform_agent_obj)

        log.debug("executing lifecycle transition to PLANNED")
        self.client.IMS.execute_platform_agent_lifecycle(platform_agent_id, LCE.PLAN)

        log.debug("reading current revision")
        resource_obj = self.client.IMS.read_platform_agent(platform_agent_id)

        log.debug("checking current state of resource")
        self.assertEqual(LCS.PLANNED, resource_obj.lcstate)

        log.debug("executing test-specific failure check")
        fail_fn(platform_agent_id, LCE.DEVELOP)








def _gw_call(service_name, op, platform_agent_id='', lifecycle_event='', requester=None):

    ims_request = {  "serviceRequest": {
        "serviceName": service_name,
        "serviceOp": op,
        "params": {
            "platform_agent_id": platform_agent_id,
            "lifecycle_event": lifecycle_event
        }
    }
    }
    return _process_gateway_request(service_name, op, ims_request, requester)


def _service_gateway_request(uri, payload):

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

    log.info("Service Gateway Request: %s %s" % (uri, payload))

    result = simplejson.load(urllib.urlopen(url, 'payload=' + str(payload ) ))
    if not result.has_key('data'):
        log.error('Not a correct JSON response: %s' & result)

    #log.debug("Service Gateway Response: %s" % result)

    return result


def _process_gateway_request(service_name, operation, json_request, requester):

    if requester is not None:
        json_request["serviceRequest"]["requester"] = requester


    decoder = IonObjectSerializer()
    decoded_msg = decoder.serialize(json_request)
    payload = simplejson.dumps(decoded_msg)

    response = _service_gateway_request(service_name + '/' + operation,   payload)

    return response

