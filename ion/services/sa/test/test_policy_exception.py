
from pyon.public import IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.ion.resource import LCS

from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from pyon.core.exception import NotFound, Unauthorized #, Conflict
from pyon.public import RT, LCS, LCE, PRED, CFG
from pyon.ion.resource import get_maturity_visibility
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





    #@unittest.skip('refactoring')
    def test_policy_exception_ims(self):
        """

        """

        log.info("Create platform agent")
        platform_agent_obj = any_old(RT.PlatformAgent)
        platform_agent_id = self.client.IMS.create_platform_agent(platform_agent_obj)

        self.generic_lcs_pass(self.client.IMS, "platform_agent", platform_agent_id, LCE.PLAN, LCS.PLANNED)
        self.generic_lcs_fail(self.client.IMS, "platform_agent", platform_agent_id, LCE.DEVELOP)



    #############################
    #
    # HELPER STUFF
    #
    #############################


    def generic_lcs_fail(self,
                         owner_service,
                         resource_label,
                         resource_id,
                         lc_event):
        """
        execute an lcs event and verify that it fails

        @param owner_service instance of service client that will handle the request
        @param resource_label string like "instrument_device"
        @param resource_id string
        @param lc_event string like LCE.INTEGRATE
        """

        lcsmethod = getattr(owner_service, "execute_%s_lifecycle" % resource_label)
        # lcsmethod(resource_id, lc_event)
        log.debug("asserting that %s of '%s' on %s '%s' raises Unauthorized",
                  lcsmethod, lc_event, resource_label, resource_id)
        #self.assertRaises(Unauthorized, lcsmethod, resource_id, lc_event)

        with self.assertRaises(Unauthorized) as cm:
            lcsmethod(resource_id, lc_event)
        self.assertIn('No model associated with agent',cm.exception.message)

        response = _gw_call('instrument_management', "execute_%s_lifecycle" % resource_label, resource_id, lc_event)

        self.assertIn(GATEWAY_ERROR, response['data'])
        self.assertEqual(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_EXCEPTION], 'Unauthorized')
        self.assertEqual(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE], 'No model associated with agent')
        log.debug("Service Gateway Response: %s" % response['data'][GATEWAY_ERROR])

    def generic_lcs_pass(self,
                         owner_service,
                         resource_label,
                         resource_id,
                         lc_event,
                         lc_state):
        """
        execute an lcs event and verify that it passes and affects state

        @param owner_service instance of service client that will handle the request
        @param resource_label string like "instrument_device"
        @param resource_id string
        @param lc_event string like LCE.INTEGRATE
        @param lc_state string like LCS.INTEGRATED (where the state should end up
        """

        lcsmethod  = getattr(owner_service, "execute_%s_lifecycle" % resource_label)
        readmethod = getattr(owner_service, "read_%s" % resource_label)

        log.debug("performing %s of '%s' on %s '%s'", lcsmethod, lc_event, resource_label, resource_id)

        lcsmethod(resource_id, lc_event)

        log.debug("reading current revision of %s '%s'", resource_label, resource_id)
        resource_obj = readmethod(resource_id)

        log.debug("checking current state of resource")
        parts = get_maturity_visibility(resource_obj.lcstate)

        self.assertEqual(lc_state, parts[0])


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

