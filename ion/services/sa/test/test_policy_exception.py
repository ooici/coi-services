
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

import string

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
        self.assertRaises(Unauthorized, lcsmethod, resource_id, lc_event)
        self.assertRaisesRegexp(Unauthorized, "401 - No model associated with agent", lcsmethod, resource_id, lc_event)

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


