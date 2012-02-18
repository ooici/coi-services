#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, IonObject
#from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS # , PRED
from nose.plugins.attrib import attr
import unittest

from ion.services.sa.test.helpers import any_old

from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.imarine_facility_management_service import MarineFacilityManagementServiceClient


from ion.services.sa.preload.preload_csv import PreloadCSV

# some stuff for logging info to the console
import sys
log = DotDict()
printout = sys.stderr.write
printout = lambda x: None

log.debug = lambda x: printout("DEBUG: %s\n" % x)
log.info = lambda x: printout("INFO: %s\n" % x)
log.warn = lambda x: printout("WARNING: %s\n" % x)



@attr('INT', group='sa')
@unittest.skip('https://github.com/ooici/ion-definitions/pull/94')
class TestLCAServiceGateway(IonIntegrationTestCase):
    """
    LCA integration tests at the service gateway level
    """

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2sa.yml')

        # Now create client to DataProductManagementService
        self.client = DotDict()
        self.client.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.client.DPMS = DataProductManagementServiceClient(node=self.container.node)
        self.client.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.client.MFMS = MarineFacilityManagementServiceClient(node=self.container.node)

    def test_just_the_setup(self):
        return

    def test_csv_loader(self):
        loader = PreloadCSV("localhost", 5000)

        loader.preload(["ion/services/sa/preload/LogicalInstrument.csv",
                        "ion/services/sa/preload/InstrumentDevice.csv",
                        "ion/services/sa/preload/associations.csv"])

        log_inst_ids = self.client.MFMS.find_logical_instruments()
        self.assertEqual(1, len(log_inst_ids))

        inst_ids = self.client.IMS.find_instrument_devices()
        self.assertEqual(1, len(inst_ids))

        associated_ids = self.client.IMS.find_logical_instrument_by_instrument_device(inst_ids[0])
        self.assertEqual(1, len(associated_ids))
