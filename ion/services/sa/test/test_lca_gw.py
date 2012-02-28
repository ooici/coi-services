#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import  CFG
#from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

#from pyon.core.exception import BadRequest, NotFound, Conflict
#from pyon.public import RT, LCS # , PRED
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

    def test_csv_loader_all(self):
        loader = PreloadCSV(CFG.web_server.hostname, CFG.web_server.port)

        loader.preload(["ion/services/sa/preload/LogicalInstrument.csv",
                        "ion/services/sa/preload/InstrumentDevice.csv",
                        "ion/services/sa/preload/associations.csv"])

        log_inst_ids = self.client.MFMS.find_logical_instruments()
        self.assertEqual(2, len(log_inst_ids))

        inst_ids = self.client.IMS.find_instrument_devices()
        self.assertEqual(2, len(inst_ids))

        associated_ids = self.client.IMS.find_logical_instrument_by_instrument_device(inst_ids[0])
        self.assertEqual(1, len(associated_ids))


    def test_csv_loader_tagged(self):
        loader = PreloadCSV(CFG.web_server.hostname, CFG.web_server.port)

        loader.preload(["ion/services/sa/preload/LogicalInstrument.csv",
                        "ion/services/sa/preload/InstrumentDevice.csv",
                        "ion/services/sa/preload/associations.csv"],
                       "LCA")

        log_inst_ids = self.client.MFMS.find_logical_instruments()
        self.assertEqual(1, len(log_inst_ids))
        log_inst = self.client.MFMS.read_logical_instrument(logical_instrument_id=log_inst_ids[0])
        self.assertEqual(log_inst.name, "Logical Instrument 1")

        
        inst_ids = self.client.IMS.find_instrument_devices()
        self.assertEqual(1, len(inst_ids))
        inst = self.client.IMS.read_instrument_device(instrument_device_id=inst_ids[0])
        self.assertEqual(inst.name, "Instrument Device 1")

        associated_ids = self.client.IMS.find_logical_instrument_by_instrument_device(inst_ids[0])
        self.assertEqual(1, len(associated_ids))
