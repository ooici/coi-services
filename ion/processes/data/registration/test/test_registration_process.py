from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from ion.processes.data.registration.registration_process import RegistrationProcess
from pyon.public import CFG
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule_utils import time_series_domain
from xml.dom.minidom import parseString
from coverage_model import SimplexCoverage, QuantityType

@attr('INT')
class RegistrationProcessTest(IonIntegrationTestCase):
    def setUp(self):
        #print >> sys.stderr, "setup"
        self._start_container()
        #print >> sys.stderr, "start container"
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        #print >> sys.stderr, "deploy"
        self.dataset_management = DatasetManagementServiceClient()
        #print >> sys.stderr, "dataset management"
        
        #setup registry process and patch in CFG
        def init(self):
            super(RegistrationProcess, self).__init__()
            self.CFG = CFG
        RegistrationProcess.__init__ = init
        self.rp = RegistrationProcess()
        self.rp.on_start()
            
    def test_get_dataset_to_xml(self):
        dataset_id = self._make_dataset()
        coverage_path = DatasetManagementService()._get_coverage_path(dataset_id)
        cov = SimplexCoverage.load(coverage_path)
        
        xml_str = self.rp.get_dataset_xml(coverage_path)
        dom = parseString(xml_str)
        node = dom.getElementsByTagName('addAttributes')
        
        metadata = node[0]
        for n in metadata.childNodes:
            if n.nodeType != 3:
                if n.attributes["name"].value == "title":
                    self.assertEquals(cov.name, n.childNodes[0].nodeValue)
                if n.attributes["name"].value == "institution":
                    self.assertEquals('OOI', n.childNodes[0].nodeValue)
                if n.attributes["name"].value == "infoUrl":
                    self.assertEquals(self.rp.pydap_url+cov.name, n.childNodes[0].nodeValue)
        parameters = []
        node = dom.getElementsByTagName('sourceName')
        for n in node:
            if n.nodeType != 3:
                parameters.append(str(n.childNodes[0].nodeValue))
        cov_params = self._get_parameters(cov)
        self.assertEquals(parameters, cov_params)
        cov.close()

    def _get_parameters(self, cov):
        result = []
        for key in cov.list_parameters():
            pc = cov.get_parameter_context(key)
            if isinstance(pc.param_type, QuantityType):
                result.append(key)
        return result

    def _make_dataset(self):
        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()
        parameter_dict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        dataset_id = self.dataset_management.create_dataset('test_dataset', parameter_dictionary_id=parameter_dict_id, spatial_domain=sdom, temporal_domain=tdom)
        return dataset_id
