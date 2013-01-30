from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from ion.processes.data.registration.registration_process import RegistrationProcess
from pyon.public import CFG
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule_utils import time_series_domain
from xml.dom.minidom import parseString
from coverage_model.coverage import SimplexCoverage

@attr('INT')
class RegistrationProcessUnitTest(IonIntegrationTestCase):
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
        import sys
        #print >> sys.stderr, "make dataset"
        dataset_id = self._make_dataset()
        coverage_path = DatasetManagementService()._get_coverage_path(dataset_id)
        cov = SimplexCoverage.load(coverage_path)
        
        print >> sys.stderr, coverage_path
        xml_str = self.rp.get_dataset_xml(coverage_path)
        dom = parseString(xml_str)
        print >> sys.stderr, dom.toprettyxml()
        node = dom.getElementsByTagName('addAttributes')
        
        metadata = node[0]
        for node in metadata.childNodes:
            if node.nodeType != 3:
                #print >> sys.stderr, node.attributes["name"].value
                #print >> sys.stderr, node.childNodes[0].nodeValue
                if node.attributes["name"].value == "title":
                    self.assertEquals(cov.name, node.childNodes[0].nodeValue)
                if node.attributes["name"].value == "institution":
                    self.assertEquals('OOI', node.childNodes[0].nodeValue)
                if node.attributes["name"].value == "infoUrl":
                    print sys.stderr, self.rp.pydap_url
                    self.assertEquals(self.rp.pydap_url+cov.name, node.childNodes[0].nodeValue)
        for key in cov.list_parameters():
            print >> sys.stderr, key

    def _make_dataset(self):
        tdom, sdom = time_series_domain()
        sdom = sdom.dump()
        tdom = tdom.dump()
        parameter_dict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        dataset_id = self.dataset_management.create_dataset('test_dataset', parameter_dictionary_id=parameter_dict_id, spatial_domain=sdom, temporal_domain=tdom)
        return dataset_id
