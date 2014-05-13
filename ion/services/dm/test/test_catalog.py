
from bs4 import BeautifulSoup

from ion.services.dm.test.dm_test_case import DMTestCase
from pyon.util.breakpoint import breakpoint

from interface.objects import ParameterContext

class TestCatalog(DMTestCase):
    def test_basic_catalog(self):
        data_product_id = self.make_ctd_data_product()
        entry = self.data_product_management.read_catalog_entry(data_product_id)
        bs = BeautifulSoup(entry)
        dataset_globals = {}
        for attr in bs.dataset.addattributes.find_all('att'):
            dataset_globals[attr.get('name')] = attr.get_text()
        expectations = {
            'institution' : 'OOI',
            'summary' : 'ctd 0',
            'title' : 'ctd 0',
            'cdm_data_type' : 'Other'
        }

        for key,val in expectations.iteritems():
            self.assertEquals(dataset_globals[key], val)


    def test_add_parameter(self):
        '''
        Ensures that adding a parameter properly updates the catalog
        '''
        data_product_id = self.make_ctd_data_product()

        # Grab the XML Catalog entry for this data product and make sure that
        # it doesn't already contain the parameter.
        entry = self.data_product_management.read_catalog_entry(data_product_id)
        bs = BeautifulSoup(entry)
        var_names = []
        for var_entry in bs.dataset.find_all('datavariable'):
            var_names.append(var_entry.sourcename.get_text())

        self.assertNotIn('extra_parameter', var_names)

        parameter = ParameterContext(name='extra_parameter',
                                     parameter_type='quantity',
                                     value_encoding='float32',
                                     units='1')

        parameter_id = self.dataset_management.create_parameter(parameter)
        self.data_product_management.add_parameter_to_data_product(parameter_id, data_product_id)
                                     
        entry = self.data_product_management.read_catalog_entry(data_product_id)
        bs = BeautifulSoup(entry)
        var_names = []
        for var_entry in bs.dataset.find_all('datavariable'):
            var_names.append(var_entry.sourcename.get_text())

        self.assertIn('extra_parameter', var_names)
