from ion.services.dm.test.dm_test_case import DMTestCase
from pyon.util.breakpoint import breakpoint

class TestCatalog(DMTestCase):
    def test_basic_catalog(self):
        data_product_id = self.make_ctd_data_product()
        entry = self.data_product_management.read_catalog_entry(data_product_id)
        breakpoint(locals(), globals())

