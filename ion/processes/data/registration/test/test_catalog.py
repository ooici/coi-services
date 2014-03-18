from pyon.util.unit_test import PyonTestCase
from jinja2 import Environment, FileSystemLoader

class TestCatalog(PyonTestCase):
    def test_catalog(self):
        env = Environment(loader=FileSystemLoader('res/templates'), trim_blocks=True, lstrip_blocks=True)
        with open('res/templates/datasets.xml') as f:
            template = f.read()
        with open('ion/processes/data/registration/test/expected_datasets.xml') as f:
            expected = f.read()
        jtemplate = env.from_string(template)
        dataset = self.datasets()
        rendered_buffer = jtemplate.render(**dataset) + '\n'
        self.assertEquals(rendered_buffer, expected)

    def datasets(self):
        dataset = {
            "dataset_id" : 'abc123',
            "url" : 'http://www.google.com/',
            "face_page" : 'http://www.google.com/',
            "title" : 'sample dataset',
            "summary" : 'Sample Dataset',
            "vars" : [
                {'name':'time', 'attrs':{'units':'seconds since 1900-01-01', 'ioos_category':'Time', 'long_name':'Time', 'standard_name':'time', 'time_precision':'blah', 'ooi_short_name':'time', 'data_product_level':'axis'}},
                {'name':'temperature', 'attrs':{'units':'deg_C', 'ioos_category':'Temperature', 'long_name':'Temperature Corrected', 'standard_name':'temperature', 'function_name':'linear_corr'}}]
        }
        return dataset


