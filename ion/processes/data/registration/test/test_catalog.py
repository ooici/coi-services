from pyon.util.unit_test import PyonTestCase
from jinja2 import Environment, FileSystemLoader
from nose.plugins.attrib import attr

class TestCatalog(PyonTestCase):
    def setUp(self):
        self.env = Environment(loader=FileSystemLoader('res/templates'), trim_blocks=True, lstrip_blocks=True)

    @attr('UTIL')
    def test_catalog(self):
        with open('ion/processes/data/registration/test/expected_datasets.xml') as f:
            expected = f.read()
        jtemplate = self.env.get_template('datasets.xml')
        dataset = self.dataset()
        rendered_buffer = jtemplate.render(**dataset) + '\n'
        #self.assertEquals(rendered_buffer, expected)
        print rendered_buffer

    @attr('UTIL')
    def test_dataset(self):
        jtemplate = self.env.get_template('dataset.xml')
        dataset = self.dataset()
        rendered_buffer = jtemplate.render(**dataset) + '\n'
        print rendered_buffer


    def dataset(self):
        dataset = {
            "dataset_id" : 'abc123',
            "url" : 'http://www.google.com/',
            "face_page" : 'http://www.google.com/',
            "title" : 'sample dataset',
            "summary" : 'Sample Dataset',
            "attrs": {
                "comment":"a dataset",
                "ooi_short_name":"TEMPWAT",
                "qc_glblrng":"applicable"
            },
            "vars" : [
                {'name':'time', 'attrs':{'units':'seconds since 1900-01-01', 'ioos_category':'Time', 'long_name':'Time', 'standard_name':'time', 'time_precision':'blah', 'ooi_short_name':'time', 'data_product_level':'axis'}},
                {'name':'temperature', 'attrs':{'units':'deg_C', 'ioos_category':'Temperature', 'long_name':'Temperature Corrected', 'standard_name':'temperature', 'function_name':'linear_corr'}}]
        }
        return dataset


