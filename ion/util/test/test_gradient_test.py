#!/usr/bin/env python
'''
@author Luke C
@date Wed Mar 27 09:27:08 EDT 2013
'''

from pyon.util.int_test import IonIntegrationTestCase
from ion.util.stored_values import StoredValueManager
from ion.util.parsers.gradient_test import gradient_test_parser
from nose.plugins.attrib import attr

@attr('INT',group='dm')
class TestGradientTestParser(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()

    def test_gradient_test(self):
        svm = StoredValueManager(self.container)
        for key,doc in gradient_test_parser(gradient_test_sample_doc):
            svm.stored_value_cas(key,doc)
            self.addCleanup(svm.delete_stored_value,key)

        doc = svm.read_value('grad_CHEL-C-32-12_PRESSURE_DENSITY')
        self.assertEquals(doc['units_dat'], 'dbar')
        self.assertEquals(doc['d_dat_dx'], 2.781)

        doc = svm.read_value('grad_CHEW-BA-C-A12_PRESSURE_DENSITY')
        self.assertEquals(doc['units_x'], 'kg m-3')
        self.assertEquals(doc['tol_dat'], 12.)


gradient_test_sample_doc='''Array,Instrument Class,Reference Designator,Data Product used as Input Data (DAT),Data Product used as Input Parameter X,Units of DAT,Units of X,DDATDX,MINDX,STARTDAT,TOLDAT
array 1,CHEL32,CHEL-C-32-12,PRESSURE,DENSITY,dbar,kg m-3,2.781,0.02,3573381636.43771,12
array 2,CHEW2,CHEW-BA-C-A12,PRESSURE,DENSITY,dbar,kg m-3,2.781,0.02,3573381636.43771,12'''


