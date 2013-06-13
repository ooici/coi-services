#!/usr/bin/env python
'''
@author Luke Campbell
@file ion/util/parsers/test/test_parsers.py
@description Test for the parsers
'''

from pyon.util.int_test import IonIntegrationTestCase
from pyon.core.exception import NotFound
from ion.util.stored_values import StoredValueManager
from ion.util.parsers.global_range_test import grt_parser
from ion.util.parsers.spike_test import spike_parser
from ion.util.parsers.stuck_value_test import stuck_value_test_parser
from ion.util.parsers.trend_test import trend_parser
from ion.util.parsers.local_range_test import lrt_parser
from nose.plugins.attrib import attr
import numpy as np

qc_paths = {
    'grt'   : 'res/preload/r2_ioc/attachments/Data_QC_Lookup_Table_Global_Range_Test_2013-2-21.csv',
    'spike' : 'res/preload/r2_ioc/attachments/Data_QC_Lookup_Table_spike_test_updated.csv',
    'stuck' : 'res/preload/r2_ioc/attachments/Data_QC_Lookup_Table_Stuck_Value_Test.csv',
    'trend' : 'res/preload/r2_ioc/attachments/Data_QC_Lookup_Table_Trend_Test.csv',
    'lrt' : 'res/preload/r2_ioc/attachments/Data_QC_Lookup_Table_Local_Range_Test.lrt',
    }

@attr('INT',group='dm')
class TestParsers(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.svm = StoredValueManager(self.container)

    @classmethod
    def parse_document(cls, container, parser, document_path):
        svm = StoredValueManager(container)
        document = ''
        with open(document_path,'r') as f:
            document = f.read()
        for k,v in parser(document):
            svm.stored_value_cas(k,v)
        return


    def test_grt_parser(self):
        self.parse_document(self.container, grt_parser, qc_paths['grt'])

        ret_doc = self.svm.read_value('grt_%s_%s' % ('CE04OSBP-LJ01C-06-CTDBPO108', 'CONDWAT'))
        np.testing.assert_almost_equal(ret_doc['grt_min_value'], 0.)
        np.testing.assert_almost_equal(ret_doc['grt_max_value'], 66000.)

    def test_spike_parser(self):
        self.parse_document(self.container, spike_parser, qc_paths['spike'])

        ret_doc = self.svm.read_value('spike_%s_%s' % ('CE04OSBP-LJ01C-06-CTDBPO108', 'PRACSAL'))
        np.testing.assert_almost_equal(ret_doc['acc'], 0.005)
        np.testing.assert_almost_equal(ret_doc['spike_n'], 11.)
        np.testing.assert_almost_equal(ret_doc['spike_l'], 15.)

        self.assertRaises(NotFound, self.svm.read_value,'spike_%s_%s' % ('CE04OSBP-LJ01C-06-CTDBPO108', 'CLOWN'))


    def test_stuck_value_parser(self):
        self.parse_document(self.container, stuck_value_test_parser, qc_paths['stuck'])

        ret_doc = self.svm.read_value('svt_%s_%s' % ('CE04OSBP-LJ01C-06-CTDBPO108', 'PRACSAL'))
        np.testing.assert_almost_equal(ret_doc['svt_resolution'], 1e-9)
        np.testing.assert_almost_equal(ret_doc['svt_n'], 1000000)

    def test_trend_value_parser(self):
        self.parse_document(self.container, trend_parser, qc_paths['trend'])

        ret_doc = self.svm.read_value('trend_%s_%s' % ('CE04OSBP-LJ01C-06-CTDBPO108', 'PRACSAL'))
        np.testing.assert_almost_equal(ret_doc['time_interval'], 365.0)
        np.testing.assert_almost_equal(ret_doc['polynomial_order'], 1.0)
        np.testing.assert_almost_equal(ret_doc['standard_deviation'], 0.0)

    
    def test_lrt_parser(self):
        self.parse_document(self.container, lrt_parser, qc_paths['lrt'])

        ret_doc = self.svm.read_value('lrt_%s_%s' %('GP02HYPM-SP001-04-CTDPF0999', 'PRACSAL'))

        np.testing.assert_array_equal(ret_doc['datlim'][0], np.array([32.289, 32.927]))


