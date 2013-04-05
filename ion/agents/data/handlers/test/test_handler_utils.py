from mock import MagicMock, patch
from nose.plugins.attrib import attr
from ion.agents.data.handlers.handler_utils import _get_type, list_file_info, \
    list_file_info_http, list_file_info_ftp, list_file_info_fs, \
    get_time_from_filename, calculate_iteration_count, get_sbuffer
from pyon.util.unit_test import PyonTestCase

import requests
from ftplib import FTP
from StringIO import StringIO


@attr('UNIT', group='eoi')
class TestHandlerUtils(PyonTestCase):

    def test__get_type_http(self):
        self.assertEqual(_get_type('http://'), 'http')

    def test__get_type_ftp(self):
        self.assertEqual(_get_type('ftp://'), 'ftp')

    def test__get_type_fs(self):
        self.assertEqual(_get_type(''), 'fs')

    @patch('ion.agents.data.handlers.handler_utils._get_type')
    @patch('ion.agents.data.handlers.handler_utils.list_file_info_http')
    def test_list_file_info_by_http(self, list_file_info_http_mock, _get_type_mock):
        _get_type_mock.return_value = 'http'
        list_file_info_http_mock.return_value = ['file1', 'file2']
        self.assertEqual(list_file_info('http', 'pattern'), ['file1', 'file2'])

    @patch('ion.agents.data.handlers.handler_utils._get_type')
    @patch('ion.agents.data.handlers.handler_utils.list_file_info_ftp')
    def test_list_file_info_by_ftp(self, list_file_info_ftp_mock, _get_type_mock):
        _get_type_mock.return_value = 'ftp'
        list_file_info_ftp_mock.return_value = ['file1', 'file2']
        self.assertEqual(list_file_info('ftp', 'pattern'), ['file1', 'file2'])

    @patch('ion.agents.data.handlers.handler_utils._get_type')
    @patch('ion.agents.data.handlers.handler_utils.list_file_info_fs')
    def test_list_file_info_by_fs(self, list_file_info_fs_mock, _get_type_mock):
        _get_type_mock.return_value = 'fs'
        list_file_info_fs_mock.return_value = ['file1', 'file2']
        self.assertEqual(list_file_info('fs', 'pattern'), ['file1', 'file2'])

    @patch('ion.agents.data.handlers.handler_utils.re.findall')
    @patch('ion.agents.data.handlers.handler_utils.requests.get')
    def test_list_file_info_http(self, requests_mock, re_mock):
        retval = MagicMock(spec=requests.models.Response)
        retval.url = 'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/'
        retval.content = '<http><body>' \
                         '<a href="RDLm_BELM_2012_08_14_1200.ruv">RDLm_BELM_2012_08_14_1200.ruv</a>     ' \
                         '14-Aug-2012 08:42   88K  \n<img src="/icons/unknown.gif" alt="[   ]"> ' \
                         '<a href="RDLm_BELM_2012_08_14_1300.ruv">RDLm_BELM_2012_08_14_1300.ruv</a>     ' \
                         '14-Aug-2012 09:41   90K  \n</body></html>'
        requests_mock.return_value = retval
        re_mock.return_value = ['RDLm_BELM_2012_08_14_1200.ruv', 'RDLm_BELM_2012_08_14_1300.ruv']
        lst = [('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLm_BELM_2012_08_14_1200.ruv',),
               ('http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/RDLm_BELM_2012_08_14_1300.ruv',)]
        self.assertEqual(list_file_info_http(base='http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/', pattern='*.ruv'), lst)

    @patch('ion.agents.data.handlers.handler_utils.FTP')
    def test_list_file_info_ftp(self, ftp_mock):
        retval = MagicMock(spec=FTP)
        retval.nlst.return_value = ['RDLm_BELM_2012_08_14_1200.ruv', 'RDLm_BELM_2012_08_14_1300.ruv']
        ftp_mock.return_value = retval
        lst = ['RDLm_BELM_2012_08_14_1200.ruv',
               'RDLm_BELM_2012_08_14_1300.ruv']

        self.assertEqual(list_file_info_ftp(base='ftp://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/', pattern='*.ruv'), lst)

    @patch('ion.agents.data.handlers.handler_utils.glob.glob')
    @patch('ion.agents.data.handlers.handler_utils.os.path.getmtime')
    @patch('ion.agents.data.handlers.handler_utils.os.path.getsize')
    @patch('ion.agents.data.handlers.handler_utils.os.path.isdir')
    @patch('ion.agents.data.handlers.handler_utils.os.path.exists')
    def test_list_file_info_fs(self, exists_mock, isdir_mock, getsize_mock, getmtime_mock, glob_mock):
        exists_mock.return_value = True
        isdir_mock.return_value = True
        getsize_mock.return_value = 100
        getmtime_mock.return_value = 1313712000
        lst1 = ['RDLm_BELM_2012_08_14_1200.ruv',
                'RDLm_BELM_2012_08_14_1300.ruv']
        glob_mock.return_value = lst1

        lst2 = [('RDLm_BELM_2012_08_14_1200.ruv', 1313712000, 100, 0),
                ('RDLm_BELM_2012_08_14_1300.ruv', 1313712000, 100, 0)]

        self.assertEqual(list_file_info_fs(base='test_data/ruv', pattern='*.ruv'), lst2)

    @patch('ion.agents.data.handlers.handler_utils.os.path.exists')
    def test_list_file_info_fs_exists_false(self, exists_mock):
        exists_mock.return_value = False
        with self.assertRaises(StandardError) as cm:
            list_file_info_fs(base='test_data/ruv', pattern='*.ruv')

        ex = cm.exception
        self.assertEqual(ex.message, 'base \'test_data/ruv\' does not exist')

    @patch('ion.agents.data.handlers.handler_utils.os.path.isdir')
    @patch('ion.agents.data.handlers.handler_utils.os.path.exists')
    def test_list_file_info_fs_isdir_false(self, exists_mock, isdir_mock):
        exists_mock.return_value = True
        isdir_mock.return_value = False
        with self.assertRaises(StandardError) as cm:
            list_file_info_fs(base='test_data/ruv', pattern='*.ruv')

        ex = cm.exception
        self.assertEqual(ex.message, 'base \'test_data/ruv\' is not a directory')

    @patch('ion.agents.data.handlers.handler_utils.time.mktime')
    @patch('ion.agents.data.handlers.handler_utils.re.match')
    @patch('ion.agents.data.handlers.handler_utils.os.path.basename')
    def test_get_time_from_filename(self, basename_mock, re_mock, mktime_mock):
        basename_mock.return_value = 'test_data/ruv'
        retval = MagicMock()
        retval.groups.return_value = ('2012', '06', '06', '12', '00')
        re_mock.return_value = retval

        mktime_mock.return_value = 1338998400.0
        self.assertEqual(get_time_from_filename(file_name='test_data/ruv/RDLm_SEAB_2012_06_06_1200.ruv',
                         date_extraction_pattern='RDLm_SEAB_([\d]{4})_([\d]{2})_([\d]{2})_([\d]{2})([\d]{2}).ruv',
                         date_pattern='%Y %m %d %H %M'), 1338998400.0)

    def test_calculate_iteration_count(self):
        total_recs = 100
        max_rec = 10
        self.assertEqual(calculate_iteration_count(total_recs=total_recs, max_rec=max_rec), 10)

    def test_calculate_iteration_count_not_even(self):
        total_recs = 101
        max_rec = 10
        self.assertEqual(calculate_iteration_count(total_recs=total_recs, max_rec=max_rec), 11)

    @patch('ion.agents.data.handlers.handler_utils.StringIO')
    @patch('ion.agents.data.handlers.handler_utils._get_type')
    @patch('ion.agents.data.handlers.handler_utils.requests.get')
    def test_get_sbuffer_http(self, requests_mock, get_type_mock, StringIO_mock):
        retval = MagicMock(spec=requests.models.Response)
        retval.url = 'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/'
        retval.content = '<http><body>'\
                         '<a href="RDLm_BELM_2012_08_14_1200.ruv">RDLm_BELM_2012_08_14_1200.ruv</a>     '\
                         '14-Aug-2012 08:42   88K  \n<img src="/icons/unknown.gif" alt="[   ]"> '\
                         '<a href="RDLm_BELM_2012_08_14_1300.ruv">RDLm_BELM_2012_08_14_1300.ruv</a>     '\
                         '14-Aug-2012 09:41   90K  \n</body></html>'
        requests_mock.return_value = retval

        get_type_mock.return_value = 'http'

        StringIO_mock.return_value = MagicMock(spec=StringIO)

        self.assertTrue(isinstance(get_sbuffer(url=retval.url), StringIO))

    def test_get_sbuffer_ftp(self):
        with self.assertRaises(NotImplementedError):
            get_sbuffer(url='http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/BELM/', type='ftp')
