from nose.plugins.attrib import attr
from mock import sentinel, patch, Mock, MagicMock
from pyon.util.unit_test import PyonTestCase
from ion.util.pydap.handlers.coverage.coverage_handler import Handler
from coverage_model.coverage import SimplexCoverage

@attr('UNIT', group='dm')
class TestPydapCoverageHandlerUnit(PyonTestCase):

    def setUp(self):
        self._handler = Handler(filepath=sentinel.filepath)

    @patch('ion.util.pydap.handlers.coverage.coverage_handler.os')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.SimplexCoverage.load', spec=SimplexCoverage)
    def test_parse_constraints(self, SimplexCoverage_mock, os_mock):
        os_mock.path.split.return_value = ['file_path', 'file_name']
        os_mock.stat.return_value = (33188, 3379594, 234881026L, 1, 501, 20, 92023, 1352928035, 1343653166, 1343653167)

        environ = {'pydap.headers': [], 'pydap.ce': (None, [])}

        cov = Mock(spec=SimplexCoverage)
        cov.name = 'coverage name'
        cov.list_parameters().return_value = ['var_1', 'var_2']
        SimplexCoverage_mock.return_value = cov
        #SimplexCoverage_mock.load.list_parameters.return_value = ['var_1', 'var_2']
        #SimplexCoverage_mock.get_parameter_context.return_value = Mock()

        self._handler.parse_constraints(environ=environ)
        #SimplexCoverage_mock.load.assert_called_once_with('file_path', 'file_name', mode='r')



