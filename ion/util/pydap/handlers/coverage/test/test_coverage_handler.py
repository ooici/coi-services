from nose.plugins.attrib import attr
from mock import sentinel, patch, Mock, MagicMock
from pyon.util.unit_test import PyonTestCase
from ion.util.pydap.handlers.coverage.coverage_handler import Handler
from coverage_model.coverage import SimplexCoverage
from coverage_model.parameter import Parameter, ParameterContext
from coverage_model.parameter_types import QuantityType

import numpy

@attr('UNIT', group='dm')
class TestPydapCoverageHandlerUnit(PyonTestCase):

    def setUp(self):
        self._handler = Handler(filepath=sentinel.filepath)

    @patch('ion.util.pydap.handlers.coverage.coverage_handler.numpy')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.os')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.SimplexCoverage.load', spec=SimplexCoverage)
    def test_parse_constraints(self, SimplexCoverage_mock, os_mock, numpy_mock):
        os_mock.path.split.return_value = ['file_path', 'file_name']
        os_mock.stat.return_value = (33188, 3379594, 234881026L, 1, 501, 20, 92023, 1352928035, 1343653166, 1343653167)

        numpy_mock.dtype.return_value = Mock(spec=numpy.dtype(numpy.float32))
        numpy_mock.where.return_value = [[-1]]

        environ = {'pydap.headers': [], 'pydap.ce': (None, [])}

        cov = Mock(spec=SimplexCoverage)
        cov.name = 'coverage name'
        cov.temporal_parameter_name = 'var_1'

        cov.list_parameters.return_value = ['var_1', 'var_2']

        param1 = Mock(spec=Parameter)
        param1.is_coordinate = True
        param2 = Mock(spec=Parameter)
        param2.is_coordinate = False
        cov.get_parameter.side_effect = [param1, param2]

        con = Mock(spec=ParameterContext)
        con.param_type = Mock(spec=QuantityType)
        con.param_type.value_encoding = '<f4'
        con.fill_value = -999
        con.uom = 'unknown'
        cov.get_parameter_context.return_value = con

        values1 = numpy.arange(100, dtype=numpy.float32)
        values2 = numpy.arange(100, 200, dtype=numpy.float32)
        cov.get_parameter_values.side_effect = [values1, values1, values2, values1]

        SimplexCoverage_mock.return_value = cov

        self._handler.parse_constraints(environ=environ)

        SimplexCoverage_mock.assert_called_once_with('file_path', 'file_name', mode='r')
        self.assertEqual(cov.list_parameters.call_count, 1)
        self.assertEqual(cov.get_parameter.call_count, 2)
        self.assertEqual(cov.get_parameter_values.call_count, 4)
        self.assertEqual(cov.get_parameter_context.call_count, 6)

    @patch('ion.util.pydap.handlers.coverage.coverage_handler.numpy')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.os')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.SimplexCoverage.load', spec=SimplexCoverage)
    def test_parse_constraints_with_where_error(self, SimplexCoverage_mock, os_mock, numpy_mock):
        os_mock.path.split.return_value = ['file_path', 'file_name']
        os_mock.stat.return_value = (33188, 3379594, 234881026L, 1, 501, 20, 92023, 1352928035, 1343653166, 1343653167)

        numpy_mock.dtype.return_value = Mock(spec=numpy.dtype(numpy.float32))
        numpy_mock.where.side_effect = IndexError

        environ = {'pydap.headers': [], 'pydap.ce': (None, [])}

        cov = Mock(spec=SimplexCoverage)
        cov.name = 'coverage name'
        cov.temporal_parameter_name = 'var_1'

        cov.list_parameters.return_value = ['var_1', 'var_2']

        param1 = Mock(spec=Parameter)
        param1.is_coordinate = True
        param2 = Mock(spec=Parameter)
        param2.is_coordinate = False
        cov.get_parameter.side_effect = [param1, param2]

        con = Mock(spec=ParameterContext)
        con.param_type = Mock(spec=QuantityType)
        con.param_type.value_encoding = '<f4'
        con.fill_value = -999
        con.uom = 'unknown'
        cov.get_parameter_context.return_value = con

        values1 = numpy.arange(100, dtype=numpy.float32)
        values2 = numpy.arange(100, 200, dtype=numpy.float32)
        cov.get_parameter_values.side_effect = [values1, values1, values2, values1]

        SimplexCoverage_mock.return_value = cov

        self._handler.parse_constraints(environ=environ)

        SimplexCoverage_mock.assert_called_once_with('file_path', 'file_name', mode='r')
        self.assertEqual(cov.list_parameters.call_count, 1)
        self.assertEqual(cov.get_parameter.call_count, 2)
        self.assertEqual(cov.get_parameter_values.call_count, 4)
        self.assertEqual(cov.get_parameter_context.call_count, 6)

    @patch('ion.util.pydap.handlers.coverage.coverage_handler.numpy')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.os')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.SimplexCoverage.load', spec=SimplexCoverage)
    def test_parse_constraints_with_object(self, SimplexCoverage_mock, os_mock, numpy_mock):
        os_mock.path.split.return_value = ['file_path', 'file_name']
        os_mock.stat.return_value = (33188, 3379594, 234881026L, 1, 501, 20, 92023, 1352928035, 1343653166, 1343653167)

        np_type1 = Mock(spec=numpy.dtype(numpy.float32))
        np_type1.char = 'f'
        np_type2 = Mock(spec=numpy.dtype(numpy.object))
        np_type2.char = 'O'
        numpy_mock.dtype.side_effect = [np_type1, np_type2]
        numpy_mock.where.return_value = [[-1]]

        environ = {'pydap.headers': [], 'pydap.ce': (None, [])}

        cov = Mock(spec=SimplexCoverage)
        cov.name = 'coverage name'
        cov.temporal_parameter_name = 'var_1'

        cov.list_parameters.return_value = ['var_1', 'var_2']

        param1 = Mock(spec=Parameter)
        param1.is_coordinate = True
        param2 = Mock(spec=Parameter)
        param2.is_coordinate = False
        cov.get_parameter.side_effect = [param1, param2]

        con = Mock(spec=ParameterContext)
        con.param_type = Mock(spec=QuantityType)
        con.param_type.value_encoding = '<f4'
        con.fill_value = -999
        con.uom = 'unknown'
        con2 = Mock(spec=ParameterContext)
        con2.param_type = Mock(spec=QuantityType)
        con2.param_type.value_encoding = '|O8'
        con2.fill_value = -999
        con2.uom = 'unknown'
        cov.get_parameter_context.side_effect = [con, con2, con, con, con, con]

        values1 = numpy.arange(100, dtype=numpy.float32)
        values2 = numpy.arange(100, 200, dtype=numpy.float32)
        cov.get_parameter_values.side_effect = [values1, values1, values2, values1]

        SimplexCoverage_mock.return_value = cov

        self._handler.parse_constraints(environ=environ)

        SimplexCoverage_mock.assert_called_once_with('file_path', 'file_name', mode='r')
        self.assertEqual(cov.list_parameters.call_count, 1)
        self.assertEqual(cov.get_parameter.call_count, 2)
        self.assertEqual(cov.get_parameter_values.call_count, 4)
        self.assertEqual(cov.get_parameter_context.call_count, 6)

    @patch('ion.util.pydap.handlers.coverage.coverage_handler.log')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.get_var')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.numpy')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.os')
    @patch('ion.util.pydap.handlers.coverage.coverage_handler.SimplexCoverage.load', spec=SimplexCoverage)
    def test_parse_constraints_with_get_var_error(self, SimplexCoverage_mock, os_mock, numpy_mock, get_var_mock, log_mock):
        os_mock.path.split.return_value = ['file_path', 'file_name']
        os_mock.stat.return_value = (33188, 3379594, 234881026L, 1, 501, 20, 92023, 1352928035, 1343653166, 1343653167)

        numpy_mock.dtype.return_value = Mock(spec=numpy.dtype(numpy.float32))
        numpy_mock.where.return_value = [[-1]]

        environ = {'pydap.headers': [], 'pydap.ce': (None, [])}

        cov = Mock(spec=SimplexCoverage)
        cov.name = 'coverage name'
        cov.temporal_parameter_name = 'var_1'

        cov.list_parameters.return_value = ['var_1', 'var_2']

        param1 = Mock(spec=Parameter)
        param1.is_coordinate = True
        param2 = Mock(spec=Parameter)
        param2.is_coordinate = False
        cov.get_parameter.side_effect = [param1, param2]

        con = Mock(spec=ParameterContext)
        con.param_type = Mock(spec=QuantityType)
        con.param_type.value_encoding = '<f4'
        con.fill_value = -999
        con.uom = 'unknown'
        cov.get_parameter_context.return_value = con

        values1 = numpy.arange(100, dtype=numpy.float32)
        values2 = numpy.arange(100, 200, dtype=numpy.float32)
        cov.get_parameter_values.side_effect = [values1, values1, values2, values1]

        SimplexCoverage_mock.return_value = cov

        get_var_mock.side_effect = Exception()

        self.assertRaises(Exception, self._handler.parse_constraints(environ=environ))
        self.assertEqual(cov.get_parameter_values.call_count, 1)
        self.assertEqual(cov.get_parameter.call_count, 2)
        self.assertEqual(cov.get_parameter_context.call_count, 3)
        self.assertEqual(log_mock.exception.call_count, 2)
