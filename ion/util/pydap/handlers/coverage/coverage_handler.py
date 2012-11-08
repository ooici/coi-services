import re
import os
import numpy
import urllib

from coverage_model.coverage import SimplexCoverage
from pydap.model import DatasetType,BaseType, GridType, StructureType
from pydap.handlers.lib import BaseHandler

class Handler(BaseHandler):

    #extensions = re.compile(r"^.*\.cov", re.IGNORECASE)
    extensions = re.compile(r'^.*[0-9A-Za-z]{32}', re.IGNORECASE)

    def __init__(self, filepath):
        self.filepath = filepath

    def parse_constraints(self, environ):
        base = os.path.split(self.filepath)
        coverage = SimplexCoverage.load(base[0], base[1])

        atts = {}
        atts['title'] = coverage.name
        dataset = DatasetType(coverage.name) #, attributes=atts)
        fields, queries = environ['pydap.ce']
        queries = filter(bool, queries)  # fix for older version of pydap

        all_vars = coverage.list_parameters()
        t = []

        for param in all_vars:
            if numpy.dtype(coverage.get_parameter_context(param).param_type.value_encoding).char == 'O':
                t.append(param)
        [all_vars.remove(i) for i in t]

        time_context = coverage.get_parameter_context(coverage.temporal_parameter_name)
        time_fill_value = time_context.fill_value
        time_data = coverage.get_parameter_values(coverage.temporal_parameter_name)
        fill_index = -1
        try:
            fill_index = numpy.where(time_data == time_fill_value)[0][0]
        except IndexError:
            pass

        # If no fields have been explicitly requested, of if the sequence
        # has been requested directly, return all variables.

        if not fields:
            fields = [[(name, ())] for name in all_vars]

        for var in fields:
            target = dataset
            while var:
                name, slice_ = var.pop(0)
                covname = urllib.unquote(name)
                param = coverage.get_parameter(covname)
                dims = (coverage.temporal_parameter_name,)

                #need to truncate slice here in case time has fill values
                if len(slice_) == 0 and fill_index >= 0:
                    slice_ = slice(0, fill_index, 1)

                if len(slice_) == 1:
                    slice_ = slice_[0]

                    if fill_index > slice_.start:
                        continue

                    if fill_index > slice_.stop:
                        slice_.stop = fill_index

                if param.is_coordinate or target is not dataset:
                    data = coverage.get_parameter_values(covname, tdoa=slice_)
                    atts = {'units': coverage.get_parameter_context(covname).uom}
                    target[name] = BaseType(name=name, data=data, shape=data.shape,
                    type=numpy.dtype(param.context.param_type.value_encoding).char, dimensions=(covname,),
                    attributes=atts)
                elif var:
                    target.setdefault(name, StructureType(name=name))
                    target = target[name]
                else:  # return grid
                    grid = target[name] = GridType(name=name)
                    data = None
                    if len(slice_) == 0:
                        data = coverage.get_parameter_values(name)
                    elif len(slice_) == 1:
                        data = coverage.get_parameter_values(name, tdoa=slice_)

                    atts = {'units': coverage.get_parameter_context(name).uom }
                    type_ = numpy.dtype(param.context.param_type.value_encoding).char
                    grid[name] = BaseType(name=name, data=data, shape=data.shape,
                        type=type_, dimensions=dims, attributes=atts)
                    for dim in dims:
                        atts = {'units': coverage.get_parameter_context(dim).uom }
                        data = numpy.arange(data.shape[dims.index(dim)])
                        grid[dim] = BaseType(name=dim, data=data, shape=data.shape,
                            type=data.dtype.char, attributes=atts)

        dataset._set_id()
        dataset.close = coverage.close
        return dataset

if __name__ == '__main__':
    import sys
    from paste.httpserver import serve

    application = Handler(sys.argv[1])
    serve(application, port=8001)
