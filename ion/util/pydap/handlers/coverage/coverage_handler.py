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
        #atts['infoUrl'] = self.filepath
        #atts['summary'] = coverage.name
        #atts['institution'] = ''
        dataset = DatasetType(coverage.name) #, attributes=atts)
        fields, queries = environ['pydap.ce']
        queries = filter(bool, queries)  # fix for older version of pydap


        all_vars = coverage.list_parameters()
        t = []

        for param in all_vars:
            if numpy.dtype(coverage.get_parameter_context(param).param_type.value_encoding).char == 'O':
                t.append(param)
        [all_vars.remove(i) for i in t]


#        slices = [f[0][1] for f in fields if f[0][0] == sequence_type.name]
#        if slices:
#            slice_ = slices[0]
#        else:
#            slice_ = None
#            # Check that all slices are equal.
#        if [s for s in slices if s != slice_]:
#            raise ConstraintExpressionError('Slices are not unique!')

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
                if len(param.shape) == 2:
                    dims = (coverage.temporal_parameter_name, coverage.spatial_domain.shape.name)
                if param.is_coordinate or target is not dataset:
                    data = coverage.get_parameter_values(covname, tdoa=slice_)
                    atts = {'units': coverage.get_parameter_context(covname).uom }
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
                        data = coverage.get_parameter_values(name, tdoa=slice_[0])
                    elif len(slice_) == 2:
                        data = coverage.get_parameter_values(name, tdoa=slice_[0], sdoa=slice_[1])
                    atts = {'units': coverage.get_parameter_context(name).uom }
                    type_ = numpy.dtype(param.context.param_type.value_encoding).char
                    grid[name] = BaseType(name=name, data=data, shape=data.shape,
                        type=type_, dimensions=dims, attributes=atts)
                    for dim in dims:
                        atts = {'units': coverage.get_parameter_context(dim).uom }
                        data = numpy.arange(data.shape[dims.index(dim)])
                        grid[dim] = BaseType(name=dim, data=data, shape=data.shape,
                            type=data.dtype.char, attributes=atts)
                    #slice_ = list(slice_) + [slice(None)] * (len(grid.array.shape) - len(slice_))

        dataset._set_id()
        dataset.close = coverage.close
        return dataset

if __name__ == '__main__':
    import sys
    from paste.httpserver import serve

    application = Handler(sys.argv[1])
    serve(application, port=8001)
