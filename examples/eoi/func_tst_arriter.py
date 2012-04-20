from __future__ import division

from netCDF4 import Dataset
from ion.agents.eoi.utils import ArrayIterator

class TestArriter():
    def __init__(self, block_size=10000):
        self.ds=Dataset("test_data/ncom.nc")
        self._block_size = block_size

    def acquire_data(self, var_name=None, slice_=()):
        if var_name is None:
            vars = self.ds.variables.keys()
        else:
            vars = [var_name]

        if not isinstance(slice_, tuple): slice_ = (slice_,)

        for vn in vars:
            var=self.ds.variables[vn]
            ndims = len(var.shape)

            # Ensure the slice_ is the appropriate length
            if len(slice_) < ndims:
                slice_ += (slice(None),) * (ndims-len(slice_))

            arri = ArrayIterator(var, self._block_size)[slice_]
            for d in arri:
                rng = (d.min(), d.max())
                yield vn, arri.curr_slice, rng, d

        return
