#!/usr/bin/env python

__author__ = 'cmueller'

from pyon.public import log
from interface.objects import DatasetDescriptionDataSamplingEnum, CompareResult, CompareResultEnum, Variable, Attribute, Dimension
from ion.agents.eoi.handler.base_external_data_handler import *
from ion.agents.eoi.utils import ArrayIterator
from netCDF4 import Dataset
import cdms2
import hashlib
import numpy

class DapExternalDataHandler(BaseExternalDataHandler):

#    _ds_url = None
#    _ds = None
#    _cdms_ds = None
#    _tvar = None

    def __init__(self, data_provider=None, data_source=None, ext_dataset=None, *args, **kwargs):
        BaseExternalDataHandler.__init__(self, data_provider, data_source, ext_dataset, *args, **kwargs)

        if self._ext_dataset_res is not None:
            base_url=""
            if self._ext_data_source_res is not None and "base_data_url" in self._ext_data_source_res.connection_params:
                base_url = self._ext_data_source_res.connection_params["base_data_url"]
            self._ds_url = base_url + self._ext_dataset_res.dataset_description.parameters["dataset_path"]
        else:
            raise InstantiationError("Invalid DatasetHandler: ExternalDataset resource cannot be 'None'")

        log.debug("Dataset URL: %s" % self._ds_url)
        self._ds = Dataset(self._ds_url)
        self._load_global_attributes()
        self._load_dimensions()
        self._load_variables()
        self._cdms_ds = None
        self._tvar = None

    def _load_variables(self):
        self._variables = []
        for vn in self._ds.variables.keys():
            var = self._ds.variables[vn]
            newvar = Variable()
            newvar.index_key = vn
            newvar.name = vn
            newvar.attributes = []
            for ak in var.ncattrs():
                att = var.getncattr(ak)
                if ak == 'units':
                    newvar.units = att
                attr = Attribute()
                attr.name = ak
                attr.value = att
                newvar.attributes.append(attr)
            self._variables.append(newvar)

    def _load_dimensions(self):
        self._dimensions = []
        for dk in self._ds.dimensions:
            dim = self._ds.dimensions[dk]
            newdim = Dimension()
            newdim.name = dk
            newdim.size = len(dim)
            newdim.isunlimited = dim.isunlimited()
            self._dimensions.append(newdim)

    def _load_global_attributes(self):
        self._global_attributes = []
        for gk in self._ds.ncattrs():
            gbl = self._ds.getncattr(gk)
            attr = Attribute()
            attr.name = gk
            attr.value = gbl
            self._global_attributes.append(attr)

    def close(self):
        log.debug("Close the local Dataset object")
        self._ds.close()

    def get_attributes(self, scope=None):
        """
        Returns a dictionary containing the name/value pairs for all attributes in the given scope.
        @param scope The name of a variable in this dataset.  If no scope is provided, returns the global_attributes for the dataset
        """
        if (scope is None) or (scope not in self._ds.variables):
            var = self._ds
        else:
            var = self._ds.variables[scope]
        
        return dict((a, getattr(var, a)) for a in var.ncattrs())

    def get_variable_data(self, key=''):
        if key in self._ds.variables:
            return self._ds.variables[key]

    def acquire_data(self, var_name=None, slice_=()):
        if var_name is None:
            vars = self._ds.variables.keys()
        else:
            if not isinstance(var_name, list): var_name = [var_name]
            vars = var_name

        if not isinstance(slice_, tuple): slice_ = (slice_,)

        for vn in vars:
            var = self._ds.variables[vn]

            ndims = len(var.shape)
            # Ensure the slice_ is the appropriate length
            if len(slice_) < ndims:
                slice_ += (slice(None),) * (ndims-len(slice_))

            arri = ArrayIterator(var, self._block_size)[slice_]
            for d in arri:
                if d.dtype.char is "S":
                    # Obviously, we can't get the range of values for a string data type!
                    rng = None
                elif isinstance(d, numpy.ma.masked_array):
                    # TODO: This is a temporary fix because numpy 'nanmin' and 'nanmax'
                    # are currently broken for masked_arrays:
                    # http://mail.scipy.org/pipermail/numpy-discussion/2011-July/057806.html
                    dc=d.compressed()
                    if dc.size == 0:
                        rng = None
                    else:
                        rng = (numpy.nanmin(dc), numpy.nanmax(dc))
                else:
                    rng = (numpy.nanmin(d), numpy.nanmax(d))
                yield vn, arri.curr_slice, rng, d

        return

    def has_new_data(self, **kwargs):
        if 'new_data_check' in self._ext_dataset_res.update_description.parameters:
            old_new_data = self._ext_dataset_res.update_description.parameters['new_data_check']
        else:
            # No data in new_data_check - assume new data (probably initial acquisition)
            return True

        # Get the new array of times from the dataset
        timearr = self.find_time_axis()[:]
        # Find the index in the 'old' array that matches the first value of the 'new' array and trim the 'old' array
        # Avoids the issue of 'windowed' data (old items removed)
        sidx=numpy.where(old_new_data==timearr[0])[0]
        if sidx.size == 0:
            # the first time in the new array wasn't present in the old array - assume new data
            return True

        old_new_data=old_new_data[sidx[0]:]

        log.debug('>>>> old: %s' % old_new_data)
        log.debug('>>>> new: %s' % timearr)

        if numpy.array_equal(timearr, old_new_data):
            return False

        return True

#        if not 'timesteps' in kwargs:
#            return result
#
#        timevar = self.find_time_axis()
#        timesteps = kwargs['timesteps']
#
#        result = not numpy.array_equal(timevar[:], timesteps)
#
#        return result

    def acquire_new_data(self):
        tvar = self.find_time_axis()

        #TODO: use the last state of the data to determine what's new and build an appropriate slice_
        slice_ = (slice(None),)

        return self.acquire_data(slice_=slice_)

    def acquire_data_by_request(self, request=None, **kwargs):
        """
        Returns data based on a request containing the name of a variable (request.name) and a tuple of slice objects (slice_)
        @param request An object (nominally an IonObject of type PydapVarDataRequest) with "name" and "slice" attributes where: "name" == the name of the variable; and "slice" is a tuple ('var_name, (slice_1(), ..., slice_n()))
        """
        name = request.name
        slice_ = request.slice

        if name in self._ds.variables:
            var = self._ds.variables[name]
            # Must turn off auto mask&scale - causes downstream issues if left on (default)
            var.set_auto_maskandscale(False)
            if var.shape:
                data = ArrayIterator(var, self._block_size)[slice_]
            else:
                data = numpy.array(var.getValue())
            typecode = var.dtype.char
            dims = var.dimensions
            attrs = self.get_attributes(scope=name)
        else:
#            print "====> HERE WE ARE"
            #TODO:  Not really sure what the heck this is doing...seems to be making up data??
            size = None
            for var in self._ds.variables:
                var = self._ds.variables[var]
                if name in var.dimensions:
                    size = var.shape[list(var.dimensions).index(name)]
                    break
            if size is None:
                raise DataAcquisitionError("The name '%s' does not appear to be available in the dataset, cannot acquire data" % name)

            data = numpy.arange(size)[slice_]
            typecode = data.dtype.char
            dims, attrs = (name,), {}

        if typecode == 'S1' or typecode == 'S':
            typecode = 'S'
            data = numpy.array([''.join(row) for row in numpy.asarray(data)])
            dims = dims[:-1]

        return name, data, typecode, dims, attrs

    def find_time_axis(self):
        if self._tvar is None:
            tdim = self._ext_dataset_res.dataset_description.parameters["temporal_dimension"]
            if tdim in self._ds.dimensions:
                if tdim in self._ds.variables:
                    self._tvar = self._ds.variables[tdim]
                else:
                    # find the first variable that has the tdim as the outer dimension
                    for vk in self._ds.variables:
                        var = self._ds.variables[vk]
                        if tdim in var.dimensions and var.dimensions.index(tdim) == 0:
                            self._tvar = var
                            break
            else:
                # try to figure out which is the time axis based on standard attributes
                if self._cdms_ds is None:
                    self._cdms_ds = cdms2.openDataset(self._ds_url)

                taxis = self._cdms_ds.getAxis('time')
                if taxis is not None:
                    self._tvar = self._ds.variables[taxis.id]
                else:
                    self._tvar = None

        return self._tvar

    def _pprint_fingerprint(self):
        sig = self.get_fingerprint()
        nl = "\n"
        str_lst = []
        str_lst.append(sig[0])
        str_lst.append(nl)
        str_lst.append("\tdims: ")
        str_lst.append(sig[1]["dims"][0])
        str_lst.append(nl)
        for dk in sig[1]["dims"][1]:
            str_lst.append("\t\t")
            str_lst.append(dk)
            str_lst.append(": ")
            str_lst.append(sig[1]["dims"][1][dk])
            str_lst.append(nl)

        str_lst.append("\tgbl_atts: ")
        str_lst.append(sig[1]["gbl_atts"][0])
        str_lst.append(nl)
        for gk in sig[1]["gbl_atts"][1]:
            str_lst.append("\t\t")
            str_lst.append(gk)
            str_lst.append(": ")
            str_lst.append(sig[1]["gbl_atts"][1][gk])
            str_lst.append(nl)

        str_lst.append("\tvars: ")
        str_lst.append(sig[1]["vars"][0])
        str_lst.append(nl)
        for vk in sig[1]["vars"][1]:
            str_lst.append("\t\t")
            str_lst.append(vk)
            str_lst.append(": ")
            str_lst.append(sig[1]["vars"][1][vk][0])
            str_lst.append(nl)
            for vak in sig[1]["vars"][1][vk][1]:
                str_lst.append("\t\t\t")
                str_lst.append(vak)
                str_lst.append(": ")
                str_lst.append(sig[1]["vars"][1][vk][1][vak])
                str_lst.append(nl)

        return "".join(str_lst)

    def __repr__(self):
#        return "%s\n***\ndataset keys: %s" % (BaseExternalObservatoryHandler.__repr__(self), self._ds.keys())
        return "%s\n***\ndataset:\n%s\ntime_var: %s\ndataset_fingerprint(sha1): %s" % (BaseExternalDataHandler.__repr__(self), self._ds, str(self.find_time_axis()), self._pprint_fingerprint())
