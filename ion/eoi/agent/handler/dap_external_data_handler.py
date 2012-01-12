#!/usr/bin/env python

__author__ = 'cmueller'

from pyon.public import log
from ion.eoi.agent.handler.base_external_data_handler import BaseExternalDataHandler
from netCDF4 import Dataset, date2index
from datetime import datetime, timedelta
import cdms2
import hashlib
import pydap
from pydap.client import open_url
from pyon.util.log import log


class DapExternalDataHandler(BaseExternalDataHandler):

    SIGNATURE_DATA_SHOTGUN = "SHOTGUN"
    SIGNATURE_DATA_SHOTGUN_COUNT = 5
    SIGNATURE_DATA_FIRST_LAST = "FIRST_LAST"
    SIGNATURE_DATA_FULL = "FULL"

    ds_url = None
    ds = None
    cdms_ds = None
    tvar = None
    signature = None

    def __init__(self, data_provider=None, data_source=None, ext_dataset=None, dataset_desc=None, update_desc=None, *args, **kwargs):
        BaseExternalDataHandler.__init__(self, data_provider, data_source, ext_dataset, dataset_desc, update_desc, *args, **kwargs)

#        # Pop the "DS_DESC"
#        if "DS_DESC" in kwargs:
#            self._dap_ds_desc = kwargs.pop("DS_DESC")
#            print self._dap_ds_desc
#
#        if "UPDATE_DESC" in kwargs:
#            self._update_desc = kwargs.pop("UPDATE_DESC")
#
#
#        self.base_initialize(data_provider, data_source, ext_dataset, *args, **kwargs)

        if self._ext_data_source_res is not None and self._dataset_desc_obj is not None:
            self.ds_url = self._ext_data_source_res.base_data_url + self._dataset_desc_obj.dataset_path
        else:
            raise Exception("Cannot construct dataset url")

        self.ds = Dataset(self.ds_url)
#       self.ds = open_url(self.ds_url)

    def acquire_new_data(self, request=None, **kwargs):

        td=timedelta(hours=-1)
        edt=datetime.utcnow()
        sdt=edt+td

        if request is not None:
            if "start_time" in request:
                sdt = request.start_time
            if "end_time" in request:
                edt = request.end_time

        tvar = self.ds.variables[self._dataset_desc_obj.temporal_dimension]
        tindices = date2index([sdt, edt], tvar, 'standard', 'nearest')
        if tindices[0] == tindices[1]:
            # add one to the end index to ensure we get everything
            tindices[1] = tindices[1] + 1

        print ">>> tindices [start end]: %s" % tindices

        dims=self.ds.dimensions.items()

        ret={}
#        ret["times"] = tvar[sti:eti]
        for vk in self.ds.variables:
            print "***"
            print vk
            var = self.ds.variables[vk]
            t_idx = -1
            if self._dataset_desc_obj.temporal_dimension in var.dimensions:
                t_idx = var.dimensions.index(self._dataset_desc_obj.temporal_dimension)

            lst=[]
            for i in range(len(var.dimensions)):
                if i == t_idx:
                    lst.append(slice(tindices[0], tindices[1]))
                else:
                    lst.append(slice(0, len(dims[i][1])))
            print lst
            print "==="
            tpl = tuple(lst)
            ret[vk] = tpl, var[tpl]
            print ret[vk]
            print "***"
            
#                if idx == 0:
#                    ret[vk] = var[sti:eti]
#                elif idx == 1:
#                    ret[vk] = var[:, sti:eti]
#                elif idx == 2:
#                    ret[vk] = var[:, :, sti:eti]
#                else:
#                    ret[vk] = "Temporal index > 2, WTF"
#            else:
#                ret[vk] = "Non-temporal Dimension ==> ignore for now"
#                continue
        


        return ret

    def find_time_axis(self):
        if self.tvar is None:
            if self.cdms_ds is None:
                self.cdms_ds = cdms2.openDataset(self.ds_url)

            taxis = self.cdms_ds.getAxis('time')
            if taxis is not None:
                self.tvar = self.ds.variables[taxis.id]
            else:
                self.tvar = None

        return self.tvar

    def get_signature(self, recalculate=False):
        """
        Calculate the signature of the dataset
        """
        if recalculate:
            self.signature = None
            
        if self.signature is not None:
            return self.signature
        
        ret={}
        # sha for time values
#        tvar=self.find_time_axis()
#        if tvar is not None:
#            sha_time=hashlib.sha1()
#            sha_time.update(str(tvar[:]))
#            ret['times']=sha_time.hexdigest()
#        else:
#            ret['times']=None

        # sha for variables
        var_map={}
        for vk in self.ds.variables:
        #        sha_vars.update(str(self.ds))
            var = self.ds.variables[vk]
            #            sha_vars.update(str(var)) # includes the "current size"
            var_sha = hashlib.sha1()
            var_atts = {}
            for ak in var.ncattrs():
                att = var.getncattr(ak)
                var_atts[ak] = hashlib.sha1(str(att)).hexdigest()

            for key in var_atts:
                var_sha.update(var_atts[key])

            var_map[vk] = var_sha.hexdigest(), var_atts

        sha_vars=hashlib.sha1()
        for key in var_map:
            sha_vars.update(var_map[key][0])

        ret["vars"] = sha_vars.hexdigest(), var_map

        # sha for dimensions
        dim_map = {}
        for dk in self.ds.dimensions:
            dim = self.ds.dimensions[dk]
            dim_map[dk] = hashlib.sha1(str(dim)).hexdigest()

        sha_dim = hashlib.sha1()
        for key in dim_map:
            sha_dim.update(dim_map[key])

        ret["dims"] = sha_dim.hexdigest(), dim_map

        # sha for globals
        gbl_map = {}
        for gk in self.ds.ncattrs():
            gbl = self.ds.getncattr(gk)
            gbl_map[gk] = hashlib.sha1(str(gbl)).hexdigest()

        sha_gbl = hashlib.sha1()
        for key in gbl_map:
            sha_gbl.update(gbl_map[key])

        ret["gbl_atts"] = sha_gbl.hexdigest(), gbl_map

        sha_full = hashlib.sha1()
        for key in ret:
            if ret[key] is not None:
                sha_full.update(ret[key][0])

        self.signature = sha_full.hexdigest(), ret
        return self.signature

    def compare(self, dsh):
        assert isinstance(dsh, DapExternalDataHandler)
        
        my_sig = self.get_signature(recalculate=True)
        sig2 = dsh.get_signature(recalculate=True)

        if my_sig[0] != sig2[0]:
            #TODO: make info
            print "=!> Full signatures differ"
            if my_sig[1]["dims"][0] != sig2[1]["dims"][0]:
                #TODO: make info
                print "==!> Dimensions differ"
                for dk in my_sig[1]["dims"][1]:
                    v1 = my_sig[1]["dims"][1][dk]
                    if dk in sig2[1]["dims"][1]:
                        v2 = sig2[1]["dims"][1][dk]
                    else:
                        #TODO: make info
                        print "===!> Dimension '%s' does not exist in 2nd dataset" % dk
                        continue

                    if v1 != v2:
                        #TODO: make info
                        print "===!> Dimension '%s' differs" % dk
                    else:
                        #TODO: make debug
#                        print "====> Dimension '%s' is equal" % dk
                        continue
            else:
                #TODO: make info
                print "===> Dimensions are equal"

            if my_sig[1]["gbl_atts"][0] != sig2[1]["gbl_atts"][0]:
                #TODO: make info
                print "==!> Global Attributes differ"
                for gk in my_sig[1]["gbl_atts"][1]:
                    v1 = my_sig[1]["gbl_atts"][1][gk]
                    if gk in sig2[1]["gbl_atts"][1]:
                        v2 = sig2[1]["gbl_atts"][1][gk]
                    else:
                        #TODO: make info
                        print "===!> Global Attribute '%s' does not exist in 2nd dataset" % gk
                        continue

                    if v1 != v2:
                        #TODO: make info
                        print "===!> Global Attribute '%s' differs" % gk
                    else:
                        #TODO: make debug
#                        print "====> Global Attribute '%s' is equal" % gk
                        continue
                        
            else:
                #TODO: make info
                print "===> Global Attributes are equal"

            if my_sig[1]["vars"][0] != sig2[1]["vars"][0]:
                #TODO: make info
                print "==!> Variable attributes differ"
                for vk in my_sig[1]["vars"][1]:
                    v1 = my_sig[1]["vars"][1][vk][0]
                    if vk in sig2[1]["vars"][1]:
                        v2 = sig2[1]["vars"][1][vk][0]
                    else:
                        #TODO: make info
                        print "===!> Variable '%s' does not exist in 2nd dataset" % vk
                        continue

                    if v1 != v2:
                        #TODO: make info
                        print "===!> Variable '%s' differ" % vk
                        for vak in my_sig[1]["vars"][1][vk][1]:
                            va1 = my_sig[1]["vars"][1][vk][1][vak]
                            if vak in sig2[1]["vars"][1][vk][1]:
                                va2 = sig2[1]["vars"][1][vk][1][vak]
                            else:
                                #TODO: make info
                                print "====!> Variable Attribute '%s' does not exist in 2nd dataset" % vak
                                continue

                            if va1 != va2:
                                #TODO: make info
                                print "====!> Variable Attribute '%s' differs" % vak
                            else:
                                #TODO: make debug
#                                print "======> Variable Attribute '%s' is equal" % vak
                                continue
            else:
                #TODO: make info
                print "===> Variable attributes are equal"

        else:
            #TODO: make debug
            print "==> Datasets are equal"

#    def walk_dataset(self, top):
#        values = top.group.values()
#        yield values
#        for value in values:
#            for children in walk_dataset(value):
#                yield children
#
#    def print_dataset_tree(self):
#        if self.ds is None:
#            return "Dataset is None"
#
#        print self.ds.path, self.ds
#        for children in walk_dataset(self.ds):
#            for child in children:
#                print child.path, child


#    def get_dataset_size(self):
#        return self.ds
#        return reduce(lambda x, y: x * self.calculate_data_size(y), self.ds.keys())

#    def calculate_data_size(self, variable=None):
#        if variable is None and self.ds is not None:
#            variable = self.ds[self.ds.keys()[0]]
#
#        if type(variable) == pydap.model.GridType:
#            return reduce(lambda x, y: x * y, variable.shape) * variable.type.size
#        else:
#            log.warn("variable is of unhandled type: %s" % type(variable))

    def _pprint_signature(self):
        sig = self.get_signature()
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
#        return "%s\n***\ndataset keys: %s" % (BaseExternalObservatoryHandler.__repr__(self), self.ds.keys())
        return "%s\n***\ndataset:\n%s\ntime_var: %s\ndataset_signature(sha1): %s" % (BaseExternalDataHandler.__repr__(self), self.ds, str(self.find_time_axis()), self._pprint_signature())