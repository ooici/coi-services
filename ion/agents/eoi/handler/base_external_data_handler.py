#!/usr/bin/env python

__author__ = 'cmueller'

from ion.agents.eoi.handler.iface.iexternal_data_handler_controller import IExternalDataHandlerController
from zope.interface import implements
from interface.objects import CompareResult, CompareResultEnum, DatasetDescriptionDataSamplingEnum
import hashlib

# Observatory Status Types
OBSERVATORY_ONLINE = 'ONLINE'
OBSERVATORY_OFFLINE = 'OFFLINE'

# Protocol Types
PROTOCOL_TYPE_DAP = "DAP"

class BaseExternalDataHandler():
    """ Base implementation of the External Observatory Handler"""
    implements(IExternalDataHandlerController)

    _ext_provider_res = None
    _ext_data_source_res = None
    _ext_dataset_res = None
    _fingerprint = None
    _block_size = 10000

    def __init__(self, data_provider=None, data_source=None, ext_dset=None, *args, **kwargs):

        self._ext_provider_res = data_provider
        self._ext_data_source_res = data_source
        self._ext_dataset_res = ext_dset

        if "BLOCK_SIZE" in kwargs:
            self._block_size = int(kwargs.pop("BLOCK_SIZE"))

        self._variables = None
        self._global_attributes = None
        self._dimensions = None
    # Generic, utility and helper methods

    def get_fingerprint(self, recalculate=False, **kwargs):
        """
        Calculate the _fingerprint of the dataset
        """
        #        if self._dataset_desc_obj.data_sampling is None or self._dataset_desc_obj.data_sampling == "":
        #            data_sampling = BaseExternalDataHandler.DATA_SAMPLING_NONE
        #        else:
        #            data_sampling = self._dataset_desc_obj.data_sampling

        data_sampling = self._ext_dataset_res.dataset_description.data_sampling

        if recalculate:
            self._fingerprint = None

        if self._fingerprint is not None:
            return self._fingerprint

        ret = {}

        if not self._variables is None:
            var_map = {}
            for vk in self._variables:
                #var = self._data_array[vk.column_name]
                #print vk.key
                #print var[:]

                var_sha = hashlib.sha1()
                var_atts = {}
                for att in vk.attributes:
                    var_atts[att.name] = hashlib.sha1(str(att.value)).hexdigest()
                    var_sha.update(var_atts[att.name])

                if not data_sampling is DatasetDescriptionDataSamplingEnum.NONE:
                    var = self.get_variable_data(vk.index_key)
                    if data_sampling is DatasetDescriptionDataSamplingEnum.FIRST_LAST:
                        slice_first = []
                        slice_last = []
                        for s in var.shape:
                            # get the first...where outer dims == 0 and the innermost == 1
                            slice_first.append(slice(0, 1))
                            # get the last...where outer dims == max for dim
                            slice_last.append(slice(s-1,s))

                        dat_f = var[slice_first]
                        dat_l = var[slice_last]

                        # add the string data arrays into the sha
                        var_sha.update(str(dat_f))
                        var_sha.update(str(dat_l))

                    elif data_sampling is DatasetDescriptionDataSamplingEnum.FULL:
                        var_sha.update(str(var[:]))
                        pass
                    elif data_sampling is DatasetDescriptionDataSamplingEnum.SHOTGUN:
                        shotgun_count = kwargs[DatasetDescriptionDataSamplingEnum.SHOTGUN_COUNT] or 10
                        pass
                    else:
                        pass

                var_map[vk.name] = var_sha.hexdigest(), var_atts

            sha_vars = hashlib.sha1()
            for key in var_map:
                sha_vars.update(var_map[key][0])

            ret["vars"] = sha_vars.hexdigest(), var_map

        if not self._dimensions is None:
            # sha for dimensions
            dim_map = {}
            for dk in self._dimensions:
                #print dk.name, dk.size
                dim_map[dk.name] = hashlib.sha1(str(dk)).hexdigest()

            sha_dim = hashlib.sha1()
            for key in dim_map:
                sha_dim.update(dim_map[key])

            ret["dims"] = sha_dim.hexdigest(), dim_map

        if not self._global_attributes is None:
            # sha for globals
            gbl_map = {}
            for gk in self._global_attributes:
                #print gk.name, gk.value
                gbl_map[gk.name] = hashlib.sha1(str(gk.value)).hexdigest()

            sha_gbl = hashlib.sha1()
            for key in gbl_map:
                sha_gbl.update(gbl_map[key])

            ret["gbl_atts"] = sha_gbl.hexdigest(), gbl_map

        sha_full = hashlib.sha1()
        for key in ret:
            if ret[key] is not None:
                sha_full.update(ret[key][0])

        self._fingerprint = sha_full.hexdigest(), ret
        return self._fingerprint

    def has_data_changed(self, fingerprint='', **kwargs):
        if fingerprint is None or fingerprint == "":
            return True

        # compare the last_fingerprint to the current dataset _fingerprint
        dcr = self.compare(fingerprint)
        result = False
        for x in dcr:
            if x.difference != CompareResultEnum.EQUAL:
                result = True

        return result

    def compare(self, data_fingerprint):

        if data_fingerprint == '' or data_fingerprint is None:
            return None

        my_sig = self.get_fingerprint(recalculate=True)

        result = []

        if my_sig[0] != data_fingerprint[0]:
            #TODO: make info
            print "=!> Full fingerprints differ"
            if "dims" in my_sig[1] and "dims" in data_fingerprint[1]:

                if my_sig[1]["dims"][0] != data_fingerprint[1]["dims"][0]:
                    #TODO: make info
                    print "==!> Dimensions differ"
                    for dk in my_sig[1]["dims"][1]:
                        v1 = my_sig[1]["dims"][1][dk]
                        if dk in data_fingerprint[1]["dims"][1]:
                            v2 = data_fingerprint[1]["dims"][1][dk]
                        else:
                            #TODO: make info
                            print "===!> Dimension '%s' does not exist in 2nd dataset" % dk
                            res = CompareResult()
                            res.field_name = dk
                            res.difference = CompareResultEnum.NEW_DIM
                            result.append(res)
                            continue

                        if v1 != v2:
                            #TODO: make info
                            print "===!> Dimension '%s' differs" % dk
                            res = CompareResult()
                            res.field_name = dk
                            res.difference = CompareResultEnum.MOD_DIM
                            result.append(res)
                        else:
                        #TODO: make debug
                        #                        print "====> Dimension '%s' is equal" % dk
                            continue

                    for dk in data_fingerprint[1]["dims"][1]:
                        v1 = data_fingerprint[1]["dims"][1][dk]
                        if dk in my_sig[1]["dims"][1]:
                            v2 = my_sig[1]["dims"][1][dk]
                        else:
                            #TODO: make info
                            print "===!> Dimension '%s' does not exist in 1st dataset" % dk
                            res = CompareResult()
                            res.field_name = dk
                            res.difference = CompareResultEnum.NEW_DIM
                            result.append(res)
                            continue

                else:
                    #TODO: make info
                    print "===> Dimensions are equal"

            if "gbl_atts" in my_sig[1] and "gbl_atts" in data_fingerprint[1]:
                if my_sig[1]["gbl_atts"][0] != data_fingerprint[1]["gbl_atts"][0]:
                    #TODO: make info
                    print "==!> Global Attributes differ"
                    for gk in my_sig[1]["gbl_atts"][1]:
                        v1 = my_sig[1]["gbl_atts"][1][gk]
                        if gk in data_fingerprint[1]["gbl_atts"][1]:
                            v2 = data_fingerprint[1]["gbl_atts"][1][gk]
                        else:
                            #TODO: make info
                            print "===!> Global Attribute '%s' does not exist in 2nd dataset" % gk
                            res = CompareResult()
                            res.field_name = gk
                            res.difference = CompareResultEnum.NEW_GATT
                            result.append(res)
                            continue

                        if v1 != v2:
                            #TODO: make info
                            print "===!> Global Attribute '%s' differs" % gk
                            res = CompareResult()
                            res.field_name = gk
                            res.difference = CompareResultEnum.MOD_GATT
                            result.append(res)
                        else:
                        #TODO: make debug
                        #                        print "====> Global Attribute '%s' is equal" % gk
                            continue

                    for gk in data_fingerprint[1]["gbl_atts"][1]:
                        v1 = data_fingerprint[1]["gbl_atts"][1][gk]

                        if gk in my_sig[1]["gbl_atts"][1]:
                            v2 = my_sig[1]["gbl_atts"][1][gk]
                        else:
                            #TODO: make info
                            print "===!> Global Attribute '%s' does not exist in 1st dataset" % gk
                            res = CompareResult()
                            res.field_name = gk
                            res.difference = CompareResultEnum.NEW_GATT
                            result.append(res)
                            #dcr.add_gbl_attr(gk, True)
                            continue

                else:
                    #TODO: make info
                    print "===> Global Attributes are equal"

            if "vars" in my_sig[1] and "vars" in data_fingerprint[1]:
                if my_sig[1]["vars"][0] != data_fingerprint[1]["vars"][0]:
                    #TODO: make info
                    print "==!> Variable attributes differ"
                    for vk in my_sig[1]["vars"][1]:
                        v1 = my_sig[1]["vars"][1][vk][0]
                        if vk in data_fingerprint[1]["vars"][1]:
                            v2 = data_fingerprint[1]["vars"][1][vk][0]
                        else:
                            #TODO: make info
                            print "===!> Variable '%s' does not exist in 2nd dataset" % vk
                            res = CompareResult()
                            res.field_name = vk
                            res.difference = CompareResultEnum.NEW_VAR
                            result.append(res)
                            continue

                        if v1 != v2:
                            #TODO: make info
                            print "===!> Variable '%s' differ" % vk
                            res = CompareResult()
                            res.field_name = vk
                            res.difference = CompareResultEnum.MOD_VAR
                            result.append(res)
                            for vak in my_sig[1]["vars"][1][vk][1]:
                                va1 = my_sig[1]["vars"][1][vk][1][vak]
                                if vak in data_fingerprint[1]["vars"][1][vk][1]:
                                    va2 = data_fingerprint[1]["vars"][1][vk][1][vak]
                                else:
                                    #TODO: make info
                                    print "====!> Variable Attribute '%s' does not exist in 2nd dataset" % vak
                                    res = CompareResult()
                                    res.field_name = vak
                                    res.difference = CompareResultEnum.NEW_VARATT
                                    result.append(res)
                                    continue

                                if va1 != va2:
                                    #TODO: make info
                                    print "====!> Variable Attribute '%s' differs" % vak
                                    res = CompareResult()
                                    res.field_name = vak
                                    res.difference = CompareResultEnum.MOD_VARATT
                                    result.append(res)
                                else:
                                #TODO: make debug
                                #                                print "======> Variable Attribute '%s' is equal" % vak
                                    continue

                    for vk in data_fingerprint[1]["vars"][1]:
                        v1 = data_fingerprint[1]["vars"][1][vk][0]
                        if vk in my_sig[1]["vars"][1]:
                            v2 = my_sig[1]["vars"][1][vk][0]
                        else:
                            #TODO: make info
                            print "===!> Variable '%s' does not exist in 1st dataset" % vk
                            res = CompareResult()
                            res.field_name = vk
                            res.difference = CompareResultEnum.NEW_VAR
                            result.append(res)
                            continue
                else:
                    #TODO: make info
                    print "===> Variable attributes are equal"

        else:
            #TODO: make debug
            print "==> Datasets are equal"
            res = CompareResult()
            res.field_name = ""
            res.difference = CompareResultEnum.EQUAL
            result.append(res)

        return result

    def scan(self):
        return {'variables': self._variables, 'attributes': self._global_attributes, 'dimensions': self._dimensions}

    def __repr__(self):
#        return "\n>> ExternalDataProvider:\n%s\n>> DataSource:\n%s\n>>ExternalDataset\n%s" % (self._ext_provider_res, self._ext_data_source_res, self._ext_dataset_res)
        #TODO: Add printing of the ExternalDataset resource back in.
        # The above breaks tests because the id's of the nested "DatasetDescription" and "UpdateDescription" objects change by instantiation.
        # Must manually iterate the objects
        return "\n>> ExternalDataProvider:\n%s\n>> DataSource:\n%s" % (self._ext_provider_res, self._ext_data_source_res)

class ExternalDataHandlerError(Exception):
    """
    Base class for ExternalDataHandler errors.
    """
    pass

class InstantiationError(ExternalDataHandlerError):
    """
    Exception raised when an ExternalDataHandler cannot be instantiated
    """
    pass

class DataAcquisitionError(ExternalDataHandlerError):
    """
    Exception raised when there is a problem acquiring data from an external dataset.
    """
    pass
