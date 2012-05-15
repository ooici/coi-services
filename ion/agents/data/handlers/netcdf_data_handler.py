#!/usr/bin/env python

"""
@package ion.agents.data.handlers.netcdf_data_handler
@file ion/agents/data/handlers/netcdf_data_handler.py
@author Christopher Mueller
@brief

"""

from pyon.public import log
from pyon.util.containers import get_safe
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.base_data_handler import BaseDataHandler, DataHandlerParameter
import hashlib
import numpy as np

import time

from netCDF4 import Dataset


PACKET_CONFIG = {
    'data_stream' : ('prototype.sci_data.stream_defs', 'ctd_stream_packet')
}

class NetcdfDataHandler(BaseDataHandler):

    @classmethod
    def _new_data_constraints(cls, config):
        """
        Returns a constraints dictionary with
        @param config Dict of configuration parameters - may be used to generate the returned 'constraints' dict
        """
        #TODO: Sort out what the config needs to look like - dataset_in??
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        log.debug('ExternalDataset Resource: {0}'.format(ext_dset_res))
        if ext_dset_res:
            #TODO: Use the external dataset resource to determine what data is new (i.e. pull 'old' fingerprint from here)
            log.debug('ext_dset_res.dataset_description = {0}'.format(ext_dset_res.dataset_description))
            log.debug('ext_dset_res.update_description = {0}'.format(ext_dset_res.update_description))
            base_fingerprint = ext_dset_res.update_description
            new_fingerprint = _get_fingerprint(get_safe(config, 'dataset_object', None))


        return {'temporal_slice':'(slice(0,10))'}

    @classmethod
    def _get_data(cls, config):
        """
        Retrieves config['constraints']['count'] number of random samples of length config['constraints']['array_len']
        @param config Dict of configuration parameters - must contain ['constraints']['count'] and ['constraints']['count']
        """
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        if ext_dset_res:
            ds_url = ext_dset_res.dataset_description.parameters['dataset_path']
            log.debug('External Dataset URL: \'{0}\''.format(ds_url))

            t_vname = ext_dset_res.dataset_description.parameters['temporal_dimension']
            x_vname = ext_dset_res.dataset_description.parameters['zonal_dimension']
            y_vname = ext_dset_res.dataset_description.parameters['meridional_dimension']
            z_vname = ext_dset_res.dataset_description.parameters['vertical_dimension']
            var_lst = ext_dset_res.dataset_description.parameters['variables']

            t_slice = get_safe(config, 'constraints.temporal_slice', (slice(0,1)))
            #TODO: Using 'eval' here is BAD - need to find a less sketchy way to pass constraints
            if isinstance(t_slice,str):
                t_slice=eval(t_slice)

            # Open the netcdf file, obtain the appropriate variables
            ds=Dataset(ds_url)
            lon = ds.variables[x_vname][:]
            lat = ds.variables[y_vname][:]
            z = ds.variables[z_vname][:]

            t_arr = ds.variables[t_vname][t_slice]
            data_arrays = {}
            for varn in var_lst:
                data_arrays[varn] = ds.variables[varn][t_slice]

            max_rec = get_safe(config, 'max_records', 1)
            dprod_id = get_safe(config, 'data_producer_id', 'unknown data producer')
            tx_yml = get_safe(config, 'taxonomy')
            ttool = TaxyTool.load(tx_yml) #CBM: Assertion inside RDT.__setitem__ requires same instance of TaxyTool

            cnt = cls._calc_iter_cnt(t_arr.size, max_rec)
            for x in xrange(cnt):
                ta = t_arr[x*max_rec:(x+1)*max_rec]

                # Make a 'master' RecDict
                rdt = RecordDictionaryTool(taxonomy=ttool)
                # Make a 'coordinate' RecDict
                rdt_c = RecordDictionaryTool(taxonomy=ttool)
                # Make a 'data' RecDict
                rdt_d = RecordDictionaryTool(taxonomy=ttool)

                # Assign values to the coordinate RecDict
                rdt_c[x_vname] = lon
                rdt_c[y_vname] = lat
                rdt_c[z_vname] = z

                # Assign values to the data RecDict
                rdt_d[t_vname] = ta
                for key, arr in data_arrays.iteritems():
                    d = arr[x*max_rec:(x+1)*max_rec]
                    rdt_d[key] = d

                # Add the coordinate and data RecDicts to the master RecDict
                rdt['coords'] = rdt_c
                rdt['data'] = rdt_d

                # Build and return a granule
                # CBM: ttool must be passed
                g = build_granule(data_producer_id=dprod_id, taxonomy=ttool, record_dictionary=rdt)
                yield g

            ds.close()

    @classmethod
    def _init_acquisition_cycle(cls, config):
        """
        Initialize anything the data handler will need to use, such as a dataset
        """
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        if ext_dset_res:
            ds_url = ext_dset_res.dataset_description.parameters['dataset_path']
            config['dataset_object'] = Dataset(ds_url)

    @classmethod
    def _get_fingerprint(cls, ds):
        """
        Calculate the fingerprint of the dataset
        """

        ret = {}

        #ds_url = dataset_description.parameters['dataset_path']
        ## Open the netcdf file, obtain the appropriate variables
        #ds = Dataset(ds_url)

        if not ds.variables is None:
            var_map = {}
            for vk in ds.variables:
                #var = self._data_array[vk.column_name]
                #print vk.key
                #print var[:]

                var_sha = hashlib.sha1()
                var_atts = {}
                for att in vk.attributes:
                    var_atts[att.name] = hashlib.sha1(str(att.value)).hexdigest()
                    var_sha.update(var_atts[att.name])

#                if not data_sampling is DatasetDescriptionDataSamplingEnum.NONE:
#                    var = self.get_variable_data(vk.index_key)
#                    if data_sampling is DatasetDescriptionDataSamplingEnum.FIRST_LAST:
#                        slice_first = []
#                        slice_last = []
#                        for s in var.shape:
#                            # get the first...where outer dims == 0 and the innermost == 1
#                            slice_first.append(slice(0, 1))
#                            # get the last...where outer dims == max for dim
#                            slice_last.append(slice(s-1,s))
#
#                        dat_f = var[slice_first]
#                        dat_l = var[slice_last]
#
#                        # add the string data arrays into the sha
#                        var_sha.update(str(dat_f))
#                        var_sha.update(str(dat_l))
#
#                    elif data_sampling is DatasetDescriptionDataSamplingEnum.FULL:
#                        var_sha.update(str(var[:]))
#                        pass
#                    elif data_sampling is DatasetDescriptionDataSamplingEnum.SHOTGUN:
#                        shotgun_count = kwargs[DatasetDescriptionDataSamplingEnum.SHOTGUN_COUNT] or 10
#                        pass
#                    else:
#                        pass

                var_map[vk.name] = var_sha.hexdigest(), var_atts

            sha_vars = hashlib.sha1()
            for key in var_map:
                sha_vars.update(var_map[key][0])

            ret["vars"] = sha_vars.hexdigest(), var_map

        if not ds.dimensions is None:
            # sha for dimensions
            dim_map = {}
            for dk in ds.dimensions:
                #print dk.name, dk.size
                dim_map[dk.name] = hashlib.sha1(str(dk)).hexdigest()

            sha_dim = hashlib.sha1()
            for key in dim_map:
                sha_dim.update(dim_map[key])

            ret["dims"] = sha_dim.hexdigest(), dim_map

        if not ds.ncattrs() is None:
            # sha for globals
            gbl_map = {}
            for gk in ds.ncattrs():
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

        return sha_full.hexdigest(), ret

    def _compare(self, base_fingerprint, new_fingerprint):

        if base_fingerprint == '' or base_fingerprint is None:
            return None

        if new_fingerprint == '' or new_fingerprint is None:
            return None

        my_sig = base_fingerprint

        result = []

        if my_sig[0] != new_fingerprint[0]:
            #TODO: make info
            log.info("=!> Full fingerprints differ")
            if "dims" in my_sig[1] and "dims" in new_fingerprint[1]:

                if my_sig[1]["dims"][0] != new_fingerprint[1]["dims"][0]:
                    #TODO: make info
                    log.info("==!> Dimensions differ")
                    for dk in my_sig[1]["dims"][1]:
                        v1 = my_sig[1]["dims"][1][dk]
                        if dk in new_fingerprint[1]["dims"][1]:
                            v2 = new_fingerprint[1]["dims"][1][dk]
                        else:
                            #TODO: make info
                            log.info("===!> Dimension '%s' does not exist in 2nd dataset" % dk)
                            res = CompareResult()
                            res.field_name = dk
                            res.difference = CompareResultEnum.NEW_DIM
                            result.append(res)
                            continue

                        if v1 != v2:
                            #TODO: make info
                            log.info("===!> Dimension '%s' differs" % dk)
                            res = CompareResult()
                            res.field_name = dk
                            res.difference = CompareResultEnum.MOD_DIM
                            result.append(res)
                        else:
                        #TODO: make debug
                        #                        print "====> Dimension '%s' is equal" % dk
                            continue

                    for dk in new_fingerprint[1]["dims"][1]:
                        v1 = new_fingerprint[1]["dims"][1][dk]
                        if dk in my_sig[1]["dims"][1]:
                            v2 = my_sig[1]["dims"][1][dk]
                        else:
                            #TODO: make info
                            log.info("===!> Dimension '%s' does not exist in 1st dataset" % dk)
                            res = CompareResult()
                            res.field_name = dk
                            res.difference = CompareResultEnum.NEW_DIM
                            result.append(res)
                            continue

                else:
                    #TODO: make info
                    log.info("===> Dimensions are equal")

            if "gbl_atts" in my_sig[1] and "gbl_atts" in new_fingerprint[1]:
                if my_sig[1]["gbl_atts"][0] != new_fingerprint[1]["gbl_atts"][0]:
                    #TODO: make info
                    log.info("==!> Global Attributes differ")
                    for gk in my_sig[1]["gbl_atts"][1]:
                        v1 = my_sig[1]["gbl_atts"][1][gk]
                        if gk in new_fingerprint[1]["gbl_atts"][1]:
                            v2 = new_fingerprint[1]["gbl_atts"][1][gk]
                        else:
                            #TODO: make info
                            log.info("===!> Global Attribute '%s' does not exist in 2nd dataset" % gk)
                            res = CompareResult()
                            res.field_name = gk
                            res.difference = CompareResultEnum.NEW_GATT
                            result.append(res)
                            continue

                        if v1 != v2:
                            #TODO: make info
                            log.info("===!> Global Attribute '%s' differs" % gk)
                            res = CompareResult()
                            res.field_name = gk
                            res.difference = CompareResultEnum.MOD_GATT
                            result.append(res)
                        else:
                        #TODO: make debug
                        #                        print "====> Global Attribute '%s' is equal" % gk
                            continue

                    for gk in new_fingerprint[1]["gbl_atts"][1]:
                        v1 = new_fingerprint[1]["gbl_atts"][1][gk]

                        if gk in my_sig[1]["gbl_atts"][1]:
                            v2 = my_sig[1]["gbl_atts"][1][gk]
                        else:
                            #TODO: make info
                            log.info("===!> Global Attribute '%s' does not exist in 1st dataset" % gk)
                            res = CompareResult()
                            res.field_name = gk
                            res.difference = CompareResultEnum.NEW_GATT
                            result.append(res)
                            #dcr.add_gbl_attr(gk, True)
                            continue

                else:
                    #TODO: make info
                    log.info("===> Global Attributes are equal")

            if "vars" in my_sig[1] and "vars" in new_fingerprint[1]:
                if my_sig[1]["vars"][0] != new_fingerprint[1]["vars"][0]:
                    #TODO: make info
                    log.info("==!> Variable attributes differ")
                    for vk in my_sig[1]["vars"][1]:
                        v1 = my_sig[1]["vars"][1][vk][0]
                        if vk in new_fingerprint[1]["vars"][1]:
                            v2 = new_fingerprint[1]["vars"][1][vk][0]
                        else:
                            #TODO: make info
                            log.info("===!> Variable '%s' does not exist in 2nd dataset" % vk)
                            res = CompareResult()
                            res.field_name = vk
                            res.difference = CompareResultEnum.NEW_VAR
                            result.append(res)
                            continue

                        if v1 != v2:
                            #TODO: make info
                            log.info("===!> Variable '%s' differ" % vk)
                            res = CompareResult()
                            res.field_name = vk
                            res.difference = CompareResultEnum.MOD_VAR
                            result.append(res)
                            for vak in my_sig[1]["vars"][1][vk][1]:
                                va1 = my_sig[1]["vars"][1][vk][1][vak]
                                if vak in new_fingerprint[1]["vars"][1][vk][1]:
                                    va2 = new_fingerprint[1]["vars"][1][vk][1][vak]
                                else:
                                    #TODO: make info
                                    log.info("====!> Variable Attribute '%s' does not exist in 2nd dataset" % vak)
                                    res = CompareResult()
                                    res.field_name = vak
                                    res.difference = CompareResultEnum.NEW_VARATT
                                    result.append(res)
                                    continue

                                if va1 != va2:
                                    #TODO: make info
                                    log.info("====!> Variable Attribute '%s' differs" % vak)
                                    res = CompareResult()
                                    res.field_name = vak
                                    res.difference = CompareResultEnum.MOD_VARATT
                                    result.append(res)
                                else:
                                #TODO: make debug
                                #                                print "======> Variable Attribute '%s' is equal" % vak
                                    continue

                    for vk in new_fingerprint[1]["vars"][1]:
                        v1 = new_fingerprint[1]["vars"][1][vk][0]
                        if vk in my_sig[1]["vars"][1]:
                            v2 = my_sig[1]["vars"][1][vk][0]
                        else:
                            #TODO: make info
                            log.info("===!> Variable '%s' does not exist in 1st dataset" % vk)
                            res = CompareResult()
                            res.field_name = vk
                            res.difference = CompareResultEnum.NEW_VAR
                            result.append(res)
                            continue
                else:
                    #TODO: make info
                    log.info("===> Variable attributes are equal")

        else:
            #TODO: make debug
            log.info("==> Datasets are equal")
            res = CompareResult()
            res.field_name = ""
            res.difference = CompareResultEnum.EQUAL
            result.append(res)

        return result
