#!/usr/bin/env python

"""
@package ion.agents.data.handlers.netcdf_data_handler
@file ion/agents/data/handlers/netcdf_data_handler.py
@author Christopher Mueller
@brief

"""

from pyon.public import log
from pyon.util.containers import get_safe
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.base_data_handler import BaseDataHandler
from ion.agents.data.handlers.handler_utils import calculate_iteration_count
import hashlib
import numpy as np
from pyon.core.interceptor.encode import encode_ion, decode_ion
import msgpack
from interface.objects import CompareResult, CompareResultEnum

from netCDF4 import Dataset


class NetcdfDataHandler(BaseDataHandler):

    @classmethod
    def _constraints_for_new_request(cls, config):
        """
        Returns a constraints dictionary with
        @param config Dict of configuration parameters - may be used to generate the returned 'constraints' dict
        @retval dict that contains the constraints for retrieval of new data from the external dataset or None
        """
        #TODO: Sort out what the config needs to look like - dataset_in??
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        #log.debug('ExternalDataset Resource: {0}'.format(ext_dset_res))
        if ext_dset_res:
            #TODO: Use the external dataset resource to determine what data is new (i.e. pull 'old' fingerprint from here)
            #log.debug('ext_dset_res.dataset_description = {0}'.format(ext_dset_res.dataset_description))
            #log.debug('ext_dset_res.update_description = {0}'.format(ext_dset_res.update_description))

            # Get the Dataset object from the config (should have been instantiated in _init_acquisition_cycle)
            ds = get_safe(config, 'dataset_object')

            base_nd_check = get_safe(ext_dset_res.update_description.parameters, 'new_data_check')

            t_slice = slice(None)
            if base_nd_check:
                t_new_vname = ext_dset_res.dataset_description.parameters['temporal_dimension']
                t_new_arr = ds.variables[t_new_vname][t_slice]

                new_data = msgpack.packb(t_new_arr, default=encode_ion)
                if new_data != base_nd_check:
                    #new time data has arrived, figure out what's different and build the new slice
                    first_index = -1
                    last_index = -1
                    t_old_arr = msgpack.unpackb(base_nd_check, object_hook=decode_ion)
                    for new_data in t_new_arr:
                        if not new_data in t_old_arr:
                            if first_index == -1:
                                first_index = np.nonzero(t_new_arr == new_data)[0][0]
                                last_index = np.nonzero(t_new_arr == new_data)[0][0]
                            else:
                                last_index = np.nonzero(t_new_arr == new_data)[0][0]

                    t_slice = slice(first_index, last_index)

            return {
                'temporal_slice': t_slice
            }

        return None

    @classmethod
    def _constraints_for_historical_request(cls, config):
        """
        Determines any constraints that must be added to the constraints configuration.
        This should present a uniform constraints configuration to be sent to _get_data
        @param config Dict containing configuration parameters, may include constraints, formatters, etc
        @retval dictionary containing the restraints
        """
        return {}

    @classmethod
    def _get_data(cls, config):
        """
        Retrieves config['constraints']['count'] number of random samples of length config['constraints']['array_len']
        @param config Dict of configuration parameters - must contain ['constraints']['count'] and ['constraints']['count']
        """
        ext_dset_res = get_safe(config, 'external_dataset_res', None)

        # Get the Dataset object from the config (should have been instantiated in _init_acquisition_cycle)
        ds = get_safe(config, 'dataset_object')

        if ext_dset_res and ds:
            t_vname = ext_dset_res.dataset_description.parameters['temporal_dimension']
            x_vname = ext_dset_res.dataset_description.parameters['zonal_dimension']
            y_vname = ext_dset_res.dataset_description.parameters['meridional_dimension']
            z_vname = ext_dset_res.dataset_description.parameters['vertical_dimension']
            var_lst = ext_dset_res.dataset_description.parameters['variables']

            t_slice = get_safe(config, 'constraints.temporal_slice', (slice(0, 1)))
            #TODO: Using 'eval' here is BAD - need to find a less sketchy way to pass constraints
            if isinstance(t_slice, str):
                t_slice = eval(t_slice)

            lon = ds.variables[x_vname][:]
            lat = ds.variables[y_vname][:]
            z = ds.variables[z_vname][:]

            t_arr = ds.variables[t_vname][t_slice]
            data_arrays = {}
            for varn in var_lst:
                data_arrays[varn] = ds.variables[varn][t_slice]

            max_rec = get_safe(config, 'max_records', 1)
            #dprod_id = get_safe(config, 'data_producer_id', 'unknown data producer')

            stream_def = get_safe(config, 'stream_def')

            cnt = calculate_iteration_count(t_arr.size, max_rec)
            for x in xrange(cnt):
                ta = t_arr[x * max_rec:(x + 1) * max_rec]

                # Make a 'master' RecDict
                rdt = RecordDictionaryTool(stream_definition_id=stream_def)

                # Assign coordinate values to the RecDict
                rdt[x_vname] = lon
                rdt[y_vname] = lat
                rdt[z_vname] = z

                # Assign data values to the RecDict
                rdt[t_vname] = ta
                for key, arr in data_arrays.iteritems():
                    d = arr[x * max_rec:(x + 1) * max_rec]
                    rdt[key] = d

                g = rdt.to_granule()
                yield g

            ds.close()

    @classmethod
    def _init_acquisition_cycle(cls, config):
        """
        Initialize anything the data handler will need to use, such as a dataset
        @param config Dict containing configuration parameters, may include constraints, formatters, etc
        """
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        if ext_dset_res:
            ds_url = ext_dset_res.dataset_description.parameters['dataset_path']
            log.debug('External Dataset URL: \'{0}\''.format(ds_url))
            config['dataset_object'] = Dataset(ds_url)

    @classmethod
    def _get_fingerprint(cls, ds):
        """
        Calculate the fingerprint of the dataset
        @param ds the dataset
        @retval a fingerprint representing the dataset and its contents
        """

        ret = {}

        if len(ds.variables) > 0:
            var_map = {}
            for vk, var in ds.variables.iteritems():
                var_sha = hashlib.sha1()
                var_atts = {}
                for ak in var.ncattrs():
                    att = var.getncattr(ak)
                    var_atts[ak] = hashlib.sha1(str(att)).hexdigest()
                    var_sha.update(var_atts[ak])

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

                var_map[vk] = var_sha.hexdigest(), var_atts

            sha_vars = hashlib.sha1()
            for key in var_map:
                sha_vars.update(var_map[key][0])

            ret["vars"] = sha_vars.hexdigest(), var_map

        if len(ds.dimensions) > 0:
            # sha for dimensions
            dim_map = {}
            for dk, dim in ds.dimensions.iteritems():
                dim_map[dk] = hashlib.sha1(str(dim)).hexdigest()

            sha_dim = hashlib.sha1()
            for key in dim_map:
                sha_dim.update(dim_map[key])

            ret["dims"] = sha_dim.hexdigest(), dim_map

        if len(ds.ncattrs()) > 0:
            # sha for globals
            gbl_map = {}
            for gk in ds.ncattrs():
                gatt = ds.getncattr(gk)
                gbl_map[gk] = hashlib.sha1(str(gatt)).hexdigest()

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
        """
        Compares two fingerprints to see what, if anything, is different
        @param base_fingerprint first fingerprint used in the comparison
        @param new_fingerprint second fingerprint used in the comparison
        """

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
