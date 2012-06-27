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
from ion.agents.data.handlers.handler_utils import calculate_iteration_count
import hashlib
import numpy as np
from pyon.core.interceptor.encode import encode_ion, decode_ion
import msgpack

import time

from netCDF4 import Dataset

class NetcdfDataHandler(BaseDataHandler):

    @classmethod
    def _constraints_for_new_request(cls, config):
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
#            base_fingerprint = ext_dset_res.update_description
            base_nd_check = get_safe(ext_dset_res.update_description.parameters,'new_data_check')
#            base_nd_check = '\x83\xa7content\xdc\x00\xc9\xceM\xa0\xf3\x00\xceM\xa2D\x80\xceM\xa3\x96\x00\xceM\xa4\xe7\x80\xceM\xa69\x00\xceM\xa7\x8a\x80\xceM\xa8\xdc\x00\xceM\xaa-\x80\xceM\xab\x7f\x00\xceM\xac\xd0\x80\xceM\xae"\x00\xceM\xafs\x80\xceM\xb0\xc5\x00\xceM\xb2\x16\x80\xceM\xb3h\x00\xceM\xb4\xb9\x80\xceM\xb6\x0b\x00\xceM\xb7\\\x80\xceM\xb8\xae\x00\xceM\xb9\xff\x80\xceM\xbbQ\x00\xceM\xbc\xa2\x80\xceM\xbd\xf4\x00\xceM\xbfE\x80\xceM\xc0\x97\x00\xceM\xc1\xe8\x80\xceM\xc3:\x00\xceM\xc4\x8b\x80\xceM\xc5\xdd\x00\xceM\xc7.\x80\xceM\xc8\x80\x00\xceM\xc9\xd1\x80\xceM\xcb#\x00\xceM\xcct\x80\xceM\xcd\xc6\x00\xceM\xcf\x17\x80\xceM\xd0i\x00\xceM\xd1\xba\x80\xceM\xd3\x0c\x00\xceM\xd4]\x80\xceM\xd5\xaf\x00\xceM\xd7\x00\x80\xceM\xd8R\x00\xceM\xd9\xa3\x80\xceM\xda\xf5\x00\xceM\xdcF\x80\xceM\xdd\x98\x00\xceM\xde\xe9\x80\xceM\xe0;\x00\xceM\xe1\x8c\x80\xceM\xe2\xde\x00\xceM\xe4/\x80\xceM\xe5\x81\x00\xceM\xe6\xd2\x80\xceM\xe8$\x00\xceM\xe9u\x80\xceM\xea\xc7\x00\xceM\xec\x18\x80\xceM\xedj\x00\xceM\xee\xbb\x80\xceM\xf0\r\x00\xceM\xf1^\x80\xceM\xf2\xb0\x00\xceM\xf4\x01\x80\xceM\xf5S\x00\xceM\xf6\xa4\x80\xceM\xf7\xf6\x00\xceM\xf9G\x80\xceM\xfa\x99\x00\xceM\xfb\xea\x80\xceM\xfd<\x00\xceM\xfe\x8d\x80\xceM\xff\xdf\x00\xceN\x010\x80\xceN\x02\x82\x00\xceN\x03\xd3\x80\xceN\x05%\x00\xceN\x06v\x80\xceN\x07\xc8\x00\xceN\t\x19\x80\xceN\nk\x00\xceN\x0b\xbc\x80\xceN\r\x0e\x00\xceN\x0e_\x80\xceN\x0f\xb1\x00\xceN\x11\x02\x80\xceN\x12T\x00\xceN\x13\xa5\x80\xceN\x14\xf7\x00\xceN\x16H\x80\xceN\x17\x9a\x00\xceN\x18\xeb\x80\xceN\x1a=\x00\xceN\x1b\x8e\x80\xceN\x1c\xe0\x00\xceN\x1e1\x80\xceN\x1f\x83\x00\xceN \xd4\x80\xceN"&\x00\xceN#w\x80\xceN$\xc9\x00\xceN&\x1a\x80\xceN\'l\x00\xceN(\xbd\x80\xceN*\x0f\x00\xceN+`\x80\xceN,\xb2\x00\xceN.\x03\x80\xceN/U\x00\xceN0\xa6\x80\xceN1\xf8\x00\xceN3I\x80\xceN4\x9b\x00\xceN5\xec\x80\xceN7>\x00\xceN8\x8f\x80\xceN9\xe1\x00\xceN;2\x80\xceN<\x84\x00\xceN=\xd5\x80\xceN?\'\x00\xceN@x\x80\xceNA\xca\x00\xceNC\x1b\x80\xceNDm\x00\xceNE\xbe\x80\xceNG\x10\x00\xceNHa\x80\xceNI\xb3\x00\xceNK\x04\x80\xceNLV\x00\xceNM\xa7\x80\xceNN\xf9\x00\xceNPJ\x80\xceNQ\x9c\x00\xceNR\xed\x80\xceNT?\x00\xceNU\x90\x80\xceNV\xe2\x00\xceNX3\x80\xceNY\x85\x00\xceNZ\xd6\x80\xceN\\(\x00\xceN]y\x80\xceN^\xcb\x00\xceN`\x1c\x80\xceNan\x00\xceNb\xbf\x80\xceNd\x11\x00\xceNeb\x80\xceNf\xb4\x00\xceNh\x05\x80\xceNiW\x00\xceNj\xa8\x80\xceNk\xfa\x00\xceNmK\x80\xceNn\x9d\x00\xceNo\xee\x80\xceNq@\x00\xceNr\x91\x80\xceNs\xe3\x00\xceNu4\x80\xceNv\x86\x00\xceNw\xd7\x80\xceNy)\x00\xceNzz\x80\xceN{\xcc\x00\xceN}\x1d\x80\xceN~o\x00\xceN\x7f\xc0\x80\xceN\x81\x12\x00\xceN\x82c\x80\xceN\x83\xb5\x00\xceN\x85\x06\x80\xceN\x86X\x00\xceN\x87\xa9\x80\xceN\x88\xfb\x00\xceN\x8aL\x80\xceN\x8b\x9e\x00\xceN\x8c\xef\x80\xceN\x8eA\x00\xceN\x8f\x92\x80\xceN\x90\xe4\x00\xceN\x925\x80\xceN\x93\x87\x00\xceN\x94\xd8\x80\xceN\x96*\x00\xceN\x97{\x80\xceN\x98\xcd\x00\xceN\x9a\x1e\x80\xceN\x9bp\x00\xceN\x9c\xc1\x80\xceN\x9e\x13\x00\xceN\x9fd\x80\xceN\xa0\xb6\x00\xceN\xa2\x07\x80\xceN\xa3Y\x00\xceN\xa4\xaa\x80\xceN\xa5\xfc\x00\xceN\xa7M\x80\xceN\xa8\x9f\x00\xa6header\x83\xa2nd\x01\xa5shape\x91\xcc\xc9\xa4type\xa5int32\xad__ion_array__\xc3'

#            log.warn(base_nd_check)

#
#
#
#            log.warn(old_arr)


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
                    for old_data in t_old_arr:
                        if not old_data in t_new_arr:
                            if first_index == -1:
                                first_index = np.nonzero(t_new_arr == old_data)[0][0]
                                last_index = np.nonzero(t_new_arr == old_data)[0][0]
                            else:
                                last_index = np.nonzero(t_new_arr == old_data)[0][0]

                    t_slice = slice(first_index, last_index)


                #TG: Get new temporal data and encode it
                #TG: Compare the old with the new, if different, decode old and sort out what's different
                #TG: Build appropriate temproral_slice

            return {
                'temporal_slice':t_slice
            }

        return None

    @classmethod
    def _constraints_for_historical_request(cls, config):
        return {}

    @classmethod
    def _get_data(cls, config):
        """
        Retrieves config['constraints']['count'] number of random samples of length config['constraints']['array_len']
        @param config Dict of configuration parameters - must contain ['constraints']['count'] and ['constraints']['count']
        """
        ext_dset_res = get_safe(config, 'external_dataset_res', None)

        # Get the Dataset object from the config (should have been instantiated in _init_acquisition_cycle)
        ds=get_safe(config, 'dataset_object')
        if ext_dset_res and ds:
            t_vname = ext_dset_res.dataset_description.parameters['temporal_dimension']
            x_vname = ext_dset_res.dataset_description.parameters['zonal_dimension']
            y_vname = ext_dset_res.dataset_description.parameters['meridional_dimension']
            z_vname = ext_dset_res.dataset_description.parameters['vertical_dimension']
            var_lst = ext_dset_res.dataset_description.parameters['variables']

            t_slice = get_safe(config, 'constraints.temporal_slice', (slice(0,1)))
            #TODO: Using 'eval' here is BAD - need to find a less sketchy way to pass constraints
            if isinstance(t_slice,str):
                t_slice=eval(t_slice)

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

            cnt = calculate_iteration_count(t_arr.size, max_rec)
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
            log.debug('External Dataset URL: \'{0}\''.format(ds_url))
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
