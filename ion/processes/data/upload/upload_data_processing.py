#!/usr/bin/env python
'''
@author Luke Campbell <My email is around here somewhere>
@file ion/processes/data/transforms/qc_post_processing.py
@date Tue May  7 15:34:54 EDT 2013
'''

from pyon.core.exception import BadRequest
from pyon.ion.process import ImmediateProcess, SimpleProcess
from interface.services.dm.idata_retriever_service import DataRetrieverServiceProcessClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.util.direct_coverage_utils import DirectCoverageAccess
import time
from pyon.ion.event import EventPublisher
from pyon.public import OT,RT,PRED
from pyon.util.arg_check import validate_is_not_none
from pyon.ion.event import EventSubscriber
from pyon.util.log import log
import re
import numpy as np
import netCDF4
import os

class UploadDataProcessing(ImmediateProcess):
    '''
    Upload Data Processing Process

    This process provides the capability to ION clients and operators to process uploaded data to create
    data products.

    This parameters that this process accepts as configurations are:
        - dp_id: The DataProduct identifier, required.

    '''

    def on_start(self):

        ImmediateProcess.on_start(self)

        # necessary arguments, passed in via configuration kwarg to schedule_process. process namespace to avoid collisions
        fuc_id = self.CFG.get_safe('process.fuc_id',None) # FileUploadContext ID
        dp_id = self.CFG.get_safe('process.dp_id',None) # DataProduct ID

        # clients we'll need
        resource_registry = self.container.resource_registry
        object_store = self.container.object_store
        dataset_management =  DatasetManagementServiceClient()
        data_product_management = DataProductManagementServiceClient()

        # get the Object (dict) containing details of the uploaded file
        fuc = object_store.read(fuc_id)

        # get the ParameterContexts associated with this DataProduct
        sd_id = resource_registry.find_objects(dp_id, PRED.hasStreamDefinition, id_only=True)[0][0] # TODO loop
        pd_id = resource_registry.find_objects(sd_id, PRED.hasParameterDictionary, id_only=True)[0][0] # TODO loop
        pc_list, _ = resource_registry.find_objects(pd_id, PRED.hasParameterContext, id_only=False) # parameter contexts

        # NetCDF file open here
        nc_filename = fuc.get('path', None)
        if nc_filename is None:
            raise BadRequest("uploaded file has no path")

        # keep track of the number of fields we actually process
        nfields = 0

        with netCDF4.Dataset(nc_filename,'r') as nc:

            nc_time = nc.variables['time'][:] # don't modify nc_time below, read once use many times

            for v in nc.variables:
                variable = nc.variables[v]
                nc_name = str(v) # name of variable should be the same as what was downloaded, we'll append the c here
                # check for REQUIRED attributes
                author = getattr(variable, 'author', None)
                reason = getattr(variable, 'reason', None)
                if not all([author,reason]):
                    log.info('skipping parameter %s - no author or reason' % nc_name)
                    continue
                # get all ParameterContexts (from pc_list) with this 'name' (should be one at the moment)
                pc_matches_nc_name_list = [c for c in pc_list if c.name == nc_name]
                # is variable already present?
                if len(pc_matches_nc_name_list) < 1:
                    log.info('skipping parameter %s - not found in ParameterContexts associated with DataProduct' % nc_name)
                    continue

                # we are using this ParameterContext as a copy
                pc = pc_matches_nc_name_list[0] # TODO should only have 1 context per 'name' but could be checked for completeness
                # only allow L1/L2 paramters (check against ooi_short_name which should end with this)
                m = re.compile('(_L[12])$').search(pc.ooi_short_name.upper()) # capture L1/L2 for use in new name
                if not m: # if not _L1 or _L2 move on
                    log.info('skipping parameter %s - not L1 or L2' % nc_name)
                    continue
                processing_level = m.group(1)
                # remove attributes we should not copy [_id,_rev,ts_created,ts_updated]
                delattr(pc, '_id')
                delattr(pc, '_rev')
                delattr(pc, 'ts_created')
                delattr(pc, 'ts_updated')
                # append L[12]c to name attribute (new parameter name)
                c_name = ''.join([pc['name'],processing_level,'c'])
                pc['name'] = c_name
                # copy attributes from NetCDF file
                pc['units'] = variable.units
                pc['value_encoding'] = str(variable.dtype)
                #TODO ERDAP files don't have fill_value, but should probably get from there, leaving copy for now
                # create ParameterContext
                pc_id = dataset_management.create_parameter(pc)
                data_product_management.add_parameter_to_data_product(pc_id,dp_id)

                # get NetCDF data for this variable
                nc_data = variable[:]

                with DirectCoverageAccess() as dca:
                    # get the Dataset IDs associated with this DataProduct
                    ds_id_list, _ = resource_registry.find_objects(dp_id, PRED.hasDataset, id_only=True)
                    for ds_id in ds_id_list: # could be multiple Datasets for this DataProduct
                        with dca.get_editable_coverage(ds_id) as cov: # <-- This pauses ingestion
                            # times in this Dataset
                            cov_time = cov.get_parameter_values(['time']).get_data()['time']
                            # subset nc_time (only where nc_time matches cov_time)
                            nc_indicies = [i for i,x in enumerate(nc_time) if x in cov_time]
                            subset_nc_time = nc_time[nc_indicies] + 2208988800 # TODO REMOVE THIS? ERDAP 1970 vs NTP 1900
                            # don't forget to subset the data too
                            subset_nc_data = [nc_data[i] for i in nc_indicies]
                            # use indicies of where subset_nc_time exists in cov_time to update coverage
                            cov_indicies = np.flatnonzero(np.in1d(cov_time, subset_nc_time)) # returns numpy.ndarray of indicies
                            cov_indicies = list(cov_indicies) # converts to list for coverage
                            #cov._range_value[c_name][cov_indicies] = subset_nc_data # TODO this should eventually work
                            for i,x in enumerate(cov_indicies):
                                cov._range_value[c_name][x] = subset_nc_data[i]

                nfields = nfields + 1

        fuc['status'] = 'UploadDataProcessing process complete - %d fields created/updated' % nfields
        self.container.object_store.update_doc(fuc)

        # remove uploaded file
        try:
            os.remove(nc_filename)
        except OSError:
            pass # TODO take action to get this removed

