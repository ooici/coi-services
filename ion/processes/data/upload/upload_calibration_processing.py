#!/usr/bin/env python
'''
@author Brian McKenna <bmckenna@asascience.com>
@file ion/processes/data/upload/upload_calibration_processing.py
'''

from pyon.public import OT, RT, PRED
from pyon.core.exception import BadRequest
from pyon.ion.process import ImmediateProcess
from pyon.event.event import EventPublisher
from pyon.util.log import log
from interface.services.sa.idata_product_management_service import DataProductManagementServiceProcessClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.util.time_utils import TimeUtils
from coverage_model import ConstantOverTime
import time
import re
import os
import csv
from zipfile import ZipFile

class UploadCalibrationProcessing(ImmediateProcess):
    """
    Upload Calibration Processing Process

    This process provides the capability to ION clients and operators to process uploaded calibration
    coefficients to calibrate data products.

    This parameters that this process accepts as configurations are:
        - fuc_id: The FileUploadContext identifier, required, stores where the file was written
    """

    def on_start(self):

        ImmediateProcess.on_start(self)

        # necessary arguments, passed in via configuration kwarg to schedule_process. process namespace to avoid collisions
        fuc_id = self.CFG.get_safe('process.fuc_id',None) # FileUploadContext ID

        # Clients
        self.object_store = self.container.object_store
        self.resource_registry = self.container.resource_registry
        self.event_publisher = EventPublisher(OT.ResetQCEvent)
        self.data_product_management = DataProductManagementServiceProcessClient(process=self)
        self.create_map()

        # run process
        if fuc_id:
            self.process(fuc_id)

        # cleanup
        self.event_publisher.close()



    def process(self,fuc_id):

        # get the Object (dict) containing details of the uploaded file
        fuc = self.object_store.read(fuc_id)

        if fuc['filetype'] == 'ZIP':
            raise BadRequest("ZIP format not determined by project scientists yet (2014-04-21)")
            #self.process_zip(fuc)
        else:
            self.process_csv(fuc)

    def create_map(self):
        '''
        Creates a map from property numbers to datasets
        '''
        self.property_map = {}

        for instrument_device in self.resource_registry.find_resources(restype=RT.InstrumentDevice)[0]:
            if instrument_device.ooi_property_number:
                self.property_map[instrument_device.ooi_property_number] = self.data_products_for_device(instrument_device)

    def data_products_for_device(self, device):
        data_products, _ = self.resource_registry.find_objects(device, PRED.hasOutputProduct, id_only=True)
        return data_products

    def dataset_for_data_product(self, data_product):
        datasets, _ = self.resource_registry.find_objects(data_product, PRED.hasDataset, id_only=True)
        return datasets[0]

    def do_something_with_the_update(self, updates):
        for property_no, calibration_update in updates.iteritems():
            # Check to see if we even have an instrument with this property number
            if property_no not in self.property_map:
                continue

            # Get the data product listings for this instrument
            data_products = self.property_map[property_no]
            # Go through each data product and update the data IF
            #  - There is a set of parameters that match those in the calibration

            for data_product in data_products:
                self.update_data_product(data_product, calibration_update)

    def update_data_product(self, data_product, calibration_update):
        parameters = [p.name for p in self.data_product_management.get_data_product_parameters(data_product)]

        dataset_updates = []
        for cal_name in calibration_update.iterkeys():
            if cal_name in parameters:
                dataset_id = self.dataset_for_data_product(data_product)
                dataset_updates.append(dataset_id)


        for dataset in dataset_updates:
            self.apply_to_dataset(dataset, calibration_update)

    def apply_to_dataset(self, dataset, calibration_update):
        cov = DatasetManagementService._get_coverage(dataset, mode='r+')
        try:
            self.set_sparse_values(cov, calibration_update)
            self.publish_calibration_event(dataset, calibration_update.keys())

        finally:
            cov.close()

    def set_sparse_values(self, cov, calibration_update):
        for calibration_name, updates in calibration_update.iteritems():
            if calibration_name not in cov.list_parameters():
                continue

            for update in updates:
                np_dict = {}
                self.check_units(cov, calibration_name, update['units'])
                start_date = self.ntp_from_iso(update['start_date'])
                np_dict[calibration_name] = ConstantOverTime(calibration_name, update['value'], time_start=start_date)

                cov.set_parameter_values(np_dict)


    def check_units(self, cov, calibration_name, units):
        pass

    def publish_calibration_event(self, dataset, calibrations):
        publisher = EventPublisher(OT.DatasetCalibrationEvent)
        publisher.publish_event(origin=dataset, calibrations=calibrations)

    def ntp_from_iso(self, iso):
        return TimeUtils.ntp_from_iso(iso)

    def process_csv(self, fuc):

        # CSV file open here
        csv_filename = fuc.get('path', None)
        if csv_filename is None:
            raise BadRequest("uploaded file has no path")

        # keep track of the number of calibrations we actually process
        nupdates = 0

        updates = {} # keys are reference_designators, use to update object store after parsing CSV

        with open(csv_filename, 'rb') as csvfile:
            # eliminate blank lines
            csvfile = (row for row in csvfile if len(row.strip()) > 0)
            # eliminate commented lines
            csvfile = (row for row in csvfile if not row.startswith('#'))
            # open CSV reader
            csv_reader = csv.reader(csvfile, delimiter=',') # skip commented lines
            # iterate the rows returned by csv.reader
            for row in csv_reader:
                if len(row) != 6:
                    log.warn("invalid calibration line %s" % ','.join(row))
                    continue
                try:
                    ipn = row[0] # instrument_property_number
                    name = row[1] # calibration_name
                    value = float(row[2]) # calibration_value
                    units = row[3]
                    description = row[4] # description
                    start_date = row[5] # start_date TODO date object?
                    d = {
                        'value':value,
                        'units':units,
                        'description':description,
                        'start_date':start_date
                    }
                except ValueError as e:
                    continue #TODO error message? or just skip?
                # get ipn key
                if ipn not in updates:
                    updates[ipn] = {} # initialize empty array
                if name not in updates[ipn]:
                    updates[ipn][name] = [] # will be array of dicts
                updates[ipn][name].append(d)
                
                nupdates = nupdates + 1

        self.do_something_with_the_update(updates)
        # insert the updates into object store
        self.update_object_store(updates)

        # update FileUploadContext object (change status to complete)
        fuc['status'] = 'UploadCalibrationProcessing process complete - %d updates added to object store' % nupdates
        self.object_store.update_doc(fuc)

        # remove uploaded file
        try:
            os.remove(csv_filename)
        except OSError:
            pass # TODO take action to get this removed
    
    def process_zip(self,fuc):
        pass

    def update_object_store(self, updates):
        '''inserts the updates into object store'''
        for i in updates: # loops the instrument_property_number(ipn) in the updates object
            try: # if i exists in object_store, read it                           
                ipn = self.object_store.read(i) #TODO: what will this be?
            except: # if does not yet exist in object_store, create it (can't use update_doc because need to set id)
                ipn = self.object_store.create_doc({'_type':'CALIBRATION'},i) # CAUTION: this returns a tuple, not a dict like read() returns
                ipn = self.object_store.read(i) # read so we have a dict like we expect
            # merge all from updates[i] into dict destined for the object_store (ipn)
            for name in updates[i]: # loops the calibration_names under each IPN in updates
                #TODO: if name not initialized, will append work? if so, can use same op for both
                if name not in ipn: # if name doesn't exist, we can just add the entire object (dict of lists)
                    ipn[name] = updates[i][name]
                else: # if it does, we need to append to each of the lists
                    ipn[name].append(updates[i][name]) # append the list from updates
            # store updated ipn keyed object in object_store (should overwrite full object, contains all previous too)
            self.object_store.update_doc(ipn)
            # publish ResetQCEvent event (one for each instrument_property_number [AKA ipn])
            self.event_publisher.publish_event(origin=i)


'''
{
    "property" : {
        "cc_black" : [(start, value)],
        "cc_white" : [(start, value), (start, value)]
    },
    "property" : { 
        ...
    }
}
'''
