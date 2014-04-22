#!/usr/bin/env python
'''
@author Brian McKenna <bmckenna@asascience.com>
@file ion/processes/data/upload/upload_calibration_processing.py
'''

from pyon.core.exception import BadRequest
from pyon.ion.process import ImmediateProcess
from pyon.util.log import log
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

        # run process
        self.process(fuc_id)

    def process(self,fuc_id):

        # get the Object (dict) containing details of the uploaded file
        fuc = self.object_store.read(fuc_id)

        if fuc['filetype'] == 'ZIP':
            raise BadRequest("ZIP format not determined by project scientists yet (2014-04-21)")
            #self.process_zip(fuc)
        else:
            self.process_csv(fuc)

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