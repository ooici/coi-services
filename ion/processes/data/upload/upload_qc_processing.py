#!/usr/bin/env python
'''
@author Brian McKenna <bmckenna@asascience.com>
@file ion/processes/data/transforms/qc_post_processing.py
@date Wed Apr  2 11:49:10 EDT 2014
'''

from pyon.core.exception import BadRequest
from pyon.ion.process import ImmediateProcess
from pyon.util.log import log
import time
import re
import os
import csv

class UploadQcProcessing(ImmediateProcess):
    '''
    Upload QC Processing Process

    This process provides the capability to ION clients and operators to process uploaded data to create
    data products.

    This parameters that this process accepts as configurations are:
        - fuc_id: The FileUploadContext identifier, required, stores where the file was written

    '''

    def on_start(self):

        ImmediateProcess.on_start(self)

        # necessary arguments, passed in via configuration kwarg to schedule_process. process namespace to avoid collisions
        fuc_id = self.CFG.get_safe('process.fuc_id',None) # FileUploadContext ID

        # run process
        self.process(fuc_id)

    def process(self,fuc_id):

        # clients we'll need
        object_store = self.container.object_store

        # get the Object (dict) containing details of the uploaded file
        fuc = object_store.read(fuc_id)

        # CSV file open here
        csv_filename = fuc.get('path', None)
        if csv_filename is None:
            raise BadRequest("uploaded file has no path")

        # keep track of the number of updates we actually process
        nupdates = 0

        updates = {} # keys are reference_designators, use to update object store after parsing CSV

        with open(csv_filename, 'rb') as csvfile:
            '''
            This filter loads entire file into memory before ignoring commented lines
            It's intended that the QC won't stress the memory here, however, 
            if they do, try generator expression eg. (row for row in fp if not row.startswith('#'))
            '''
            csv_reader = csv.reader(filter(lambda row: row[0]!='#', csvfile), delimiter=',') # skip commented lines
            # iterate the rows returned by csv.reader
            for row in csv_reader:
                qc_type = row[0]
                rd = row[1]
                dp = row[2]
                # get rd key
                if rd not in updates:
                    updates[rd] = {} # initialize empty reference_designator dict (to contain data_products)
                if dp not in updates[rd]:
                    updates[rd][dp] = { # initialize the supported tests, each with empty lists
                        'global_range':[],
                        'stuck_value':[],
                        'trend_test':[],
                        'spike_test':[]
                    }
                # updates[rd][dp] object is now available to have QC 'tables' added (in dict form)
                # actually process the row (global|stuck|trend|spike)
                if qc_type == 'global_range':
                    if len(row) != 7:
                        log.warn("invalid global_range line %s" % ','.join(row))
                        continue
                    d = self.parse_global_range(row)
                    updates[rd][dp]['global_range'].append(d)
                elif qc_type == "stuck_value":
                    if len(row) != 7:
                        log.warn("invalid stuck_value line %s" % ','.join(row))
                        continue
                    d = self.parse_stuck_value(row)
                    updates[rd][dp]['stuck_value'].append(d)
                elif qc_type == "trend_test":
                    if len(row) != 8:
                        log.warn("invalid trend_test line %s" % ','.join(row))
                        continue
                    d = self.parse_trend_test(row)
                    updates[rd][dp]['trend_test'].append(d)
                elif qc_type == "spike_test":
                    if len(row) != 8:
                        log.warn("invalid spike_test line %s" % ','.join(row))
                        continue
                    d = self.parse_spike_test(row)
                    updates[rd][dp]['spike_test'].append(d)
                else:
                    log.warn("unknown QC type %s" % qc_type)
                    continue

                nupdates = nupdates + 1

        # insert the updates into object store
        for r in updates: # loops the reference_designators in the updates object
            try: # if reference_designator exists in object_store, read it                           
                rd = object_store.read(r)
            except: # if does not yet exist in object_store, create it (can't use update_doc because need to set id)
                rd = object_store.create_doc({},r) # CAUTION: this returns a tuple, not a dict like read() returns
                rd = object_store.read(r) # read so we have a dict like we expect
            # merge all from updates[r] into dict destined for the object_store (rd)
            for dp in updates[r]: # loops the dataproducts under each reference_designator in updates
                if dp not in rd: # if dp doesn't exist, we can just add the entire object (dict of lists)
                    rd[dp] = updates[r][dp]
                else: # if it does, we need to append to each of the lists
                    for qc in updates[r][dp]:
                        if qc not in rd[dp]:
                            rd[dp][qc] = [] # initialize (these should always be initialized, but to be safe)
                        rd[dp][qc].append(updates[r][dp][qc]) # append the list from updates
            # store updated reference_designator keyed object in object_store (should overwrite full object)
            object_store.update_doc(rd)

        fuc['status'] = 'UploadQcProcessing process complete - %d updates added to object store' % nupdates
        object_store.update_doc(fuc)

        # remove uploaded file
        try:
            os.remove(csv_filename)
        except OSError:
            pass # TODO take action to get this removed

    def parse_common(self, row, d=None):
        if not d:
            d={}
        d.update({
            'units':row[3],
            'author':row[4],
            'ts_created':time.time()
        })
        return d

    def parse_global_range(self, row, d=None):
        if not d:
            d={}
        d = self.parse_common(row,d)
        d.update({
            'min_value':row[5],
            'max_value':row[6]
        })
        return d

    def parse_stuck_value(self, row, d=None):
        if not d:
            d={}
        d = self.parse_common(row,d)
        d.update({
            'resolution':row[5],
            'consecutive_values':row[6]
        })
        return d

    def parse_trend_test(self, row, d=None):
        if not d:
            d={}
        d = self.parse_common(row,d)
        d.update({
            'sample_length':row[5],
            'polynomial_order':row[6],
            'standard_deviation':row[7]
        })
        return d

    def parse_spike_test(self, row, d=None):
        if not d:
            d={}
        d = self.parse_common(row,d)
        d.update({
            'accuracy':row[5],
            'range_multiplier':row[6],
            'window_length':row[7]
        })
        return d
