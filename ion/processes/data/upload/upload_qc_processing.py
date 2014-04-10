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
from zipfile import ZipFile

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

        # Clients
        self.object_store = self.container.object_store

        # run process
        self.process(fuc_id)

    def process(self,fuc_id):

        # get the Object (dict) containing details of the uploaded file
        fuc = self.object_store.read(fuc_id)

        if fuc['filetype'] == 'ZIP':
            self.process_zip(fuc)
        else:
            self.process_csv(fuc)

    def process_csv(self, fuc):

        # CSV file open here
        csv_filename = fuc.get('path', None)
        if csv_filename is None:
            raise BadRequest("uploaded file has no path")

        # keep track of the number of updates we actually process
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
                qc_type = row[0]
                rd = row[1]
                dp = row[2]
                # get rd key
                if rd not in updates:
                    updates[rd] = {} # initialize empty reference_designator dict (to contain data_products)
                if dp not in updates[rd]:
                    updates[rd][dp] = {}
                # updates[rd][dp] object is now available to have QC 'tables' added (in dict form)
                # actually process the row (global|stuck|trend|spike|gradient)
                if qc_type == 'global_range':
                    if len(row) != 7:
                        log.warn("invalid global_range line %s" % ','.join(row))
                        continue
                    d = self.parse_global_range(row)
                    if d is None:
                        log.warn("parse global_range line failed %s" % ','.join(row))
                        continue
                    if 'global_range' not in updates[rd][dp]:
                        updates[rd][dp]['global_range'] = []
                    updates[rd][dp]['global_range'].append(d)
                elif qc_type == "stuck_value":
                    if len(row) != 7:
                        log.warn("invalid stuck_value line %s" % ','.join(row))
                        continue
                    d = self.parse_stuck_value(row)
                    if d is None:
                        log.warn("parse stuck_value line failed %s" % ','.join(row))
                        continue
                    if 'stuck_value' not in updates[rd][dp]:
                        updates[rd][dp]['stuck_value'] = []
                    updates[rd][dp]['stuck_value'].append(d)
                elif qc_type == "trend_test":
                    if len(row) != 8:
                        log.warn("invalid trend_test line %s" % ','.join(row))
                        continue
                    d = self.parse_trend_test(row)
                    if d is None:
                        log.warn("parse trend_test line failed %s" % ','.join(row))
                        continue
                    if 'trend_test' not in updates[rd][dp]:
                        updates[rd][dp]['trend_test'] = []
                    updates[rd][dp]['trend_test'].append(d)
                elif qc_type == "spike_test":
                    if len(row) != 8:
                        log.warn("invalid spike_test line %s" % ','.join(row))
                        continue
                    d = self.parse_spike_test(row)
                    if d is None:
                        log.warn("parse spike_test line failed %s" % ','.join(row))
                        continue
                    if 'spike_test' not in updates[rd][dp]:
                        updates[rd][dp]['spike_test'] = []
                    updates[rd][dp]['spike_test'].append(d)
                elif qc_type == "gradient_test":
                    if len(row) != 10:
                        log.warn("invalid gradient_test line %s" % ','.join(row))
                        continue
                    d = self.parse_gradient_test(row)
                    if d is None:
                        log.warn("parse gradient_test line failed %s" % ','.join(row))
                        continue
                    if 'gradient_test' not in updates[rd][dp]:
                        updates[rd][dp]['gradient_test'] = []
                    updates[rd][dp]['gradient_test'].append(d)
                else:
                    log.warn("unknown QC type %s" % qc_type)
                    continue

                nupdates = nupdates + 1

        # insert the updates into object store
        self.update_object_store(updates)

        # update FileUploadContext object (change status to complete)
        fuc['status'] = 'UploadQcProcessing process complete - %d updates added to object store' % nupdates
        self.object_store.update_doc(fuc)

        # remove uploaded file
        try:
            os.remove(csv_filename)
        except OSError:
            pass # TODO take action to get this removed

    def process_zip(self,fuc):

        # ZIP file open here
        zip_filename = fuc.get('path', None)
        if zip_filename is None:
            raise BadRequest("uploaded file has no path")

        # keep track of the number of updates we actually process
        nupdates = 0

        updates = {} # keys are reference_designators, use to update object store after parsing CSV

        with ZipFile(zip_filename) as zipfile:
            files = zipfile.namelist()
            t = {} # dict to store tables, keyed by filename
            # first, we extract out all the tables (not master.csv)
            for tablefile in files:
                if tablefile.lower() == 'master.csv':
                    continue # skip here, we'll process after we've read the rest of the files
                with zipfile.open(tablefile) as csvfile:
                    csvfile = (row for row in csvfile if len(row.strip()) > 0)
                    csvfile = (row for row in csvfile if not row.startswith('#'))
                    d = [row for row in csv.DictReader(csvfile, delimiter=',')]
                    t[tablefile] = self.transpose_list_of_dicts(d)
            with zipfile.open('master.csv') as csvfile:
                csvfile = (row for row in csvfile if len(row.strip()) > 0)
                csvfile = (row for row in csvfile if not row.startswith('#'))
                csv_reader = csv.reader(csvfile, delimiter=',') # skip commented lines
                # iterate the rows returned by csv.reader
                csv_reader.next() # skip the header line (specified in ZIP format)
                for row in csv_reader:
                    if len(row) != 5:
                        log.warn("invalid local_range line %s" % ','.join(row))
                        continue
                    # we need to add a value to the front of list to use parse_common
                    row.insert(0,'local_range') # ignored below but corrects indexes used in parse_common
                    rd = row[1]
                    dp = row[2]
                    d = self.parse_common(row)
                    # get table specified by row
                    table = t.get(row[5], None)
                    if table is None:
                        log.warn("no tablefile found for local_range %s" % ','.join(row))
                        continue
                    # check table keys all have same len
                    n = [len(filter(None, table[key])) for key in table.keys()] # counts non-empty elements in columns
                    if not len(n) > 0 and all(n[0] == x for x in n): # more than zero columns and all same length
                        log.warn("invalid tablefile found for local_range %s" % ','.join(row))
                        continue
                    d['table'] = table
                    # get rd key
                    if rd not in updates:
                        updates[rd] = {} # initialize empty reference_designator dict (to contain data_products)
                    if dp not in updates[rd]:
                        updates[rd][dp] = {}
                    if 'local_range' not in updates[rd][dp]:
                        updates[rd][dp]['local_range'] = []
                    updates[rd][dp]['local_range'].append(d)

                    nupdates = nupdates + 1

        # insert the updates into object store
        self.update_object_store(updates)

        # update FileUploadContext object (change status to complete)
        fuc['status'] = 'UploadQcProcessing process complete - %d updates added to object store' % nupdates
        self.object_store.update_doc(fuc)

        # remove uploaded file
        try:
            os.remove(zip_filename)
        except OSError:
            pass # TODO take action to get this removed

    def update_object_store(self, updates):
        '''inserts the updates into object store'''
        for r in updates: # loops the reference_designators in the updates object
            try: # if reference_designator exists in object_store, read it                           
                rd = self.object_store.read(r)
            except: # if does not yet exist in object_store, create it (can't use update_doc because need to set id)
                rd = self.object_store.create_doc({},r) # CAUTION: this returns a tuple, not a dict like read() returns
                rd = self.object_store.read(r) # read so we have a dict like we expect
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
            self.object_store.update_doc(rd)

    def parse_common(self, row, d=None):
        if not d:
            d={}
        d.update({
            'units':row[3], #str
            'author':row[4], #str
            'ts_created':time.time() #float
        })
        return d

    def parse_global_range(self, row, d=None):
        try:
            min_value = float(row[5])
            max_value = float(row[6])
        except ValueError as e:
            return None
        if not d:
            d={}
        d = self.parse_common(row,d)
        d.update({
            'min_value':min_value, #float
            'max_value':max_value #float
        })
        return d

    def parse_stuck_value(self, row, d=None):
        try:
            resolution = float(row[5])
            consecutive_values = int(row[6])
        except ValueError as e:
            return None
        if not d:
            d={}
        d = self.parse_common(row,d)
        d.update({
            'resolution':resolution, #float
            'consecutive_values':consecutive_values #int
        })
        return d

    def parse_trend_test(self, row, d=None):
        try:
            sample_length = int(row[5])
            polynomial_order = int(row[6])
            standard_deviation = float(row[7])
        except ValueError as e:
            return None
        if not d:
            d={}
        d = self.parse_common(row,d)
        d.update({
            'sample_length':sample_length, #int
            'polynomial_order':polynomial_order, #int
            'standard_deviation':standard_deviation #float
        })
        return d

    def parse_spike_test(self, row, d=None):
        try:
            accuracy = float(row[5])
            range_multiplier = int(row[6])
            window_length = int(row[7])
        except ValueError as e:
            return None
        if not d:
            d={}
        d = self.parse_common(row,d)
        d.update({
            'accuracy':accuracy,
            'range_multiplier':range_multiplier,
            'window_length':window_length
        })
        return d

    def parse_gradient_test(self, row, d=None):
        try:
            xunits = row[5]
            ddatdx = [float(x) for x in row[6].strip('[]').split()]
            mindx = float(row[7])
            try:
                startdat = float(row[8])
            except:
                startdat = None # can be blank
            toldat = float(row[9])
        except ValueError as e:
            return None
        if not d:
            d={}
        d = self.parse_common(row,d)
        d.update({
            'xunits':xunits,
            'ddatdx':ddatdx,
            'mindx':mindx,
            'startdat':startdat,
            'toldat':toldat
        })
        return d

    def transpose_list_of_dicts(self, list_of_dicts):
        '''assumes all dicts in the list have the same keys'''
        keys = list_of_dicts[0].iterkeys()
        try:
            return {key: [float(d[key]) for d in list_of_dicts] for key in keys}
        except:
            return None
