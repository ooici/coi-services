#!/usr/bin/env python

"""Parsing of XLS files"""

__author__ = 'Michael Meisinger'

import csv
import StringIO
import xlrd


class XLSParser(object):
    """Class that transforms an XLS file into a dict of csv files (str)"""

    def extract_csvs(self, file_content):
        sheets = self.extract_worksheets(file_content)
        csv_docs = {}
        for sheet_name, sheet in sheets.iteritems():
            csv_doc = self.dumps_csv(sheet)
            csv_docs[sheet_name] = csv_doc.splitlines()
#            csv_doc = self.dumps_csv_list(sheet)
#            csv_docs[sheet_name] = csv_doc
        return csv_docs

    def extract_worksheets(self, file_content):
        book = xlrd.open_workbook(file_contents=file_content)
        sheets = {}
        formatter = lambda(t,v): self.format_excelval(book,t,v,False)

        for sheet_name in book.sheet_names():
            raw_sheet = book.sheet_by_name(sheet_name)
            data = []
            for row in range(raw_sheet.nrows):
                (types, values) = (raw_sheet.row_types(row), raw_sheet.row_values(row))
                data.append(map(formatter, zip(types, values)))
            sheets[sheet_name] = data
        return sheets

    def dumps_csv(self, sheet):
        stream = StringIO.StringIO()
        csvout = csv.writer(stream, delimiter=',', doublequote=False, escapechar='\\')
        csvout.writerows( map(self.utf8ize, sheet) )
        csv_doc = stream.getvalue()
        stream.close()
        return csv_doc

    def dumps_csv_list(self, sheet):
        cvs_lines = []
        for line in sheet:
            stream = StringIO.StringIO()
            csvout = csv.writer(stream, delimiter=',', doublequote=False, escapechar='\\')
            csvout.writerow(self.utf8ize(line))
            csv_doc = stream.getvalue()
            stream.close()
            cvs_lines.append(csv_doc)
        return cvs_lines

    def tupledate_to_isodate(self, tupledate):
        (y,m,d, hh,mm,ss) = tupledate
        nonzero = lambda n: n!=0
        date = "%04d-%02d-%02d"  % (y,m,d)    if filter(nonzero, (y,m,d))                else ''
        time = "T%02d:%02d:%02d" % (hh,mm,ss) if filter(nonzero, (hh,mm,ss)) or not date else ''
        return date+time

    def format_excelval(self, book, type, value, wanttupledate):
        if   type == 2: # TEXT
            if value == int(value): value = int(value)
        elif type == 3: # NUMBER
            datetuple = xlrd.xldate_as_tuple(value, book.datemode)
            value = datetuple if wanttupledate else self.tupledate_to_isodate(datetuple)
        elif type == 5: # ERROR
            value = xlrd.error_text_from_code[value]
        return value

    def utf8ize(self, l):
        return [unicode(s).encode("utf-8") if hasattr(s,'encode') else s for s in l]


