#!/usr/bin/env python
'''
@author Tim G
@date Wed Mar 27
'''

from csv import DictReader
from StringIO import StringIO

def trend_parser(document):
    '''
    This parser YIELDS a document per call or until it's done
    The format for the document is CSV in this table format
    Array,Instrument Class,Reference Designator,Data Products,Time interval length in days,Polynomial order,Standard deviation reduction factor (nstd)
    

    Document Schema:
        array:
            origin: Array
            type: String
        instrument_class:
            origin: Instrument Class
            type: String
        reference_designator:
            origin: Reference Designator
            type: String
        data_product_in:
            origin: Data Products
            type: String
        time_interval:
            origin: Time interval length in days
            type: float64
        polynomial_order:
            origin: Polynomial order
            type: String
        standard_deviation:
            origin: Standard deviation reduction factor (nstd)
            type: float64

    '''
    sio = StringIO()
    sio.write(document)
    sio.seek(0)


    dr = DictReader(sio)
    for row in dr:
        key = '_'.join(['trend', row['Reference Designator'], row['Data Products']])
        document = {}
        document['array']                = row['Array']
        document['instrument_class']     = row['Instrument Class']
        document['reference_designator'] = row['Reference Designator']
        document['data_product_in']      = row['Data Products']
        document['time_interval']        = float(row['Time interval length in days'])
        document['polynomial_order']     = float(row['Polynomial order'])
        document['standard_deviation']   = float(row['Standard deviation reduction factor (nstd)'])
        yield key,document
    return
