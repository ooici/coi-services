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
    Array,Instrument Class,Reference Designator,Data Product In,Time interval length in days,Polynomial order, Standard deviation reduction factor (nstd)

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
            origin: Data Product In
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
        key = '_'.join([row['Reference Designator'], row['Data Product In']])
        document = {}
        document['array']                = row['Array']
        document['instrument_class']     = row['Instrument Class']
        document['reference_designator'] = row['Reference Designator']
        document['data_product_in']      = row['Data Product In']
        document['time_interval']        = row['Time interval length in days']
        document['polynomial_order']     = row['Polynomial Order']
        document['standard_deviation']   = float(row['Standard deviation reduction factor (nstd)'])
        yield key,document
    return
