#!/usr/bin/env python

from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
#from pydap.xdr import DapPacker
from pyon.util.breakpoint import breakpoint
from pydap.model import *
import numpy as np
class TestPydap(PyonTestCase):

    def setUp(self):
        pass

    @attr('UTIL')
    def test_sequences(self):
        s = SequenceType('s')
        i = BaseType('i', data=np.arange(10), type='i')
        c = BaseType('c', data=['hi'] * 10, type='S')
        f = BaseType('f', data=np.arange(20,30), type='f')
        d = BaseType('d', data=np.arange(30,40), type='d')
        b = BaseType('b', data=np.arange(40,50), type='B')
        s['i'] = i
        s['c'] = c
        s['f'] = f
        s['d'] = d
        s['b'] = b
        breakpoint(locals(), globals())
