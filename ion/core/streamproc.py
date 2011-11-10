#!/usr/bin/env python

"""Base classes and utils for stream processes"""

__author__ = 'Michael Meisinger'

from pyon.public import CFG
from pyon.util.log import log

from interface.services.istream_process import BaseStreamProcess

class StreamProcess(BaseStreamProcess):

    def process(self, packet):
        pass
