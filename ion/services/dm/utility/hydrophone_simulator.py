#!/usr/bin/env python
'''
@author Luke Campbell
@file ion/services/dm/utility/hydrophone_simulator.py
@description A simulator that publishes high-frequency dense data
'''
from ion.services.dm.test.dm_test_case import Streamer, ParameterHelper
import time
import numpy as np

class HydrophoneSimulator(Streamer):
    def publish(self):
        print 'publishing data'
        rdt = ParameterHelper.rdt_for_data_product(self.data_product_id)
        now = time.time()
        if self.simple_time:
            rdt['time'] = [self.i]
            t = self.i
        else:
            t = now + 2208988800
            rdt['time'] = [t]
        rdt['temp_sample'] = [np.random.random(1000000) * 5 + 20]

        ParameterHelper.publish_rdt_to_data_product(self.data_product_id, rdt)
        self.i += 1

    def run(self):
        while not self.finished.wait(self.interval):
            self.publish()

