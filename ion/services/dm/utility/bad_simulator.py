from ion.services.dm.test.dm_test_case import Streamer, ParameterHelper
import time
import numpy as np

class BadSimulator(Streamer):
    def publish(self):
        rdt = ParameterHelper.rdt_for_data_product(self.data_product_id)
        rdt['time'] = np.random.random(1) * 3600 * 24 + 3587463242.7904043
        rdt['temp'] = [np.random.random(1) * 5 + 24]
        rdt['port_timestamp'] = [self.i] 
        rdt['driver_timestamp'] = [self.i] 
        #rdt['preferred_timestamp'] = ['driver_timestamp']
        #rdt['battery_voltage'] = [10.0] 

        ParameterHelper.publish_rdt_to_data_product(self.data_product_id, rdt)
        self.i += 1

    def run(self):
        while not self.finished.wait(self.interval):
            self.publish()

