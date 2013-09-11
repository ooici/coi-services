from ion.services.dm.test.dm_test_case import Streamer, ParameterHelper
import time
import numpy as np
class TMPSFSimulator(Streamer):

    def publish(self):
        rdt = ParameterHelper.rdt_for_data_product(self.data_product_id)
        now = time.time()
        if self.simple_time:
            rdt['time'] = [self.i]
            t = self.i
        else:
            rdt['time'] = np.array([now + 2208988800])
            t = now + 2208988800
        rdt['temperature'] = [np.random.random(24) * 5 + 20]
        rdt['port_timestamp'] = [t]
        rdt['driver_timestamp'] = [t]
        rdt['preferred_timestamp'] = ['driver_timestamp']
        rdt['date_time_string'] = ['something']
        rdt['battery_voltage'] = [10.0]

        ParameterHelper.publish_rdt_to_data_product(self.data_product_id, rdt)
        self.i += 1

    def run(self):
        while not self.finished.wait(self.interval):
            self.publish()

