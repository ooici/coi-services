__author__ = 'Bill French'

from ion.processes.idk.nose_test import NoseTest
from ion.processes.idk.metadata import Metadata

def run():
    app = NoseTest(Metadata())
    app.run()
   

if __name__ == '__main__':
    run()
