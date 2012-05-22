__author__ = 'Bill French'

import argparse

from ion.idk.start_driver import IDKRebase

def run():
    app = IDKRebase()
    app.run()
   

if __name__ == '__main__':
    run()
