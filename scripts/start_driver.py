__author__ = 'Bill French'

import argparse

from ion.processes.idk.start_driver import StartDriver

def run():
    app = StartDriver()
    opts = parseArgs()

    if( opts.overwrite ):
        app.overwrite()
    else:
        app.run()
   

def parseArgs():
    parser = argparse.ArgumentParser(description="IDK Start Driver")
    parser.add_argument("--overwrite", dest='overwrite', action="store_true",
                        help="rewrite generated files from current metadata" )
    return parser.parse_args()


if __name__ == '__main__':
    run()
