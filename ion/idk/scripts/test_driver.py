__author__ = 'Bill French'

import argparse

from ion.idk.nose_test import NoseTest
from ion.idk.metadata import Metadata

def run():
    app = NoseTest(Metadata())
    opts = parseArgs()

    if( opts.logger ):
        launch_logger_window();
    
    if( opts.stream ):
        launch_stream_window();
    
    if( opts.unit ):
        app.report_header()
        app.run_unit()
    elif( opts.integration ):
        app.report_header()
        app.run_integration()
    elif( opts.qualification ):
        app.report_header()
        app.run_qualification()
    else:
        app.run()
   
def launch_logger_window():
    pass

def launch_stream_window():
    pass

def parseArgs():
    parser = argparse.ArgumentParser(description="IDK Start Driver")
    parser.add_argument("-u", dest='unit', action="store_true",
                        help="only run unit tests" )
    parser.add_argument("-i", dest='integration', action="store_true",
                        help="only run integration tests" )
    parser.add_argument("-q", dest='qualification', action="store_true",
                        help="only run qualification tests" )
    parser.add_argument("-l", dest='logger', action="store_true",
                        help="launch a window with test log output" )
    parser.add_argument("-s", dest='stream', action="store_true",
                        help="launch a window monitoring port agent sniffer" )
    return parser.parse_args()


if __name__ == '__main__':
    run()
