import sys
import os.path
import json, time
import urllib, urllib2

# $ pip install requests
import requests

#################################
# Configuration Parameters
#################################

dataproducts_to_monitor_file = ""

polling_delay = 60.0 # In seconds

chunk_size = 4096

data_folder = '/Users/mauricemanning/Documents/Dev/data'

temporal_axis_param_name = 'time'

timestamp_file_name = 'last_update_check.txt'

# erddap_url = 'http://erddap-test.oceanobservatories.org:8080/erddap/tabledap/'
erddap_url = 'http://localhost:8181/erddap/tabledap/'

# data product ids in erddap are prefixed with 'data' becuase erddap does not support ids that begin with a digit
# if the ids in the dataproducts_to_monitor_file already have this prefix appended then this should be set to ''
erddap_id_prefix = 'data'

#################################
# Utility Functions
#################################

def _verify_data_folder():
    #verify that the data folder exists
    valid = os.path.isdir(data_folder)
    if not valid:
        print 'Data folder does not exist: ' + data_folder + '. Set this path in the config section of this script'
        sys.exit(2)
    return

def _verify_data_product_folder(data_product_id):
    #verify that the data folder for this data product if exists
    data_product_folder = data_folder + "/" + data_product_id
    if not (os.path.exists(data_product_folder)  and os.path.isdir(data_product_folder)):
        print 'Data product folder does not exist: ' + data_folder
        try:
            os.makedirs(data_product_folder)
        except OSError:
            raise
    return

def _compute_since_timestamp(since_timestamp, current_timestamp):

    since_timestamp_gmt = time.gmtime(since_timestamp/1000)
    current_timestamp_gmt = time.gmtime(current_timestamp/1000)
    # format the timestamp to be used in the erddap request string
    since_timestamp_format = time.strftime('%Y-%m-%dT%H:%M:%SZ', since_timestamp_gmt )
    current_timestamp_format = time.strftime('%Y-%m-%dT%H:%M:%SZ', current_timestamp_gmt )
    print "since_timestamp_format time: ", since_timestamp_format
    print "current_timestamp_format time: ", current_timestamp_format

    return since_timestamp_format, current_timestamp_format


def _construct_data_request(data_product_id, since_timestamp, current_timestamp):
    # example url request:
    # http://erddap-test.oceanobservatories.org:8080/erddap/tabledap/data0ce03ea9472e4b619e55c0e3bdec1974.nc?&orderBy%28%22time%22%29&time%3E2013-07-19T18:36:46&time%3C=2013-07-19T22:52:03
    order_by_tag = '.nc?&orderBy%28%22time%22%29'

    # &time%3E2013-07-19T18:36:46&time%3C=2013-07-19T22:52:03
    #  time bounds is: > since_timestamp and <= current_timestamp
    #time_bounds_tag = ''.join( ['&time%3E',  since_timestamp,  '&time%3C=', current_timestamp] )

    time_bounds_tag = ''.join( ['&', temporal_axis_param_name, '%3E',  since_timestamp,  '&', temporal_axis_param_name, '%3C=', current_timestamp] )
    request = ''.join( [erddap_url, erddap_id_prefix, data_product_id, order_by_tag, time_bounds_tag] )
    print "request: ", request
    return request

def get_file(data_product_id, current_timestamp):
    current_timestamp = current_timestamp / 1000
    filename =  ''.join( [data_folder, "/",  data_product_id, "/", str(current_timestamp)] )
    print "filename: ", filename
    return filename

def write_data(filename, result):
    with open(filename, 'wb') as fd:
        for chunk in result.read(chunk_size):
            fd.write(chunk)
    return



#################################
# Data Product Monitor
#################################

def data_product_monitor():

    global polling_delay
    global dataproducts_to_monitor_file

    # validate command line
    # example: python data_product_monitor.py http://127.0.0.1:5000 data_products_to_monitor.txt 3
    if len(sys.argv) < 3:
        print "Usage: python ", sys.argv[0], " <Server Address> <List of data product ids> <polling time period>"
        print "  Command line parameters:\n  -----------------------\n"
        print "  Server Address             : Specified as http://server:port"
        print "  List of data product ids   : Path to a file containing a list of ids, one per line."
        print "  Polling time period        : (Optional) How often the script checks for new events. Specified in seconds. Defaults to", \
                polling_delay, " secs."

        print "\n"
        return

    server_addr = sys.argv[1]
    dataproducts_to_monitor_file = sys.argv[2]
    if len(sys.argv) > 3:
        polling_delay = float(sys.argv[3])

    # check if the file exists
    if not os.path.isfile(dataproducts_to_monitor_file):
        print "Please provide list of data products in ", dataproducts_to_monitor_file, "\n"
        return

    print "Using list in ", dataproducts_to_monitor_file

    #check that data folder exists
    _verify_data_folder()

    # Read the list of data products into a list
    with open(dataproducts_to_monitor_file) as f:
        data_product_ids = f.read().splitlines()

    # Override the last_since_timestamp variable with last recorded value
    # Or if there is no last recorded value, leave it as None
    last_since_timestamp = None
    current_timestamp = None
    since_timestamp = None

    # Now that we have the list of data product IDs, get events for each
    # Poll every few seconds
    url = server_addr + "/ion-service/data_product_management/get_data_product_updates"
    while True:
        current_timestamp = int(time.time() * 1000)

        # Calculate the since_timestamp for the events query
        if (last_since_timestamp == None):
            since_timestamp = int(current_timestamp - polling_delay * 1000)
        else:
            if (current_timestamp - last_since_timestamp) > 60 * 60 * 1000:
                print "\nLooks like the last check for updates was over an hour ago."
                print "Maybe your machine was in sleep mode. Restarting the check for updates"
                since_timestamp = int(current_timestamp - polling_delay * 1000)
            else:
                since_timestamp = int(last_since_timestamp)

        #print ">>>>>>>>>>since_timestamp : ", str(since_timestamp)
        #print ">>>>>>>>>>current_timestamp : ", str(current_timestamp)
        last_since_timestamp = current_timestamp

        parameters = {"data_product_id_list" : data_product_ids,
                      "since_timestamp" : str(since_timestamp)}

        try:
            parameter_data = urllib.urlencode(parameters)
            req = urllib2.Request(url + "?" + parameter_data)
            response = urllib2.urlopen(req)
        except urllib2.URLError as e:
            print "Error fetching event information for data products : ", e.reason
            time.sleep(polling_delay)
            continue

        response_data = json.load(response)


        events_data = response_data["data"]["GatewayResponse"]

        # events_data is a dictionary (events_data) updated
        # every X seconds with the information for every data product id's events.

        print ">>>>>>>>> events_data :", events_data
        for data_product_id, value in events_data.iteritems():
            update_flag = value.get('updated', False)
            print ">>>>>>>>> update_flag :", update_flag

            #new data has arrived

            #verify that we have a folder for this data
            _verify_data_product_folder(data_product_id)

            if update_flag:
                start_time_tag, stop_time_tag = _compute_since_timestamp(since_timestamp, current_timestamp)

                request = _construct_data_request(data_product_id, start_time_tag, stop_time_tag)

                try:
                    #result = requests.get(request)
                    result = urllib2.urlopen(request)
                    print "request result: ", result

                    #construct a file for this update based on the current timestamp
                    filename = get_file(data_product_id, current_timestamp)
                    write_data(filename, result)

                except urllib2.URLError as e:
                    print "Error fetching data products : ", str(e)
                except:
                    from traceback import print_exc
                    excep_str = print_exc()
                    print "excep_str", excep_str

        # Wait for some time and start polling for event data again
        time.sleep(polling_delay)

    return

if __name__ == "__main__":
    data_product_monitor()