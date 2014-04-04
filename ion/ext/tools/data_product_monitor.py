import sys
import os.path
import json, time
import urllib, urllib2
import inspect
import logging
import argparse


# to run this script with a delay interval of 5 seconds:
# python data_product_monitor.py -i 5


#################################
# Configuration Parameters
#################################

dataproducts_to_monitor_file = 'data_products_to_monitor.txt'

# gateway address is the url of the ion-ux web server
data_product_monitor_url = "http://localhost:3000/get_data_product_updates/"

# frequency to check for data product updates. recommended to not be less than 60 seconds
polling_delay = 60.0 # In seconds

chunk_size = 4096

# this folder will contain a set of subfolders, one for each data product that is being monitored
# the subfolders are named with the data product ids
data_folder = '/Users/mauricemanning/Documents/Dev/data'

# temporal_axis_param_name is the name of the parameter in the data product to use for subsetting update slices
temporal_axis_param_name = 'time'

# erddap_url = 'http://erddap-test.oceanobservatories.org:8080/erddap/tabledap/'
erddap_url = 'http://localhost:8181/erddap/tabledap/'

# download format
# may be any valid erddap type, default is NetCDF
# see download format descriptions here: http://erddap-test.oceanobservatories.org:8080/erddap/tabledap/documentation.html#fileType
download_format = '.nc'
#download_format = '.csv'

# data product ids in erddap are prefixed with 'data' because erddap does not support ids that begin with a digit
# if the ids in the data products_to_monitor_file already have this prefix appended then this should be set to ''
erddap_id_prefix = 'data'

#################################
# Utility Functions
#################################

def monitor_logger(file_level, console_level = None):
    function_name = inspect.stack()[1][3]
    logger = logging.getLogger(function_name)
    logger.setLevel(logging.DEBUG) #By default, logs all messages

    if console_level != None:
        ch = logging.StreamHandler() #StreamHandler logs to console
        ch.setLevel(console_level)
        ch_format = logging.Formatter('%(asctime)s - %(message)s')
        ch.setFormatter(ch_format)
        logger.addHandler(ch)

    fh = logging.FileHandler("{0}.log".format(function_name))
    fh.setLevel(file_level)
    fh_format = logging.Formatter('%(asctime)s - %(lineno)d - %(levelname)-8s - %(message)s')
    fh.setFormatter(fh_format)
    logger.addHandler(fh)

    return logger

def _verify_data_folder():
    #verify that the data folder exists
    valid = os.path.isdir(data_folder)
    if not valid:
        logger.warning('Data folder does not exist: ' + data_folder + '. Set this path in the config section of this script')
        sys.exit(2)
    return

def _verify_data_product_folder(data_product_id):
    #verify that the data folder for this data product if exists
    data_product_folder = data_folder + "/" + data_product_id
    if not (os.path.exists(data_product_folder)  and os.path.isdir(data_product_folder)):
        logger.warning('Data product folder does not exist: ' + data_folder )
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

    return since_timestamp_format, current_timestamp_format


def _construct_data_request(data_product_id, since_timestamp, current_timestamp):
    # example url request:
    # http://erddap-test.oceanobservatories.org:8080/erddap/tabledap/data0ce03ea9472e4b619e55c0e3bdec1974.nc?&orderBy%28%22time%22%29&time%3E2013-07-19T18:36:46&time%3C=2013-07-19T22:52:03
    order_by_tag = download_format + '?&orderBy%28%22time%22%29'

    #  time bounds is: > since_timestamp and <= current_timestamp
    time_bounds_tag = ''.join( ['&', temporal_axis_param_name, '%3E',  since_timestamp,  '&', temporal_axis_param_name, '%3C=', current_timestamp] )
    request = ''.join( [erddap_url, erddap_id_prefix, data_product_id, order_by_tag, time_bounds_tag] )
    return request

def get_file(data_product_id, current_timestamp):
    current_timestamp_gmt = time.gmtime(current_timestamp/1000)
    dtstr = time.strftime('%Y%m%d_%H%M%S', current_timestamp_gmt )
    filename =  ''.join( [data_folder, "/",  data_product_id, "/", dtstr] )
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
    global logger

    logger = monitor_logger(logging.DEBUG, logging.DEBUG)

    #set up optional arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--interval", type=int, help="polling interval")
    parser.add_argument("-f", "--file",  help="path to file with data product ids")
    # process command line
    args = parser.parse_args()
    if args.interval:
        polling_delay = args.interval

    if args.file:
        dataproducts_to_monitor_file = args.file

    print 'dataproducts_to_monitor_file  ', dataproducts_to_monitor_file
    print 'polling_delay  ', polling_delay

    # check if the file exists
    if not os.path.isfile(dataproducts_to_monitor_file):
        print "Please provide list of data products in ", dataproducts_to_monitor_file, "\n"
        return

    logger.debug('Using data products listed in this file : ' + str(dataproducts_to_monitor_file))


    #check that data folder exists
    _verify_data_folder()

    # Read the list of data products into a comma separated string
    file = open(dataproducts_to_monitor_file, 'r')
    data_product_ids_string = ''
    for line in file:
        line = line.strip(',\n')
        data_product_ids_string += line + ','
    data_product_ids_string = data_product_ids_string.strip(',')


    # Override the last_since_timestamp variable with last recorded value
    # Or if there is no last recorded value, leave it as None
    last_since_timestamp = None
    current_timestamp = None
    since_timestamp = None

    # Now that we have the list of data product IDs, get events for each
    # Poll every few seconds

    while True:
        current_timestamp = int(time.time() * 1000)

        # Calculate the since_timestamp for the events query
        if (last_since_timestamp == None):
            since_timestamp = int(current_timestamp - polling_delay * 1000)
        else:
            if (current_timestamp - last_since_timestamp) > 60 * 60 * 1000:
                logger.warning('\nLooks like the last check for updates was over an hour ago. Maybe your machine was in sleep mode. Restarting the check for updates ')
                since_timestamp = int(current_timestamp - polling_delay * 1000)
            else:
                since_timestamp = int(last_since_timestamp)

        logger.debug('since_timestamp : ' + str(since_timestamp))
        logger.debug('current_timestamp : ' + str(current_timestamp))

        last_since_timestamp = current_timestamp

        parameters = {"data_product_id_list" : data_product_ids_string,
                      "since_timestamp" : str(since_timestamp)}

        print "parameters:  ", str(parameters)
        try:
            data = urllib.urlencode(parameters)
            # call ion-ux web server to request information on data products
            request = urllib2.Request(data_product_monitor_url, data)

            response = urllib2.urlopen(request)
            logger.debug('request result:  : ' + str(response))

        except urllib2.URLError as e:
            print "Error fetching event information for data products : ", e.reason
            time.sleep(polling_delay)
            continue

        response_data = json.load(response)
        events_data = response_data["data"]

        # events_data is a dictionary (events_data) updated
        # every X seconds with the information for every data product id's events.

        logger.debug('events_data : ' + str(events_data))
        for data_product_id, value in events_data.iteritems():
            update_flag = value.get('updated', False)
            logger.debug('data_product_id : ' + data_product_id + '   update_flag : ' + str(update_flag))

            #new data has arrived

            #verify that we have a folder for this data
            _verify_data_product_folder(data_product_id)

            if update_flag:
                start_time_tag, stop_time_tag = _compute_since_timestamp(since_timestamp, current_timestamp)
                logger.debug('start_time_tag : ' + str(start_time_tag) +   '   stop_time_tag : ' + str(stop_time_tag)  )

                request = _construct_data_request(data_product_id, start_time_tag, stop_time_tag)
                logger.debug('Request for data update : ' + str(request))

                try:
                    #call ooici erddap service with request for data update
                    result = urllib2.urlopen(request)
                    logger.debug('request result:  : ' + str(result))

                    #construct a file for this update based on the current timestamp
                    filename = get_file(data_product_id, current_timestamp)
                    logger.debug('Placing data update in this file : ' + str(filename))
                    write_data(filename, result)

                except urllib2.URLError as e:
                    logger.warning('Error fetching data products : ' + str(e))
                except:
                    from traceback import print_exc
                    excep_str = print_exc()
                    logger.warning('Exception string : ' + str(excep_str))

        # Wait for some time and start polling for event data again
        time.sleep(polling_delay)

    return

if __name__ == "__main__":
    data_product_monitor()