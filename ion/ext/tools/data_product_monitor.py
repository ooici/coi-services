import sys
import os.path
import json, time
import urllib, urllib2

dataproducts_to_monitor_file = ""
polling_delay = 60.0 # In seconds


def data_product_monitor():

    global polling_delay
    global dataproducts_to_monitor_file

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

    # Read the list of dataproducts into a list
    with open(dataproducts_to_monitor_file) as f:
        data_product_ids = f.read().splitlines()

    # @MAurice. Override the last_since_timestamp variable with last recorded value
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

        # Need to add error handling for when no valid data is returned by the http response

        # @Maurice - Here you will have a dictionary (events_data) updated
        # every X seconds with the information for every data product id's events.

        #print ">>>>>>>>> events_data :", events_data


        # Wait for some time and start polling for event data again
        time.sleep(polling_delay)

    return

if __name__ == "__main__":
    data_product_monitor()