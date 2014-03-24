import sys
import os.path
import ast
import json, time
import urllib, urllib2

dataproducts_to_monitor_file = "dataproducts_to_monitor.txt"
polling_delay = 5.0 # In seconds


def event_poll():

    if len(sys.argv) < 2:
        print "Usage: python ", sys.argv[0], " <server name:port> <polling time period in seconds (Default 5 secs)>"
        return

    server_addr = sys.argv[1]
    if len(sys.argv) > 2:
        polling_delay = float(sys.argv[2])

    # check if the file exists
    if not os.path.isfile(dataproducts_to_monitor_file):
        print "Please provide list of data products in ", dataproducts_to_monitor_file, "\n"
        return

    print "Using list in ", dataproducts_to_monitor_file

    dataset_names = []
    # Read the list of dataproducts into a list
    with open(dataproducts_to_monitor_file) as f:
        data_product_ids = f.read().splitlines()


    # Now that we have the list of data product IDs, get events for each
    # Poll every few seconds
    url = server_addr + "/ion-service/data_product_management/get_data_product_updates"
    while True:

        parameters = {"data_product_id_list" : data_product_ids,
                      "since_timestamp" : str(int(time.time() * 1000)-1000*60*60)}

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
        # Also change the since_timestamp value on line 39 accordingly.

        print ">>>>>>>>> events_data :", events_data


        # Wait for some time and start polling for event data again
        time.sleep(polling_delay)

    return

if __name__ == "__main__":
    event_poll()