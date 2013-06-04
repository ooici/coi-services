#!/bin/bash

GATEWAY_URL="http://127.0.0.1:5000"

POLLING_SECONDS=5

INIT_TIMEOUT_SECONDS=120
TERM_TIMEOUT_SECONDS=10

function on_exit()
{
   echo "Terminating Visualization operations...please wait"

   curl -s $GATEWAY_URL/ion-service/visualization/terminate_realtime_visualization_data?query_token=$QUERY_TOKEN\&timeout=$TERM_TIMEOUT_SECONDS | grep 'GatewayError'

}

trap on_exit EXIT


DPID=`curl -s $GATEWAY_URL/ion-service/resource_registry/find_resources?restype=DataProduct\&name=sine_wave_generator\&id_only=True | grep '"id":' | cut -f4 -d\"`
echo "Data Product id="$DPID

QUERY_TOKEN=`curl -s $GATEWAY_URL/ion-service/visualization/initiate_realtime_visualization_data?data_product_id=$DPID\&timeout=$INIT_TIMEOUT_SECONDS | grep 'GatewayResponse' | cut -f7 -d\" | tr -d '\'`
echo "query_token="$QUERY_TOKEN

sleep 1

while true ; do curl $GATEWAY_URL/ion-service/visualization/get_realtime_visualization_data?query_token=$QUERY_TOKEN; echo "\n"; sleep $POLLING_SECONDS; done


