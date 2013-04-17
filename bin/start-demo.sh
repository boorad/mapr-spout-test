#!/bin/bash -x

#
# The shell scripts are in the 'bin' directory. They are written to restart the services
# on exit. The 'server' and 'client' are the Franz server and the Tweetlogger client.
# The 'tweets' script starts the Storm topology.

./env.sh

 cd ${BIN_DIR}

# run Server
./server
sleep 1

# run Client
./client
sleep 1

# run Storm Topology
./tweets &
echo $! > ${RUN_DIR}/tweets.pid
sleep 1

# run Web Server
./webserver
echo $! > ${RUN_DIR}/webserver.pid
