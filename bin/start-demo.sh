#!/bin/bash -x

#
# The shell scripts are in the 'bin' directory. They are written to restart the services
# on exit. The 'server' and 'client' are the Franz server and the Tweetlogger client.
# The 'tweets' script starts the Storm topology.

BIN_DIR="$( cd "$( dirname "$0" )" && pwd )"
. $BIN_DIR/env.sh

 cd ${BIN_DIR}

# zap old data
rm ${SERVER_DIR}/tweets
touch ${SERVER_DIR}/tweets

# run Server
./server
sleep 1

# run Client
./client
sleep 1

# run Storm Topology
./storm &
sleep 1

# run Web Server
./webserver &
sleep 1
