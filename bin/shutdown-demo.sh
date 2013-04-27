#!/bin/bash

BIN_DIR="$( cd "$( dirname "$0" )" && pwd )"
. $BIN_DIR/env.sh

for PID in `cat ${RUN_DIR}/*.pid`
do
   kill -TERM ${PID}
done

#for PID in `ps ax | grep mapr_spout | awk '{print $1}'`
#do
#   kill -TERM ${PID}
#done
