#!/bin/bash

BIN_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEMO_DIR="${BIN_DIR}/.."
export RUN_DIR="${BIN_DIR}/../run"
export QUERY_PATH="${BIN_DIR}/../data/www/in"
export JAR_DIR="${BIN_DIR}/../target"
export JAR=`ls ${JAR_DIR}/mapr-storm-demo-*-jar-with-dependencies.jar`
export LOG_DIR="${BIN_DIR}/../log"
export DEMO_HOST="localhost"
export ZK_HOST="localhost"
export ZK_PORT=5181
