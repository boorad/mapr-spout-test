#!/bin/bash

BIN_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEMO_DIR="${BIN_DIR}/.."
export CONF_DIR="${DEMO_DIR}/conf"
export RUN_DIR="${DEMO_DIR}/run"
export DATA_DIR="${DEMO_DIR}/data"
export SERVER_DIR="${DATA_DIR}/mapr-storm"
export QUERY_PATH="${DATA_DIR}/www/in"
export JAR_DIR="${DEMO_DIR}/target"
export JAR=`ls ${JAR_DIR}/mapr-storm-demo-*-jar-with-dependencies.jar`
export LOG_DIR="${DEMO_DIR}/log"
export DEMO_HOST="localhost"
export DEMO_PORT=8080
export ZK_HOST="localhost"
export ZK_PORT=5181
export WWW_DIR="${DEMO_DIR}/www"
