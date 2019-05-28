#!/bin/bash

# TODO: compress plugins/ dir before start-waterdrop.sh

# copy command line arguments
CMD_ARGUMENTS=$@

PARAMS=""
while (( "$#" )); do
  case "$1" in
    -m|--master)
      MASTER=$2
      shift 2
      ;;

    -e|--deploy-mode)
      DEPLOY_MODE=$2
      shift 2
      ;;

    -c|--config)
      CONFIG_FILE=$2
      shift 2
      ;;

    -s|--metrics-sink)
      METRICS_SINK=$2
      CMD_ARGUMENTS=${CMD_ARGUMENTS//' '$1/''}
      CMD_ARGUMENTS=${CMD_ARGUMENTS//' '$2/''}
      shift 2
      ;;

    --) # end argument parsing
      shift
      break
      ;;

    # -*|--*=) # unsupported flags
    #  echo "Error: Unsupported flag $1" >&2
    #  exit 1
    #  ;;

    *) # preserve positional arguments
      PARAM="$PARAMS $1"
      shift
      ;;

  esac
done
# set positional arguments in their proper place
eval set -- "$PARAMS"


BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UTILS_DIR=${BIN_DIR}/utils
APP_DIR=$(dirname ${BIN_DIR})
CONF_DIR=${APP_DIR}/config
LIB_DIR=${APP_DIR}/lib
PLUGINS_DIR=${APP_DIR}/plugins

DEFAULT_CONFIG=${CONF_DIR}/application.conf
CONFIG_FILE=${CONFIG_FILE:-$DEFAULT_CONFIG}

DEFAULT_MASTER=local[2]
MASTER=${MASTER:-$DEFAULT_MASTER}

DEFAULT_DEPLOY_MODE=client
DEPLOY_MODE=${DEPLOY_MODE:-$DEFAULT_DEPLOY_MODE}

# sink metrics 2 influxdb default false
DEFAULT_METRICS_SINK=false
METRICS_SINK=${METRICS_SINK:-$DEFAULT_METRICS_SINK}

# scan jar dependencies for all plugins
source ${UTILS_DIR}/file.sh
source ${UTILS_DIR}/app.sh
jarDependencies=$(listJarDependenciesOfPlugins ${PLUGINS_DIR})
JarDepOpts=""
if [ "$jarDependencies" != "" ]; then
    JarDepOpts="--jars $jarDependencies"
fi

FilesDepOpts=""
ConfDepOpts=""
if [ "$DEPLOY_MODE" == "cluster" ]; then

    ## add config file
    FilesDepOpts="--files ${CONFIG_FILE}"

    ## add plugin files
    #FilesDepOpts="${FilesDepOpts},${APP_DIR}/plugins.tar.gz"

    ## add kafka sasl conf
    FilesDepOpts="${FilesDepOpts},${CONF_DIR}/kafka_client_jaas.conf"

    if [ "$METRICS_SINK" == "true" ]; then
       FilesDepOpts="${FilesDepOpts},${CONF_DIR}/metrics.properties"

       ##add driver and executor extra class
       DRIVER_EXTRACLASS="--conf spark.driver.extraClassPath=spark-influx-sink-0.4.0.jar:metrics-influxdb-1.1.8.jar"
       EXECUTOR_DRIVER_EXTRACLASS="--conf spark.executor.extraClassPath=spark-influx-sink-0.4.0.jar:metrics-influxdb-1.1.8.jar"
       ConfDepOpts="$DRIVER_EXTRACLASS $EXECUTOR_DRIVER_EXTRACLASS"
    fi

    echo ""

elif [ "$DEPLOY_MODE" == "client" ]; then

    echo ""
fi

assemblyJarName=$(find ${LIB_DIR} -name Waterdrop-*.jar)

source ${CONF_DIR}/waterdrop-env.sh

exec ${SPARK_HOME}/bin/spark-submit --class io.github.interestinglab.waterdrop.Waterdrop \
    --name $(getAppName ${CONFIG_FILE}) \
    --master ${MASTER} \
    --deploy-mode ${DEPLOY_MODE} \
    --driver-java-options "-Djava.security.auth.login.config=./kafka_client_jaas.conf" \
    --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_client_jaas.conf" \
    ${JarDepOpts} \
    ${ConfDepOpts} \
    ${FilesDepOpts} \
    ${assemblyJarName} ${CMD_ARGUMENTS}