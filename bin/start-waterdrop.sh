#!/bin/bash

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

    -d|--driver-memory)
      DRIVER_MEMORY=$2
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

if [ ! -f $CONFIG_FILE  ];then
  echo '[ERROR] conf file not exists!!! exit'
  exit 1
fi

DEFAULT_MASTER=local[2]
MASTER=${MASTER:-$DEFAULT_MASTER}

DEFAULT_DEPLOY_MODE=client
DEPLOY_MODE=${DEPLOY_MODE:-$DEFAULT_DEPLOY_MODE}

# sink metrics 2 influxdb default false
DEFAULT_METRICS_SINK=false
METRICS_SINK=${METRICS_SINK:-$DEFAULT_METRICS_SINK}

DEFAULT_DRIVER_MEMORY='2g'
DRIVER_MEMORY=${DRIVER_MEMORY:-$DEFAULT_DRIVER_MEMORY}

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

    ## add kafka sasl conf
    FilesDepOpts="${FilesDepOpts},${CONF_DIR}/kafka_client_jaas.conf"

    DriverKafkaJavaOption="--conf spark.driver.extraJavaOptions=-Djava.security.auth.login.config=./kafka_client_jaas.conf"
    ExecutorKafkaJavaOption="--conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_client_jaas.conf"
    ConfDepOpts="$DriverKafkaJavaOption $ExecutorKafkaJavaOption"

elif [ "$DEPLOY_MODE" == "client" ]; then

    if [ "$MASTER" == "yarn" ]; then
        FilesDepOpts="--files ${CONF_DIR}/kafka_client_jaas.conf"
        ExecutorKafkaJavaOption="--conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_client_jaas.conf"
    else
        ExecutorKafkaJavaOption="--conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=${CONF_DIR}/kafka_client_jaas.conf"
    fi

    DriverKafkaJavaOption="--conf spark.driver.extraJavaOptions=-Djava.security.auth.login.config=${CONF_DIR}/kafka_client_jaas.conf"
    ConfDepOpts="$DriverKafkaJavaOption $ExecutorKafkaJavaOption"
fi

assemblyJarName=$(find ${LIB_DIR} -name Waterdrop-*.jar)

echo "[INFO] FilesDepOpts: "$FilesDepOpts
echo "[INFO] assemblyJarName: "$assemblyJarName
echo "[INFO] ConfDepOpts: "$ConfDepOpts
echo "[INFO] DriverMemory: "$DRIVER_MEMORY

source ${CONF_DIR}/waterdrop-env.sh

appname=`cat ${CONFIG_FILE}|grep "spark.app.name"|awk '{print $3}'`
appname=${appname:1:0-1}
echo $appname

if [ "$MASTER" == "yarn" ]; then
    {
    while :
    do
      sleep 1s
      yarnAppInfo=`yarn application -list|grep $appname`
      if [ ${#yarnAppInfo} -gt 0 ]; then
        echo `echo $yarnAppInfo|awk '{print $1}'` > ./applicationid
        break
        exit 2
      fi

      let i++
      if [ $i == 30 ]; then
          break
          exit 3
      fi
    done
    } &
fi

${SPARK_HOME}/bin/spark-submit --class io.github.interestinglab.waterdrop.Waterdrop \
    --name $(getAppName ${CONFIG_FILE}) \
    --master ${MASTER} \
    --driver-memory ${DRIVER_MEMORY} \
    --deploy-mode ${DEPLOY_MODE} \
    ${JarDepOpts} \
    ${ConfDepOpts} \
    ${FilesDepOpts} \
    ${assemblyJarName} ${CMD_ARGUMENTS}


if [ "$MASTER" == "yarn" ]; then
    appid=`cat ./applicationid`
    jobStatus=`yarn application -status ${appid}|grep 'Final-State'|awk '{print $3}'`

    if [ $jobStatus != "SUCCEEDED" ]; then
        echo "[ERROR] job status: $jobStatus"
        exit 4
    fi
fi