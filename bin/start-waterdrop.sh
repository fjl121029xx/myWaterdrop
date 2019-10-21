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


if [[ "/bin/waterdrop" == $BASH_SOURCE || "/usr/bin/waterdrop" == $BASH_SOURCE ]];then
    WATERDROP=`whereis waterdrop|awk '{print $2}'|xargs readlink -f`
    BIN_DIR="$( cd "$( dirname "${WATERDROP}" )" && pwd )"
else
    BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
fi

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

appName=`cat ${CONFIG_FILE}|grep "spark.app.name"|awk '{print $3}'`
appName=${appName:1:0-1}

DEFAULT_DRIVER_MEMORY='2g'
DRIVER_MEMORY=`cat ${CONFIG_FILE}|grep "spark.driver.memory"|grep -v '#'|awk '{print $3}'`
DRIVER_MEMORY=${DRIVER_MEMORY:1:0-1}
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

fi

assemblyJarName=$(find ${LIB_DIR} -name Waterdrop-*.jar)

echo "[INFO] appName: "$appName
echo "[INFO] FilesDepOpts: "$FilesDepOpts
echo "[INFO] assemblyJarName: "$assemblyJarName
echo "[INFO] ConfDepOpts: "$ConfDepOpts
echo "[INFO] DriverMemory: "$DRIVER_MEMORY

source ${CONF_DIR}/waterdrop-env.sh

${SPARK_HOME}/bin/spark-submit --class io.github.interestinglab.waterdrop.Waterdrop \
    --name $(getAppName ${CONFIG_FILE}) \
    --master ${MASTER} \
    --driver-memory ${DRIVER_MEMORY} \
    --deploy-mode ${DEPLOY_MODE} \
    --driver-java-options "-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled
    -XX:+ParallelRefProcEnabled -XX:+CMSClassUnloadingEnabled -Dlog4j.configuration=file:${CONF_DIR}/log4j.properties" \
    ${JarDepOpts} \
    ${ConfDepOpts} \
    ${FilesDepOpts} \
    ${assemblyJarName} ${CMD_ARGUMENTS}

if [ $? != 0 ]; then
   exit 1
fi