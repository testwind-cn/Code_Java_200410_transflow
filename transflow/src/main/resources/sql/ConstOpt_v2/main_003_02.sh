#!/bin/bash

OLDDIR=$(pwd)
echo "===================   $0 原始目录  ${OLDDIR}   =============="
THEDIR="$(dirname $0)/"
cd ${THEDIR}
THEDIR=$(pwd)
echo "===================   当前目录  ${THEDIR}"
DATE_L=$(date "+%Y-%m-%d %H:%M:%S")


echo "当天日期："${DATE_L}
echo "参数1 ${1}"
echo "参数2 ${2}"
echo "参数3 ${3}"


if [[ -z "${1}" ]]
then
    SQL_F="./003_dml_spark_insert_bl_flow_by_month_01.sql"
else
    SQL_F="${1}"
fi

if [[ -z "${2}" ]]
then
    START_DAY=$(date -d "-10 day" "+%Y-%m-%d")
else
    START_DAY=${2}
fi

if [[ -z "${3}" ]]
then
    END_DAY=$(date -d "+2 day" "+%Y-%m-%d")
else
    END_DAY=${3}
fi


source /etc/profile
source /root/.bash_profile

# spark-submit 配置
NUM_EXE=8
EXE_CORE=6
EXE_MEM="16G"
jar_transflow=/home/transflow/transflow-2.0-jar-with-distribution.jar
pkg_service="com.thtk.service"
BLFile="BLFile"

echo "====== 运行 Spark File ${START_DAY}  ${END_DAY}  ${SQL_F}"

sudo -u admin spark-submit \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j-driver.properties" \
    --class ${pkg_service}.${BLFile} \
    --master yarn-client \
    --num-executors $NUM_EXE \
    --executor-cores $EXE_CORE \
    --executor-memory $EXE_MEM \
    --name "${BLFile}" \
    $jar_transflow \
    "${START_DAY}" "${END_DAY}" "${SQL_F}"


echo "===========  开始时间：${DATE_L}"
echo "===========  结束时间：$(date "+%Y-%m-%d %H:%M:%S")"
cd ${OLDDIR}
