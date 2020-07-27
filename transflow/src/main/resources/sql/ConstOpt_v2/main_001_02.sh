#!/bin/bash
### AUTHOR: zhaohu
### EMAIL: zhaohu@allinpay.com
### DATE: 2020/01/18
### DESC: BLTransFlow-spark-submit
### REV: 1.0


OLDDIR=$(pwd)
THEDIR="$(dirname ${0})/"
cd ${THEDIR}
THEDIR=$(pwd)
echo "====信息1 SHELL 文件：${0}  ===="
echo "====信息2 原始目录：${OLDDIR}  ===="
echo "====信息3 当前目录：${THEDIR} ===="
echo "====信息4 $(date "+%Y-%m-%d %H:%M:%S") ===="


DATE_L=$(date "+%Y-%m-%d %H:%M:%S")

echo "当天时间："${DATE_L}
echo "参数1 ${1}"
echo "参数2 ${2}"
echo "参数3 ${3}"


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
obj_BLTransFlowOpt_v2="BLTransFlowServiceOpt_v2"


### BLTransFlow-spark-submit
### 生成：bl_total_flow【统一流水】保理流水表

echo "====== 运行 Spark File ${START_DAY}  ${END_DAY}  ${1}"
# --conf "spark.yarn.executor.memoryOverhead=4096" \
sudo -u admin spark-submit \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j-driver.properties" \
    --class ${pkg_service}.${obj_BLTransFlowOpt_v2} \
    --master yarn-client \
    --num-executors ${NUM_EXE} \
    --executor-cores ${EXE_CORE} \
    --executor-memory ${EXE_MEM} \
    --name "$obj_BLTransFlowOpt_v2" \
    ${jar_transflow} "${2}" "${3}"

#    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j-driver.properties" \
#    --files ./log4j-driver.properties,./log4j-executor.properties \

echo "===========  开始时间：${DATE_L}"
echo "===========  结束时间：$(date "+%Y-%m-%d %H:%M:%S")"
cd ${OLDDIR}