#!/bin/bash

OLDDIR=$(pwd)
echo "===================   $0 原始目录  ${OLDDIR}   =============="
THEDIR="$(dirname $0)/"
cd ${THEDIR}
THEDIR=$(pwd)
echo "===================   当前目录  ${THEDIR}"
DATE_L=$(date "+%Y-%m-%d %H:%M:%S")

echo "当天时间："${DATE_L}
echo "参数1 ${1}"
echo "参数2 ${2}"
echo "参数3 ${3}"


if [[ -z "${1}" ]]
then
    SQL_F="./002_dml_hive_insert_bl_flow_by_day.sql"
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

echo "====== 运行 Spark File ${START_DAY}  ${END_DAY}  ${SQL_F}"

# sudo -u admin hive --hivevar THE_TABLE=${TABLE_S} --hivevar THE_DATE=${DATE_L} -e "$(cat main_ddl_01.sql ; cat main_dml_01.sql)"
# sudo -u hdfs hive --hivevar THE_DATE=${DATE_L} --hivevar BACKUP_DATE=${DATE_BK} -f main_dml_02.sql

echo "sudo -u admin hive -e \"\$(sed 's/\${1}/'${START_DAY}'/g' ${SQL_F}  | sed 's/\${2}/'${END_DAY}'/g')\""
echo "=====     ${SQL_F} ======"
sed 's/${1}/'${START_DAY}'/g' ${SQL_F} | sed 's/${2}/'${END_DAY}'/g'


sudo -u admin hive -e "$(sed 's/${1}/'${START_DAY}'/g' ${SQL_F} | sed 's/${2}/'${END_DAY}'/g')"

echo "===========  开始时间：${DATE_L}"
echo "===========  结束时间：$(date "+%Y-%m-%d %H:%M:%S")"
cd ${OLDDIR}
