#!/bin/bash

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

TABLE_S=rds_posflow.bl_flow_by_30day



echo "开始处理SQL逻辑：${TABLE_S} 日期： ${DATE_L} ***********************"

# sudo -u admin hive --hivevar THE_TABLE=${TABLE_S} --hivevar THE_DATE=${DATE_L} -e "$(cat main_ddl_01.sql ; cat main_dml_01.sql)"
# sudo -u hdfs hive --hivevar THE_DATE=${DATE_L} --hivevar BACKUP_DATE=${DATE_BK} -f main_dml_02.sql

echo "=====     004_ddl_create_bl_flow_by_30day.sql ======"

cat ./004_ddl_create_bl_flow_by_30day.sql

sudo -u admin hive -e "$(cat ./004_ddl_create_bl_flow_by_30day.sql)"

echo "===========  开始时间：${DATE_L}"
echo "===========  结束时间：$(date "+%Y-%m-%d %H:%M:%S")"
cd ${OLDDIR}
