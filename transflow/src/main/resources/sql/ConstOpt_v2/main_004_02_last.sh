#!/bin/bash


DATE_CURRENT=$(date -d "+1 day" "+%Y-%m-%d")

for ((i=0;i<31;i++))
do
    DATE_L1=$(date -d "${DATE_CURRENT} -`expr ${i} + 90` day" "+%Y-%m-%d")
    DATE_L2=$(date -d "${DATE_CURRENT} -`expr ${i}` day" "+%Y-%m-%d")

echo "处理日期 ${i} sh \"${DATE_L1}\" \"${DATE_L2}\"    ***********************"

    sh /home/transflow/script_v2/run_file.sh "main_004_02.sh" \
        "004_dml_spark_insert_bl_flow_by_30day_01.sql" "one" \
        "${DATE_L1}" "${DATE_L2}"

done