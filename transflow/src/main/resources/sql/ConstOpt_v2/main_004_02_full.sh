#!/bin/bash


for ((i=0;i<30;i++))
do
    DATE_L1=$(date -d "2017-10-01 +${i} day" "+%Y-%m-%d")
    DATE_L2=$(date -d "2017-10-01 +`expr ${i} + 360` day" "+%Y-%m-%d")

echo "处理日期 ${i} sh \"${DATE_L1}\" \"${DATE_L2}\"    ***********************"

    sh /home/transflow/script_v2/run_file.sh "main_004_02.sh" \
        "004_dml_spark_insert_bl_flow_by_30day_01.sql" "one" \
        "${DATE_L1}" "${DATE_L2}"



    DATE_L1=$(date -d "2018-9-26 +${i} day" "+%Y-%m-%d")
    DATE_L2=$(date -d "2018-9-26 +`expr ${i} + 360` day" "+%Y-%m-%d")

echo "处理日期 ${i} sh \"${DATE_L1}\" \"${DATE_L2}\"    ***********************"

    sh /home/transflow/script_v2/run_file.sh "main_004_02.sh" \
        "004_dml_spark_insert_bl_flow_by_30day_01.sql" "one" \
        "${DATE_L1}" "${DATE_L2}"



    DATE_L1=$(date -d "2019-9-21 +${i} day" "+%Y-%m-%d")
    DATE_L2=$(date -d "2019-9-21 +`expr ${i} + 360` day" "+%Y-%m-%d")

echo "处理日期 ${i} sh \"${DATE_L1}\" \"${DATE_L2}\"    ***********************"

    sh /home/transflow/script_v2/run_file.sh "main_004_02.sh" \
        "004_dml_spark_insert_bl_flow_by_30day_01.sql" "one" \
        "${DATE_L1}" "${DATE_L2}"

done