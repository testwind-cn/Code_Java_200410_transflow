#!/bin/bash

OLDDIR=$(pwd)
THEDIR="$(dirname ${0})/"
cd ${THEDIR}
THEDIR=$(pwd)
echo "原始目录：${OLDDIR}  ==== 当前目录：  ${THEDIR}"

sh /home/transflow/script_v2/run_file.sh "main_001_02.sh" "nofile"

sh /home/transflow/script_v2/run_file.sh "main_001_03.sh" "001_dml_hive_insert_bl_total_transflow_v2.sql"

sh /home/transflow/script_v2/run_file.sh "main_002_02.sh" "002_dml_spark_insert_bl_flow_by_day_01.sql"

sh /home/transflow/script_v2/run_file.sh "main_002_03.sh" "002_dml_hive_insert_bl_flow_by_day.sql"

sh /home/transflow/script_v2/run_file.sh "main_003_02.sh" "003_dml_spark_insert_bl_flow_by_month_01.sql"

sh /home/transflow/script_v2/run_file.sh "main_003_03.sh" "003_dml_hive_insert_bl_flow_by_month.sql"