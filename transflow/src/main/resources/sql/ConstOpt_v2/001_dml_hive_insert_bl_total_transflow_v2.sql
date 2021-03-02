SET hivevar:MAIN_DB=rds_posflow;
SET hivevar:TEMP_DB=deprecated_db;

set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
-- set mapreduce.job.reduces=1;

-- 设置 map输出和reduce输出进行合并的相关参数
set hive.merge.mapredfiles =true;
set hive.merge.mapfiles=true;
set hive.merge.size.per.task=512000000;
set hive.merge.smallfiles.avgsize=512000000;

-- 设置 mapper输入文件合并一些参数：
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=1024000000;
set mapred.min.split.size.per.node=768000000;
set mapred.min.split.size.per.rack=768000000;

INSERT OVERWRITE TABLE ${hivevar:MAIN_DB}.bl_total_transflow_v2
    PARTITION (inst_date)
SELECT
    sn,
    coalesce(
        cast(substring(inst_time ,1,2) as int)*10000 +
        cast(substring(inst_time ,3,2) as int)*100 +
        cast(substring(inst_time ,5,2) as int)
        , 120000
    ) as inst_time ,
    settle_date,
    mcht_cd,
    mcht_type,
    term_id,
    case
        when card_type='00000'                  then  0Y
        when card_type='00001'                  then  1Y
        when card_type='00002'                  then  2Y
        when card_type='00010'                  then 10Y
        when card_type='00011'                  then 11Y
        when coalesce(length(card_type),0)>0    then cast(card_type as tinyint)
        when coalesce(length(card_type),0)=0    then -1Y
        else                                         -1Y
    end as card_type,
    currcy_code_trans,
    card_no,
    issue_bank,
    txn_type,
    txn_amt,
    trans_amt,
    fee,
    txn_status,
    org_flg,
    org_ext,
    area_flag,
    ori_dt,
    product_name,
    agt_type,
    create_time,
    create_user,
    inst_date
from ${hivevar:TEMP_DB}.bl_total_transflow_v2_01
    where inst_date >= to_date('${1}') and inst_date< to_date('${2}')
;
