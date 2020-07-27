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

INSERT OVERWRITE TABLE rds_posflow.bl_flow_by_week
    PARTITION (p_week,inst_date)
SELECT
    mcht_cd
    ,agt_type
    ,product_name1
    ,product_name2

    ,flow_amt_sell
    ,flow_vol_sell
    ,flow_amt_return
    ,flow_vol_return

    ,active_day

    ,p_week
    ,inst_date
from
    deprecated_db.bl_flow_by_week_01
WHERE
    p_week = datediff('${2}','2000-01-02') % 7 and
    inst_date <  date_sub('${2}', 0) AND
    inst_date >= date_sub('${2}', cast(ceil(datediff('${2}','${1}') /7)*7 as INT))
;