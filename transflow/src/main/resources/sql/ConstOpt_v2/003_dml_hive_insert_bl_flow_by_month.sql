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

INSERT OVERWRITE TABLE ${hivevar:MAIN_DB}.bl_flow_by_month
    PARTITION (inst_date)
SELECT
    mcht_cd
    ,agt_type
    ,product_name1
    ,product_name2

    ,flow_amt_sell
    ,flow_vol_sell
    ,flow_amt_return
    ,flow_vol_return

    ,flow_amt_net_gt_5w
    ,flow_amt_net_4w_5w
    ,flow_amt_net_3w_4w
    ,flow_amt_net_2w_3w
    ,flow_amt_net_1w_2w
    ,flow_amt_net_5k_1w
    ,flow_amt_net_4k_5k
    ,flow_amt_net_3k_4k
    ,flow_amt_net_2k_3k
    ,flow_amt_net_1k_2k
    ,flow_amt_net_5h_1k
    ,flow_amt_net_4h_5h
    ,flow_amt_net_3h_4h
    ,flow_amt_net_2h_3h
    ,flow_amt_net_1h_2h
    ,flow_amt_net_50_1h
    ,flow_amt_net_10_50
    ,flow_amt_net_05_10
    ,flow_amt_net_01_05
    ,flow_amt_net_lt_01

    ,flow_vol_net_gt_5w
    ,flow_vol_net_4w_5w
    ,flow_vol_net_3w_4w
    ,flow_vol_net_2w_3w
    ,flow_vol_net_1w_2w
    ,flow_vol_net_5k_1w
    ,flow_vol_net_4k_5k
    ,flow_vol_net_3k_4k
    ,flow_vol_net_2k_3k
    ,flow_vol_net_1k_2k
    ,flow_vol_net_5h_1k
    ,flow_vol_net_4h_5h
    ,flow_vol_net_3h_4h
    ,flow_vol_net_2h_3h
    ,flow_vol_net_1h_2h
    ,flow_vol_net_50_1h
    ,flow_vol_net_10_50
    ,flow_vol_net_05_10
    ,flow_vol_net_01_05
    ,flow_vol_net_lt_01

    ,flow_amt_sell_at_22
    ,flow_amt_sell_at_20
    ,flow_amt_sell_at_18
    ,flow_amt_sell_at_16
    ,flow_amt_sell_at_14
    ,flow_amt_sell_at_12
    ,flow_amt_sell_at_10
    ,flow_amt_sell_at_8
    ,flow_amt_sell_at_6
    ,flow_amt_sell_at_4
    ,flow_amt_sell_at_2
    ,flow_amt_sell_at_0

    ,flow_vol_sell_at_22
    ,flow_vol_sell_at_20
    ,flow_vol_sell_at_18
    ,flow_vol_sell_at_16
    ,flow_vol_sell_at_14
    ,flow_vol_sell_at_12
    ,flow_vol_sell_at_10
    ,flow_vol_sell_at_8
    ,flow_vol_sell_at_6
    ,flow_vol_sell_at_4
    ,flow_vol_sell_at_2
    ,flow_vol_sell_at_0

    ,flow_amt_net_credit_card
    ,flow_amt_net_debit_card
    ,flow_amt_net_not_card
    ,flow_vol_net_credit_card
    ,flow_vol_net_debit_card
    ,flow_vol_net_not_card

    ,flow_max_credit_card
    ,flow_min_credit_card
    ,flow_max_debit_card
    ,flow_min_debit_card
    ,flow_max_not_card
    ,flow_min_not_card

    ,active_day

    ,deal_day_01
    ,deal_day_02
    ,deal_day_03
    ,deal_day_04
    ,deal_day_05
    ,deal_day_06
    ,deal_day_07
    ,deal_day_08
    ,deal_day_09
    ,deal_day_10
    ,deal_day_11
    ,deal_day_12
    ,deal_day_13
    ,deal_day_14
    ,deal_day_15
    ,deal_day_16
    ,deal_day_17
    ,deal_day_18
    ,deal_day_19
    ,deal_day_20
    ,deal_day_21
    ,deal_day_22
    ,deal_day_23
    ,deal_day_24
    ,deal_day_25
    ,deal_day_26
    ,deal_day_27
    ,deal_day_28
    ,deal_day_29
    ,deal_day_30
    ,deal_day_31

    ,first_date
    ,last_date

    ,inst_date
from
    ${hivevar:TEMP_DB}.bl_flow_by_month_01
WHERE
    inst_date >= to_date(date_format('${1}','yyyy-MM-01')) and
    inst_date <  to_date(date_format(date_add(to_date(date_format(date_sub('${2}',1),'yyyy-MM-01')),35),'yyyy-MM-01'))
;