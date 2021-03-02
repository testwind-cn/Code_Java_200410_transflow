SET hivevar:MAIN_DB=rds_posflow;
SET hivevar:TEMP_DB=deprecated_db;

INSERT OVERWRITE TABLE ${hivevar:TEMP_DB}.bl_flow_by_month_01
    PARTITION (inst_date)
SELECT
    mcht_cd
    ,MAX(agt_type)                  AS agt_type
    ,MAX(product_name1)             AS product_name1
    ,MIN(product_name2)             AS product_name2

    ,SUM(flow_amt_sell)             AS flow_amt_sell
    ,SUM(flow_vol_sell)             AS flow_vol_sell
    ,SUM(flow_amt_return)           AS flow_amt_return
    ,SUM(flow_vol_return)           AS flow_vol_return

    ,SUM(flow_amt_net_gt_5w)        AS flow_amt_net_gt_5w
    ,SUM(flow_amt_net_4w_5w)        AS flow_amt_net_4w_5w
    ,SUM(flow_amt_net_3w_4w)        AS flow_amt_net_3w_4w
    ,SUM(flow_amt_net_2w_3w)        AS flow_amt_net_2w_3w
    ,SUM(flow_amt_net_1w_2w)        AS flow_amt_net_1w_2w
    ,SUM(flow_amt_net_5k_1w)        AS flow_amt_net_5k_1w
    ,SUM(flow_amt_net_4k_5k)        AS flow_amt_net_4k_5k
    ,SUM(flow_amt_net_3k_4k)        AS flow_amt_net_3k_4k
    ,SUM(flow_amt_net_2k_3k)        AS flow_amt_net_2k_3k
    ,SUM(flow_amt_net_1k_2k)        AS flow_amt_net_1k_2k
    ,SUM(flow_amt_net_5h_1k)        AS flow_amt_net_5h_1k
    ,SUM(flow_amt_net_4h_5h)        AS flow_amt_net_4h_5h
    ,SUM(flow_amt_net_3h_4h)        AS flow_amt_net_3h_4h
    ,SUM(flow_amt_net_2h_3h)        AS flow_amt_net_2h_3h
    ,SUM(flow_amt_net_1h_2h)        AS flow_amt_net_1h_2h
    ,SUM(flow_amt_net_50_1h)        AS flow_amt_net_50_1h
    ,SUM(flow_amt_net_10_50)        AS flow_amt_net_10_50
    ,SUM(flow_amt_net_05_10)        AS flow_amt_net_05_10
    ,SUM(flow_amt_net_01_05)        AS flow_amt_net_01_05
    ,SUM(flow_amt_net_lt_01)        AS flow_amt_net_lt_01

    ,SUM(flow_vol_net_gt_5w)        AS flow_vol_net_gt_5w
    ,SUM(flow_vol_net_4w_5w)        AS flow_vol_net_4w_5w
    ,SUM(flow_vol_net_3w_4w)        AS flow_vol_net_3w_4w
    ,SUM(flow_vol_net_2w_3w)        AS flow_vol_net_2w_3w
    ,SUM(flow_vol_net_1w_2w)        AS flow_vol_net_1w_2w
    ,SUM(flow_vol_net_5k_1w)        AS flow_vol_net_5k_1w
    ,SUM(flow_vol_net_4k_5k)        AS flow_vol_net_4k_5k
    ,SUM(flow_vol_net_3k_4k)        AS flow_vol_net_3k_4k
    ,SUM(flow_vol_net_2k_3k)        AS flow_vol_net_2k_3k
    ,SUM(flow_vol_net_1k_2k)        AS flow_vol_net_1k_2k
    ,SUM(flow_vol_net_5h_1k)        AS flow_vol_net_5h_1k
    ,SUM(flow_vol_net_4h_5h)        AS flow_vol_net_4h_5h
    ,SUM(flow_vol_net_3h_4h)        AS flow_vol_net_3h_4h
    ,SUM(flow_vol_net_2h_3h)        AS flow_vol_net_2h_3h
    ,SUM(flow_vol_net_1h_2h)        AS flow_vol_net_1h_2h
    ,SUM(flow_vol_net_50_1h)        AS flow_vol_net_50_1h
    ,SUM(flow_vol_net_10_50)        AS flow_vol_net_10_50
    ,SUM(flow_vol_net_05_10)        AS flow_vol_net_05_10
    ,SUM(flow_vol_net_01_05)        AS flow_vol_net_01_05
    ,SUM(flow_vol_net_lt_01)        AS flow_vol_net_lt_01

    ,SUM(flow_amt_sell_at_22)       AS flow_amt_sell_at_22
    ,SUM(flow_amt_sell_at_20)       AS flow_amt_sell_at_20
    ,SUM(flow_amt_sell_at_18)       AS flow_amt_sell_at_18
    ,SUM(flow_amt_sell_at_16)       AS flow_amt_sell_at_16
    ,SUM(flow_amt_sell_at_14)       AS flow_amt_sell_at_14
    ,SUM(flow_amt_sell_at_12)       AS flow_amt_sell_at_12
    ,SUM(flow_amt_sell_at_10)       AS flow_amt_sell_at_10
    ,SUM(flow_amt_sell_at_8)        AS flow_amt_sell_at_8
    ,SUM(flow_amt_sell_at_6)        AS flow_amt_sell_at_6
    ,SUM(flow_amt_sell_at_4)        AS flow_amt_sell_at_4
    ,SUM(flow_amt_sell_at_2)        AS flow_amt_sell_at_2
    ,SUM(flow_amt_sell_at_0)        AS flow_amt_sell_at_0

    ,SUM(flow_vol_sell_at_22)       AS flow_vol_sell_at_22
    ,SUM(flow_vol_sell_at_20)       AS flow_vol_sell_at_20
    ,SUM(flow_vol_sell_at_18)       AS flow_vol_sell_at_18
    ,SUM(flow_vol_sell_at_16)       AS flow_vol_sell_at_16
    ,SUM(flow_vol_sell_at_14)       AS flow_vol_sell_at_14
    ,SUM(flow_vol_sell_at_12)       AS flow_vol_sell_at_12
    ,SUM(flow_vol_sell_at_10)       AS flow_vol_sell_at_10
    ,SUM(flow_vol_sell_at_8)        AS flow_vol_sell_at_8
    ,SUM(flow_vol_sell_at_6)        AS flow_vol_sell_at_6
    ,SUM(flow_vol_sell_at_4)        AS flow_vol_sell_at_4
    ,SUM(flow_vol_sell_at_2)        AS flow_vol_sell_at_2
    ,SUM(flow_vol_sell_at_0)        AS flow_vol_sell_at_0

    ,SUM(flow_amt_net_credit_card)  AS flow_amt_net_credit_card
    ,SUM(flow_amt_net_debit_card)   AS flow_amt_net_debit_card
    ,SUM(flow_amt_net_not_card)     AS flow_amt_net_not_card
    ,SUM(flow_vol_net_credit_card)  AS flow_vol_net_credit_card
    ,SUM(flow_vol_net_debit_card)   AS flow_vol_net_debit_card
    ,SUM(flow_vol_net_not_card)     AS flow_vol_net_not_card

    ,MAX(flow_max_credit_card)      AS flow_max_credit_card
    ,MIN(flow_min_credit_card)      AS flow_min_credit_card
    ,MAX(flow_max_debit_card)       AS flow_max_debit_card
    ,MIN(flow_min_debit_card)       AS flow_min_debit_card
    ,MAX(flow_max_not_card)         AS flow_max_not_card
    ,MIN(flow_min_not_card)         AS flow_min_not_card


    ,SUM(CASE WHEN flow_amt_sell > 0 THEN 1 ELSE 0 END) as active_day

    ,SUM(CASE WHEN dayofmonth(inst_date)= 1 THEN flow_vol_sell ELSE 0 END) as deal_day_01
    ,SUM(CASE WHEN dayofmonth(inst_date)= 2 THEN flow_vol_sell ELSE 0 END) as deal_day_02
    ,SUM(CASE WHEN dayofmonth(inst_date)= 3 THEN flow_vol_sell ELSE 0 END) as deal_day_03
    ,SUM(CASE WHEN dayofmonth(inst_date)= 4 THEN flow_vol_sell ELSE 0 END) as deal_day_04
    ,SUM(CASE WHEN dayofmonth(inst_date)= 5 THEN flow_vol_sell ELSE 0 END) as deal_day_05
    ,SUM(CASE WHEN dayofmonth(inst_date)= 6 THEN flow_vol_sell ELSE 0 END) as deal_day_06
    ,SUM(CASE WHEN dayofmonth(inst_date)= 7 THEN flow_vol_sell ELSE 0 END) as deal_day_07
    ,SUM(CASE WHEN dayofmonth(inst_date)= 8 THEN flow_vol_sell ELSE 0 END) as deal_day_08
    ,SUM(CASE WHEN dayofmonth(inst_date)= 9 THEN flow_vol_sell ELSE 0 END) as deal_day_09
    ,SUM(CASE WHEN dayofmonth(inst_date)=10 THEN flow_vol_sell ELSE 0 END) as deal_day_10

    ,SUM(CASE WHEN dayofmonth(inst_date)=11 THEN flow_vol_sell ELSE 0 END) as deal_day_11
    ,SUM(CASE WHEN dayofmonth(inst_date)=12 THEN flow_vol_sell ELSE 0 END) as deal_day_12
    ,SUM(CASE WHEN dayofmonth(inst_date)=13 THEN flow_vol_sell ELSE 0 END) as deal_day_13
    ,SUM(CASE WHEN dayofmonth(inst_date)=14 THEN flow_vol_sell ELSE 0 END) as deal_day_14
    ,SUM(CASE WHEN dayofmonth(inst_date)=15 THEN flow_vol_sell ELSE 0 END) as deal_day_15
    ,SUM(CASE WHEN dayofmonth(inst_date)=16 THEN flow_vol_sell ELSE 0 END) as deal_day_16
    ,SUM(CASE WHEN dayofmonth(inst_date)=17 THEN flow_vol_sell ELSE 0 END) as deal_day_17
    ,SUM(CASE WHEN dayofmonth(inst_date)=18 THEN flow_vol_sell ELSE 0 END) as deal_day_18
    ,SUM(CASE WHEN dayofmonth(inst_date)=19 THEN flow_vol_sell ELSE 0 END) as deal_day_19
    ,SUM(CASE WHEN dayofmonth(inst_date)=20 THEN flow_vol_sell ELSE 0 END) as deal_day_20

    ,SUM(CASE WHEN dayofmonth(inst_date)=21 THEN flow_vol_sell ELSE 0 END) as deal_day_21
    ,SUM(CASE WHEN dayofmonth(inst_date)=22 THEN flow_vol_sell ELSE 0 END) as deal_day_22
    ,SUM(CASE WHEN dayofmonth(inst_date)=23 THEN flow_vol_sell ELSE 0 END) as deal_day_23
    ,SUM(CASE WHEN dayofmonth(inst_date)=24 THEN flow_vol_sell ELSE 0 END) as deal_day_24
    ,SUM(CASE WHEN dayofmonth(inst_date)=25 THEN flow_vol_sell ELSE 0 END) as deal_day_25
    ,SUM(CASE WHEN dayofmonth(inst_date)=26 THEN flow_vol_sell ELSE 0 END) as deal_day_26
    ,SUM(CASE WHEN dayofmonth(inst_date)=27 THEN flow_vol_sell ELSE 0 END) as deal_day_27
    ,SUM(CASE WHEN dayofmonth(inst_date)=28 THEN flow_vol_sell ELSE 0 END) as deal_day_28
    ,SUM(CASE WHEN dayofmonth(inst_date)=29 THEN flow_vol_sell ELSE 0 END) as deal_day_29
    ,SUM(CASE WHEN dayofmonth(inst_date)=30 THEN flow_vol_sell ELSE 0 END) as deal_day_30
    ,SUM(CASE WHEN dayofmonth(inst_date)=31 THEN flow_vol_sell ELSE 0 END) as deal_day_31

    ,MIN(CASE WHEN flow_vol_sell > 0 THEN inst_date END)                   as first_date
    ,MAX(CASE WHEN flow_vol_sell > 0 THEN inst_date END)                   as last_date

    ,to_date(date_format(inst_date,'yyyy-MM-01'))                          as inst_date
from
    ${hivevar:MAIN_DB}.bl_flow_by_day
WHERE
--  (inst_date >= '2017-11-01' and inst_date < '2017-12-01' )
-- 开始日期的当月的1号，结尾日期前一天的下月1号
    inst_date >= to_date(date_format('${1}','yyyy-MM-01')) and
    inst_date <  to_date(date_format(date_add(to_date(date_format(date_sub('${2}',1),'yyyy-MM-01')),35),'yyyy-MM-01'))
GROUP BY
    mcht_cd, to_date(date_format(inst_date,'yyyy-MM-01'))
-- select to_date(date_format(date_add(to_date(date_format(date_sub('2020-12-01',1),'yyyy-MM-01')),35),'yyyy-MM-01'))
