INSERT OVERWRITE TABLE deprecated_db.bl_flow_by_30day_01
    PARTITION (p_day, inst_date)
select
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

    ,SUM(CASE WHEN flow_amt_sell > 0 THEN 1 ELSE 0 END)     as active_day

    ,MIN(CASE WHEN flow_vol_sell > 0 THEN inst_date END)    as first_date
    ,MAX(CASE WHEN flow_vol_sell > 0 THEN inst_date END)    as last_date

    ,datediff(current_date(),'2000-01-02') % 30
                                    AS p_day
    ,p_inst_date                    AS inst_date
from
(
    select
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

        ,inst_date,

        CASE -- 不用减日期，正好搭在等于日期上，这里多减10天，是防止漏跑批的情况下，10天内都可以工作
            WHEN inst_date >= date_sub(current_date(), 30) AND inst_date < date_sub(current_date(),  0) THEN date_sub(current_date(), 30)
            WHEN inst_date >= date_sub(current_date(), 60) AND inst_date < date_sub(current_date(), 30) THEN date_sub(current_date(), 60)
            WHEN inst_date >= date_sub(current_date(), 90) AND inst_date < date_sub(current_date(), 60) THEN date_sub(current_date(), 90)
            WHEN inst_date >= date_sub(current_date(),120) AND inst_date < date_sub(current_date(), 90) THEN date_sub(current_date(),120)
            WHEN inst_date >= date_sub(current_date(),150) AND inst_date < date_sub(current_date(),120) THEN date_sub(current_date(),150)
            WHEN inst_date >= date_sub(current_date(),180) AND inst_date < date_sub(current_date(),150) THEN date_sub(current_date(),180)
            WHEN inst_date >= date_sub(current_date(),210) AND inst_date < date_sub(current_date(),180) THEN date_sub(current_date(),210)
            WHEN inst_date >= date_sub(current_date(),240) AND inst_date < date_sub(current_date(),210) THEN date_sub(current_date(),240)
            WHEN inst_date >= date_sub(current_date(),270) AND inst_date < date_sub(current_date(),240) THEN date_sub(current_date(),270)
            WHEN inst_date >= date_sub(current_date(),300) AND inst_date < date_sub(current_date(),270) THEN date_sub(current_date(),300)
            WHEN inst_date >= date_sub(current_date(),330) AND inst_date < date_sub(current_date(),300) THEN date_sub(current_date(),330)
            WHEN inst_date >= date_sub(current_date(),360) AND inst_date < date_sub(current_date(),330) THEN date_sub(current_date(),360)
        END AS p_inst_date
    from rds_posflow.bl_flow_by_day
    where
        inst_date >= date_sub(current_date(),360) AND inst_date < date_sub(current_date(),  0)
) t1
group by
    mcht_cd,p_inst_date
