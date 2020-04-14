
        SUM(CASE WHEN                    diffdays<= 31+1 THEN txn_amt ELSE 0 END) AS amt_12M_1,
        SUM(CASE WHEN diffdays> 31+1 AND diffdays<= 61+1 THEN txn_amt ELSE 0 END) AS amt_12M_2,
        SUM(CASE WHEN diffdays> 61+1 AND diffdays<= 91+1 THEN txn_amt ELSE 0 END) AS amt_12M_3,
        SUM(CASE WHEN diffdays> 91+1 AND diffdays<=121+1 THEN txn_amt ELSE 0 END) AS amt_12M_4,
        SUM(CASE WHEN diffdays>121+1 AND diffdays<=151+1 THEN txn_amt ELSE 0 END) AS amt_12M_5,
        SUM(CASE WHEN diffdays>151+1 AND diffdays<=181+1 THEN txn_amt ELSE 0 END) AS amt_12M_6,
        SUM(CASE WHEN diffdays>181+1 AND diffdays<=211+1 THEN txn_amt ELSE 0 END) AS amt_12M_7,
        SUM(CASE WHEN diffdays>211+1 AND diffdays<=241+1 THEN txn_amt ELSE 0 END) AS amt_12M_8,
        SUM(CASE WHEN diffdays>241+1 AND diffdays<=271+1 THEN txn_amt ELSE 0 END) AS amt_12M_9,
        SUM(CASE WHEN diffdays>271+1 AND diffdays<=311+1 THEN txn_amt ELSE 0 END) AS amt_12M_10,
        SUM(CASE WHEN diffdays>311+1 AND diffdays<=331+1 THEN txn_amt ELSE 0 END) AS amt_12M_11,
        SUM(CASE WHEN diffdays>331+1 AND diffdays<=361+1 THEN txn_amt ELSE 0 END) AS amt_12M_12,


select
    mcht_cd
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

    ,MIN(CASE WHEN flow_vol_sell > 0 THEN inst_date END)                   as first_date
    ,MAX(CASE WHEN flow_vol_sell > 0 THEN inst_date END)                   as last_date
    ,p_inst_date                                                           as inst_date
from
(
    select
        bl_flow_by_day.*,
        CASE
            WHEN inst_date >= date_sub(current_date(), 31+1)                                                THEN date_sub(current_date(), 31+1)
            WHEN inst_date >= date_sub(current_date(), 61+1) AND inst_date < date_sub(current_date(), 31+1) THEN date_sub(current_date(), 61+1)
            WHEN inst_date >= date_sub(current_date(), 91+1) AND inst_date < date_sub(current_date(), 61+1) THEN date_sub(current_date(), 91+1)
            WHEN inst_date >= date_sub(current_date(),121+1) AND inst_date < date_sub(current_date(), 91+1) THEN date_sub(current_date(),121+1)
            WHEN inst_date >= date_sub(current_date(),151+1) AND inst_date < date_sub(current_date(),121+1) THEN date_sub(current_date(),151+1)
            WHEN inst_date >= date_sub(current_date(),181+1) AND inst_date < date_sub(current_date(),151+1) THEN date_sub(current_date(),181+1)
            WHEN inst_date >= date_sub(current_date(),211+1) AND inst_date < date_sub(current_date(),181+1) THEN date_sub(current_date(),211+1)
            WHEN inst_date >= date_sub(current_date(),241+1) AND inst_date < date_sub(current_date(),211+1) THEN date_sub(current_date(),241+1)
            WHEN inst_date >= date_sub(current_date(),271+1) AND inst_date < date_sub(current_date(),241+1) THEN date_sub(current_date(),271+1)
            WHEN inst_date >= date_sub(current_date(),301+1) AND inst_date < date_sub(current_date(),271+1) THEN date_sub(current_date(),301+1)
            WHEN inst_date >= date_sub(current_date(),331+1) AND inst_date < date_sub(current_date(),301+1) THEN date_sub(current_date(),331+1)
            WHEN inst_date >= date_sub(current_date(),361+1) AND inst_date < date_sub(current_date(),331+1) THEN date_sub(current_date(),361+1)
        END AS p_inst_date
    from rds_posflow.bl_flow_by_day
    where
        inst_date >= date_sub(current_date(),361+1)
) t1
group by mcht_cd,p_inst_date

        SUM(CASE WHEN diffdays<= 31                  THEN txn_amt ELSE 0 END) AS amt_1M,
        SUM(CASE WHEN diffdays<= 61                  THEN txn_amt ELSE 0 END) AS amt_2M,
        SUM(CASE WHEN diffdays<= 91                  THEN txn_amt ELSE 0 END) AS amt_3M,
        SUM(CASE WHEN diffdays<=181                  THEN txn_amt ELSE 0 END) AS amt_6M,
        SUM(CASE WHEN diffdays<=361                  THEN txn_amt ELSE 0 END) AS amt_12M,

        SUM(CASE WHEN diffdays> 31 AND diffdays<= 61 THEN txn_amt ELSE 0 END) AS amt_1M_mir,
        SUM(CASE WHEN diffdays> 91 AND diffdays<=181 THEN txn_amt ELSE 0 END) AS amt_3M_mir,
        SUM(CASE WHEN diffdays>181 AND diffdays<=361 THEN txn_amt ELSE 0 END) AS amt_6M_mir,


        SUM(CASE WHEN diffdays<=31                   THEN txn_amt ELSE 0 END) AS amt_12M_1,
        SUM(CASE WHEN diffdays> 31 AND diffdays<= 61 THEN txn_amt ELSE 0 END) AS amt_12M_2,
        SUM(CASE WHEN diffdays> 61 AND diffdays<= 91 THEN txn_amt ELSE 0 END) AS amt_12M_3,
        SUM(CASE WHEN diffdays> 91 AND diffdays<=121 THEN txn_amt ELSE 0 END) AS amt_12M_4,
        SUM(CASE WHEN diffdays>121 AND diffdays<=151 THEN txn_amt ELSE 0 END) AS amt_12M_5,
        SUM(CASE WHEN diffdays>151 AND diffdays<=181 THEN txn_amt ELSE 0 END) AS amt_12M_6,
        SUM(CASE WHEN diffdays>181 AND diffdays<=211 THEN txn_amt ELSE 0 END) AS amt_12M_7,
        SUM(CASE WHEN diffdays>211 AND diffdays<=241 THEN txn_amt ELSE 0 END) AS amt_12M_8,
        SUM(CASE WHEN diffdays>241 AND diffdays<=271 THEN txn_amt ELSE 0 END) AS amt_12M_9,
        SUM(CASE WHEN diffdays>271 AND diffdays<=311 THEN txn_amt ELSE 0 END) AS amt_12M_10,
        SUM(CASE WHEN diffdays>311 AND diffdays<=331 THEN txn_amt ELSE 0 END) AS amt_12M_11,
        SUM(CASE WHEN diffdays>331 AND diffdays<=361 THEN txn_amt ELSE 0 END) AS amt_12M_12,

        SUM(CASE WHEN diffdays<= 7                   THEN txn_amt ELSE 0 END) AS amt_8W_1,
        SUM(CASE WHEN diffdays>  7 AND diffdays<= 14 THEN txn_amt ELSE 0 END) AS amt_8W_2,
        SUM(CASE WHEN diffdays> 14 AND diffdays<= 21 THEN txn_amt ELSE 0 END) AS amt_8W_3,
        SUM(CASE WHEN diffdays> 21 AND diffdays<= 28 THEN txn_amt ELSE 0 END) AS amt_8W_4,
        SUM(CASE WHEN diffdays> 28 AND diffdays<= 35 THEN txn_amt ELSE 0 END) AS amt_8W_5,
        SUM(CASE WHEN diffdays> 35 AND diffdays<= 42 THEN txn_amt ELSE 0 END) AS amt_8W_6,
        SUM(CASE WHEN diffdays> 42 AND diffdays<= 49 THEN txn_amt ELSE 0 END) AS amt_8W_7,
        SUM(CASE WHEN diffdays> 49 AND diffdays<= 56 THEN txn_amt ELSE 0 END) AS amt_8W_8,

        COUNT(DISTINCT(CASE WHEN diffdays <= 31 THEN inst_date ELSE NULL END)) AS active_days_1M

