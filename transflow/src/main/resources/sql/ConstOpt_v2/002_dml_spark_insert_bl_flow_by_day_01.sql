INSERT OVERWRITE TABLE deprecated_db.bl_flow_by_day_01
    PARTITION (inst_date)
SELECT
    mcht_cd
    ,MAX(agt_type)      AS agt_type
    ,MAX(product_name)  AS product_name1
    ,MIN(product_name)  AS product_name2

    ,SUM(CASE WHEN                                  txn_amt >       0 THEN                                      txn_amt ELSE 0 END) AS flow_amt_sell
    ,SUM(CASE WHEN                                  txn_amt >       0 THEN                                            1 ELSE 0 END) AS flow_vol_sell
    ,SUM(CASE WHEN                                  txn_amt <       0 THEN                                      txn_amt ELSE 0 END) AS flow_amt_return
    ,SUM(CASE WHEN                                  txn_amt <       0 THEN                                            1 ELSE 0 END) AS flow_vol_return

    ,SUM(CASE WHEN abs(txn_amt) >= 5000000                            THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_gt_5w
    ,SUM(CASE WHEN abs(txn_amt) >= 4000000 and abs(txn_amt) < 5000000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_4w_5w
    ,SUM(CASE WHEN abs(txn_amt) >= 3000000 and abs(txn_amt) < 4000000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_3w_4w
    ,SUM(CASE WHEN abs(txn_amt) >= 2000000 and abs(txn_amt) < 3000000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_2w_3w
    ,SUM(CASE WHEN abs(txn_amt) >= 1000000 and abs(txn_amt) < 2000000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_1w_2w
    ,SUM(CASE WHEN abs(txn_amt) >=  500000 and abs(txn_amt) < 1000000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_5k_1w
    ,SUM(CASE WHEN abs(txn_amt) >=  400000 and abs(txn_amt) <  500000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_4k_5k
    ,SUM(CASE WHEN abs(txn_amt) >=  300000 and abs(txn_amt) <  400000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_3k_4k
    ,SUM(CASE WHEN abs(txn_amt) >=  200000 and abs(txn_amt) <  300000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_2k_3k
    ,SUM(CASE WHEN abs(txn_amt) >=  100000 and abs(txn_amt) <  200000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_1k_2k
    ,SUM(CASE WHEN abs(txn_amt) >=   50000 and abs(txn_amt) <  100000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_5h_1k
    ,SUM(CASE WHEN abs(txn_amt) >=   40000 and abs(txn_amt) <   50000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_4h_5h
    ,SUM(CASE WHEN abs(txn_amt) >=   30000 and abs(txn_amt) <   40000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_3h_4h
    ,SUM(CASE WHEN abs(txn_amt) >=   20000 and abs(txn_amt) <   30000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_2h_3h
    ,SUM(CASE WHEN abs(txn_amt) >=   10000 and abs(txn_amt) <   20000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_1h_2h
    ,SUM(CASE WHEN abs(txn_amt) >=    5000 and abs(txn_amt) <   10000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_50_1h
    ,SUM(CASE WHEN abs(txn_amt) >=    1000 and abs(txn_amt) <    5000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_10_50
    ,SUM(CASE WHEN abs(txn_amt) >=     500 and abs(txn_amt) <    1000 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_05_10
    ,SUM(CASE WHEN abs(txn_amt) >=     100 and abs(txn_amt) <     500 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_01_05
    ,SUM(CASE WHEN abs(txn_amt) >        0 and abs(txn_amt) <     100 THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_lt_01

    ,SUM(CASE WHEN abs(txn_amt) >= 5000000                            THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_gt_5w
    ,SUM(CASE WHEN abs(txn_amt) >= 4000000 and abs(txn_amt) < 5000000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_4w_5w
    ,SUM(CASE WHEN abs(txn_amt) >= 3000000 and abs(txn_amt) < 4000000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_3w_4w
    ,SUM(CASE WHEN abs(txn_amt) >= 2000000 and abs(txn_amt) < 3000000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_2w_3w
    ,SUM(CASE WHEN abs(txn_amt) >= 1000000 and abs(txn_amt) < 2000000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_1w_2w
    ,SUM(CASE WHEN abs(txn_amt) >=  500000 and abs(txn_amt) < 1000000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_5k_1w
    ,SUM(CASE WHEN abs(txn_amt) >=  400000 and abs(txn_amt) <  500000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_4k_5k
    ,SUM(CASE WHEN abs(txn_amt) >=  300000 and abs(txn_amt) <  400000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_3k_4k
    ,SUM(CASE WHEN abs(txn_amt) >=  200000 and abs(txn_amt) <  300000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_2k_3k
    ,SUM(CASE WHEN abs(txn_amt) >=  100000 and abs(txn_amt) <  200000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_1k_2k
    ,SUM(CASE WHEN abs(txn_amt) >=   50000 and abs(txn_amt) <  100000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_5h_1k
    ,SUM(CASE WHEN abs(txn_amt) >=   40000 and abs(txn_amt) <   50000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_4h_5h
    ,SUM(CASE WHEN abs(txn_amt) >=   30000 and abs(txn_amt) <   40000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_3h_4h
    ,SUM(CASE WHEN abs(txn_amt) >=   20000 and abs(txn_amt) <   30000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_2h_3h
    ,SUM(CASE WHEN abs(txn_amt) >=   10000 and abs(txn_amt) <   20000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_1h_2h
    ,SUM(CASE WHEN abs(txn_amt) >=    5000 and abs(txn_amt) <   10000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_50_1h
    ,SUM(CASE WHEN abs(txn_amt) >=    1000 and abs(txn_amt) <    5000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_10_50
    ,SUM(CASE WHEN abs(txn_amt) >=     500 and abs(txn_amt) <    1000 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_05_10
    ,SUM(CASE WHEN abs(txn_amt) >=     100 and abs(txn_amt) <     500 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_01_05
    ,SUM(CASE WHEN abs(txn_amt) >        0 and abs(txn_amt) <     100 THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_lt_01

    ,SUM(CASE WHEN inst_time >=220000                           THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_22
    ,SUM(CASE WHEN inst_time >=200000 and inst_time <220000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_20
    ,SUM(CASE WHEN inst_time >=180000 and inst_time <200000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_18
    ,SUM(CASE WHEN inst_time >=160000 and inst_time <180000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_16
    ,SUM(CASE WHEN inst_time >=140000 and inst_time <160000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_14
    ,SUM(CASE WHEN inst_time >=120000 and inst_time <140000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_12
    ,SUM(CASE WHEN inst_time >=100000 and inst_time <120000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_10
    ,SUM(CASE WHEN inst_time >= 80000 and inst_time <100000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_8
    ,SUM(CASE WHEN inst_time >= 60000 and inst_time < 80000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_6
    ,SUM(CASE WHEN inst_time >= 40000 and inst_time < 60000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_4
    ,SUM(CASE WHEN inst_time >= 20000 and inst_time < 40000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_2
    ,SUM(CASE WHEN                        inst_time < 20000     THEN ( CASE WHEN txn_amt <= 0 THEN 0 ELSE txn_amt END ) ELSE 0 END) AS flow_amt_sell_at_0

    ,SUM(CASE WHEN inst_time >=220000                                THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_22
    ,SUM(CASE WHEN inst_time >=200000 and inst_time <220000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_20
    ,SUM(CASE WHEN inst_time >=180000 and inst_time <200000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_18
    ,SUM(CASE WHEN inst_time >=160000 and inst_time <180000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_16
    ,SUM(CASE WHEN inst_time >=140000 and inst_time <160000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_14
    ,SUM(CASE WHEN inst_time >=120000 and inst_time <140000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_12
    ,SUM(CASE WHEN inst_time >=100000 and inst_time <120000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_10
    ,SUM(CASE WHEN inst_time >= 80000 and inst_time <100000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_8
    ,SUM(CASE WHEN inst_time >= 60000 and inst_time < 80000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_6
    ,SUM(CASE WHEN inst_time >= 40000 and inst_time < 60000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_4
    ,SUM(CASE WHEN inst_time >= 20000 and inst_time < 40000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_2
    ,SUM(CASE WHEN                        inst_time < 20000          THEN ( CASE WHEN txn_amt <= 0 THEN  0 ELSE 1 END ) ELSE 0 END) AS flow_vol_sell_at_0

    ,SUM(CASE WHEN card_type = 0                                      THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_credit_card
    ,SUM(CASE WHEN card_type > 0                                      THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_debit_card
    ,SUM(CASE WHEN card_type < 0                                      THEN                                      txn_amt ELSE 0 END) AS flow_amt_net_not_card
    ,SUM(CASE WHEN card_type = 0                                      THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_credit_card
    ,SUM(CASE WHEN card_type > 0                                      THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_debit_card
    ,SUM(CASE WHEN card_type < 0                                      THEN ( CASE WHEN txn_amt < 0 THEN -1 ELSE 1 END ) ELSE 0 END) AS flow_vol_net_not_card

    ,MAX(CASE WHEN card_type = 0 and txn_amt > 0                      THEN                                      txn_amt        END) AS flow_max_credit_card
    ,MIN(CASE WHEN card_type = 0 and txn_amt > 0                      THEN                                      txn_amt        END) AS flow_min_credit_card
    ,MAX(CASE WHEN card_type > 0 and txn_amt > 0                      THEN                                      txn_amt        END) AS flow_max_debit_card
    ,MIN(CASE WHEN card_type > 0 and txn_amt > 0                      THEN                                      txn_amt        END) AS flow_min_debit_card
    ,MAX(CASE WHEN card_type < 0 and txn_amt > 0                      THEN                                      txn_amt        END) AS flow_max_not_card
    ,MIN(CASE WHEN card_type < 0 and txn_amt > 0                      THEN                                      txn_amt        END) AS flow_min_not_card

    ,inst_date
from
    rds_posflow.bl_total_transflow_v2
WHERE
    (inst_date >= '${1}' and inst_date < '${2}' ) and
    txn_status = '处理成功' and ( txn_amt > 0 or txn_type = '3081' )
GROUP BY mcht_cd, inst_date
-- 要
-- “处理成功”微信退货，负的，退货 3081
--  “处理成功”微信支付，正的，1011，后来被退货，但还是处理成功

-- 不要
-- 已撤销， 微信支付 的原始消费被撤销，正的，不要，1011
--  处理成功，微信撤销，撤销的消费，负的，不要 3011

-- 不要
-- 已冲正， 消费， 1011
-- 处理成功，消费冲正 ，2011