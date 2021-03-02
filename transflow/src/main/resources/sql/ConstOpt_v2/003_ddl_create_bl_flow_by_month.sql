SET hivevar:MAIN_DB=rds_posflow;
SET hivevar:TEMP_DB=deprecated_db;

DROP TABLE IF EXISTS `${hivevar:TEMP_DB}.bl_flow_by_month_01`;

CREATE TABLE IF NOT EXISTS `${hivevar:TEMP_DB}.bl_flow_by_month_01`(
    mcht_cd                     string
    ,agt_type                   string
    ,product_name1              string
    ,product_name2              string

    ,flow_amt_sell              bigint
    ,flow_vol_sell              int
    ,flow_amt_return            bigint
    ,flow_vol_return            int

    ,flow_amt_net_gt_5w         bigint
    ,flow_amt_net_4w_5w         bigint
    ,flow_amt_net_3w_4w         bigint
    ,flow_amt_net_2w_3w         bigint
    ,flow_amt_net_1w_2w         bigint
    ,flow_amt_net_5k_1w         bigint
    ,flow_amt_net_4k_5k         bigint
    ,flow_amt_net_3k_4k         bigint
    ,flow_amt_net_2k_3k         bigint
    ,flow_amt_net_1k_2k         bigint
    ,flow_amt_net_5h_1k         bigint
    ,flow_amt_net_4h_5h         bigint
    ,flow_amt_net_3h_4h         bigint
    ,flow_amt_net_2h_3h         bigint
    ,flow_amt_net_1h_2h         bigint
    ,flow_amt_net_50_1h         bigint
    ,flow_amt_net_10_50         bigint
    ,flow_amt_net_05_10         bigint
    ,flow_amt_net_01_05         bigint
    ,flow_amt_net_lt_01         bigint

    ,flow_vol_net_gt_5w         int
    ,flow_vol_net_4w_5w         int
    ,flow_vol_net_3w_4w         int
    ,flow_vol_net_2w_3w         int
    ,flow_vol_net_1w_2w         int
    ,flow_vol_net_5k_1w         int
    ,flow_vol_net_4k_5k         int
    ,flow_vol_net_3k_4k         int
    ,flow_vol_net_2k_3k         int
    ,flow_vol_net_1k_2k         int
    ,flow_vol_net_5h_1k         int
    ,flow_vol_net_4h_5h         int
    ,flow_vol_net_3h_4h         int
    ,flow_vol_net_2h_3h         int
    ,flow_vol_net_1h_2h         int
    ,flow_vol_net_50_1h         int
    ,flow_vol_net_10_50         int
    ,flow_vol_net_05_10         int
    ,flow_vol_net_01_05         int
    ,flow_vol_net_lt_01         int

    ,flow_amt_sell_at_22        bigint
    ,flow_amt_sell_at_20        bigint
    ,flow_amt_sell_at_18        bigint
    ,flow_amt_sell_at_16        bigint
    ,flow_amt_sell_at_14        bigint
    ,flow_amt_sell_at_12        bigint
    ,flow_amt_sell_at_10        bigint
    ,flow_amt_sell_at_8         bigint
    ,flow_amt_sell_at_6         bigint
    ,flow_amt_sell_at_4         bigint
    ,flow_amt_sell_at_2         bigint
    ,flow_amt_sell_at_0         bigint

    ,flow_vol_sell_at_22        int
    ,flow_vol_sell_at_20        int
    ,flow_vol_sell_at_18        int
    ,flow_vol_sell_at_16        int
    ,flow_vol_sell_at_14        int
    ,flow_vol_sell_at_12        int
    ,flow_vol_sell_at_10        int
    ,flow_vol_sell_at_8         int
    ,flow_vol_sell_at_6         int
    ,flow_vol_sell_at_4         int
    ,flow_vol_sell_at_2         int
    ,flow_vol_sell_at_0         int

    ,flow_amt_net_credit_card   bigint
    ,flow_amt_net_debit_card    bigint
    ,flow_amt_net_not_card      bigint
    ,flow_vol_net_credit_card   int
    ,flow_vol_net_debit_card    int
    ,flow_vol_net_not_card      int

    ,flow_max_credit_card       bigint
    ,flow_min_credit_card       bigint
    ,flow_max_debit_card        bigint
    ,flow_min_debit_card        bigint
    ,flow_max_not_card          bigint
    ,flow_min_not_card          bigint

    ,active_day                 tinyint

    ,deal_day_01                int
    ,deal_day_02                int
    ,deal_day_03                int
    ,deal_day_04                int
    ,deal_day_05                int
    ,deal_day_06                int
    ,deal_day_07                int
    ,deal_day_08                int
    ,deal_day_09                int
    ,deal_day_10                int
    ,deal_day_11                int
    ,deal_day_12                int
    ,deal_day_13                int
    ,deal_day_14                int
    ,deal_day_15                int
    ,deal_day_16                int
    ,deal_day_17                int
    ,deal_day_18                int
    ,deal_day_19                int
    ,deal_day_20                int
    ,deal_day_21                int
    ,deal_day_22                int
    ,deal_day_23                int
    ,deal_day_24                int
    ,deal_day_25                int
    ,deal_day_26                int
    ,deal_day_27                int
    ,deal_day_28                int
    ,deal_day_29                int
    ,deal_day_30                int
    ,deal_day_31                int

    ,first_date                 date
    ,last_date                  date
) COMMENT '流水月聚合表'
PARTITIONED BY
(
  `inst_date` date
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS
    `${hivevar:MAIN_DB}.bl_flow_by_month`
LIKE
    `${hivevar:TEMP_DB}.bl_flow_by_month_01`;
