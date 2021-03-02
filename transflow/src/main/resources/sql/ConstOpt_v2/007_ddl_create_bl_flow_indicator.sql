SET hivevar:MAIN_DB=rds_posflow;
SET hivevar:TEMP_DB=deprecated_db;

-- DROP TABLE IF EXISTS `${hivevar:MAIN_DB}.bl_flow_indicator`;

CREATE TABLE IF NOT EXISTS `${hivevar:MAIN_DB}.bl_flow_indicator`(
    mcht_cd                     string
    ,agt_type                   string
    ,product_name1              string
    ,product_name2              string

    ,amt_1M                     bigint
    ,amt_2M                     bigint
    ,amt_3M                     bigint
    ,amt_6M                     bigint
    ,amt_12M                    bigint
    ,amt_1M_mir                 bigint
    ,amt_3M_mir                 bigint
    ,amt_6M_mir                 bigint
    ,amt_12M_1                  bigint
    ,amt_12M_2                  bigint
    ,amt_12M_3                  bigint
    ,amt_12M_4                  bigint
    ,amt_12M_5                  bigint
    ,amt_12M_6                  bigint
    ,amt_12M_7                  bigint
    ,amt_12M_8                  bigint
    ,amt_12M_9                  bigint
    ,amt_12M_10                 bigint
    ,amt_12M_11                 bigint
    ,amt_12M_12                 bigint
    ,active_days_1M             int

    ,amt_9W_1                   bigint
    ,amt_9W_2                   bigint
    ,amt_9W_3                   bigint
    ,amt_9W_4                   bigint
    ,amt_9W_5                   bigint
    ,amt_9W_6                   bigint
    ,amt_9W_7                   bigint
    ,amt_9W_8                   bigint
    ,amt_9W_9                   bigint

) COMMENT '流水周聚合表-按照岑亮实现的通金逻辑'
PARTITIONED BY
(
  `inst_date`   date
)
STORED AS ORC;

-- CREATE TABLE IF NOT EXISTS
--     `${hivevar:MAIN_DB}.bl_flow_by_week`
-- LIKE
--     `${hivevar:TEMP_DB}.bl_flow_by_week_01`;
