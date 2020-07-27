DROP TABLE IF EXISTS `deprecated_db.bl_flow_by_week_01`;

CREATE TABLE IF NOT EXISTS `deprecated_db.bl_flow_by_week_01`(
    mcht_cd                     string
    ,agt_type                   string
    ,product_name1              string
    ,product_name2              string

    ,flow_amt_sell              bigint
    ,flow_vol_sell              int
    ,flow_amt_return            bigint
    ,flow_vol_return            int

    ,active_day                 tinyint
) COMMENT '流水周聚合表-按照岑亮实现的通金逻辑'
PARTITIONED BY
(
  `p_week`      tinyint,
  `inst_date`   date
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS
    `rds_posflow.bl_flow_by_week`
LIKE
    `deprecated_db.bl_flow_by_week_01`;
