DROP TABLE IF EXISTS `deprecated_db.bl_total_transflow_v2_01`;

CREATE TABLE IF NOT EXISTS `deprecated_db.bl_total_transflow_v2_01`(
  `sn` string COMMENT '',
  `inst_time` string COMMENT '',
  `settle_date` date COMMENT '',
  `mcht_cd` string COMMENT '',
  `mcht_type` string COMMENT '',
  `term_id` string COMMENT '',
  `card_type` string COMMENT '',
  `currcy_code_trans` string COMMENT '',
  `card_no` string COMMENT '交易卡号',
  `issue_bank` string COMMENT '银行名称',
  `txn_type` string COMMENT '',
  `txn_amt` bigint COMMENT '',
  `trans_amt` bigint COMMENT '',
  `fee` bigint COMMENT '',
  `txn_status` string COMMENT '',
  `org_flg` string COMMENT '发卡机构',
  `org_ext` string COMMENT '银行代码-发卡机构前4位',
  `area_flag` string COMMENT '地区代码-收银宝分公司',
  `ori_dt` string COMMENT '',
  `product_name` string COMMENT '产品名称',
  `agt_type` tinyint,
  `create_time` string COMMENT '创建日期',
  `create_user` string COMMENT '创建人'
) COMMENT '保理流水合并表'
PARTITIONED BY
(
  `inst_date` date
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `rds_posflow.bl_total_transflow_v2`(
  `sn` string COMMENT '',
  `inst_time` int COMMENT '',
  `settle_date` date COMMENT '',
  `mcht_cd` string COMMENT '',
  `mcht_type` string COMMENT '',
  `term_id` string COMMENT '',
  `card_type` tinyint COMMENT '',
  `currcy_code_trans` string COMMENT '',
  `card_no` string COMMENT '交易卡号',
  `issue_bank` string COMMENT '银行名称',
  `txn_type` string COMMENT '',
  `txn_amt` bigint COMMENT '',
  `trans_amt` bigint COMMENT '',
  `fee` bigint COMMENT '',
  `txn_status` string COMMENT '',
  `org_flg` string COMMENT '发卡机构',
  `org_ext` string COMMENT '银行代码-发卡机构前4位',
  `area_flag` string COMMENT '地区代码-收银宝分公司',
  `ori_dt` string COMMENT '',
  `product_name` string COMMENT '产品名称',
  `agt_type` tinyint,
  `create_time` string COMMENT '创建日期',
  `create_user` string COMMENT '创建人'
) COMMENT '保理流水合并表'
PARTITIONED BY
(
  `inst_date` date
)
STORED AS ORC;

