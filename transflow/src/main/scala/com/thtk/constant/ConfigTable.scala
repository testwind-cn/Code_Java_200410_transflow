package com.thtk.constant

/**
  * 配置表参数常量类
  * 作者：guop
  * 时间：2019/1/8
  **/
object ConfigTable {

  val DATABASE_NAME = "rds_posflow"

  // trxnum 对应表t99_trxnum_code的 columns
  //  收银宝代码
  val T99_TRXNUM_CODE_SYB_CD = "syb_cd"
  //  受理中文
  val T99_TRXNUM_CODE_CN = "cn"
  //  受理代码
  val T99_TRXNUM_CODE_SL_CD = "sl_cd"

  //pan columns
  //收银宝代码
  val T99_PAN_SYB_CD = "syb_cd"
  //受理代码
  val T99_PAN_SL_CD = "sl_cd"

  // cardbank columns
  //发卡机构代码
  val T99_CARD_BANK_CARD_ORG_REF = "card_org_ref"
  //发卡机构名称
  val T99_CARD_BANK_CARD_ORG_NAME = "card_org_name"
}



