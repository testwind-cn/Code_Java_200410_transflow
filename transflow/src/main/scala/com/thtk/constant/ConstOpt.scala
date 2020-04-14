package com.thtk.constant

object ConstOpt {

  /**
    * 资产数据处理
    */
  //821流水处理
  val LOGINFO_RSP_AGT_ZC_FIELD = " LoginfoRspAgtZcUDF.replaceBlank(sn) as sn ," +
    "inst_date," +
    "inst_time," +
    "LoginfoRspAgtZcUDF.replaceBlank(mcht_cd) as mcht_cd," +
    "LoginfoRspAgtZcUDF.subStringStr(LoginfoRspAgtZcUDF.replaceBlank(term_id),8) as term_id," +
    "LoginfoRspAgtZcUDF.replaceBlank(card_type) as card_type," +
    "currcy_code_trans," +
    "pan1," +
    "txn_amt," +
    "trans_amt "

  //收银宝流水处理
  val T1_TRXRECPRD_V2_ZC = " LoginfoRspAgtZcUDF.replaceBlank(sn) as sn,  " +
    "LoginfoRspAgtZcUDF.handleDate(trans_date,\"date\") as inst_date, "+
    "LoginfoRspAgtZcUDF.handleDate(trans_date,\"time\") as inst_time, "+
    "LoginfoRspAgtZcUDF.replaceBlank(mcht_cd) as mcht_cd, "+
    "LoginfoRspAgtZcUDF.subStringStr(LoginfoRspAgtZcUDF.replaceBlank(term_id),8) as term_id, "+
    "LoginfoRspAgtZcUDF.transferCardTypeByDesc(LoginfoRspAgtZcUDF.replaceBlank(card_type)) as card_type," +
    "\"156\" as currcy_code_trans," +
    "LoginfoRspAgtZcUDF.handlePan1(trans_card) as pan1, "+
    "LoginfoRspAgtZcUDF.transferTranTypeByDesc(LoginfoRspAgtZcUDF.replaceBlank(trans_type)) as txn_amt," +
    "LoginfoRspAgtZcUDF.handleTransAmt(trans_amt) as trans_amt," +
    "LoginfoRspAgtZcUDF.replaceBlank(trans_status) as trans_status"

  //  object Const {
  /**
    * 保理数据处理
    *
    */
  //查询收银宝原始数据
  val shouyinbao_original_sql =
  """
    select
   |trim(sn) as sn ,
   |trim(psn) as psn ,
   |trim(mcht_cd) as mcht_cd ,
   |trim(ssfgs) as corp ,
   |trim(tzjg) as ins_dev ,
   |trim(whjg) as branch ,
   |trim(md_id) as s_no ,
   |trim(term_id) as term_id ,
   |trim(trans_date) as txn_time ,
   |trim(inst_date) as setl_date ,
   |trim(product_name) as prd_name ,
   |trim(trans_type) as txn_type ,
   |trim(trans_status) as txn_status ,
   |trim(trans_card) as card_no ,
   |trim(card_type) as card_type ,
   |trim(card_org) as card_org ,
   |trim(txn_amt) as txn_initamt ,
   |trim(trans_amt) as txn_amt ,
   |trim(jfzq) as fee_period ,
   |trim(fee_status) as fee_status ,
   |trim(fee) as fee ,
   |trim(cost) as channel_cost ,
   |trim(service_cost) as b_fee ,
   |trim(income) as netincome ,
   |trim(mcc18) as mcc_18 ,
   |trim(mcc42) as mcc_42 ,
   |trim(zhhqfsbs) as get_id ,
   |trim(ywlx) as bus_type ,
   |trim(bill_no) as order_no ,
   |trim(dfsh_no) as other_cd ,
   |trim(dfzh_no) as other_account ,
   |trim(sstj) as way ,
   |trim(zdsqm) as term_grant_no ,
   |trim(zdpch) as term_batch_no ,
   |trim(zdgzh) as term_track_no ,
   |trim(jyck_no) as txnm_ref_no ,
   |trim(qd_no) as channel_no ,
   |trim(dyqd_mcht_no) as channel_cd ,
   |trim(trans_remark) as txn_commet ,
   |trim(trans_zy) as txn_remark ,
   |trim(trans_ip) as trans_ip ,
   |trim(res_code) as res_code ,
   |trim(commit_time) as commit_time ,
   |trim(error_code) as error_code ,
   |trim(error_reason) as error_reason ,
   |trim(qdjy_type) as qdjy_type
   |from t1_trxrecprd_v2
      """.stripMargin

  //查询收银宝原始数据
  val loginfo_rsp_agt_bl_sql = ""


  val rowString = """line(0),
                    |line(1),
                    |line(2),
                    |line(3),
                    |line(4),
                    |line(5),
                    |line(6),
                    |substr(line(7),16),
                    |line(8),
                    |line(9),
                    |line(10),
                    |line(11),
                    |line(12),
                    |line(13),
                    |line(14),
                    |line(15),
                    |line(16),
                    |line(17).toDouble,Double
                    |line(18),
                    |line(19),
                    |line(20).toDouble,Double
                    |line(21),
                    |line(22),
                    |line(23),
                    |line(24),
                    |line(25),
                    |line(26),
                    |line(27),
                    |line(28),
                    |line(29),
                    |line(30),
                    |line(31),
                    |line(32),
                    |line(33),
                    |line(34),
                    |line(35),
                    |line(36),
                    |line(37),
                    |line(38),
                    |line(39)
                  """.stripMargin
  //  }

  //  -- t1_trxrecprd_v2
  val bl_shouyinbao_sql =
    """
      |select   trim(pos.sn) as SN
      |            		,translate(substring(trim(pos.trans_date),1,10),'-','') as INST_DATE
      |            		,translate(substring(trim(pos.trans_date),12,8),':','') as INST_TIME
      |            		,trim(pos.inst_date) as SETTLE_DATE
      |            		,trim(pos.mcht_cd) as MCHT_CD
      |            		,substring(trim(pos.mcht_cd),8,4) as MCHT_TYPE
      |            		,trim(pos.term_id) as TERM_ID
      |            		,case when pan.sl_cd is null then '' else pan.sl_cd end as  CARD_TYPE
      |            		,'156' as  CURRCY_CODE_TRANS
      |            		,'00000000' as CARD_NO
      |            		,cardbank.card_org_name as  ISSUE_BANK
      |            		,trxnum.sl_cd as  TXN_TYPE
      |            		,trim(pos.trans_amt) as  TXN_AMT
      |            		,trim(pos.trans_amt)-trim(pos.fee) as TRANS_AMT
      |            		,trim(pos.fee) as FEE
      |            		,trim(pos.trans_status) as txn_status
      |            		,trim(pos.card_org) as ORG_FLG
      |            		,'' as  AREA_FLAG
      |            		,translate(concat(translate(substring(trim(pos.trans_date),3,8),'-',''),translate(substring(trim(pos.trans_date),12,8),':','')),' ','') as  ORI_DT
      |               ,"2" as AGT_TYPE
      |               ,current_date() as create_time
      |               ,current_user() as create_user
      |            from t1_trxrecprd_v2 pos
      |            left join t99_trxnum_code trxnum
      |            on trim(pos.trans_type) = trxnum.syb_cd
      |            left join t99_pan1_code pan
      |            on trim(pos.card_type) = pan.syb_cd
      |            left join T99_cardbank_code cardbank
      |            on trim(pos.card_org) = cardbank.card_org_ref
      |            where trxnum.sl_cd <> '9999'
    """.stripMargin('|')

  //保理原受821表直连
  val ys_821_original_sql = """select
                              |trim(sn) as SN,
                              |trim(tx_date) as INST_DATE,
                              |trim(tx_time) as INST_TIME,
                              |trim(settle_time) as SETTLE_DATE,
                              |trim(mcht_cd) as MCHT_CD,
                              |trim(mcht_type) as MCHT_TYPE,
                              |trim(device_id) as TERM_ID,
                              |trim(card_type) as CARD_TYPE,
                              |trim(currency) as CURRCY_CODE_TRANS,
                              |trim(card_no) as CARD_NO,
                              |trim(card_bank) as ISSUE_BANK,
                              |trim(trans_type) as TXN_TYPE,
                              |trim(trans_amt) as TXN_amt,
                              |trim(settle_amt) as TRANS_AMT,
                              |trim(fee) as fee,
                              |case when trim(trans_status) = "成功" then "处理成功" end as TXN_STATUS,
                              |trim(card_inst) as ORG_FLG,
                              |trim(trans_area) as AREA_FLAG,
                              |trim(origin_time) as ORI_DT,
                              |"0" as AGT_TYPE,
                              |current_date() as create_time,
                              |current_user() as create_user
                              |from
                              |rds_posflow.loginfo_rsp_bl
                              |
                              """.stripMargin('|')

  //保理原受821表间连
  val ys_821_agt_original_sql = """select
                                  |trim(sn) as SN,
                                  |trim(tx_date) as INST_DATE,
                                  |trim(tx_time) as INST_TIME,
                                  |trim(settle_time) as SETTLE_DATE,
                                  |trim(mcht_cd) as MCHT_CD,
                                  |trim(mcht_type) as MCHT_TYPE,
                                  |trim(device_id) as TERM_ID,
                                  |trim(card_type) as CARD_TYPE,
                                  |trim(currency) as CURRCY_CODE_TRANS,
                                  |trim(card_no) as CARD_NO,
                                  |trim(card_bank) as ISSUE_BANK,
                                  |trim(trans_type) as TXN_TYPE,
                                  |trim(trans_amt) as TXN_amt,
                                  |trim(settle_amt) as TRANS_AMT,
                                  |trim(fee) as FEE,
                                  |case when trim(trans_status) = "成功" then "处理成功" end as TXN_STATUS,
                                  |trim(card_inst) as ORG_FLG,
                                  |trim(trans_area) as AREA_FLAG,
                                  |trim(origin_time) as ORI_DT,
                                  |"1" as AGT_TYPE,
                                  |current_date() as create_time,
                                  |current_user() as create_user
                                  |from
                                  |rds_posflow.loginfo_rsp_agt_bl
                                  |
                                  """.stripMargin('|')

  // //821表直连插入到hive
  val insertSqlOne = """INSERT INTO TABLE rds_posflow.bl_total_transflow
                    |PARTITION(inst_date,agt_type)
                    |SELECT
                    |sn,
                   |inst_time,
                   |settle_date,
                   |mcht_cd,
                   |mcht_type,
                   |term_id,
                   |card_type,
                   |currcy_code_trans,
                   |card_no,
                   |issue_bank,
                   |txn_type,
                   |txn_amt,
                   |trans_amt,
                   |fee,
                   |txn_status,
                   |org_flg,
                   |area_flag,
                   |ori_dt,
                   |create_time,
                   |create_user,
                   |inst_date,
                   |'0' as agt_type
                   |from tmp_original_pos_flow
                  """.stripMargin

  // //821表间连插入到hive
  val insertSqlTwo = """INSERT INTO TABLE rds_posflow.bl_total_transflow
                       |PARTITION(inst_date,agt_type)
                       |SELECT
                       |sn,
                       |inst_time,
                       |settle_date,
                       |mcht_cd,
                       |mcht_type,
                       |term_id,
                       |card_type,
                       |currcy_code_trans,
                       |card_no,
                       |issue_bank,
                       |txn_type,
                       |txn_amt,
                       |trans_amt,
                       |fee,
                       |txn_status,
                       |org_flg,
                       |area_flag,
                       |ori_dt,
                       |create_time,
                       |create_user,
                       |inst_date,
                       |'1' as agt_type
                       |from tmp_agt_original_pos_flow
                     """.stripMargin

  // //收银宝流水表插入到hive
  val insertSqlThree = """INSERT INTO TABLE rds_posflow.bl_total_transflow
                       |PARTITION(inst_date,agt_type)
                       |SELECT
                       |sn,
                       |inst_time,
                       |settle_date,
                       |mcht_cd,
                       |mcht_type,
                       |term_id,
                       |card_type,
                       |currcy_code_trans,
                       |card_no,
                       |issue_bank,
                       |txn_type,
                       |txn_amt,
                       |trans_amt,
                       |fee,
                       |txn_status,
                       |org_flg,
                       |area_flag,
                       |ori_dt,
                       |create_time,
                       |create_user,
                       |inst_date,
                       |'2' as agt_type
                       |from tmp_syb_pos_flow
                     """.stripMargin
}
