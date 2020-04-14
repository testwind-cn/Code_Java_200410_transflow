package com.thtk.constant

object Const {

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
        |select
        |sn,
        |psn,
        |mcht_cd,
        |ssfgs,
        |tzjg,
        |whjg,
        |md_id,
        |term_id,
        |trans_date,
        |inst_date,
        |product_name,
        |trans_type,
        |trans_status,
        |trans_card,
        |card_type,
        |card_org,
        |txn_amt,
        |trans_amt,
        |jfzq,
        |fee_status,
        |fee,
        |cost,
        |service_cost,
        |income,
        |mcc18,
        |mcc42,
        |zhhqfsbs,
        |ywlx,
        |bill_no,
        |dfsh_no,
        |dfzh_no,
        |sstj,
        |zdsqm,
        |zdpch,
        |zdgzh,
        |jyck_no,
        |qd_no,
        |dyqd_mcht_no,
        |trans_remark,
        |trans_zy,
        |trans_ip,
        |res_code,
        |commit_time,
        |error_code,
        |error_reason,
        |qdjy_type
        |from t1_trxrecprd_v2
        |
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
    """select   pos.sn as SN
      |      		,translate(substring(txn_time,1,10),'-','') as INST_DATE
      |      		,translate(substring(txn_time,12,8),':','') as INST_TIME
      |      		,pos.setl_date as SETTLE_DATE
      |      		,pos.mcht_cd as MCHT_CD
      |      		,substring(pos.mcht_cd,8,4) as MCHT_TYPE
      |      		,pos.term_id as TERM_ID
      |      		,case when pan.sl_cd is null then '' else pan.sl_cd end as  CARD_TYPE
      |      		,'156' as  CURRCY_CODE_TRANS
      |      		,'00000000' as CARD_NO
      |      		,cardbank.card_org_name as  ISSUE_BANK
      |      		,trxnum.sl_cd as  TXN_TYPE
      |      		,pos.txn_amt as  TXN_AMT
      |      		,pos.txn_amt-pos.fee as TRANS_AMT
      |      		,pos.fee as FEE
      |      		,txn_status as STATUS
      |      		,pos.card_org as ORG_FLG
      |      		,'' as  AREA_FLAG
      |      		,translate(concat(translate(substring(txn_time,3,8),'-',''),translate(substring(txn_time,12,8),':','')),' ','') as  ORI_DT
      |         ,"2" as AGT_TYPE
      |      from syb_posflow pos
      |      left join t99_trxnum_code trxnum
      |      on pos.txn_type = trxnum.syb_cd
      |      left join t99_pan1_code pan
      |      on pos.card_type = pan.syb_cd
      |      left join T99_cardbank_code cardbank
      |      on pos.card_org = cardbank.card_org_ref
      |      where trxnum.sl_cd <> '9999'
      """.stripMargin('|')

    //保理原受821间连
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
                              |"0" as AGT_TYPE
                              |from
                              |rds_posflow.loginfo_rsp_agt_bl
                              |
                              """.stripMargin('|')

  //保理原受821直连
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
                                  |"1" as AGT_TYPE
                                  |from
                                  |rds_posflow.loginfo_rsp_bl
                                  |
                                  """.stripMargin('|')
}
