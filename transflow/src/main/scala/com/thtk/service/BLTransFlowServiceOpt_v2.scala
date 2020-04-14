package com.thtk.service

import java.time.LocalDateTime
import java.util.Calendar

import com.plj.tools.{LoadString, TimeTools}
import com.thtk.constant.ConfigTable
import org.apache.spark.sql.hive.HiveContext

//import com.thtk.common.factory.Trx
import com.thtk.constant.ConstOpt
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 保理的流水处理，数据源表由python导入到Hive中
  * 821表 <loginfo_rsp_agt_bl> <loginfo_rsp_bl>
  * 收银宝 <t1_trxrecprd_v2_zc>
  * 作者：zhaohu
  * 时间：2019/01/17
  * 处理全量数据首次导入动态分区表
  **/
object BLTransFlowServiceOpt_v2 {

  def get_condition(cond_1:String, cond_2:String, start:String, end:String):String = {
    // current_date()
    var start_v:String = null
    var end_v:String = null

    if ( start != null && start.length == 10)
      start_v = "'%s'".format(start)
    else if ( start == null || start.length == 0 )
      start_v = "'%s'".format(TimeTools.GetDateStr(TimeTools.GetCalendar(null, -10)))
    else
      start_v = start

    if ( end != null && end.length == 10)
      end_v = "'%s'".format(end)
    else
      end_v = end

    if ( start_v == null || start_v.length == 0 )
      return ""
    else if ( end_v == null || end_v.length == 0 )
      return cond_1.format(start_v,start_v)
    else
      return cond_2.format(start_v,end_v,start_v,end_v)
  }

  def get_agt_condition(start:String, end:String):String = {
    var cond_1 = """
        p_date >= date_add( %s , -2 ) and
        from_unixtime(unix_timestamp(trim(tx_date), 'yyyyMMdd'), 'yyyy-MM-dd') >= %s and
"""
    var cond_2 = """
        p_date >= date_add( %s , -2 ) and
        p_date <  date_add( %s , 14 ) and
        from_unixtime(unix_timestamp(trim(tx_date), 'yyyyMMdd'), 'yyyy-MM-dd') >= %s and
        from_unixtime(unix_timestamp(trim(tx_date), 'yyyyMMdd'), 'yyyy-MM-dd') <  %s and
"""
    return get_condition(cond_1,cond_2,start,end)
  }

  def get_syb_condition(start:String, end:String):String = {
    var cond_1 = """
        p_date >= date_add( %s , -2 ) and
        to_date( substring(trim(pos.trans_date), 1, 10) ) >= %s and
"""
    var cond_2 = """
        p_date >= date_add( %s , -2 ) and
        p_date <  date_add( %s , 14 ) and
        to_date( substring(trim(pos.trans_date), 1, 10) ) >= %s and
        to_date( substring(trim(pos.trans_date), 1, 10) ) <  %s and
"""
    return get_condition(cond_1,cond_2,start,end)
  }

  def main(args: Array[String]): Unit = {
    var sql_cmd:String = ""
    val loader: LoadString = new LoadString(this)
    var start:String = if ( args.length > 0 ) args(0) else null
    var end:String = if ( args.length > 1 ) args(1) else null

    println("############# 参数个数: %d , 参数1: %s ,参数2: %s ".format(args.length,start,end))

    if ( start == null || start.length <= 0)
      start = TimeTools.GetDateStr(null.asInstanceOf[LocalDateTime], -10, "yyyy-MM-dd")

    if ( end == null || end.length <= 0)
      end = TimeTools.GetDateStr(null.asInstanceOf[LocalDateTime], 2, "yyyy-MM-dd")

    println("############# 参数个数: %d , 参数1: %s ,参数2: %s ".format(args.length,start,end))

    // 首先还是创建SparkConf
    val conf = new SparkConf().setAppName("BLTransFlowOpt_v2")
    // 创建JavaSparkContext
    val sc = new SparkContext(conf)
    // 创建HiveContext，注意，这里，它接收的是SparkContext作为参数，不是JavaSparkContext
    val hc: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //切换数据库schema
    // hc.sql("show databases").show()
    hc.sql(s"use ${ConfigTable.DATABASE_NAME}")
    hc.udf.register("month_head",
      (inst_date: java.sql.Date,day:java.lang.Integer) =>
      if ( inst_date == null || day == null || day < 1 || day > 31 )
        null
      else {
        var the_date = inst_date.toLocalDate
        var the_date0 = java.time.LocalDate.of(the_date.getYear-1,12,day)
        var the_date1 = the_date0.plusMonths(the_date.getMonthValue-1)
        var the_date2 = the_date0.plusMonths(the_date.getMonthValue)
        if ( the_date2.compareTo(the_date) <= 0)
          java.sql.Date.valueOf(the_date2)
        else
          java.sql.Date.valueOf(the_date1)
      }
    )

    //收银宝
    sql_cmd = loader.getString("/sql/ConstOpt_v2/bl_shouyinbao_v2.sql", start,end )
    println("############# bl_shouyinbao_v2 : " + sql_cmd)
    var bl_shouyinbao_transflow_df = hc.sql(sql_cmd)
    // 注册DataFrame为临时表
    bl_shouyinbao_transflow_df.explain(true)
    bl_shouyinbao_transflow_df.registerTempTable("tmp_syb_pos_flow")
    /*bl_shouyinbao_transflow_df.write.format("orc").
      mode("overwrite").
      saveAsTable ("tmp_db.tmp_syb_pos_flow")

    bl_shouyinbao_transflow_df = null
     */

    //821表直连
    sql_cmd = loader.getString("/sql/ConstOpt_v2/ys_821_original_v2.sql", start,end )
    println("############# ys_821_original_sql : " + sql_cmd)
    var BL821OriginalDF = hc.sql(sql_cmd)
    // 注册DataFrame为临时表
    BL821OriginalDF.explain(true)
    BL821OriginalDF.registerTempTable("tmp_original_pos_flow")
    /*
    BL821OriginalDF.write.format("orc").
      mode("overwrite").
      saveAsTable ("tmp_db.tmp_original_pos_flow")
    BL821OriginalDF = null

     */

    //821表间连
    sql_cmd = loader.getString("/sql/ConstOpt_v2/ys_821_agt_original_v2.sql", start,end )
    println("############# ys_821_agt_original_sql : " + sql_cmd)
    var BL821AgtOriginalDF = hc.sql(sql_cmd)
    // 注册DataFrame为临时表
    BL821AgtOriginalDF.explain(true)
    BL821AgtOriginalDF.registerTempTable("tmp_agt_original_pos_flow")
    /*
    BL821AgtOriginalDF.write.format("orc").
      mode("overwrite").
      saveAsTable ("tmp_db.tmp_agt_original_pos_flow")
    BL821AgtOriginalDF = null
     */




    /*
    val bl_total_transflow = BL821OriginalDF.
      unionAll(BL821AgtOriginalDF).
      unionAll(bl_shouyinbao_transflow_df).
      coalesce(1)

    bl_total_transflow.registerTempTable("tmp_pos_flow_all")
     */

    //插入bl_total_transflow


    sql_cmd = loader.getString("/sql/ConstOpt_v2/insertSql_All_3.sql")
    println("############# insertSql_All_3 : " + sql_cmd)
    var flow_all = hc.sql(sql_cmd)
    flow_all.explain(true)

    flow_all. // repartition(1).
      write.
      format("orc").
      mode(org.apache.spark.sql.SaveMode.Overwrite).
      partitionBy("inst_date").
      insertInto("deprecated_db.bl_total_transflow_v2_01")

    println("#############收银宝流水插入完毕###############")


/*
    flow_all.write.
      format("orc").
      mode(org.apache.spark.sql.SaveMode.Overwrite).
      partitionBy("inst_date").
      saveAsTable("rds_posflow.bl_total_transflow_v2_01")
sqlContext.sql("""alter table `rds_posflow`.`bl_flow_by_month_tj` drop partition (inst_date <> '2000-01-01')""")

    aaa.write.
      format("orc").
      mode(org.apache.spark.sql.SaveMode.Overwrite).
      partitionBy("inst_date").
      insertInto("rds_posflow.bl_flow_by_month_tj")
*/
  }

}
