package com.thtk.service

import com.plj.tools.LoadString
import com.thtk.constant.ConfigTable
import com.thtk.service.BLTransFlowServiceOpt_v2.{get_agt_condition, get_syb_condition}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object BLFile {

  def main(args: Array[String]): Unit = {
    var sql_cmd:String = ""
    val loader: LoadString = new LoadString(this)
    var start:String = if ( args.length > 0 ) args(0) else null
    var end:String = if ( args.length > 1 ) args(1) else null
    var sql_file:String = if ( args.length > 2 ) args(2) else null

    println("############# BL_File 参数个数: %d , 参数1: %s ,参数2: %s ,参数3: %s ".format(args.length,start,end,sql_file))

    // 首先还是创建SparkConf
    val conf = new SparkConf().setAppName("BL_File 参数1: %s ,参数2: %s ,参数3: %s".format(start,end,sql_file))
    // 创建JavaSparkContext
    val sc = new SparkContext(conf)
    // 创建HiveContext，注意，这里，它接收的是SparkContext作为参数，不是JavaSparkContext
    val hc: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //切换数据库schema
    // hc.sql("show databases").show()

    if (sql_file != null && sql_file.length > 0 ) {
      sql_cmd = loader.getString(sql_file,start,end)

      if (sql_cmd == null || sql_cmd.length <= 0 ) {
        println("############# 找不到文件####  替换为 ： /sql/ConstOpt_v2/".concat(sql_file))
        sql_cmd = loader.getString("/sql/ConstOpt_v2/".concat(sql_file)  ,start,end )
      } else {
        println("############# 找到文件####  为 ： ".concat(sql_file))
      }

      if (sql_cmd == null || sql_cmd.length <= 0 ) {
        println("#############找不到文件####" + sql_cmd)
      } else {
        println("############# 执行语句 : " + sql_cmd)
        hc.sql(sql_cmd)
        println("#############执行完毕###############")

      }

    }
    //bl_total_transflow.write.format("orc").mode("overwrite").saveAsTable ("bl_total_transflow")

  }

}
