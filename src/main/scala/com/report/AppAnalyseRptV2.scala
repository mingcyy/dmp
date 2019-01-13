package com.report

import com.beans.Log
import com.utils.{JedisPools, RptUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object AppAnalyseRptV2 {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      """
        |cn.dmp.report.AppAnalyseRptV2
        |参数：
        | 输入路径
        | 输出路径
      """.stripMargin
      sys.exit()
    }

    val Array(inputPath,outputPath) = args

//    创建sparkConf对象
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

//    val broadcastRulse: Map[String, String] = sc.textFile("aaa").map(line => {
//      val split: Array[String] = line.split(",")
//      (split(0), split(1))
//    }).collect().toMap
//
//    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(broadcastRulse)

    sc.textFile(inputPath)
      .map(_.split(","))
      .filter(_.length >= 85)
      .map(Log(_))
      .filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
      .mapPartitions(itr => {
        val jedis: Jedis = JedisPools.getJedis()

        var parResult = new collection.mutable.ListBuffer[(String,List[Double])]()
        itr.foreach(log => {
          var newAppName: String = log.appname
          if (StringUtils.isEmpty(newAppName)){
//            newAppName = broadcast.value.getOrElse(log.appid,"未知")
            newAppName = jedis.get(log.appid)
          }
          val req = RptUtils.caculateReq(log.requestmode, log.processnode)
          val rtb = RptUtils.caculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
          val showClick = RptUtils.caculateShowClick(log.requestmode, log.iseffective)

          parResult += ((newAppName, req ++ rtb ++ showClick))

        })
        jedis.close()
        parResult.toIterator
      }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1 + "," + t._2.mkString(","))
      .saveAsTextFile(outputPath)
    sc.stop()
  }
}
