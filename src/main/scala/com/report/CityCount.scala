package com.report

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.SchemaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object CityCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[*]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val dataRDD: RDD[Row] = sc.textFile(args(0)).map(_.split(",")).map(line => {
      Row(line)
    })

    val dataFrame: DataFrame = sqlContext.createDataFrame(dataRDD,SchemaUtils.logStructType)

    dataFrame.registerTempTable("log")

    val result: DataFrame = sqlContext.sql(
      """ select
        | provincename, cityname,
        | sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) 有效请求,
        | sum(case when requestmode=1 and processnode =3 then 1 else 0 end) 广告请求,
        | sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 then 1 else 0 end) 参与竞价数,
        | sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) 竞价成功数,
        | sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) 展示数,
        | sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) 点击数,
        | sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*adpayment/1000 else 0 end) 广告成本,
        | sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*winprice/1000 else 0 end) 广告消费
        | from log
        | group by provincename,cityname""".stripMargin)

    val load: Config = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user",load.getString("jdbc.user"))
    props.setProperty("password",load.getString("jdbc.password"))

    result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),props)

    sc.stop()

  }
}

