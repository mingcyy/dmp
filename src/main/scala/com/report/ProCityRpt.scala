package com.report

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object ProCityRpt {
  def main(args: Array[String]): Unit = {
    //    判断输入参数个数书否正确，否则退出
    if(args.length != 2){
      println(
        """
          |ProCityRpt job 需要有三个参数
          |参数1：输入文件
          |参数2：输出文件
          |参数3：输出文件压缩格式
        """.stripMargin)
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[*]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df: DataFrame = sqlContext.read.parquet(inputPath)
    df.registerTempTable("log")
    val result: DataFrame = sqlContext.sql("select provincename, cityname, count(*) ct from log group by provincename,cityname")

    val hadoopConfiguration: Configuration = sc.hadoopConfiguration

    val fs: FileSystem = FileSystem.get(hadoopConfiguration)

    val resultPath = new Path(outputPath)
    if(fs.exists(resultPath)){
      fs.delete(resultPath,true)
    }
//    result.coalesce(1).write.json(outputPath)
    val load: Config = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),props)

    sc.stop()

  }
}
