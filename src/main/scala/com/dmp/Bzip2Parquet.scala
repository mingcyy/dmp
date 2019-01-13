package com.dmp

import com.utils.{NBF, SchemaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将原始文件转换成parquet文件
  */
object Bzip2Parquet {

  def main(args: Array[String]): Unit = {

    //    判断输入参数个数书否正确，否则退出
    if (args.length != 3) {
      println(
        """
          |Bzip2Parquet job 需要有三个参数
          |参数1：输入文件
          |参数2：输出文件
          |参数3：输出文件压缩格式
        """.stripMargin)
      sys.exit()
    }

    //    接收传递的参数
    val Array(inputPath, outputPath, compressionCode) = args

    val conf = new SparkConf()

    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", compressionCode)
    val inputData: RDD[Row] = sc.textFile(inputPath)
      .map(_.split(",", -1))
      .filter(_.length >= 85)
      .map(arr => {
        Row(
          arr(0),
          NBF.toInt(arr(1)),
          NBF.toInt(arr(2)),
          NBF.toInt(arr(3)),
          NBF.toInt(arr(4)),
          arr(5),
          arr(6),
          NBF.toInt(arr(7)),
          NBF.toInt(arr(8)),
          NBF.toDouble(arr(9)),
          NBF.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          NBF.toInt(arr(17)),
          arr(18),
          arr(19),
          NBF.toInt(arr(20)),
          NBF.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          NBF.toInt(arr(26)),
          arr(27),
          NBF.toInt(arr(28)),
          arr(29),
          NBF.toInt(arr(30)),
          NBF.toInt(arr(31)),
          NBF.toInt(arr(32)),
          arr(33),
          NBF.toInt(arr(34)),
          NBF.toInt(arr(35)),
          NBF.toInt(arr(36)),
          arr(37),
          NBF.toInt(arr(38)),
          NBF.toInt(arr(39)),
          NBF.toDouble(arr(40)),
          NBF.toDouble(arr(41)),
          NBF.toInt(arr(42)),
          arr(43),
          NBF.toDouble(arr(44)),
          NBF.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          NBF.toInt(arr(57)),
          NBF.toDouble(arr(58)),
          NBF.toInt(arr(59)),
          NBF.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          NBF.toInt(arr(73)),
          NBF.toDouble(arr(74)),
          NBF.toDouble(arr(75)),
          NBF.toDouble(arr(76)),
          NBF.toDouble(arr(77)),
          NBF.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          NBF.toInt(arr(84))
        )
      })
    val dataFrame: DataFrame = sqlContext.createDataFrame(inputData, SchemaUtils.logStructType)

    dataFrame.write.partitionBy("provincename", "cityname").parquet(outputPath)
  }
}
