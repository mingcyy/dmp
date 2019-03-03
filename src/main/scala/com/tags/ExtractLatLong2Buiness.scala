package com.tags

import ch.hsr.geohash.GeoHash
import com.tools.BaiduGeoApi
import com.utils.JedisPools
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object ExtractLatLong2Buiness {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      """
        |cn.dmp.report.AppAnalyseRptV2
        |参数：
        | 输入路径
      """.stripMargin
      sys.exit()
    }
    val Array(inputPath) = args

    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sc)

    sQLContext.read.parquet(inputPath)
      .select("lat","long")
      .where("lat > 3 and lat < 54 and long >73 and long <136").distinct()
      .foreachPartition(iter => {
        val jedis: Jedis = JedisPools.getJedis()
        iter.foreach(row => {
          val lat: Double = row.getAs[String]("lat").toDouble
          val long: Double = row.getAs[String]("long").toDouble
          val geoHashCode: String = GeoHash.withCharacterPrecision(lat,long,8).toBase32
          val business: String = BaiduGeoApi.getBusiness(lat + "," + long)
          if (StringUtils.isNotEmpty(business)) {
            jedis.set(geoHashCode, business)
          }
        })
        jedis.close()
      })
  }

}

















