package com.tags

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object Tags4Buiness extends Tags{
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {

    var map = Map[String,Int]()
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    val lat = row.getAs[String]("lat")
    val long = row.getAs[String]("long")

    if (StringUtils.isNotEmpty(lat) && StringUtils.isNotEmpty(long)){

      var lat2 = lat.toDouble
      var long2 = long.toDouble

      if(lat2 > 3 && lat2 < 54 && long2 > 73 && long2 < 136){
        val geoHashCode: String = GeoHash.withCharacterPrecision(lat2,long2,8).toBase32
        val business: String = jedis.get(geoHashCode)
        if(StringUtils.isNotEmpty(business)) business.split(",").foreach(bs => "BS" + bs -> 1)

      }
    }

    map
  }
}
