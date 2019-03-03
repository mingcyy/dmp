package com.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object Tags4Ads extends Tags{
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {

    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]

//    广告位类型和名称
    val adTypeId: Int = row.getAs[Int]("adspacetype")
    val adTypeName: String = row.getAs[String]("adspacetypename")

    if (adTypeId > 9) map += "LC"+adTypeId -> 1
    else if (adTypeId > 0) map += "LC0"+adTypeId -> 1

    if (StringUtils.isNotEmpty(adTypeName)) map += "LN" + adTypeName -> 1

//    渠道的
    val chanelId: Int = row.getAs[Int]("adplatformproviderid")
    if (chanelId > 0) map += (("CN"+chanelId,1))

    map
  }
}















