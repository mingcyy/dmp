package com.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object Tags4App extends Tags{
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row = args(0).asInstanceOf[Row]

    val appDict: Map[String, String] = args(1).asInstanceOf[Map[String,String]]

    val appId: String = row.getAs[String]("appid")
    val appName: String = row.getAs[String]("appname")

    if(StringUtils.isEmpty(appName)){
      appDict.contains(appId) match {
        case true => map += "App" + appDict.get(appId) -> 1
      }
    }
    map
  }
}











