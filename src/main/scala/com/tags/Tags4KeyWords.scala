package com.tags

import org.apache.spark.sql.Row

object Tags4KeyWords extends Tags{
  /**
    * 打标签的方法定义
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]
    val stopWords = args(1).asInstanceOf[Map[String,Int]]

    val kws: String = row.getAs[String]("keywords")
    kws.split("\\|").filter(kw => kw.trim.length >= 3 && kw.trim.length <= 8 && !stopWords.contains(kw.trim))
      .foreach(map += "K" + _ -> 1)

    map
  }
}









