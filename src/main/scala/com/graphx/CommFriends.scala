package com.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CommFriends {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")

    sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)

    val uv: RDD[(VertexId, (String, Int))] = sc.parallelize(Seq(
      (1, ("蔡亚婷", 18)),
      (2, ("丑苏", 18)),
      (6, ("蔡璐", 18)),
      (9, ("刘静", 18)),
      (133, ("李英英", 18)),
      (16, ("小风", 18)),
      (21, ("周洁", 18)),
      (44, ("王璐", 18)),
      (138, ("吴珊", 18)),
      (5, ("铁柱", 18)),
      (7, ("王丹", 18)),
      (18, ("段鸽", 18))
    ))

    val ue: RDD[Edge[Int]] = sc.parallelize(Seq(
      Edge(1, 133, 0),
      Edge(2, 133, 0),
      Edge(6, 133, 0),
      Edge(9, 133, 0),
      Edge(6, 138, 0),
      Edge(16, 138, 0),
      Edge(44, 138, 0),
      Edge(21, 138, 0),
      Edge(5, 158, 0),
      Edge(7, 158, 0)
    ))

    val graph = Graph(uv,ue)
    val commonV: VertexRDD[VertexId] = graph.connectedComponents().vertices

//    commonV.map(t => (t._2,List(t._1))).reduceByKey(_ ++ _).foreach(println)

    uv.join(commonV).map{
      case (userId,((name,age),cmId)) => (cmId,List((name,age)))
    }.reduceByKey(_ ++ _).foreach(println)

  }
}





