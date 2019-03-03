package com.tags

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.{JedisPools, TagsUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object Tags4Ctx {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      """
        |cn.dmp.report.AppAnalyseRptV2
        |参数：
        | 输入路径
        | 输出路径
        | 字典文件路径
        | 停用字典路径
      """.stripMargin
      sys.exit()
    }
    val Array(inputPath, outputPath, dictFilePath,stopWrodsFilePath,day) = args

    //    创建sparkConf对象
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

//    app 字典文件
    val broadcastRulse: Map[String, String] = sc.textFile(dictFilePath).map(line => {
      val split: Array[String] = line.split(",")
      (split(0), split(1))
    }).collect().toMap
//    广播app字典
    val AppDictBroadcast: Broadcast[Map[String, String]] = sc.broadcast(broadcastRulse)

//    停用字典文件
    val stopWordsMap: Map[String, Int] = sc.textFile(stopWrodsFilePath).map((_,0)).collect().toMap
//    广播停用字典
    val stopWordsBroadcast: Broadcast[Map[String, Int]] = sc.broadcast(stopWordsMap)

    //    创建load对象，以读取配置文件中的变量
    val load: Config = ConfigFactory.load()

//    创建config对象，设置zookeeperip地址
    val configuration: Configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.host"))

//    判断hbase中的表是否存在。如果不存在则创建
    val hbaseConn: Connection = ConnectionFactory.createConnection(configuration)

//    得到Admin对象，以操作hbase表
    val hbaseAdmin: Admin = hbaseConn.getAdmin

    val hbTableName: String = load.getString("hbase.table.name")
//    如果hbase中没有改表，则需要创建该表
    if(!hbaseAdmin.tableExists(TableName.valueOf(hbTableName))){
      print(s"$hbTableName 不存在" )
      print(s"$hbTableName 正在创建")
//    获取HtableDescriptor对象，传入表名称
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbTableName))
//      创建columnDescriptor对象，确定列族名称
      val columnDescriptor = new HColumnDescriptor("fs")
//      添加列族
      tableDescriptor.addFamily(columnDescriptor)
//      创建表
      hbaseAdmin.createTable(tableDescriptor)
//      关闭资源
      hbaseAdmin.close()
      hbaseConn.close()
    }

//    指定key的输入类型
    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(Class[TableOutputFormat])
//    设置输出到哪张表中
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbTableName)

    sqlContext.read.parquet(inputPath).where(TagsUtils.hasSomeUserIdCondition).mapPartitions(par => {
      val jedis: Jedis = JedisPools.getJedis()
      val listBuffer = new collection.mutable.ListBuffer[(String,List[(String,Int)])]
      par.foreach(row => {
        //行数据进行标签化处理
        //      关于广告的标签
        val ads: Map[String, Int] = Tags4Ads.makeTags(row)
        //      关于app的标签
        val apps: Map[String, Int] = Tags4App.makeTags(row, AppDictBroadcast.value)
        //      关于设备的标签
        val devices: Map[String, Int] = Tags4Devices.makeTags(row)
        //      关于停用词的标签
        val keywords: Map[String, Int] = Tags4KeyWords.makeTags(row, stopWordsBroadcast.value)
        //          关于商圈标签
        val business: Map[String, Int] = Tags4Buiness.makeTags(row, jedis)

        //      获取userId
        val allUserId: ListBuffer[String] = TagsUtils.getAllUserId(row)

        listBuffer.append((allUserId(0), (ads ++ apps ++ devices ++ keywords).toList))
        listBuffer
      })
      jedis.close()
      listBuffer.iterator
    }).rdd.reduceByKey((a,b) => {
//      第一种方式
      (a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
//      第二种方式
      /*(a ++ b).groupBy(_._1).map{
        case (k,sameTags) => (k,sameTags.map(_._2).sum)
      }.toList*/

    }).map {
      case (userId,userTags) => {
//      创建put对象  封装每条数据信息
        val put = new Put(Bytes.toBytes(userId))
//        将表现信息  转换为 String
        val tags: String = userTags.map(t => t._1 + ":" + t._2).mkString(",")
//        插入数据，在列 'cf'列族 下   day列  插入数据 tags
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(s"day$day"),Bytes.toBytes(tags))
//        map 完后，返回元组类型，第一个元素 为rowkey ，第二个元素为
        (new ImmutableBytesWritable(),put)   //ImmutableBytesWritable  ==> rowkey
      }
    }.saveAsHadoopDataset(jobConf)
//      .saveAsTextFile(outputPath)
    sc.stop()
  }
}
