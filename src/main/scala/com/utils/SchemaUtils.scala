package com.utils

import org.apache.spark.sql.types._

object SchemaUtils {

    /**
      * 定义日志的Schema结构信息
      */
    val logStructType = StructType(Seq(
        StructField("sessionid", StringType),               //会话标识
        StructField("advertisersid", IntegerType),          //广告主id
        StructField("adorderid", IntegerType),              //广告id
        StructField("adcreativeid", IntegerType),           //广告创意id
        StructField("adplatformproviderid", IntegerType),   //广告平台商id
        StructField("sdkversion", StringType),              //sdk版本号
        StructField("adplatformkey", StringType),           //平台商key
        StructField("putinmodeltype", IntegerType),         //针对广告主的投放模式，1：展示投放   2：点击量投放
        StructField("requestmode", IntegerType),            //数据请求方式(1:请求    2：展示    3:点击)
        StructField("adprice", DoubleType),                 //广告价格
        StructField("adppprice", DoubleType),               //平台商价格
        StructField("requestdate", StringType),             //请求时间，格式为 yyyy-mm-dd hh:mm:ss
        StructField("ip", StringType),                      //设备用户的真实ip地址
        StructField("appid", StringType),                   //应用id
        StructField("appname", StringType),                 //应用名称
        StructField("uuid", StringType),                    //设备唯一标识
        StructField("device", StringType),                  //设备型号，如htc、iphone
        StructField("client", IntegerType),                 //设备类型  1：android   2:ios   3:wp
        StructField("osversion", StringType),               //设备操作系统版本
        StructField("density", StringType),                 //设备屏幕的密度
        StructField("pw", IntegerType),                     //设备屏幕的宽度
        StructField("ph", IntegerType),                     //设备屏幕的高度
        StructField("long", StringType),                    //设备所在的经度
        StructField("lat", StringType),                     //设备所在的纬度
        StructField("provincename", StringType),            //设备所在省份名称
        StructField("cityname", StringType),                //设备所在城市名称
        StructField("ispid", IntegerType),                  //运营商id
        StructField("ispname", StringType),                 //运营商名称
        StructField("networkmannerid", IntegerType),        //联网方式
        StructField("networkmannername", StringType),       //联网方式名称
        StructField("iseffective", IntegerType),            //有效标识
        StructField("isbilling", IntegerType),              //是否收费   0 ： 未收费   1： 已收费
        StructField("adspacetype", IntegerType),            //广告位类型  1 banner   2 插屏   3 全屏
        StructField("adspacetypename", StringType),         //广告位类型名称  banner  chaping  全屏
        StructField("devicetype", IntegerType),             //设备类型  1 手机    2 平板
        StructField("processnode", IntegerType),            //流程节点  1 请求量kpi  2 有效请求   3  广告请求
        StructField("apptype", IntegerType),                //应用类型id
        StructField("district", StringType),                //设备所在县名称
        StructField("paymode", IntegerType),                //针对平台商的支付模式  1 展示量投放CPM   2 点击
        StructField("isbid", IntegerType),                  //是否rtb
        StructField("bidprice", DoubleType),                //rtb竞价价格
        StructField("winprice", DoubleType),                //rtb竞价成功价格
        StructField("iswin", IntegerType),                  //是佛竞价成功
        StructField("cur", StringType),                     //value usd  rmb  等
        StructField("rate", DoubleType),                    //汇率
        StructField("cnywinprice", DoubleType),             //rtb 竞价成功转换成人民币的价格
        StructField("imei", StringType),                    //imei
        StructField("mac", StringType),                     //mac
        StructField("idfa", StringType),                    //idfa
        StructField("openudid", StringType),                //openudid
        StructField("androidid", StringType),               //androidid
        StructField("rtbprovince", StringType),             //rtb 省
        StructField("rtbcity", StringType),                 //rtb 市
        StructField("rtbdistrict", StringType),             //rtb 区
        StructField("rtbstreet", StringType),               //rtb 街道
        StructField("storeurl", StringType),                //app 的市场下载地址
        StructField("realip", StringType),                  //真实ip
        StructField("isqualityapp", IntegerType),           //优选标识
        StructField("bidfloor", DoubleType),                //低价
        StructField("aw", IntegerType),                     //广告位的宽
        StructField("ah", IntegerType),                     //广告位的高
        StructField("imeimd5", StringType),                 //imei_md5
        StructField("macmd5", StringType),                  //mac_md5
        StructField("idfamd5", StringType),                 //idfa_md5
        StructField("openudidmd5", StringType),             //openudid_md5
        StructField("androididmd5", StringType),            //androidid_md5
        StructField("imeisha1", StringType),                //imei_sha1
        StructField("macsha1", StringType),                 //mac_sha1
        StructField("idfasha1", StringType),                //idfa_sha1
        StructField("openudidsha1", StringType),            //openudid_sha1
        StructField("androididsha1", StringType),           //androidid_sha1
        StructField("uuidunknow", StringType),              //uuid_unknow tanx 密文
        StructField("userid", StringType),                  //平台用户id
        StructField("iptype", IntegerType),                 //标识ip类型
        StructField("initbidprice", DoubleType),            //初始出价
        StructField("adpayment", DoubleType),               //转换后的广告消费
        StructField("agentrate", DoubleType),               //代理商利润率
        StructField("lomarkrate", DoubleType),              //代理利润率
        StructField("adxrate", DoubleType),                 //媒介利润率
        StructField("title", StringType),                   //标题
        StructField("keywords", StringType),                //关键字
        StructField("tagid", StringType),                   //广告位标识(当视频流量时值为视频ID号)
        StructField("callbackdate", StringType),            //回调时间
        StructField("channelid", StringType),               //频道ID
        StructField("mediatype", IntegerType)               //媒体类型 1 长尾媒体  2 视频媒体   3 独立媒体   默认 1
    ))

}
