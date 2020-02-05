/*
package jhbh.jobs

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.jhbh.common.HBaseUtil
import com.jhbh.util.OffsetManagerUtil
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

object Test1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.spark-project").setLevel(Level.OFF)

    if (args.length < 3 || args == null) {
      println("Parameters Errors! Usage: <batchInterval> <topic> <group>")
      System.exit(-1)
    }
    val Array(batchInterval, topic, group) = Array("3","jsontest","g2")
    //    val Array(batchInterval, topic, group) = args
    //指定列簇和列关系
    val atmap: mutable.HashMap[String, String] = mutable.HashMap[String,String ]()
    atmap.put("atcompanycode","cf1")
    atmap.put("atmancerttype","cf1")
    atmap.put("atmancertcode","cf1")
    atmap.put("atmantime","cf1")
    atmap.put("atmanname","cf1")
    atmap.put("atmanphone","cf1")
    atmap.put("atemployee","cf1")
    atmap.put("atcreatetime","cf1")
    atmap.put("atdestcity","cf1")
    atmap.put("atsource","cf1")
    //    atmap.put("atcode","cf2")
    atmap.put("atname","cf2")
    atmap.put("atweight","cf2")
    atmap.put("atspecial","cf2")
    atmap.put("atmansex","cf2")
    atmap.put("atmanphoneaddress","cf2")
    atmap.put("atmanaddress","cf2")
    atmap.put("ataddress","cf2")
    atmap.put("atlongitude","cf2")
    atmap.put("atlatitude","cf2")
    atmap.put("atdestname","cf2")
    atmap.put("atdestphone","cf2")
    atmap.put("atdestination","cf2")
    atmap.put("atmark","cf2")
    atmap.put("atsuspicious","cf2")
    atmap.put("atuser","cf2")
    atmap.put("atdate","cf2")
    atmap.put("atsendtime","cf2")
    atmap.put("atrecetime","cf2")
    atmap.put("atimgcert","cf2")
    atmap.put("atimgbill","cf2")
    atmap.put("imeicode","cf2")
    atmap.put("atdeletetime","cf2")
    atmap.put("atupdatetime","cf2")
    atmap.put("atrealname","cf2")
    atmap.put("attype","cf2")
    atmap.put("atinterfacee","cf2")
    atmap.put("atinputtype","cf2")
    atmap.put("atimg","cf2")

    val dvmap: mutable.HashMap[String, String] = mutable.HashMap[String,String]()
    dvmap.put("dvcompanycode","cf1")
    dvmap.put("dvmancerttype","cf1")
    dvmap.put("dvmancertcode","cf1")
    dvmap.put("dvmantime","cf1")
    dvmap.put("dvmanname","cf1")
    dvmap.put("dvemployee","cf1")
    dvmap.put("dvcreatetime","cf1")
    dvmap.put("dvtype","cf1")
    dvmap.put("dvinterface","cf1")
    //    dvmap.put("dvcode","cf2")
    dvmap.put("dvname","cf2")
    dvmap.put("dvweight","cf2")
    dvmap.put("dvspecial","cf2")
    dvmap.put("dvmansex","cf2")
    dvmap.put("dvmanphone","cf2")
    dvmap.put("dvmanphoneaddress","cf2")
    dvmap.put("dvmanaddress","cf2")
    dvmap.put("dvaddress","cf2")
    dvmap.put("dvlongitude","cf2")
    dvmap.put("dvlatitude","cf2")
    dvmap.put("dvoriginname","cf2")
    dvmap.put("dvorigin","cf2")
    dvmap.put("dvmark","cf2")
    dvmap.put("dvsuspicious","cf2")
    dvmap.put("dvuser","cf2")
    dvmap.put("dvdate","cf2")
    dvmap.put("dvsendtime","cf2")
    dvmap.put("dvrecetime","cf2")
    dvmap.put("dvimgcert","cf2")
    dvmap.put("dvimg","cf2")
    dvmap.put("dvimgbill","cf2")
    dvmap.put("dvdeletetime","cf2")
    dvmap.put("dvupdatetime","cf2")
    dvmap.put("dvrealname","cf2")
    dvmap.put("dvinputtype","cf2")
    dvmap.put("dvoriginphone","cf2")

    val conf = new SparkConf()
      .setAppName(s"${Test1.getClass.getSimpleName}")
      .setMaster("local[2]")  //集群中运行打包
      .set("spark.streaming.kafka.maxRatePerPartition", "1000") // 每个分区，每次拉取的最大数据条数
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 程序优雅的关闭
    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "service-01:9092,service-02:9092,service-03:9092,service-05:9092,service-06:9092",
      //      "bootstrap.servers" -> "bigdata-01:9092,bigdata-02:9092,bigdata-03:9092",
      //      "bootstrap.servers" -> "service01:9092,service02:9092,service03:9092,service05:9092,service06:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest"
    )
    val message: InputDStream[ConsumerRecord[String, String]] = OffsetManagerUtil.createMsg(ssc, topic.split(",").toSet[String], kafkaParams, group, curator)

    message.foreachRDD((rdd, time) => {
      if (!rdd.isEmpty()) {
        println("-------------------------------------------")
        rdd.foreachPartition(partition => {
          if(!partition.isEmpty){
            val connection = HBaseUtil.getConnection
            val basic_delivery: Table = connection.getTable(TableName.valueOf("basic_delivery"))
            val basic_acceptance: Table = connection.getTable(TableName.valueOf("basic_acceptance"))

            val Index_atcompanycode: Table = connection.getTable(TableName.valueOf("Index_atcompanycode"))
            val Index_atmancerttype: Table = connection.getTable(TableName.valueOf("Index_atmancerttype"))
            val Index_atmancertcode: Table = connection.getTable(TableName.valueOf("Index_atmancertcode"))
            val Index_atmantime: Table = connection.getTable(TableName.valueOf("Index_atmantime"))
            val Index_atmanname: Table = connection.getTable(TableName.valueOf("Index_atmanname"))
            val Index_atmanphone: Table = connection.getTable(TableName.valueOf("Index_atmanphone"))
            val Index_atemployee: Table = connection.getTable(TableName.valueOf("Index_atemployee"))
            val Index_atcreatetime: Table = connection.getTable(TableName.valueOf("Index_atcreatetime"))
            val Index_atdestcity: Table = connection.getTable(TableName.valueOf("Index_atdestcity"))
            val Index_atsource: Table = connection.getTable(TableName.valueOf("Index_atsource"))

            val Index_dvcompanycode: Table = connection.getTable(TableName.valueOf("Index_dvcompanycode"))
            val Index_dvmancerttype: Table = connection.getTable(TableName.valueOf("Index_dvmancerttype"))
            val Index_dvmancertcode: Table = connection.getTable(TableName.valueOf("Index_dvmancertcode"))
            val Index_dvmantime: Table = connection.getTable(TableName.valueOf("Index_dvmantime"))
            val Index_dvmanname: Table = connection.getTable(TableName.valueOf("Index_dvmanname"))
            val Index_dvemployee: Table = connection.getTable(TableName.valueOf("Index_dvemployee"))
            val Index_dvcreatetime: Table = connection.getTable(TableName.valueOf("Index_dvcreatetime"))
            val Index_dvtype: Table = connection.getTable(TableName.valueOf("Index_dvtype"))
            val Index_dvinterface: Table = connection.getTable(TableName.valueOf("Index_dvinterface"))
            partition.foreach(record => {
              val msg: String = record.value()
              if(!msg.equals("")){
                val jsonline: JSONObject = JSON.parseObject(msg)
                val action: String = jsonline.getString(("action"))
                val table: String = jsonline.getString(("table"))
                val time: String = jsonline.getString("time")
                val dataObj: JSONObject = jsonline.getJSONObject("data")
                val random: String = Random.nextInt(100).toString
                if(table.equals("acceptance")){
                  val atkeys: util.Iterator[String] = dataObj.keySet().iterator()
                  val atrowkey: String = dataObj.getString("atbillcode")
                  while (atkeys.hasNext()) {
                    val atkey: String = atkeys.next()
                    val atvalue: String = dataObj.getString(atkey)
                    //"atbillcode"和"atcode"不存入hbase的列中
                    if(!atkey.equals("atbillcode") && !atkey.equals("atcode")){
                      val put = new Put(Bytes.toBytes(atrowkey + "_" + time))
                      println("??????????????????????????????????????????????????????????????????"+atrowkey + "_" + time)
                      put.addColumn(Bytes.toBytes(atmap(atkey)),Bytes.toBytes(atkey), Bytes.toBytes(atvalue))
                      basic_acceptance.put(put)
                      if(atkey.equals("atcompanycode")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes,atrowkey.getBytes)
                        Index_atcompanycode.put(put1)
                      }else if(atkey.equals("atmancerttype")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes,atrowkey.getBytes)
                        Index_atmancerttype.put(put1)
                      }else if(atkey.equals("atmancertcode")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes,atrowkey.getBytes)
                        Index_atmancertcode.put(put1)
                      }else if(atkey.equals("atmantime")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes,atrowkey.getBytes)
                        Index_atmantime.put(put1)
                      }else if(atkey.equals("atmanname")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes,atrowkey.getBytes)
                        Index_atmanname.put(put1)
                      }else if(atkey.equals("atmanphone")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes,atrowkey.getBytes)
                        Index_atmanphone.put(put1)
                      }else if(atkey.equals("atemployee")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes,atrowkey.getBytes)
                        Index_atemployee.put(put1)
                      }else if(atkey.equals("atcreatetime")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes,atrowkey.getBytes)
                        Index_atcreatetime.put(put1)
                      }else if(atkey.equals("atdestcity")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes,atrowkey.getBytes)
                        Index_atdestcity.put(put1)
                      }else if(atkey.equals("atsource")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes,atrowkey.getBytes)
                        Index_atsource.put(put1)
                      }
                    }
                  }
                }else{
                  val dvkeys: util.Iterator[String] = dataObj.keySet().iterator()
                  val dvrowkey: String = dataObj.getString("dvbillcode")
                  while (dvkeys.hasNext()) {
                    val dvkey: String = dvkeys.next()
                    val dvvalue: String = dataObj.getString(dvkey)
                    if(!dvkey.equals("dvbillcode") && !dvkey.equals("dvcode")){
                      val put = new Put(Bytes.toBytes(dvrowkey  + "_" + time))
                      println("??????????????????????????????????????????????????????????????????"+dvrowkey + "_" + time)
                      put.addColumn(Bytes.toBytes(dvmap(dvkey)), Bytes.toBytes(dvkey), Bytes.toBytes(dvvalue))
                      basic_delivery.put(put)
                      if(dvkey.equals("dvcompanycode")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, dvkey.getBytes,dvrowkey.getBytes)
                        Index_dvcompanycode.put(put1)
                      }else if(dvkey.equals("dvmancerttype")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, dvkey.getBytes,dvrowkey.getBytes)
                        Index_dvmancerttype.put(put1)
                      }else if(dvkey.equals("dvmancertcode")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, dvkey.getBytes,dvrowkey.getBytes)
                        Index_dvmancertcode.put(put1)
                      } else if(dvkey.equals("dvmantime")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, dvkey.getBytes,dvrowkey.getBytes)
                        Index_dvmantime.put(put1)
                      }else if(dvkey.equals("dvmanname")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, dvkey.getBytes,dvrowkey.getBytes)
                        Index_dvmanname.put(put1)
                      }else if(dvkey.equals("dvemployee")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, dvkey.getBytes,dvrowkey.getBytes)
                        Index_dvemployee.put(put1)
                      }else if(dvkey.equals("dvcreatetime")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, dvkey.getBytes,dvrowkey.getBytes)
                        Index_dvcreatetime.put(put1)
                      }else if(dvkey.equals("dvtype")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, dvkey.getBytes,dvrowkey.getBytes)
                        Index_dvtype.put(put1)
                      }else if(dvkey.equals("dvinterface")){
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, dvkey.getBytes,dvrowkey.getBytes)
                        Index_dvinterface.put(put1)
                      }
                    }
                  }
                }
              }
            })
            basic_delivery.close()
            basic_acceptance.close()
            HBaseUtil.release(connection)
          }
        })
        //更新offset到zk中
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        OffsetManagerUtil.storeOffset(offsetRanges, group, curator)

      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  val curator = {
    val client = CuratorFrameworkFactory.builder().namespace("wwjtest")
      .connectString("service-01:2181,service-02:2181,service-03:2181,service-05:2181,service-06:2181")
      //      .connectString("bigdata-01:2181,bigdata-02:2181,bigdata-03:2181")
      //        .connectString("service01:2181,service02:2181,service03:2181,service05:2181,service06:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
    client.start()
    client
  }
}

*/
