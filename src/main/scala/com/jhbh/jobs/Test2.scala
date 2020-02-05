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
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

case class Relation(table:String,cf:String){}

object Test2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.spark-project").setLevel(Level.OFF)

    /*if (args.length < 3 || args == null) {
      println("Parameters Errors! Usage: <batchInterval> <topic> <group>")
      System.exit(-1)
    }*/
    val Array(batchInterval, topic, group) = Array("3","jsontest","g2")
    //    val Array(batchInterval, topic, group) = args
    //指定表、列簇、列关系
    val relations: mutable.HashMap[String,Relation] = mutable.HashMap[String,Relation]()
    val indexRelations: mutable.HashMap[String,String] = mutable.HashMap[String,String]()
    relations.put("atcompanycode",new Relation("basic_acceptance","cf1"))
    indexRelations.put("atcompanycode","index_atcompanycode")
    relations.put("atmancerttype",new Relation("basic_acceptance","cf1"))
    indexRelations.put("atmancerttype","index_atmancerttype")
    relations.put("atmancertcode",new Relation("basic_acceptance","cf1"))
    indexRelations.put("atmancertcode","index_atmancertcode")
    relations.put("atmantime",new Relation("basic_acceptance","cf1"))
    indexRelations.put("atmantime","index_atmantime")
    relations.put("atmanname",new Relation("basic_acceptance","cf1"))
    indexRelations.put("atmanname","index_atmanname")
    relations.put("atmanphone",new Relation("basic_acceptance","cf1"))
    indexRelations.put("atmanphone","index_atmanphone")
    relations.put("atemployee",new Relation("basic_acceptance","cf1"))
    indexRelations.put("atemployee","index_atemployee")
    relations.put("atcreatetime",new Relation("basic_acceptance","cf1"))
    indexRelations.put("atcreatetime","index_atcreatetime")
    relations.put("atdestcity",new Relation("basic_acceptance","cf1"))
    indexRelations.put("atdestcity","index_atdestcity")
    relations.put("atsource",new Relation("basic_acceptance","cf1"))
    indexRelations.put("atsource","index_atsource")
    relations.put("atmansex",new Relation("basic_acceptance","cf1"))
    relations.put("atmanaddress",new Relation("basic_acceptance","cf1"))
    relations.put("atdestname",new Relation("basic_acceptance","cf1"))
    relations.put("atmark",new Relation("basic_acceptance","cf1"))
    relations.put("atsuspicious",new Relation("basic_acceptance","cf1"))
    relations.put("atrealname",new Relation("basic_acceptance","cf1"))
    relations.put("atname",new Relation("basic_acceptance","cf1"))
    relations.put("atweight",new Relation("basic_acceptance","cf1"))
    relations.put("atdate",new Relation("basic_acceptance","cf1"))


    relations.put("atspecial",new Relation("basic_acceptance","cf2"))
    relations.put("atmanphoneaddress",new Relation("basic_acceptance","cf2"))
    relations.put("ataddress",new Relation("basic_acceptance","cf2"))
    relations.put("atlongitude",new Relation("basic_acceptance","cf2"))
    relations.put("atlatitude",new Relation("basic_acceptance","cf2"))
    relations.put("atdestphone",new Relation("basic_acceptance","cf2"))
    relations.put("atdestination",new Relation("basic_acceptance","cf2"))
    relations.put("atuser",new Relation("basic_acceptance","cf2"))
    relations.put("atsendtime",new Relation("basic_acceptance","cf2"))
    relations.put("atrecetime",new Relation("basic_acceptance","cf2"))
    relations.put("atimgcert",new Relation("basic_acceptance","cf2"))
    relations.put("atimgbill",new Relation("basic_acceptance","cf2"))
    relations.put("imeicode",new Relation("basic_acceptance","cf2"))
    relations.put("atdeletetime",new Relation("basic_acceptance","cf2"))
    relations.put("atupdatetime",new Relation("basic_acceptance","cf2"))
    relations.put("attype",new Relation("basic_acceptance","cf2"))
    relations.put("atinterfacee",new Relation("basic_acceptance","cf2"))
    relations.put("atinputtype",new Relation("basic_acceptance","cf2"))
    relations.put("atimg",new Relation("basic_acceptance","cf2"))

    relations.put("dvcompanycode",new Relation("basic_delivery","cf1"))
    indexRelations.put("atcompanycode","index_atcompanycode")
    relations.put("dvmancerttype",new Relation("basic_delivery","cf1"))
    indexRelations.put("atmancerttype","index_atmancerttype")
    relations.put("atmancertcode",new Relation("basic_delivery","cf1"))
    indexRelations.put("atmancertcode","index_atmancertcode")
    relations.put("dvmantime",new Relation("basic_delivery","cf1"))
    indexRelations.put("dvmantime","index_dvmantime")
    relations.put("dvmanname",new Relation("basic_delivery","cf1"))
    indexRelations.put("dvmanname","index_dvmanname")
    relations.put("dvemployee",new Relation("basic_delivery","cf1"))
    indexRelations.put("dvemployee","index_dvemployee")
    relations.put("dvcreatetime",new Relation("basic_delivery","cf1"))
    indexRelations.put("dvcreatetime","index_dvcreatetime")
    relations.put("dvtype",new Relation("basic_delivery","cf1"))
    indexRelations.put("dvtype","index_dvtype")
    relations.put("dvinterface",new Relation("basic_delivery","cf1"))
    indexRelations.put("dvinterface","index_dvinterface")
    relations.put("dvmansex",new Relation("basic_delivery","cf1"))
    relations.put("dvmanaddress",new Relation("basic_delivery","cf1"))
    relations.put("dvname",new Relation("basic_delivery","cf1"))
    relations.put("dvweight",new Relation("basic_delivery","cf1"))
    relations.put("dvspecial",new Relation("basic_delivery","cf1"))
    relations.put("dvrealname",new Relation("basic_delivery","cf1"))
    relations.put("dvmark",new Relation("basic_delivery","cf1"))
    relations.put("dvsuspicious",new Relation("basic_delivery","cf1"))
    relations.put("dvdate",new Relation("basic_delivery","cf1"))

    relations.put("dvmanphone",new Relation("basic_delivery","cf2"))
    relations.put("dvmanphoneaddress",new Relation("basic_delivery","cf2"))
    relations.put("dvaddress",new Relation("basic_delivery","cf2"))
    relations.put("dvlongitude",new Relation("basic_delivery","cf2"))
    relations.put("dvlatitude",new Relation("basic_delivery","cf2"))
    relations.put("dvoriginname",new Relation("basic_delivery","cf2"))
    relations.put("dvorigin",new Relation("basic_delivery","cf2"))
    relations.put("dvuser",new Relation("basic_delivery","cf2"))
    relations.put("dvsendtime",new Relation("basic_delivery","cf2"))
    relations.put("dvrecetime",new Relation("basic_delivery","cf2"))
    relations.put("dvimgcert",new Relation("basic_delivery","cf2"))
    relations.put("dvimg",new Relation("basic_delivery","cf2"))
    relations.put("dvimgbill",new Relation("basic_delivery","cf2"))
    relations.put("dvdeletetime",new Relation("basic_delivery","cf2"))
    relations.put("dvupdatetime",new Relation("basic_delivery","cf2"))
    relations.put("dvinputtype",new Relation("basic_delivery","cf2"))
    relations.put("dvoriginphone",new Relation("basic_delivery","cf2"))

    val conf = new SparkConf()
      .setAppName(s"${Test2.getClass.getSimpleName}")
      .setMaster("local[2]")  //集群中运行打包
      .set("spark.streaming.kafka.maxRatePerPartition", "1000") // 每个分区，每次拉取的最大数据条数
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 程序优雅的关闭
    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))
    val kafkaParams = Map[String, Object](
      //      "bootstrap.servers" -> "hdp01:9092,hdp02:9092",
      //     "bootstrap.servers" -> "service-01:9092,service-02:9092,service-03:9092,service-05:9092,service-06:9092",
      "bootstrap.servers" -> "bigdata-01:9092,bigdata-02:9092,bigdata-03:9092",
      //      "bootstrap.servers" -> "service01:9092,service02:9092,service03:9092,service05:9092,service06:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest"
    )
    val message: InputDStream[ConsumerRecord[String, String]] = OffsetManagerUtil.createMsg(ssc, topic.split(",").toSet[String], kafkaParams, group, curator)

    message.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("-------------------------------------------")
        rdd.foreachPartition(partition => {
          if(!partition.isEmpty){
            val connection = HBaseUtil.getConnection
            //基本表链接集合
            val basicTablesConnection: ArrayBuffer[Table] = ArrayBuffer[Table]()
            val btnames: mutable.ArrayBuffer[String] = mutable.ArrayBuffer("basic_acceptance","basic_delivery")

            //索引表链接集合
            val indexTablesConnection: ArrayBuffer[Table] = ArrayBuffer[Table]()
            val itnames: Iterator[String] = indexRelations.valuesIterator

//            val indtnames: Array[String] = indexRelations.keySet.toArray[String]
            for(i <- 0 until btnames.size){
              val btname: String = btnames(i)
              println("获取的基本表表名为：" + btname)
              basicTablesConnection(i) = connection.getTable(TableName.valueOf(btname))
            }
            for(i <- 0 until itnames.size){
              while (itnames.hasNext){
                println("获取的索引表表名为：" + itnames.next())
                indexTablesConnection(i) = connection.getTable(TableName.valueOf(itnames.next()))
              }
            }
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
                  val atrowkey: String = dataObj.getString(random + "atbillcode") + "_" + time
                  while (atkeys.hasNext()) {
                    val atkey: String = atkeys.next()
                    val atvalue: String = dataObj.getString(atkey)
                    //"atbillcode"和"atcode"不存入hbase的列中
                    if(!atkey.equals("atbillcode") && !atkey.equals("atcode")) {
                      val put = new Put(atrowkey.getBytes())
                      put.addColumn(relations(atkey).cf.getBytes(), atkey.getBytes(), atvalue.getBytes())
                      println("要保存的基本表数据为：" + atrowkey + ", " + relations(atkey).cf + "," + atkey + "," + atvalue)
                      for (table <- basicTablesConnection) {
                        if (table.getName.toString.equals(relations(atkey).table)) {
                          println("要存的基本表为：" + table)
                          table.put(put)
                        }
                      }
                      if (indexRelations.contains(atkey)) {
                        val put1 = new Put(Bytes.toBytes(random + "_" + atvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, atkey.getBytes, atrowkey.getBytes)
                        println("要保存的索引表数据为：" + random + "_" + atvalue + "_" + time + ", " + "cf1" + "," + atkey + "," + atrowkey)
                        for (table <- indexTablesConnection) {
                          if (table.getName.toString.equals(indexRelations(atkey))) {
                            println("要存的基本表为：" + table)
                            table.put(put1)
                          }
                        }
                      }
                    }
                  }
                }else{
                  val dvkeys: util.Iterator[String] = dataObj.keySet().iterator()
                  val dvrowkey: String = dataObj.getString(random + "dvbillcode") + "_" + time
                  while (dvkeys.hasNext()) {
                    val dvkey: String = dvkeys.next()
                    val dvvalue: String = dataObj.getString(dvkey)
                    if(!dvkey.equals("dvbillcode") && !dvkey.equals("dvcode")){
                      val put = new Put(dvrowkey.getBytes())
                      put.addColumn(relations(dvkey).cf.getBytes(), dvkey.getBytes(), dvvalue.getBytes())
                      println("要保存的基本表数据为：" + dvrowkey + ", " + relations(dvkey).cf + "," + dvkey + "," + dvvalue)
                      for (table <- basicTablesConnection) {
                        if (table.getName.toString.equals(relations(dvkey).table)) {
                          println("要存的基本表为：" + table)
                          table.put(put)
                        }
                      }
                      if (indexRelations.contains(dvkey)) {
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvvalue + "_" + time))
                        put1.addColumn("cf1".getBytes, dvkey.getBytes, dvrowkey.getBytes)
                        println("要保存的索引表数据为：" + random + "_" + dvvalue + "_" + time + ", " + "cf1" + "," + dvkey + "," + dvrowkey)
                        for (table <- indexTablesConnection) {
                          if (table.getName.toString.equals(indexRelations(dvkey))) {
                            println("要存的基本表为：" + table)
                            table.put(put1)
                          }
                        }
                      }

                    }
                  }
                }
              }
            })
            for (table <- basicTablesConnection) {
              table.close()
            }
            for(table <- indexTablesConnection){
              table.close()
            }
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
      //      .connectString("hdp01:2181,hdp02:2181,hdp03:2181")
      //     .connectString("service-01:2181,service-02:2181,service-03:2181,service-05:2181,service-06:2181")
      .connectString("bigdata-01:2181,bigdata-02:2181,bigdata-03:2181")
      //        .connectString("service01:2181,service02:2181,service03:2181,service05:2181,service06:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
    client.start()
    client
  }
}



*/
