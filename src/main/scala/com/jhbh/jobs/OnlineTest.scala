package com.jhbh.jobs

import java.io.{File, PrintWriter}
import java.util
import com.alibaba.fastjson.{JSON, JSONObject}
import com.jhbh.util.Hbaseutils
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class Relation(table: String, cf: String) {}

object OnlineTest {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.spark-project").setLevel(Level.OFF)
    val Array(topic, group) = Array("jsontesttt", "g8888")

    //指定表、列簇、列关系,b开头代表基本表，i开头代表索引表
    val brelations: mutable.HashMap[String, Relation] = mutable.HashMap[String, Relation]()
    val irelations: mutable.HashMap[String, String] = mutable.HashMap[String, String]()

    brelations.put("atcompanycode", new Relation("basic_acceptance", "cf1"))
    irelations.put("atcompanycode", "index_atcompanycode")
    brelations.put("atmancerttype", new Relation("basic_acceptance", "cf1"))
    irelations.put("atmancerttype", "index_atmancerttype")
    brelations.put("atmancertcode", new Relation("basic_acceptance", "cf1"))
    irelations.put("atmancertcode", "index_atmancertcode")
    brelations.put("atmantime", new Relation("basic_acceptance", "cf1"))
    irelations.put("atmantime", "index_atmantime")
    brelations.put("atmanname", new Relation("basic_acceptance", "cf1"))
    irelations.put("atmanname", "index_atmanname")
    brelations.put("atmanphone", new Relation("basic_acceptance", "cf1"))
    irelations.put("atmanphone", "index_atmanphone")
    brelations.put("atemployee", new Relation("basic_acceptance", "cf1"))
    irelations.put("atemployee", "index_atemployee")
    //    brelations.put("atcreatetime",new Relation("basic_acceptance","cf1"))
    //    irelations.put("atcreatetime","index_atcreatetime")
    brelations.put("atdestcity", new Relation("basic_acceptance", "cf1"))
    irelations.put("atdestcity", "index_atdestcity")
    brelations.put("atinterfacee", new Relation("basic_acceptance", "cf1"))
    irelations.put("atinterfacee", "index_atinterfacee")
    brelations.put("atmansex", new Relation("basic_acceptance", "cf1"))
    brelations.put("atmanaddress", new Relation("basic_acceptance", "cf1"))
    brelations.put("atdestname", new Relation("basic_acceptance", "cf1"))
    brelations.put("atmark", new Relation("basic_acceptance", "cf1"))
    brelations.put("atsuspicious", new Relation("basic_acceptance", "cf1"))
    brelations.put("atrealname", new Relation("basic_acceptance", "cf1"))
    brelations.put("atname", new Relation("basic_acceptance", "cf1"))
    brelations.put("atdate", new Relation("basic_acceptance", "cf1"))
    brelations.put("atspecial", new Relation("basic_acceptance", "cf1"))


    brelations.put("atmanphoneaddress", new Relation("basic_acceptance", "cf2"))
    brelations.put("ataddress", new Relation("basic_acceptance", "cf2"))
    brelations.put("atlongitude", new Relation("basic_acceptance", "cf2"))
    brelations.put("atlatitude", new Relation("basic_acceptance", "cf2"))
    brelations.put("atdestphone", new Relation("basic_acceptance", "cf2"))
    brelations.put("atdestination", new Relation("basic_acceptance", "cf2"))
    brelations.put("atuser", new Relation("basic_acceptance", "cf2"))
    //    brelations.put("atsource", new Relation("basic_acceptance", "cf1"))
    brelations.put("atsendtime", new Relation("basic_acceptance", "cf2"))
    brelations.put("atrecetime", new Relation("basic_acceptance", "cf2"))
    brelations.put("atimgcert", new Relation("basic_acceptance", "cf2"))
    brelations.put("atimgbill", new Relation("basic_acceptance", "cf2"))
    brelations.put("imeicode", new Relation("basic_acceptance", "cf2"))
    brelations.put("atdeletetime", new Relation("basic_acceptance", "cf2"))
    brelations.put("atupdatetime", new Relation("basic_acceptance", "cf2"))
    brelations.put("attype", new Relation("basic_acceptance", "cf2"))
    brelations.put("atinputtype", new Relation("basic_acceptance", "cf2"))
    brelations.put("atimg", new Relation("basic_acceptance", "cf2"))
    brelations.put("atweight", new Relation("basic_acceptance", "cf2"))
    //dv表及索引表
    brelations.put("dvcompanycode", new Relation("basic_delivery", "cf1"))
    irelations.put("dvcompanycode", "index_dvcompanycode")
    brelations.put("dvmancerttype", new Relation("basic_delivery", "cf1"))
    irelations.put("dvmancerttype", "index_dvmancerttype")
    brelations.put("dvmancertcode", new Relation("basic_delivery", "cf1"))
    irelations.put("dvmancertcode", "index_dvmancertcode")
    brelations.put("dvmantime", new Relation("basic_delivery", "cf1"))
    irelations.put("dvmantime", "index_dvmantime")
    brelations.put("dvmanname", new Relation("basic_delivery", "cf1"))
    irelations.put("dvmanname", "index_dvmanname")
    brelations.put("dvemployee", new Relation("basic_delivery", "cf1"))
    irelations.put("dvemployee", "index_dvemployee")
    //    brelations.put("dvcreatetime",new Relation("basic_delivery","cf1"))
    //    irelations.put("dvcreatetime","index_dvcreatetime")
    brelations.put("dvtype", new Relation("basic_delivery", "cf1"))
    irelations.put("dvtype", "index_dvtype")
    brelations.put("dvinterface", new Relation("basic_delivery", "cf1"))
    irelations.put("dvinterface", "index_dvinterface")
    brelations.put("dvmansex", new Relation("basic_delivery", "cf1"))
    brelations.put("dvmanaddress", new Relation("basic_delivery", "cf1"))
    brelations.put("dvname", new Relation("basic_delivery", "cf1"))
    brelations.put("dvweight", new Relation("basic_delivery", "cf1"))
    brelations.put("dvspecial", new Relation("basic_delivery", "cf1"))
    brelations.put("dvrealname", new Relation("basic_delivery", "cf1"))
    brelations.put("dvmark", new Relation("basic_delivery", "cf1"))
    brelations.put("dvsuspicious", new Relation("basic_delivery", "cf1"))
    brelations.put("dvdate", new Relation("basic_delivery", "cf1"))

    brelations.put("dvmanphone", new Relation("basic_delivery", "cf2"))
    brelations.put("dvmanphoneaddress", new Relation("basic_delivery", "cf2"))
    brelations.put("dvaddress", new Relation("basic_delivery", "cf2"))
    brelations.put("dvlongitude", new Relation("basic_delivery", "cf2"))
    brelations.put("dvlatitude", new Relation("basic_delivery", "cf2"))
    brelations.put("dvoriginname", new Relation("basic_delivery", "cf2"))
    brelations.put("dvorigin", new Relation("basic_delivery", "cf2"))
    brelations.put("dvuser", new Relation("basic_delivery", "cf2"))
    brelations.put("dvsendtime", new Relation("basic_delivery", "cf2"))
    brelations.put("dvrecetime", new Relation("basic_delivery", "cf2"))
    brelations.put("dvimgcert", new Relation("basic_delivery", "cf2"))
    brelations.put("dvimg", new Relation("basic_delivery", "cf2"))
    brelations.put("dvimgbill", new Relation("basic_delivery", "cf2"))
    brelations.put("dvdeletetime", new Relation("basic_delivery", "cf2"))
    brelations.put("dvupdatetime", new Relation("basic_delivery", "cf2"))
    brelations.put("dvinputtype", new Relation("basic_delivery", "cf2"))
    brelations.put("dvoriginphone", new Relation("basic_delivery", "cf2"))

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]") //集群中运行打包注释此句

      // 设置序列化方式， [rdd] [worker]
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 占用空间比较小
      .set("spark.rdd.compress", "true")

      .set("spark.streaming.kafka.maxRatePerPartition", "1500") // 每个分区，每次拉取的最大数据条数
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 程序优雅的关闭
    val ssc = new StreamingContext(conf, Seconds(1))



    //设置累加器和广播变量
    val count: Accumulator[Int] = ssc.sparkContext.accumulator(0)
    val relations: Broadcast[mutable.HashMap[String, Relation]] = ssc.sparkContext.broadcast(brelations)
    val indexRelations: Broadcast[mutable.HashMap[String, String]] = ssc.sparkContext.broadcast(irelations)

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


    val message = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic.split(",").toSet[String], kafkaParams))
    message.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // 过滤出充值通知日志
        /*  val baseData: RDD[JSONObject] = rdd
            // ConsumerRecord => JSONObject
            .map(line => JSON.parseObject(line.value()))
            .filter(obj => obj.getString("action").equalsIgnoreCase("create"))*/
        rdd.foreachPartition(partition => {
          //获取数据库链接
          //              val connection = HBaseUtil.getConnection
          val connection: Connection = Hbaseutils.getConnection()
          //基本表链接集合
          val btnames: Array[String] = Array("basic_acceptance", "basic_delivery")
          val basicAtTable: Table = connection.getTable(TableName.valueOf(btnames(0)))
          val basicDvTable: Table = connection.getTable(TableName.valueOf(btnames(1)))

          //索引表链接集合
          val itnames: Iterator[String] = indexRelations.value.valuesIterator
          val indexTablesConnection: ListBuffer[Table] = new ListBuffer[Table]()
          while (itnames.hasNext) {
            //            println("ttt索引表表名为：" + itnames.next())
            indexTablesConnection.append(connection.getTable(TableName.valueOf(itnames.next())))
          }
          partition.foreach(line => {
            count.add(1)
            try {
              val jsonobj: JSONObject = JSON.parseObject(line.value())
              val action: String = jsonobj.getString(("action"))
              if (action.equalsIgnoreCase("create")) {
                val table: String = jsonobj.getString(("table"))
                //            val time: String = jsonobj.getString("time")
                val dataObj: JSONObject = jsonobj.getJSONObject("data")
                val random: String = Random.nextInt(100).toString

                if (table.equals("acceptance")) {
                  val atCreateTime: String = dataObj.getString("atcreatetime")
                  val atRowkey: String = random + "_" + dataObj.getString("atbillcode") + "_" + atCreateTime
                  val atKeys: util.Iterator[String] = dataObj.keySet().iterator()
                  while (atKeys.hasNext()) {
                    val atKey: String = atKeys.next()
                    val atValue: String = dataObj.getString(atKey)
                    //"atbillcode"和"atcode"不存入hbase的列中
                    if (!atKey.equals("atbillcode") && !atKey.equals("atcode") && !atKey.equals("atcreatetime")) {
                      val put = new Put(atRowkey.getBytes())
                      put.addColumn(relations.value(atKey).cf.getBytes(), atKey.getBytes(), atValue.getBytes())
                      //                    println("  ddd要保存的基本表数据为：" + atRowkey + ", " + relations(atKey).cf + "," + atKey + "," + atValue)
                      basicAtTable.put(put)
                      //写入索引表
                      if (indexRelations.value.contains(atKey)) {
                        val put1 = new Put(Bytes.toBytes(random + "_" + atValue + "_" + atCreateTime))
                        put1.addColumn("cf1".getBytes, atKey.getBytes, atRowkey.getBytes)
                        //                        println("  ddd要保存的索引表数据为：" + random + "_" + atValue + "_" + atCreaateTime + ", " + "cf1" + "," + atKey + "," + atRowkey)
                        for (table <- indexTablesConnection) {
                          if (table.getName.toString.equals(indexRelations.value(atKey))) {
                            //                            println("   it要存的索引表为：" + table)
                            table.put(put1)
                          }
                        }
                      }
                    }
                  }
                } else if (table.equals("delivery")) {
                  val dvCreaateTime: String = dataObj.getString("dvcreatetime")
                  val dvRowkey: String = random + "_" + dataObj.getString("dvbillcode") + "_" + dvCreaateTime
                  val dvKeys: util.Iterator[String] = dataObj.keySet().iterator()
                  while (dvKeys.hasNext()) {
                    val dvKey: String = dvKeys.next()
                    val dvValue: String = dataObj.getString(dvKey)
                    //"dvbillcode"和"dvcode"不存入hbase的列中
                    if (!dvKey.equals("dvbillcode") && !dvKey.equals("dvcode") && !dvKey.equals("dvcreatetime")) {
                      val put = new Put(dvRowkey.getBytes())
                      put.addColumn(relations.value(dvKey).cf.getBytes(), dvKey.getBytes(), dvValue.getBytes())
                      //                    println("  ddd要保存的基本表数据为：" + dvRowkey + ", " + relations(dvKey).cf + "," + dvKey + "," + dvValue)
                      basicDvTable.put(put)
                      //写入索引表
                      if (indexRelations.value.contains(dvKey)) {
                        val put1 = new Put(Bytes.toBytes(random + "_" + dvValue + "_" + dvCreaateTime))
                        put1.addColumn("cf1".getBytes, dvKey.getBytes, dvRowkey.getBytes)
                        //                        println("  ddd要保存的索引表数据为：" + random + "_" + dvValue + "_" + dvCreaateTime + ", " + "cf1" + "," + dvKey + "," + dvRowkey)
                        for (table <- indexTablesConnection) {
                          if (table.getName.toString.equals(indexRelations.value(dvKey))) {
                            //                            println("   it要存的索引表为：" + table)
                            table.put(put1)
                          }
                        }
                      }
                    }
                  }
                }
              }
            } catch {
              case _ => {
                println("--------------------------------------------------------------------------------!!!!!!!!!!!本条数据错误!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!----------------------------")
                val str: String = "d://errs/" + System.currentTimeMillis() + ".json"
                val errFile = new PrintWriter(new File(str))
                errFile.println(line.value())
                errFile.close()
              }
            }
          })
          basicAtTable.close()
          basicDvTable.close()
          for (table <- indexTablesConnection) {
            table.close()
          }
          connection.close()
          //              HBaseUtil.release(connection)

        })

        val endTime = System.currentTimeMillis()
        println("---------------------------------------共" + count.value + "条数据" + "-----------------------------")
        println("!!!!!!!!!!!!!!!!!!!!!用时" + (endTime - startTime) + "ms")
      }
    })

    ssc.start()
    ssc.awaitTermination()


  }
  def transferIntoBytes(str: String): Array[Byte] = {
    try {
      str.getBytes()
    } catch {
      case _: Exception => null
    }
  }
}

