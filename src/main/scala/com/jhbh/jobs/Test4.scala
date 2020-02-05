/*
package jhbh.jobs

import java.util
import java.util.Date

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
import scala.collection.mutable.ListBuffer
import scala.util.Random



object Test4{

  def main(args: Array[String]): Unit = {
    var date = new Date()
    val startTime = date.getTime
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.spark-project").setLevel(Level.OFF)

    val Array( topic, group) = Array("jsontesttt","g1")

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
    indexRelations.put("dvcompanycode","index_dvcompanycode")
    relations.put("dvmancerttype",new Relation("basic_delivery","cf1"))
    indexRelations.put("dvmancerttype","index_dvmancerttype")
    relations.put("dvmancertcode",new Relation("basic_delivery","cf1"))
    indexRelations.put("dvmancertcode","index_dvmancertcode")
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
    //获取基本表和索引表的完整列集合，用于之后和json字段匹配
    val basicKeys: Iterator[String] = relations.keysIterator
    val basicAtKeys: mutable.HashSet[String] = mutable.HashSet[String]()
    val basicDvKeys: mutable.HashSet[String] = mutable.HashSet[String]()

    for(basicKey <- basicKeys){
      if(basicKey.startsWith("at")){
        basicAtKeys.add(basicKey)
      }else{
        basicDvKeys.add(basicKey)
      }
    }
    val indexKeys: Iterator[String] = indexRelations.keysIterator
    val indexAtKeys: mutable.HashSet[String] = mutable.HashSet[String]()
    val indexDvKeys: mutable.HashSet[String] = mutable.HashSet[String]()
    for(indexKey <- indexKeys){
      if(indexKey.startsWith("at")){
        indexAtKeys.add(indexKey)
      }else{
        indexDvKeys.add(indexKey)
      }
    }

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[2]")  //集群中运行打包
      .set("spark.streaming.kafka.maxRatePerPartition", "1500") // 每个分区，每次拉取的最大数据条数
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 程序优雅的关闭

    val ssc = new StreamingContext(conf,Seconds(1))

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
      rdd.foreach(line => {
        val str: String = line.value()
        println(str)
      })
    })
      ssc.start()
      ssc.awaitTermination()

      val endTime = date.getTime

      println(endTime - startTime)
  }

      /*val baseData: RDD[JSONObject] = rdd
        // ConsumerRecord => JSONObject
        .map(line => JSON.parseObject(line.value()))
        // 过滤出充值通知日志
        .filter(obj => obj.getString("action").equalsIgnoreCase("create"))

      baseData.foreachPartition(partition => {
        if(!partition.isEmpty) {
          //获取数据库链接
          val connection = HBaseUtil.getConnection
          //基本表链接集合
          val btnames: Array[String] = Array("basic_acceptance", "basic_delivery")
          val basicAtTable: Table = connection.getTable(TableName.valueOf(btnames(0)))
          val basicDvTable: Table = connection.getTable(TableName.valueOf(btnames(1)))

          //索引表链接集合
          val itnames: Iterator[String] = indexRelations.valuesIterator
          val indexTablesConnection: ListBuffer[Table] = new ListBuffer[Table]()
          while (itnames.hasNext) {
//            println("ttt索引表表名为：" + itnames.next())
            indexTablesConnection.append(connection.getTable(TableName.valueOf(itnames.next())))
          }
          partition.foreach(jsonobj => {
            val action: String = jsonobj.getString(("action"))
            val table: String = jsonobj.getString(("table"))
            val time: String = jsonobj.getString("time")
            val dataObj: JSONObject = jsonobj.getJSONObject("data")
            val random: String = Random.nextInt(100).toString

            if (table.equals("acceptance")) {
              val atRowkey: String = random + "_" + dataObj.getString("atbillcode") + "_" + time
              val atKeys: util.Iterator[String] = dataObj.keySet().iterator()
              while (atKeys.hasNext()) {
                val atKey: String = atKeys.next()
                val atValue: String = dataObj.getString(atKey)
                //"atbillcode"和"atcode"不存入hbase的列中
                if (!atKey.equals("atbillcode") && !atKey.equals("atcode")) {
                  val put = new Put(atRowkey.getBytes())
                  put.addColumn(relations(atKey).cf.getBytes(), atKey.getBytes(), atValue.getBytes())
                  println("  ddd要保存的基本表数据为：" + atRowkey + ", " + relations(atKey).cf + "," + atKey + "," + atValue)
                  basicAtTable.put(put)
                  //写入索引表
                  if (indexRelations.contains(atKey)) {
                    val put1 = new Put(Bytes.toBytes(random + "_" + atValue + "_" + time))
                    put1.addColumn("cf1".getBytes, atKey.getBytes, atRowkey.getBytes)
                    println("  ddd要保存的索引表数据为：" + random + "_" + atValue + "_" + time + ", " + "cf1" + "," + atKey + "," + atRowkey)
                    for (table <- indexTablesConnection) {
                      if (table.getName.toString.equals(indexRelations(atKey))) {
                        println("   it要存的索引表为：" + table)
                        table.put(put1)
                      }
                    }
                  }
                }
              }
            } else {
              val dvRowkey: String = random + "_" + dataObj.getString("dvbillcode") + "_" + time
              val dvKeys: util.Iterator[String] = dataObj.keySet().iterator()
              while (dvKeys.hasNext()) {
                val dvKey: String = dvKeys.next()
                val dvValue: String = dataObj.getString(dvKey)
                //"dvbillcode"和"dvcode"不存入hbase的列中
                if (!dvKey.equals("dvbillcode") && !dvKey.equals("dvcode")) {
                  val put = new Put(dvRowkey.getBytes())
                  put.addColumn(relations(dvKey).cf.getBytes(), dvKey.getBytes(), dvValue.getBytes())
                  println("  ddd要保存的基本表数据为：" + dvRowkey + ", " + relations(dvKey).cf + "," + dvKey + "," + dvValue)
                  basicDvTable.put(put)
                  //写入索引表
                  if (indexRelations.contains(dvKey)) {
                    val put1 = new Put(Bytes.toBytes(random + "_" + dvValue + "_" + time))
                    put1.addColumn("cf1".getBytes, dvKey.getBytes, dvRowkey.getBytes)
                    println("  ddd要保存的索引表数据为：" + random + "_" + dvValue + "_" + time + ", " + "cf1" + "," + dvKey + "," + dvRowkey)
                    for (table <- indexTablesConnection) {
                      if (table.getName.toString.equals(indexRelations(dvKey))) {
                        println("   it要存的索引表为：" + table)
                        table.put(put1)
                      }
                    }
                  }
                }
              }
            }
          })
          basicAtTable.close()
          basicDvTable.close()
          for (table <- indexTablesConnection) {
            table.close()
          }
          HBaseUtil.release(connection)
        }
      })
        //更新offset到zk中
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      OffsetManagerUtil.storeOffset(offsetRanges, group, curator)
    })*/


  val curator = {

    val client = CuratorFrameworkFactory.builder().namespace("wwjtest")
        //      .connectString("hdp01:2181,hdp02:2181,hdp03:2181")
        //     .connectString("service-01:2181,service-02:2181,service-03:2181,service-05:2181,service-06:2181")
        .connectString("bigdata-01:2181,bigdata-02:2181,bigdata-03:2181")
//        .connectString("service01:2181,service02:2181,service03:2181,service05:2181,service06:2181")
        .retryPolicy(new ExponentialBackoffRetry(2000, 3))
        .build()
      client.start()
      client
  }
}


*/
