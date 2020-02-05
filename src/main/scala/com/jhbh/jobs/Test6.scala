package com.jhbh.jobs

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, SparkConf}


object Test6 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.spark-project").setLevel(Level.OFF)

    val startTime = System.currentTimeMillis()
    val Array(topic, group) = Array("jsontesttt", "g1")
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]") //集群中运行打包

      // 设置序列化方式， [rdd] [worker]
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 占用空间比较小
      .set("spark.rdd.compress", "true")

      .set("spark.streaming.kafka.maxRatePerPartition", "2000") // 每个分区，每次拉取的最大数据条数
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 程序优雅的关闭



    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      //     "bootstrap.servers" -> "service-01:9092,service-02:9092,service-03:9092,service-05:9092,service-06:9092",
      "bootstrap.servers" -> "bigdata-01:9092,bigdata-02:9092,bigdata-03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest"
    )

    val count: Accumulator[Int] = ssc.sparkContext.accumulator(0)
    val message = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic.split(",").toSet[String], kafkaParams))

    message.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        if (!partition.isEmpty) {
          partition.foreach(line => {
            count.add(1)
//            println(line.value())
          })
        }
      })
      val endTime = System.currentTimeMillis()
      println("---------------------------------------共" + count.value + "条数据" + "-----------------------------")
      println("!!!!!!!!!!!!!!!!!!!!!用时" + (endTime - startTime) + "ms")


    })
    ssc.start()
    ssc.awaitTermination()


  }
}


