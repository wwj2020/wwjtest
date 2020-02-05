package com.jhbh.util

import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.{JavaConversions, mutable}

object OffsetManagerUtil {
    def storeOffset(offsetRanges: Array[OffsetRange], group:String, curator:CuratorFramework): Unit = {
        for(offsetRange <- offsetRanges) {
            val partition = offsetRange.partition
            val topic = offsetRange.topic
            val utiloffset = offsetRange.untilOffset
            val path = s"offsets/${topic}/${group}/${partition}"
            checkExists(path, curator)
            curator.setData().forPath(path, (utiloffset + "").getBytes())
        }
    }
    /**
      * 从zk中读取数据的偏移量
      *     两个问题：
      *         1）、使用什么读(CuratorFramework)
      *         2）、在zk的什么约定目录下面读取
      *             /mykafka/offsets/${topic}/${group.id}/${partition},topic:bigscreen-bd1809,group:bigsreen
      */
    def getOffsets(kafkaParam:Map[String,Object],topics: Set[String], group:String, curator:CuratorFramework):Map[TopicPartition, Long] = {

        var offsets = mutable.Map[TopicPartition, Long]()
        for (topic <- topics) {
            val path = s"offsets/${topic}/${group}"
            //验证路劲是否存在
            checkExists(path, curator)
            for (partition <- JavaConversions.asScalaBuffer(curator.getChildren.forPath(path))) {
                val fullPath = s"${path}/${partition}"
                val data = curator.getData.forPath(fullPath)  //byte[]
                val offset = new String(data).toLong
                offsets.put(new TopicPartition(topic, partition.toInt), offset)
            }
        }
        offsets.toMap
    }
    def checkExists(path:String, curator:CuratorFramework) {
        if(curator.checkExists().forPath(path) == null) {
            curator.create().creatingParentsIfNeeded().forPath(path)
        }
    }

    def createMsg(ssc:StreamingContext,topics: Set[String], kafkaParams: Map[String, Object],  group:String, curator:CuratorFramework):InputDStream[ConsumerRecord[String, String]] = {
        //  1、从zk中读取对应的偏移量
        val offsets: Map[TopicPartition, Long] = getOffsets(kafkaParams,topics, group, curator)
        var message:InputDStream[ConsumerRecord[String, String]] = null
        //2、偏移量是否存在
//        if(offsets.isEmpty) {//读取到的偏移量为null
            message = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe(topics,kafkaParams))
       /* } else {//不为空
            message = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe(topics,kafkaParams,offsets))
        }*/
        message
    }
}
