package com.jhbh.util

import java.io.{File, PrintWriter}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD


object DataClear {

  private def getBaseData(rdd: RDD[ConsumerRecord[String, String]]) = {

    try {
      val baseData: RDD[JSONObject] = rdd
        // ConsumerRecord => JSONObject
        .map(line => JSON.parseObject(line.value()))
        .filter(obj => obj.getString("action").equalsIgnoreCase("create"))


//      baseData.foreach()

    } catch {
      case _ => {
        println("--------------------------!!!!!!!!!!!本条数据错误!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!----------------------------")
        val str: String = "d://errs/" + System.currentTimeMillis() + ".json"
        val errFile = new PrintWriter(new File(str))
//        errFile.println(line.value())
        errFile.close()

      }
    }



  }

}
