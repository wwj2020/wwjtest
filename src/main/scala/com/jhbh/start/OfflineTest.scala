package com.jhbh.start

import java.util.Date

import com.jhbh.been.{ACCEPTANCE, DELIVERY}
import com.jhbh.util.{GetOracleData, Hbaseutils}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.util.Random

object OfflineTest {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.spark-project").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]") //集群中运行打包注释此句
    val sc = new SparkContext(conf)
    println("获取连接")
    //设置累加器
    val atCount: Accumulator[Int] = sc.accumulator(0)
    val dvCount: Accumulator[Int] = sc.accumulator(0)
    //获取atRDD和dvRDD
    val atRDD: RDD[ACCEPTANCE] = sc.makeRDD(GetOracleData.getAcceptanceList())
    val dvRDD: RDD[DELIVERY] = sc.makeRDD(GetOracleData.getDeliveryList())
    atRDD.foreachPartition(partition => {
      val connection: Connection = Hbaseutils.getConnection()
      //获取表连接
      val atTable: Table = connection.getTable(TableName.valueOf("acceptance"))

      partition.foreach(atObj => {
        println(atObj)
        atCount.add(1)
        val atCreateTime: String = atObj.atcreatetime
        val time: Date = new Date(atCreateTime.toLong * 1000)
        val pre: Int = time.getMinutes()
        val atRowkey: String = (pre + "_" + (Long.MaxValue - atCreateTime.toLong)
          + "_" + atObj.atbillcode
          + "_" + atObj.atcompanycode
          + "_" + atObj.atmancerttype
          + "_" + atObj.atmancertcode
          + "_" + atObj.atmantime
          + "_" + atObj.atmanname
          + "_" + atObj.atmanphone
          + "_" + atObj.atemployee
          + "_" + atObj.atinterfacee
          + "_" + atObj.atdestcity)

        //获取对象中的其他属性
        val atmansex: Array[Byte] = transferIntoBytes(atObj.atmansex)
        val atmanaddress: Array[Byte] = transferIntoBytes(atObj.atmanaddress)
        val atdestname: Array[Byte] = transferIntoBytes(atObj.atdestname)
        val atmark: Array[Byte] = transferIntoBytes(atObj.atmark)
        val atsuspicious: Array[Byte] = transferIntoBytes(atObj.atsuspicious)
        val atrealname: Array[Byte] = transferIntoBytes(atObj.atrealname)
        val atname: Array[Byte] = transferIntoBytes(atObj.atname)
        val atdate: Array[Byte] = transferIntoBytes(atObj.atdate)
        val atspecial: Array[Byte] = transferIntoBytes(atObj.atspecial)
        val atmanphoneaddress: Array[Byte] = transferIntoBytes(atObj.atmanphoneaddress)
        val ataddress: Array[Byte] = transferIntoBytes(atObj.ataddress)
        val atlongitude: Array[Byte] = transferIntoBytes(atObj.atlongitude)
        val atlatitude: Array[Byte] = transferIntoBytes(atObj.atlatitude)
        val atdestphone: Array[Byte] = transferIntoBytes(atObj.atdestphone)
        val atdestination: Array[Byte] = transferIntoBytes(atObj.atdestination)
        val atuser: Array[Byte] = transferIntoBytes(atObj.atuser)
        val atsendtime: Array[Byte] = transferIntoBytes(atObj.atsendtime)
        val atrecetime: Array[Byte] = transferIntoBytes(atObj.atrecetime)
        val atimgcert: Array[Byte] = transferIntoBytes(atObj.atimgcert)
        val atimgbill: Array[Byte] = transferIntoBytes(atObj.atimgbill)
        val imeicode: Array[Byte] = transferIntoBytes(atObj.imeicode)
        val atdeletetime: Array[Byte] = transferIntoBytes(atObj.atdeletetime)
        val atupdatetime: Array[Byte] = transferIntoBytes(atObj.atupdatetime)
        val attype: Array[Byte] = transferIntoBytes(atObj.attype)
        val atinputtype: Array[Byte] = transferIntoBytes(atObj.atinputtype)
        val atimg: Array[Byte] = transferIntoBytes(atObj.atimg)
        val atweight: Array[Byte] = transferIntoBytes(atObj.atweight)


        //at基本表
        val put = new Put(atRowkey.getBytes())
        put.addColumn("cf".getBytes, "atmansex".getBytes(), atmansex)
          .addColumn("cf".getBytes, "atmanaddress".getBytes(), atmanaddress)
          .addColumn("cf".getBytes, "atdestname".getBytes(), atdestname)
          .addColumn("cf".getBytes, "atmark".getBytes(), atmark)
          .addColumn("cf".getBytes, "atsuspicious".getBytes(), atsuspicious)
          .addColumn("cf".getBytes, "atrealname".getBytes(), atrealname)
          .addColumn("cf".getBytes, "atname".getBytes(), atname)
          .addColumn("cf".getBytes, "atdate".getBytes(), atdate)
          .addColumn("cf".getBytes, "atspecial".getBytes(), atspecial)
          .addColumn("cf".getBytes, "atmanphoneaddress".getBytes(), atmanphoneaddress)
          .addColumn("cf".getBytes, "ataddress".getBytes(), ataddress)
          .addColumn("cf".getBytes, "atlongitude".getBytes(), atlongitude)
          .addColumn("cf".getBytes, "atlatitude".getBytes(), atlatitude)
          .addColumn("cf".getBytes, "atdestphone".getBytes(), atdestphone)
          .addColumn("cf".getBytes, "atdestination".getBytes(), atdestination)
          .addColumn("cf".getBytes, "atuser".getBytes(), atuser)
          .addColumn("cf".getBytes, "atsendtime".getBytes(), atsendtime)
          .addColumn("cf".getBytes, "atrecetime".getBytes(), atrecetime)
          .addColumn("cf".getBytes, "atimgcert".getBytes(), atimgcert)
          .addColumn("cf".getBytes, "atimgbill".getBytes(), atimgbill)
          .addColumn("cf".getBytes, "imeicode".getBytes(), imeicode)
          .addColumn("cf".getBytes, "atdeletetime".getBytes(), atdeletetime)
          .addColumn("cf".getBytes, "atupdatetime".getBytes(), atupdatetime)
          .addColumn("cf".getBytes, "attype".getBytes(), attype)
          .addColumn("cf".getBytes, "atinputtype".getBytes(), atinputtype)
          .addColumn("cf".getBytes, "atimg".getBytes(), atimg)
          .addColumn("cf".getBytes, "atweight".getBytes(), atweight)
        //数据存入at表
        atTable.put(put)
      })
      //关闭表和HBase连接
      atTable.close()
      connection.close()
    })
    val atEndTime = System.currentTimeMillis()
    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!累计用时" + (atEndTime - startTime) + "ms")
    println("-------------------------共" + atCount.value + "条数据" + "---------------")
    //dvRDD数据处理
    dvRDD.foreachPartition(partition => {
      val connection: Connection = Hbaseutils.getConnection()
      //获取dv表连接
      val dvTable: Table = connection.getTable(TableName.valueOf("delivery"))
      partition.foreach(dvObj => {
        println(dvObj)
        dvCount.add(1)
        val random: String = Random.nextInt(100).toString
        //获取对象中的各个属性
        val dvCreateTime: String = dvObj.dvcreatetime
        val time: Date = new Date(dvCreateTime.toLong * 1000)
        val pre: Int = time.getMinutes()
        val dvRowkey: String = (pre + "_" + (Long.MaxValue -dvCreateTime.toLong)
          + "_" + dvObj.dvbillcode
          + "_" + dvObj.dvcompanycode
          + "_" + dvObj.dvmancerttype
          + "_" + dvObj.dvcompanycode
          + "_" + dvObj.dvmantime
          + "_" + dvObj.dvmanname
          + "_" + dvObj.dvemployee
          + "_" + dvObj.dvtype
          + "_" + dvObj.dvinterface)
        //获取对象中的其他属性
        val dvmansex: Array[Byte] = transferIntoBytes(dvObj.dvmansex)
        val dvmanaddress: Array[Byte] = transferIntoBytes(dvObj.dvmanaddress)
        val dvname: Array[Byte] = transferIntoBytes(dvObj.dvname)
        val dvweight: Array[Byte] = transferIntoBytes(dvObj.dvweight)
        val dvspecial: Array[Byte] = transferIntoBytes(dvObj.dvspecial)
        val dvrealname: Array[Byte] = transferIntoBytes(dvObj.dvrealname)
        val dvmark: Array[Byte] = transferIntoBytes(dvObj.dvmark)
        val dvsuspicious: Array[Byte] = transferIntoBytes(dvObj.dvsuspicious)
        val dvdate: Array[Byte] = transferIntoBytes(dvObj.dvdate)
        val dvmanphone: Array[Byte] = transferIntoBytes(dvObj.dvmanphone)
        val dvmanphoneaddress: Array[Byte] = transferIntoBytes(dvObj.dvmanphoneaddress)
        val dvaddress: Array[Byte] = transferIntoBytes(dvObj.dvaddress)
        val dvlongitude: Array[Byte] = transferIntoBytes(dvObj.dvlongitude)
        val dvlatitude: Array[Byte] = transferIntoBytes(dvObj.dvlatitude)
        val dvoriginname: Array[Byte] = transferIntoBytes(dvObj.dvoriginname)
        val dvorigin: Array[Byte] = transferIntoBytes(dvObj.dvorigin)
        val dvuser: Array[Byte] = transferIntoBytes(dvObj.dvuser)
        val dvsendtime: Array[Byte] = transferIntoBytes(dvObj.dvsendtime)
        val dvrecetime: Array[Byte] = transferIntoBytes(dvObj.dvrecetime)
        val dvimgcert: Array[Byte] = transferIntoBytes(dvObj.dvimgcert)
        val dvimg: Array[Byte] = transferIntoBytes(dvObj.dvimg)
        val dvimgbill: Array[Byte] = transferIntoBytes(dvObj.dvimgbill)
        val dvdeletetime: Array[Byte] = transferIntoBytes(dvObj.dvdeletetime)
        val dvupdatetime: Array[Byte] = transferIntoBytes(dvObj.dvupdatetime)
        val dvinputtype: Array[Byte] = transferIntoBytes(dvObj.dvinputtype)
        val dvoriginphone: Array[Byte] = transferIntoBytes(dvObj.dvoriginphone)

        //dv基本表
        val put = new Put(dvRowkey.getBytes())
        put.addColumn("cf".getBytes, "dvmansex".getBytes(), dvmansex)
          .addColumn("cf".getBytes, "dvmanaddress".getBytes(), dvmanaddress)
          .addColumn("cf".getBytes, "dvname".getBytes(), dvname)
          .addColumn("cf".getBytes, "dvweight".getBytes(), dvweight)
          .addColumn("cf".getBytes, "dvspecial".getBytes(), dvspecial)
          .addColumn("cf".getBytes, "dvrealname".getBytes(), dvrealname)
          .addColumn("cf".getBytes, "dvmark".getBytes(), dvmark)
          .addColumn("cf".getBytes, "dvsuspicious".getBytes(), dvsuspicious)
          .addColumn("cf".getBytes, "dvdate".getBytes(), dvdate)
          .addColumn("cf".getBytes, "dvmanphone".getBytes(), dvmanphone)
          .addColumn("cf".getBytes, "dvmanphoneaddress".getBytes(), dvmanphoneaddress)
          .addColumn("cf".getBytes, "dvaddress".getBytes(), dvaddress)
          .addColumn("cf".getBytes, "dvlongitude".getBytes(), dvlongitude)
          .addColumn("cf".getBytes, "dvlatitude".getBytes(), dvlatitude)
          .addColumn("cf".getBytes, "dvoriginname".getBytes(), dvoriginname)
          .addColumn("cf".getBytes, "dvorigin".getBytes(), dvorigin)
          .addColumn("cf".getBytes, "dvuser".getBytes(), dvuser)
          .addColumn("cf".getBytes, "dvsendtime".getBytes(), dvsendtime)
          .addColumn("cf".getBytes, "dvrecetime".getBytes(), dvrecetime)
          .addColumn("cf".getBytes, "dvimgcert".getBytes(), dvimgcert)
          .addColumn("cf".getBytes, "dvimg".getBytes(), dvimg)
          .addColumn("cf".getBytes, "dvimgbill".getBytes(), dvimgbill)
          .addColumn("cf".getBytes, "dvdeletetime".getBytes(), dvdeletetime)
          .addColumn("cf".getBytes, "dvupdatetime".getBytes(), dvupdatetime)
          .addColumn("cf".getBytes, "dvinputtype".getBytes(), dvinputtype)
          .addColumn("cf".getBytes, "dvoriginphone".getBytes(), dvoriginphone)
        //数据存入dv表
        dvTable.put(put)


      })
      //关闭表和HBase连接
      dvTable.close()
      connection.close()
    })
    val dvEndTime = System.currentTimeMillis()
    println("!!!!!!!!!!!!!!!!!!!!!累计用时" + (dvEndTime - startTime) + "ms")
    println("-------------------------共" + dvCount.value + "条数据" + "---------------")

    sc.stop()
  }

  def transferIntoBytes(str: String): Array[Byte] = {
    try {
      str.getBytes()
    } catch {
      case _: Exception => null
    }
  }
}



