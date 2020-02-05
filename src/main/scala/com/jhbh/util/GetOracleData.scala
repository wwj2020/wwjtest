package com.jhbh.util

import com.jhbh.been.{ACCEPTANCE, DELIVERY}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * @author xj
  **/
object GetOracleData {

  // 加载数据库配置信息
  DBs.setup()

  def main(args: Array[String]): Unit = {

   getDeliveryList().foreach(println)
    getAcceptanceList().foreach(println)

  }

  /**
    * 查数据:
    * 自定义一个 样例类 DELIVERY 来接收参数，并返回一个集合List[DELIVERY]
    */
  def getDeliveryList() = {

    val list: List[DELIVERY] = DB.readOnly {
      implicit session =>

      SQL("select * from DELIVERY").map(rs => DELIVERY(
        rs.string("DVCOMPANYCODE"),
        rs.string("DVBILLCODE"),
        rs.string("DVNAME"),
        rs.string("DVWEIGHT"),
        rs.string("DVSPECIAL"),
        rs.string("DVMANCERTTYPE"),
        rs.string("DVMANCERTCODE"),
        rs.string("DVMANTIME"),
        rs.string("DVMANSEX"),
        rs.string("DVMANNAME"),
        rs.string("DVMANADDRESS"),
        rs.string("DVMANPHONE"),
        rs.string("DVMANPHONEADDRESS"),
        rs.string("DVADDRESS"),
        rs.string("DVLONGITUDE"),
        rs.string("DVLATITUDE"),
        rs.string("DVORIGINNAME"),
        rs.string("DVORIGIN"),
        rs.string("DVEMPLOYEE"),
        rs.string("DVMARK"),
        rs.string("DVSUSPICIOUS"),
        rs.string("DVUSER"),
        rs.string("DVDATE"),
        rs.string("DVSENDTIME"),
        rs.string("DVRECETIME"),
        rs.string("DVIMGCERT"),
        rs.string("DVIMG"),
        rs.string("DVIMGBILL"),
        rs.string("DVDELETETIME"),
        rs.string("DVUPDATETIME"),
        rs.string("DVCREATETIME"),
        rs.string("DVREALNAME"),
        rs.string("DVTYPE"),
        rs.string("DVINTERFACE"),
        rs.string("DVINPUTTYPE"),
        rs.string("DVORIGINPHONE")
      )).list().apply()
    }
    list
  }

  def getAcceptanceList() = {

    val list: List[ACCEPTANCE] = DB.readOnly { implicit session =>
      SQL("select * from ACCEPTANCE").map(rs => ACCEPTANCE(
        rs.string("ATCODE"),
        rs.string("ATCOMPANYCODE"),
        rs.string("ATBILLCODE"),
        rs.string("ATNAME"),
        rs.string("ATWEIGHT"),
        rs.string("ATSPECIAL"),
        rs.string("ATMANCERTTYPE"),
        rs.string("ATMANCERTCODE"),
        rs.string("ATMANTIME"),
        rs.string("ATMANSEX"),
        rs.string("ATMANNAME"),
        rs.string("ATMANPHONE"),
        rs.string("ATMANPHONEADDRESS"),
        rs.string("ATMANADDRESS"),
        rs.string("ATADDRESS"),
        rs.string("ATLONGITUDE"),
        rs.string("ATLATITUDE"),
        rs.string("ATDESTNAME"),
        rs.string("ATDESTPHONE"),
        rs.string("ATDESTINATION"),
        rs.string("ATEMPLOYEE"),
        rs.string("ATMARK"),
        rs.string("ATSUSPICIOUS"),
        rs.string("ATUSER"),
        rs.string("ATDATE"),
        rs.string("ATDESTCITY"),
        rs.string("ATSENDTIME"),
        rs.string("ATRECETIME"),
        rs.string("ATIMGCERT"),
        rs.string("ATIMGBILL"),
        rs.string("IMEICODE"),
        rs.string("ATDELETETIME"),
        rs.string("ATUPDATETIME"),
        rs.string("ATCREATETIME"),
        rs.string("ATREALNAME"),
        rs.string("ATTYPE"),
        rs.string("ATINTERFACEE"),
        rs.string("ATINPUTTYPE"),
        rs.string("ATIMG")
      )).list().apply()
    }
    list
  }


}
