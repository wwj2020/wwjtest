package com.jhbh.jobs

import java.util.Date


object Test {
  def main(args: Array[String]): Unit = {
    /*var date = new Date("2018-03-14 11:11:11")//时间对象
    var str = date.getTime(); //转换成时间戳*/

    import java.text.SimpleDateFormat
    val lt = "1557557142".toLong * 1000
    val date = new Date(lt)
    println(date)
    println(date.getMinutes)

  }

}
