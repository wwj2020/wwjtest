package com.jhbh.util

/**
  * @author xj
  */
class SheepString (val str: String){

  def toIntPlus: Int = {
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }
  }


  def toDoublePlus: Double = {
    try {
      str.toDouble
    } catch {
      case _: Exception => 0
    }
  }

  def getBytesPlus: Array[Byte] = {
    try {
      str.getBytes()
    } catch {
      case _: Exception => null
    }
  }
}

object SheepString{
  implicit def SheepString(str: String): SheepString = new SheepString(str)
}