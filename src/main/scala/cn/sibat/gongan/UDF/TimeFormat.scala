package cn.sibat.gongan.UDF

import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.sql.functions.udf

object TimeFormat{
  /***
    * 编写SparkDataFrame的UDF
    * 将感知门的stime转成标准时间格式
    */
  val time = udf((s:String) => {
    val oldFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    newFormat.format(oldFormat.parse(s))
  })

  val timeToUnix = udf((s:String)=>{
    val oldFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try{
      oldFormat.parse(s).getTime/1000
    }catch{
      case e: Exception =>{
        oldFormat.parse(s.split("."){0}).getTime/1000
      }
    }

  })

  val timediff = udf((s1:String,s2:String)=>{
    s2.toLong - s1.toLong
  })
}