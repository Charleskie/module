package cn.sibat.gongan.UDF

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.functions.udf
import cn.sibat.util.timeformat.TimeFormat._

object TimeFormat{
  private val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  /***
    * 编写SparkDataFrame的UDF
    * 将感知门的stime转成标准时间格式
    */
  val timeParse = udf((s:String) => {
    val oldFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    newFormat.format(oldFormat.parse(s))
  })

  val TimeParse = udf((s:String, format:String)=>{
    val oldFormat = new SimpleDateFormat(format)
    newFormat.format(oldFormat.parse(s))
  })

  val UnixParse = udf((s:String)=>{
    val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    newFormat.format(new Date(s.toLong*1000))
  })
  /***
    * 字符串时间转成Unix时间戳
    */
  val timeToUnix = udf((s:String)=>{
    try{
      val oldFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      oldFormat.parse(s).getTime/1000
    }catch{
      case e: Exception =>{
        val oldFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.S")
        oldFormat.parse(s).getTime/1000
      }
    }

  })

  /***
    * 时间格式必须是Unix格式戳
    */
  val timediff = udf((s1:String,s2:String)=>{
    s2.toLong - s1.toLong
  })

  val timeSlice = udf((s:String)=>{
    changetime(s,5)
  })

  def timeParse(s: String) ={
    val oldFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    newFormat.format(oldFormat.parse(s))
  }
}