package cn.sibat.gongan.warning

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

object earlywarning{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\"

    val data =   sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](path+"text.txt")
      .map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK"))

  }

  /***
    * 计算日预警量
    * @param rdd
    */
  def calDayCount(rdd: RDD[String]): Unit ={
    rdd.map(s => {
      val line = s.split("\\t")
      (line(0),string2time(line(12)).getDate)
    }).groupBy(s => s._2).map(s => s._1+","+s._2.size)
  }

  def calHourCount(rdd: RDD[String]):Unit ={
    rdd.map(s=> {
      val line = s.split("\\t")
      (line(0),string2time(line(12)).getHours)
    }).groupBy(s=> s._2).map(s=> s._1+","+s._2.size)
  }

  /***
    * 将字符串转成时间格式
    * @param time
    * @return
    */
  def string2time(time: String):Date=  {
    val timeFormatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    timeFormatter.parse(time)
  }

  def calStationCount(rdd:RDD[String]):Unit ={
    rdd.map(s =>{
      (s(0),s(9))
    }).groupBy(s=>s._2).map(s=> s._1+","+s._2.size)
  }
}