package cn.sibat.gongan.warning

import java.text.{DateFormat, SimpleDateFormat}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

object earlywarning{
  private val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val data =   sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](path+"early_warning1101.txt")
      .map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK"))

//    val date = sparkSession.read.textFile(path+"early_warning1029.xls").rdd.foreach(println)

//    data.foreach(println)
//    calDayCount(data)
//    calHourCount(data)
//    calStationCount(data)
//    calTypeCount(data)
    calSimilarityCount(data)

  }

  /***
    * 计算日预警量
    * @param rdd
    */
  def calDayCount(rdd: RDD[String]): Unit ={
    rdd.map(s => {
      val line = s.split(",")
      (line(0),string2time(line(12)).substring(0,10))
    }).groupBy(s => s._2).map(s => s._1+","+s._2.size).coalesce(1).saveAsTextFile(path+"out/dayCount")
  }

  def calHourCount(rdd: RDD[String]):Unit ={
    rdd.map(s=> {
      val line = s.split(",")
      (line(0),string2time(line(12)).substring(10,13))
    }).groupBy(s=> s._2).map(s=> s._1+","+s._2.size).coalesce(1).saveAsTextFile(path+"out/hourCount")
  }

  /***
    * 将字符串转成时间格式
    * @param time
    * @return
    */
  def string2time(time: String)=  {
    val timeFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    try{
      timeFormatter.format(timeFormatter.parse(time))
    }catch {
      case e: Exception =>{
        val date = "01/01/1979 00:00:00"
        timeFormatter.format(timeFormatter.parse(date))
      }
    }
  }

  def calStationCount(rdd:RDD[String]):Unit ={
    rdd.map(s =>{
      val ss = s.split(",")
      (ss(0),ss(9))
    }).groupBy(s=>s._2).map(s=> s._1+","+s._2.size).coalesce(1).saveAsTextFile(path+"out/stationCount")
  }

  def calTypeCount(rdd: RDD[String]): Unit ={
    rdd.map(s =>{
      val line = s.split(",")
      (line(0),line(6),line(7))
    }).groupBy(s => (s._3,s._2)).map(s => s._1+","+s._2.size).map(s=>{
      val line = s.replaceAll("\\(","").replaceAll("\\)","").split(",")
      val sum = line.size
      line(0)+","+line(1)+","+line(2)+","+line(2).toDouble/sum
    }).coalesce(1).saveAsTextFile(path+"out/typecount")
  }

  def calSimilarityCount(rdd: RDD[String]):Unit={
    rdd.map(_.split(",")).filter(_.length==20).map(s => (s(0),s(19)))
      .groupBy(s => s._2.substring(0,5)).map(s => s._1.replaceAll("","").replaceAll("\\)","")+","+s._2.size)
      .foreach(println)
  }

  case class earlywarning(id: String, device_id: String, device_type:String, device_address:String, data_sources:String ,
                          keyperson_id:String ,keyperson_state:String,keyperson_type:String ,event_address_id:String ,
                          event_address_name:String ,event_status:String ,compare_sources:String ,create_time:String ,
                          update_time:String ,convictions:String ,job_name:String ,name:String ,pid:String ,taskid:String ,similarity:String )

}