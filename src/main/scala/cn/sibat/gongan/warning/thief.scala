package cn.sibat.gongan

import cn.sibat.gongan.GetDataService.GetWarningData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object thief{
  private val path = "C:\\Users\\administer\\Desktop\\Kim1023\\data\\"
  private val outpath = "C:\\Users\\administer\\Desktop\\Kim1023\\out\\"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext
//    val data = GetWarningData.getWarningAndExam(sparkSession)._1
    val data = sql.read.option("header",true).csv(path+"thief1129\\*").toDF("keyperson_id","keyperson_type","create_time","event_address_name")
    val thief_data = data.toDF("keyperson_id","keyperson_type","create_time","event_address_name")
      .filter(col("keyperson_type")==="扒窃")
      .select("keyperson_id","keyperson_type","create_time","event_address_name")

    println(data.select("keyperson_id").distinct().count())

    val timeAly = data.rdd.map(s => {
      val id = s.getString(0)
      val person_type = s.getString(1)
      val time = s.getString(2).substring(11,13)
      (id,person_type,time)
    }).groupBy(_._3).map(s =>{
      val hour = s._1
      val cnt = s._2.size
      hour+","+cnt
    }).foreach(println)

    val stationAly = data.rdd.groupBy(s => s.getString(3)).map(s => {
      val station = s._1
      val person = s._2.map(s => s.getString(0)).toArray.distinct.size
      station+","+person
    }).foreach(println)

    val line = data.rdd.groupBy(s => s.getString(0)).map(s =>{
      val id = s._1
      val cnt = s._2.size
      val odline = s._2.toArray.sortBy(_.getString(2)).map(s => s.getString(2).substring(0,19)+"->"+s.getString(3)).mkString(";")
      (id,cnt,odline)
    }).sortBy(s => s._2).map(s => s._1+","+s._2+","+s._3).foreach(println)

    val line2 = data.rdd.groupBy(s => s.getString(0)).map(s =>{
      val s2 = s._2.toArray.sortBy(s => s.getString(2))
      val id = s2.map(s => s.getString(0))
      val time = s2.map(s => s.getString(2))
      val station = s2.map(s => s.getString(3))
      id+","+time+","+station
    }).foreach(println)
  }
}