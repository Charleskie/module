package cn.sibat.gongan.SZT

import cn.sibat.gongan.Algorithm.CalPeakHourFactorAlgorithm
import cn.sibat.gongan.Constant.CaseConstant._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import scala.collection.mutable.HashMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import cn.sibat.util.timeformat.TimeFormat._

object CalSZTPeakHourFactor{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val stationBMMap = new HashMap[String,String]{}
    val stationBM = sc.textFile("I:\\Kim1023\\data\\subway_zdbm_station").map(line => {
      val s = line.split(",")
      val station_id = s(0)
      val station_name = s(1)
      (station_id,station_name)
    }).collect().foreach(s => {
      stationBMMap.put(s._1,s._2)
    })
    val sztcsv = spark.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat]("I:\\Kim1023\\data\\SZT\\" + "{20181211,201812")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK")).filter(!_.contains("交易"))
      .filter(!_.contains("巴士")).filter(_.contains("地铁入站")).filter(s => s.split(",").length>=9).map(line =>{
       val s = line.split(",")
       try{
         if(stationBMMap.contains(s(5).substring(0,6))){
           szt(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),stationBMMap(s(5).substring(0,6)),s(8),StringToISO(s(1),"yyyyMMddHHmmss").substring(0,10))
         }else{
           szt(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),s(7),s(8),StringToISO(s(1),"yyyyMMddHHmmss").substring(0,10))
         }
       }catch{
         case e:StringIndexOutOfBoundsException =>
           println(line)
           szt(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),s(7),s(8),StringToISO(s(1),"yyyyMMddHHmmss").substring(0,10))
       }
    }).filter(_.station_name=="世界之窗").filter(_.deal_time.substring(0,10)=="2018-12-10")
    val data = sztcsv.map(s => (s.station_name,changetime(s.deal_time,15))).toDF("station_name","deal_time")
      .groupBy("station_name","deal_time").count.toDF("station_name","deal_time","cnt")
      .withColumn("deal_time",col("deal_time").substr(11,8)).groupBy("station_name","deal_time")
      .avg("cnt").rdd.map(s => sizeflow15min(s.getString(0),s.getString(1),s.getDouble(2).toLong))
    CalPeakHourFactorAlgorithm.CalPeakHourFactor(spark,data).foreach(println)
    val sizeFlow = data.map(s => (s.station_name,changetime(s.deal_time,15))).groupBy(s => (s._1,s._2))
      .map(s => {
        val station_name = s._1._1
        val deal_time = s._1._2
        val cnt = s._2.size
        (station_name,deal_time,cnt)
      }).sortBy(_._2).toDF
//      .coalesce(1).write.csv("Kim/outdata/sizeFlow/")

  }
}
