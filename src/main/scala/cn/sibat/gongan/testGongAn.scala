package cn.sibat.gongan

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import cn.sibat.util.timeformat.TimeFormat._

object testGongAn{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\data\\"
    val data = sparkSession.sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path + "szt20181114\\200_2018111400.csv")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))

    data.take(1).foreach(println)
    data.filter(s => s.contains("地铁")).take(100).foreach(println)

    import sparkSession.implicits._
    val dataNEW = data.filter(s => s.split(",").length>=9).map(line =>{
      val s = line.split(",")
      (s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8))
    }).toDF("card_id","deal_time","deal_type","deal_money","deal_value","station_id","company","line","car_id")
      .filter(col("deal_time").substr(0,8)>="20181113")

    val dd = dataNEW.rdd.map(s => {
      val card_id = s.getString(0)
      val deal_time = s.getString(1)
      val deal_type = s.getString(2)
      val deal_money = s.getString(3)
      val deal_value = s.getString(4)
      val station_id = s.getString(5)
      val company = s.getString(6)
      val line = s.getString(7)
      val car_id = s.getString(8)
      val date = deal_time.substring(0,8)
      (card_id,changetime(StringToISO(deal_time,"yyyyMMddHHmmss"),5),deal_type,deal_money,deal_value,station_id,company,line,date)
    }).filter(_._3.contains("地铁入站")).groupBy(s => (s._6,s._2,s._3)).map(s => {
      val station_id = s._1._1
      val timeSlice = s._1._2
      val deal_type = s._1._3
      val cnt = s._2.size
      station_id+","+deal_type+","+timeSlice+","+cnt
    }).take(50).foreach(println)
  }
}