package cn.sibat.gongan.SZT

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import cn.sibat.gongan.UDF.TimeFormat._
import cn.sibat.wangsheng.timeformat.TimeFormat._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat

object CalSizeFlow{
  val path = ""
  val dateArr = Array("20181115","20181116","20181117","20181118","20181119","20181120","20181121")
  val outpath = "Kim/data/Szt/"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    val data = sparkSession.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat](path + "szt20181114\\200_2018111400.csv")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK")).filter(s => s.split(",").length>=9)
      .map(line =>{
        val s = line.split(",")
        (s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8))
      }).toDF("card_id","deal_time","deal_type","deal_money","deal_value","station_id","company","station_name","car_id")
      .filter(col("station_name")==="布吉"||"大剧院"||"市民中心"||"坪洲")

    val datain = data.filter(col("deal_time").substr(0,8)>="20181115").filter(col("deal_type")==="地铁入站")
      .filter(col("deal_time").substr(0,8)<="20181121").withColumn("deal_time",timeSlice(col("deal_time")))
      .withColumn("date",col("deal_time").substr(0,10))
      .groupBy("station_id","deal_time").count().toDF("station_name","deal_time","cnt")

    val dataout = data.filter(col("deal_time").substr(0,8)>="20181115").filter(col("deal_type")==="地铁出站")
      .filter(col("deal_time").substr(0,8)<="20181121").withColumn("deal_time",timeSlice(col("deal_time")))
      .withColumn("date",col("deal_time").substr(0,10))
      .groupBy("station_id","deal_time").count().toDF("station_name","deal_time","cnt")

    datain.coalesce(1).write.csv(outpath+"in5min")
    dataout.coalesce(1).write.csv(outpath+"out5min")

  }
}