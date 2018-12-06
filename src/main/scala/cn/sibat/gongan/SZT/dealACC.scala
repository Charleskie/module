package cn.sibat.gongan.SZT

import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat

object dealACC {
  val path = "C:\\Users\\小怪兽\\Desktop\\gongan5minInOut\\20180915-0930InOut.csv"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    val dataACC = sparkSession.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat](path)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK")).filter(s => s.split(",").length >= 7)
      .map(line => {
        val s = line.split(",")
        ( s(4),s(1),  s(5), s(6))
      }).toDF("station_name","deal_time", "ACCin","ACCout")
      .filter(col("station_name")==="布吉"||col("station_name")==="大剧院"||col("station_name")==="市民中心"||col("station_name")==="坪洲")
      .withColumn("deal_time",timeParse(col("deal_time")))
      .filter(col("deal_time").substr(0,10)>="2018-09-15")
      .filter(col("deal_time").substr(0,10)<="2018-09-21")


    val dataSZTIn = sparkSession.read.csv("C:\\Users\\小怪兽\\Desktop\\Kim1023\\SZT\\Szt\\09in5min\\*")
      .toDF("station_name","deal_time","SZTin")

    val dataSZTOut = sparkSession.read.csv("C:\\Users\\小怪兽\\Desktop\\Kim1023\\SZT\\Szt\\09out5min\\*")
      .toDF("station_name","deal_time","SZTout")
    val dataSZT = dataSZTIn.join(dataSZTOut,Seq("station_name","deal_time"))

    val dataAll = dataSZT.join(dataACC,Seq("station_name","deal_time"))

    val outpath = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\SZT\\out\\"

    dataAll.filter(col("station_name")==="布吉").sort("deal_time").coalesce(1).write.csv(outpath+"BJ")
    dataAll.filter(col("station_name")==="大剧院").sort("deal_time").coalesce(1).write.csv(outpath+"DJY")
    dataAll.filter(col("station_name")==="坪洲").sort("deal_time").coalesce(1).write.csv(outpath+"PZ")
    dataAll.filter(col("station_name")==="市民中心").sort("deal_time").coalesce(1).write.csv(outpath+"SMZX")

  }

  val newFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  /***
    * 编写SparkDataFrame的UDF
    * 将感知门的stime转成标准时间格式
    */
  val timeParse = udf((s:String) => {
    val oldFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm")
    newFormat.format(oldFormat.parse(s))
  })
}
