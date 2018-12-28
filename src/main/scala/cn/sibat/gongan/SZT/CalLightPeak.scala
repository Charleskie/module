package cn.sibat.gongan

import java.text.{ParseException, SimpleDateFormat}
import java.util.Locale
import scala.collection.mutable.HashMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import cn.sibat.gongan.Constant.CaseConstant._
import org.apache.spark.rdd.RDD
import cn.sibat.gongan.SZT.CalSizeFlow._
import cn.sibat.util.timeformat.TimeFormat._

object CalLightPeak{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val data_in_out = spark.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat]("C:\\Users\\小怪兽\\Desktop\\in_out_5min\\20180915-0930进出站.csv")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
      .filter(!_.contains("站点"))
      .map(line  => {
        val s = line.replaceAll("\"","").split(",")
        val deal_time = s(1)
        val station_name = s(4)
        val in = s(5)
        val out = s(6)
        (changetime(StringToISO(deal_time,"yyyy-MM-dd HH:mm:ss"),30).substring(0,10),in.toInt,out.toInt)
      }).toDF("deal_time","in","out").groupBy("deal_time").sum("in","out").toDF("deal_time","in","out").sort("deal_time").show(20)
    val data_change = spark.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat]("C:\\Users\\小怪兽\\Desktop\\in_out_5min\\20180915-1025换乘（分方向）.csv")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
      .filter(!_.contains("行"))
      .map(line  => {
        val s = line.replaceAll("\"","").split(",")
        val day = s(0)
        val deal_time = s(5)
        val station_name = s(2)
        val trans = s(14).toDouble+s(13).toDouble+s(12).toDouble+s(11).toDouble+s(10).toDouble+s(9).toDouble+s(8).toDouble+s(7).toDouble
        (changetime(StringToISO(day+" "+deal_time,"yyyy/MM/dd HH:mm"),30).substring(0,10),trans.toInt)
      }).toDF("deal_time","trans")
      .groupBy("deal_time").sum("trans").toDF("deal_time","trans").sort("deal_time").show(20)
  }
}
