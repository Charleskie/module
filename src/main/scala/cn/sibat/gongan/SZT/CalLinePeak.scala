package cn.sibat.gongan.SZT

import cn.sibat.gongan.Algorithm.CalPeakHourFactorAlgorithm
import cn.sibat.gongan.Constant.CaseConstant.sizeflow15min
import cn.sibat.util.timeformat.TimeFormat.changetime
import cn.sibat.util.CleanData
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object CalLinePeak{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val source_data = spark.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat]("Szt/SztCard/" + "{20181221,20181222,20181223,20181224,20181225,20181226,20181227,20190101,20190102}")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))

    val data = CleanData.SZTClean(spark,source_data).filter(s => s.deal_time.substring(11,13)>="06"||s.deal_time.substring(11,13)<"01")

    /***
      * 线网客流的小时分布，客流高峰小时
      */
    val net = data.toDF("card_id","deal_time","deal_type","station_name").withColumn("day",col("deal_time").substr(0,10))
      .withColumn("hour",col("deal_time").substr(11,2)).groupBy("day","hour").count()
      .toDF("day","hour","cnt").groupBy("hour").avg("cnt").toDF("hour","flow")

    /***
      * 线网平日客流强度，元旦平安夜的客流强度
      */
    val newyear = data.filter(_.deal_time.substring(0,10)=="2019-01-01")
    val eve = data.filter(_.deal_time.substring(0,10)=="2018-12-24")
    val common = data.filter(s => s.deal_time.substring(0,10)=="2018-12-21"||s.deal_time.substring(0,10)=="2018-12-22"||s.deal_time.substring(0,10)=="2018-12-23"||s.deal_time.substring(0,10)=="2018-12-26")
//      .map(_.deal_time.substring(0,10)).toDF("deal_time").groupBy("deal_time").count()
//      .toDF("deal_time","cnt").groupBy().avg("cnt").toDF("cnt")
    println(newyear.count())
    println(eve.count())
    common.map(_.deal_time.substring(0,10)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","cnt").groupBy().avg("cnt").toDF("cnt").show()

    /***
      * 地铁线网平时的高峰小时系数，元旦平安夜的高峰小时系数
      */
    val newyearfactor = newyear.map(s => changetime(s.deal_time,15)).groupBy(s => s).map(s =>{
      val deal_time = s._1
      val cnt = s._2.size
      sizeflow15min("",deal_time,cnt)
    })
    CalPeakHourFactorAlgorithm.CalPeakHourFactor(spark,newyearfactor).foreach(println)

    val evefactor = eve.map(s => changetime(s.deal_time,15)).groupBy(s => s).map(s =>{
      val deal_time = s._1
      val cnt = s._2.size
      sizeflow15min("",deal_time,cnt)
    })
    CalPeakHourFactorAlgorithm.CalPeakHourFactor(spark,evefactor).foreach(println)

    val commonfactor = common.map(s => changetime(s.deal_time,15)).groupBy(s => s).map(s =>{
      val deal_time = s._1
      val cnt = s._2.size
      sizeflow15min("",deal_time,cnt)
    })
    CalPeakHourFactorAlgorithm.CalPeakHourFactor(spark,commonfactor).foreach(println)
  }
}