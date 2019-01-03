package cn.sibat.gongan


import cn.sibat.gongan.Algorithm.CalPeakHourFactorAlgorithm
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import cn.sibat.gongan.Constant.CaseConstant._
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
        (changetime(StringToISO(deal_time,"yyyy-MM-dd HH:mm:ss"),15),station_name,in.toInt,out.toInt)
      }).toDF("deal_time","station_name","in","out").groupBy("station_name","deal_time")
      .sum("in","out").toDF("station_name","deal_time","in","out")
      .withColumn("deal_time",col("deal_time").substr(12,8))
      .groupBy("station_name","deal_time").avg("in","out").toDF("station_name","deal_time","in","out").sort("deal_time")
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
        (station_name,changetime(StringToISO(day+" "+deal_time,"yyyy/MM/dd HH:mm"),15),trans.toInt)
      }).toDF("station_name","deal_time","trans")
      .groupBy("station_name","deal_time").sum("trans")
      .toDF("station_name","deal_time","trans").withColumn("deal_time",col("deal_time").substr(12,8))
      .groupBy("station_name","deal_time").avg("trans").toDF("station_name","deal_time","trans")
      .sort("deal_time")

    val dataall = data_in_out.join(data_change,Seq("station_name","deal_time"),"left_outer").na.fill(0.0).withColumn("all",col("in")+col("out")+col("trans"))
      .select("station_name","deal_time","all").withColumn("all",Double2Long(col("all")))
      .rdd.map(s => {
        sizeflow15min(s.getString(0),s.getString(1),s.getLong(2))
    })

    dataall.take(100).foreach(println)

    CalPeakHourFactorAlgorithm.CalPeakHourFactor(spark,dataall).coalesce(1).saveAsTextFile("Kim/peakhour/s")
  }
  val Double2Long = udf((s:Double)=>{
    s.toLong
  })
  val null2zero = udf((s: Double)=>{
    if(s==null) 0.0 else s
  })
}
