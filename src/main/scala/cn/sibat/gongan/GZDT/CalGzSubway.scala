package cn.sibat.gongan.GZDT

import cn.sibat.gongan.Constant.CaseConstant.gzdt
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import cn.sibat.util.timeformat.TimeFormat._

object CalGzSubway{
  val datapath = "I:\\Kim1023\\data\\GZDT\\"
  val outpath = "I:\\Kim1023\\客流预测\\data\\GZ\\"
  var dataCnt = 0
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    val indata = sparkSession.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat](datapath+"{EN20171203.txt,EN20171204.txt,EN20171210.txt,EN20171217.txt,EN20171218.txt,,EN20171224.txt,EN20171225.txt,EN20180101.txt}")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    val outdata = sparkSession.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat](datapath+"{EX20171203.txt,EX20171204.txt,EX20171210.txt,EX20171217.txt,EX20171218.txt,,EX20171224.txt,EX20171225.txt,EX20180101.txt}")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    indata.take(100).foreach(println)
    outdata.take(100).foreach(println)
    indata.map(_.split("\t"){6}).filter(s => s.contains("AM")&&s.substring(12,14)=="12")
    val data = indata.union(outdata).map(line =>{
      val s = line.split("\t")
      gzdt(s(0).trim,s(1),s(2),s(3),s(4),s(5),StringToISO(s(6),"MMM d yyyy H:mm:ss:000aa"),s(7),s(8),s(9))
    })
    data.foreach(println)
    val sundaydata = data.filter(s => s.deal_time.substring(0,10)=="2017-12-03"||s.deal_time.substring(0,10)=="2018-01-01"||s.deal_time.substring(0,10)=="2017-12-10"||s.deal_time.substring(0,10)=="2017-12-17")
    val mondaydata = data.filter(s => s.deal_time.substring(0,10)=="2017-12-04"||s.deal_time.substring(0,10)=="2017-12-11"||s.deal_time.substring(0,10)=="2017-12-18"||s.deal_time.substring(0,10)=="2017-12-25")

    /***
      * 计算线网的日客流
      */
    mondaydata.map(s => s.deal_time.substring(0,10)).toDF("date").groupBy("date")
      .count().toDF("date","cnt")
      .coalesce(1).write.csv(outpath+"alldata\\sundayDayFlow")
    sundaydata.map(s => s.deal_time.substring(0,10)).toDF("date").groupBy("date")
      .count().toDF("date","cnt")
      .coalesce(1).write.csv(outpath+"alldata\\mondayDayFlow")



    /***
      * 计算线网的粒度客流
      */

    val sundaySizeflow = sundaydata.map(s => changetime(s.deal_time,5)).toDF("deal_time").groupBy(col("deal_time"))
          .count().toDF().coalesce(1).write.csv(outpath+"alldata\\sundaySizeflow")

    val mondaySizeflow = mondaydata.map(s => changetime(s.deal_time,5)).toDF("deal_time").groupBy(col("deal_time"))
          .count().toDF().coalesce(1).write.csv(outpath+"alldata\\mondaySizeflow")

    val newyearSizeFlow = sundaydata.filter(_.deal_time.substring(0,10)=="2018-01-01").union(mondaydata).map(s => changetime(s.deal_time,5)).toDF("deal_time").groupBy(col("deal_time"))
      .count().toDF().coalesce(1).write.csv(outpath+"alldata\\newyearSizeflow")

    /***
      * 计算被影响较大的站点
      */

    val Christmasdata = mondaydata.filter(s => s.deal_time.substring(0,10)=="2017-12-25")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).toDF("station_name","date")
      .groupBy(col("station_name")).count().toDF("station_name","cnt")

    val mondayStationFlow = mondaydata.filter(s => s.deal_time.substring(0,10)!="2017-12-25")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).toDF("station_name","date")
      .groupBy(col("station_name"),col("date")).count().toDF("station_name","date","cnt")
      .groupBy(col("station_name")).avg("cnt").toDF("station_name","avg")
      .join(Christmasdata,Seq("station_name")).toDF("station_name","avg","Christmas")
      .coalesce(1).write.csv(outpath+"alldata\\station\\Christmas")

    val NewYeardata = sundaydata.filter(s => s.deal_time.substring(0,10)=="2018-01-01")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).toDF("station_name","date")
      .groupBy(col("station_name")).count().toDF("station_name","cnt")


    val sundayStationFlow = sundaydata.filter(s => s.deal_time.substring(0,10)!="2018-01-01")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).toDF("station_name","date")
      .groupBy(col("station_name"),col("date")).count().toDF("station_name","date","cnt")
      .groupBy(col("station_name")).avg("cnt").toDF("station_name","avg")
      .join(NewYeardata,Seq("station_name")).toDF("station_name","avg","Christmas")
      .coalesce(1).write.csv(outpath+"alldata\\station\\NewYear")

    /***
      * 计算被影响站点的粒度客流
      */
    val mondaysize = mondaydata.map(s => calsizeflow(s.station_name,s.deal_time)).filter(s => s.deal_time.substring(11,13)>="06")
    val sundaysize = sundaydata.map(s => calsizeflow(s.station_name,s.deal_time)).filter(s => s.deal_time.substring(11,13)>="06")

//    val tiyuxilu = CalStationSizeFlow(sparkSession,mondaysize,"体育西路")


  }

  /***
    * 预测圣诞粒度客流
    */
  def PreChrisStationSizeFlow(spark: SparkSession,station_name:String,mondaysize:RDD[calsizeflow]): Unit ={
    import spark.implicits._
    val monday = mondaysize.filter(_.station_name==station_name)
    val oldDec04 = monday.filter(_.deal_time.substring(0,10)=="2017-12-04").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec04")
    val oldDec18 = monday.filter(_.deal_time.substring(0,10)=="2017-12-18").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec18")
    val oldDec25 = monday.filter(_.deal_time.substring(0,10)=="2017-12-25").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec25")
    val newOct23 = monday.filter(_.deal_time.substring(0,10)=="2018-10-24").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","newOct23")

    val Charis = oldDec04.join(oldDec18,"deal_time").join(oldDec25,"deal_time").join(newOct23,"deal_time").rdd.map(s =>{
      val time = s.getString(0)
      val oldDec04 = s.getLong(1).toInt
      val oldDec18 = s.getLong(2).toInt
      val oldDec25 = s.getLong(3).toInt
      val newOct23 = s.getLong(4).toInt
      val predict = newOct23*oldDec25/(oldDec04+oldDec18)*2
      (time,oldDec04,oldDec18,oldDec25,newOct23,predict.toInt)
    }).toDF().coalesce(1).write.csv(outpath+"predictChris/"+station_name)
  }

  /***
    * 预测元旦粒度客流
    */
  def PreNewStationSizeFlow(spark:SparkSession,station_name:String,sundaysize:RDD[calsizeflow]): Unit ={
    import spark.implicits._
    val sunday = sundaysize.filter(_.station_name==station_name)
    val oldDec03 = sunday.filter(_.deal_time.substring(0,10)=="2017-12-03").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec03")
    val oldDec10 = sunday.filter(_.deal_time.substring(0,10)=="2017-12-10").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec10")
    val oldDec17 = sunday.filter(_.deal_time.substring(0,10)=="2017-12-17").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec17")
    val oldNew01 = sunday.filter(_.deal_time.substring(0,10)=="2018-01-01").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldNew01")
    val newOct22 = sunday.filter(_.deal_time.substring(0,10)=="2018-10-22").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","newOct22")
    val newOct29 = sunday.filter(_.deal_time.substring(0,10)=="2018-10-29").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","newOct29")

    val NewYear = oldDec03.join(oldDec10,"deal_time").join(oldDec17,"deal_time").join(oldNew01,"deal_time").join(newOct22,"deal_time").join(newOct29,"deal_time")
      .rdd.map(s =>{
      val time = s.getString(0)
      val oldDec03 = s.getLong(1).toInt
      val oldDec10 = s.getLong(2).toInt
      val oldDec17 = s.getLong(3).toInt
      val oldNew01 = s.getLong(4).toInt
      val newOct22 = s.getLong(5).toInt
      val newOct29 = s.getLong(6).toInt
      val predict = oldNew01*3/(oldDec03+oldDec10+oldDec17)*(newOct22+newOct29)/2
      (time,oldDec03,oldDec10,oldDec17,oldNew01,newOct22,newOct29,predict.toInt)
    }).toDF().coalesce(1).write.csv(outpath+"predictNew/"+station_name)
  }
  /***
    * 计算粒度客流
    */
  def CalStationSizeFlow(spark: SparkSession,data:RDD[calsizeflow],station_name:String)={
    import spark.implicits._
    data.filter(_.station_name==station_name).map(s => changetime(s.deal_time,5)).toDF("deal_time").groupBy("deal_time").count().coalesce(1).write.csv(outpath+"stationSizeFlow/"+station_name)
  }


  case class calsizeflow(station_name:String,deal_time:String)
}