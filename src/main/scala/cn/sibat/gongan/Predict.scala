package cn.sibat.gongan

import scala.collection.mutable.HashMap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import cn.sibat.gongan.UDF.TimeFormat._
import cn.sibat.wangsheng.timeformat.TimeFormat._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import cn.sibat.gongan.Constant.CaseConstant._
import org.apache.spark.rdd.RDD


object Predict{
  val datapath = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\客流预测\\data\\"
  val sztpath = "I:\\Kim1023\\data\\SZT\\"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    val data = sparkSession.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat](datapath + "EX20181029.txt")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    val lendist = data
      .map(s =>{
        val ll = s.split("\t").length
        ll
      }).groupBy(_.toString).map(s => {
      val len = s._1
      val cnt = s._2.size
      len+","+cnt
    })

    data.map(line =>{
      val s = line.split("\t")
      gzdt(s(0).trim,s(1),s(2),s(3),s(4),s(5),StringToISO(s(6),"MMM dd yyyy K:mm:ss:000aa"),s(7),s(8),s(9))
    })


    val stationBMMap = new HashMap[String,String]{}
    val stationBM = sc.textFile(datapath+"subway_zdbm_station").map(line => {
      val s = line.split(",")
      val station_id = s(0)
      val station_name = s(1)
      (station_id,station_name)
    }).collect().foreach(s => {
      stationBMMap.put(s._1,s._2)
    })
    println(stationBMMap.size)
    for(i <- stationBMMap) println(i._1+","+i._2)
    val sztdata = sc.textFile(sztpath+"*")
    val sztCase = sztdata.filter(!_.contains("交易")).filter(!_.contains("巴士")).map(line =>{
      val s = line.split(",")
      szt(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),stationBMMap(s(5).substring(0,6)),s(8),isWeekend(s(1).substring(0,8),"yyyyMMdd"))
    })

    val Christmas = sztCase.filter(s => s.deal_time.substring(0,10)=="2017-12-24"||s.deal_time.substring(0,10)=="2017-12-25")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).groupBy(s => (s._1,s._2)).map(s => {
      val station_name = s._1._1
      val date = s._1._2
      val cnt = s._2.size
      (station_name,date+","+cnt)
    })
    val weekend = sztCase.filter(_.day=="weekend").filter(_.deal_time.substring(0,10)!="2017-12-24")
      .map(s => calflow(s.card_id,s.station_name,s.deal_time))
    CalAvgStationFlow(weekend).map(s =>{
      (s.split(","){0},s.split(","){1})
    }).join(Christmas).coalesce(1).saveAsTextFile(datapath + "out\\weekendChristmas")
    val workday = sztCase.filter(_.day=="workday").filter(_.deal_time.substring(0,10)!="2017-12-25").map(s => calflow(s.card_id,s.station_name,s.deal_time))
    CalAvgStationFlow(workday).map(s => {
      val station_name = s.split(","){0}
      val avg = s.split(","){1}
      (station_name,avg)
    }).join(Christmas)
  }

//  def CalStationDayFlow(data:RDD[calflow])={
//    data.map(s =>{
//      val date = s.deal_time.substring(0,10)
//      (s.station_name,date)
//    }).groupBy(_._2).map(s =>{
//      val station_name
//    })
//  }
  def CalAvgStationFlow(data: RDD[calflow]): RDD[String] ={
    val avgFlow = data.map(s =>{
      val date = s.deal_time.substring(0,10)
      val station_name = s.station_name
      (s.card_id,date,station_name)
    }).groupBy(s => (s._2,s._3)).map(s =>{
      val station_name = s._1._2
      val cnt = s._2.size
      (station_name,cnt)
    }).groupBy(_._1).map(s =>{
      val station_name = s._1
      val dayCnt = s._2.size
      val avg = s._2.map(s => s._2).toArray.sum/dayCnt
      station_name+","+avg.toInt
    })
    avgFlow
  }


  case class calflow(card_id:String, station_name:String, deal_time:String)
}