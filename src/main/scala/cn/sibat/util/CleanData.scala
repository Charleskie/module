package cn.sibat.util

import cn.sibat.gongan.Constant.CaseConstant.{szt,sztuseful}
import cn.sibat.util.timeformat.TimeFormat._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.HashMap

object CleanData{
  def SZTClean(spark: SparkSession, rdd: RDD[String]):RDD[sztuseful]={
    val stationBMMap = new HashMap[String,String]{}
    val stationBM = spark.sparkContext.textFile("Kim/data/subway_zdbm_station").map(line => {
      val s = line.split(",")
      val station_id = s(0)
      val station_name = s(1)
      (station_id,station_name)
    }).collect().foreach(s => {
      stationBMMap.put(s._1,s._2)
    })

    val data = rdd.filter(!_.contains("交易")).filter(!_.contains("巴士")).filter(_.contains("地铁"))
      .filter(s => s.split(","){1}!="").filter(s => s.split(","){5}.toCharArray.length>6)
      .filter(s => s.split(",").length>=9).map(line =>{
      val s = line.split(",")
      if(stationBMMap.contains(s(5).substring(0,6))){
        szt(s(0),s(1),s(2),s(3),s(4),s(5),s(6),stationBMMap(s(5).substring(0,6)),s(8),s(1).substring(0,8))
      }else{
        szt(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(1).substring(0,8))
      }
    }).map(s => sztuseful(s.card_id,s.deal_time,s.deal_type,s.station_name)).filter(_.deal_type.contains("地铁")).distinct()
    data
  }
}