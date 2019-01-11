package cn.sibat.util

import cn.sibat.gongan.Constant.CaseConstant.{linkOD, sztuseful, sztwithday}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import cn.sibat.util.timeformat.TimeFormat.{StringToISO,timediff}

object GetSZTOD{
  def CalSztOD(spark:SparkSession,rdd:RDD[sztuseful]): Unit ={
    val datasort = rdd.map(s => sztwithday(s.card,s.deal_time,s.deal_type,s.station_name,s.deal_time.substring(0,8)))
      .groupBy(s => (s.card_id,s.day)).map(s => {
      val card_id = s._1._1
      val data = s._2.toArray.map(s=> sztuseful(s.card_id,s.deal_time,s.deal_type,s.station_name)).toArray.sortBy(_.deal_time)
      (card_id,data)
    }).flatMap(s => makepairs(s)).filter(_.timediff != -1)
  }
  def linkpair(x: sztuseful, y: sztuseful):linkOD = {
    val timecal = timediff(StringToISO(x.deal_time,"yyyyMMddHHmmss"), StringToISO(y.deal_time,"yyyyMMddHHmmss"),"yyyyMMddHHmmss")
    if (timecal <= 10800) {
      linkOD(y.card, x.deal_time,x.station_name,y.deal_time, y.station_name, timecal)
    }else{
      linkOD("","","","","",-1)
    }//解决返回参数为Any的问题
  }
  def makepairs(s: (String, Array[sztuseful])) = {
    val arr = s._2.sortWith((o, d) => o.deal_time < d.deal_time)
    for {
      i <- 0 until arr.size - 1;
      pairs = linkpair(arr(i), arr(i + 1))
    } yield pairs
  }
}