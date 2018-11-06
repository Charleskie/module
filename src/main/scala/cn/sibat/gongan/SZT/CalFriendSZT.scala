package cn.sibat.gongan.SZT

import cn.sibat.homeAndwork.MarkHomeWork.{ODlink, sample, string2time, timediff}
import org.apache.spark.sql.SparkSession
import cn.sibat.wangsheng.timeformat.TimeFormat.{changetime, string2timeString}

object CalFriendSZT{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("yarn").getOrCreate()
    val sc = sparkSession.sparkContext
  }
  def getFile(sparkSession: SparkSession,path:String): Unit ={

    val data = sparkSession.sparkContext.textFile(path).map(line=>{
      val s = line.split(",")
      szt(s(0),string2timeString(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9))
    })
    val subwaydata = data.filter(_.deal_type == "地铁.*").groupBy(_.station_id).flatMap(s => makeOD(s))
      .filter(_.timediff!="-1").map(s => s)
      .groupBy(s => (s.in_deal_time,s.line,s.station_in_id,s.station_in_name,s.out_deal_time,s.station_out_id,s.station_out_name))
      .map(s => s)
  }

  def timeCluster(s: ((String,String,String,String,String,String),Iterable[sztlink]))={
//    val timecluster =
  }


  def linkOD(O: szt, D: szt) :sztlink = {
    val timecal = timediff(string2time(O.deal_time), string2time(D.deal_time))
    if (timecal <= 10800 && O.deal_type == "地铁入站" && D.deal_type =="地铁出站") {
      sztlink(O.card_id, O.deal_time, O.line, O.station_id, O.station_name,D.deal_time, D.station_id, D.station_name,  timecal)
    }else {
      sztlink("","","","","","","","",-1)
    }
  }

  def makeOD(s: (String, Iterable[szt])) : IndexedSeq[sztlink] = {
    val arr = s._2.toArray.sortWith((o, d) => o.deal_time < d.deal_time)
    for {
      i <- 0 until arr.size - 1;
      od = linkOD(arr(i), arr(i + 1))
    } yield od
  }

  /**
    * 深圳通的数据格式
    */
  case class szt(card_id: String, deal_time:String, deal_type: String, deal_money: String, deal_value:String, station_id:String,
                 line:String, station_name:String, busCard:String, day:String)

  /***
    *做深圳通OD连接
    */
  case class sztlink(card_id:String, in_deal_time:String, line:String, station_in_id:String, station_in_name:String,
                     out_deal_time:String, station_out_id:String, station_out_name:String,timediff:Double)
}