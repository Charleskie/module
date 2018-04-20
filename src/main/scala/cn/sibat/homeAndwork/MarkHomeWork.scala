package cn.sibat.homeAndwork

import java.text.SimpleDateFormat
import cn.sibat.homeAndwork.MarkHomeWork.{SampleWithDay, sample, work}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/***
  * 计算职住标签
  */
class MarkHomeWork extends Serializable {

  def Homecal(Databus: RDD[String], Datasub: RDD[String]):RDD[sample] ={
    val busdata = Databus.map(_.split(",")).map(s => SampleWithDay(s(0), s(1),s(1).substring(0,7), s(2), s(3), s(4), s(6), s(7), s(9).toDouble, s(10).toDouble))
    val subdata = Datasub.map(_.split(",")).map(s => SampleWithDay(s(0), s(1),s(1).substring(0,7), s(2), s(3), s(4), s(5), s(6), s(8).toDouble, s(9).toDouble)).filter(_.direction == "3")
    val grpData = subdata.union(busdata).groupBy(_.card_id)
    //字段分别为刷卡卡号、刷卡时间、线路、车牌号、方向、站点ID、站点名字、站点经度、站点纬度
    //homecal结果
    val homestation = grpData.flatMap(s => {
      val card_id = s._1
      val top = s._2.toArray.sortBy(_.deal_time).take(1)
      top
    })
    homestation.map(s=>sample(s.card_id,s.deal_time,s.line,s.car_id,s.direction,s.station_id,s.station_name,s.station_lon,s.station_lat))
  }
  /***
    *
    * @param Databus
    * @param Datasub
    * @return
    */
  def Workcal(Databus: RDD[String], Datasub: RDD[String]):RDD[work] ={
    val busdata = Databus.map(_.split(",")).map(s => SampleWithDay(s(0), s(1), s(1).substring(0,7), s(2), s(3), s(4), s(6), s(7), s(9).toDouble, s(10).toDouble))
    val subdata = Datasub.map(_.split(",")).map(s => SampleWithDay(s(0), s(1), s(1).substring(0,7), s(2), s(3), s(4), s(5), s(6), s(8).toDouble, s(9).toDouble)).filter(_.direction == "3")
    val grpData = subdata.union(busdata).groupBy(s => (s.card_id,s.day))//字段分别为刷卡卡号、刷卡时间、线路、车牌号、方向、站点ID、站点名字、站点经度、站点纬度
    val datasort = grpData.map(s => {
      val card_id = s._1._1
      val data = s._2.toArray.sortBy(_.deal_time).map(s=>sample(s.card_id,s.deal_time,s.line,s.car_id,s.direction,s.station_id,s.station_name,s.station_lon,s.station_lat))
      (card_id,data)
    }).flatMap(s => makepairs(s)).filter(_.timediff != -1)
    val data_first = grpData.flatMap(s => {
      val top = s._2.toArray.sortBy(s => s.deal_time).map(s=>sample(s.card_id,s.deal_time,s.line,s.car_id,s.direction,s.station_id,s.station_name,s.station_lon,s.station_lat)).take(1)
      top
    }).map(s => getwork(s))
    val dataunion = datasort.union(data_first).filter(_.timediff >= 10800).sortBy(s => (s.card_id,s.deal_time))//WorkCal的结果
    dataunion
  }

  def linkpair(x: sample, y: sample):work = {
    val timecal = timediff(string2time(x.deal_time), string2time(y.deal_time))
    if (timecal >= 10800) {
      work(y.card_id, y.deal_time, y.car_id, y.line, y.direction, y.station_id, y.station_name, y.station_lon, y.station_lat, timecal)
    }else{
      work("","","","","","","",-1,-1,-1)
    }//解决返回参数为Any的问题
  }
  def makepairs(s: (String, Array[sample])) = {
    val arr = s._2.sortWith((o, d) => o.deal_time < d.deal_time)
    for {
      i <- 0 until arr.size - 1;
      pairs = linkpair(arr(i), arr(i + 1))
    } yield pairs
  }

  /** *
    * 把时间字符串变成ISO时间格式
    * @param time
    * @return
    */

  def string2time(time: String) = {
    val timeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    timeFormatter.parse(time).getTime
  }

  /** *
    * 计算时间差，通过定义输出格式来输出时间差格式
    * second对应秒为单位 minute对应分钟为单位 hour对应小时为单位
    *
    * @param timeO
    * @param timeD
    * @param format
    * @return
    */
  def timediff(timeO: Long, timeD: Long, format: String): Int = {
    var mark = 0
    format match {
      case "second" => mark = 1000
      case "minute" => mark = 1000 * 60
      case "hour" => mark = 1000 * 60 * 60
    }
    ((timeD - timeO) / mark).toInt
  }

  /***
    * 计算时间差，默认输出以秒为单位
    *
    * @param timeO
    * @param timeD
    * @return
    */
  def timediff(timeO: Long, timeD: Long): Int = {
    ((timeD - timeO) / 1000).toInt
  }

  def getwork(s: sample) = {
    work(s.card_id, s.deal_time, s.line, s.car_id, s.direction, s.station_id, s.station_name, s.station_lon, s.station_lat, 0)
  }

}

object MarkHomeWork {
  def apply(): MarkHomeWork = new MarkHomeWork()
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("yarn").getOrCreate()
    val sc = sparkSession.sparkContext
    val date = args(0)
    val path1 = args(2)
    val path2 = args(3)
    val path = args(1)
    MarkHomeWork().Homecal(sc.textFile(path1 + date), sc.textFile(path2 + date)).saveAsTextFile(path + "HomeandWork/Homecal/" + date)
    MarkHomeWork().Workcal(sc.textFile(path1 + date), sc.textFile(path2 + date)).saveAsTextFile(path + "HomeandWork/Workcal/" + date)
  }
    /** *
      * iterable实现rdd操作的直接方法
      * @param sparkSession
      * @param s
      * @return
      */
    def iterable2rdd(sparkSession: SparkSession, s: Iterable[work]) = {
      sparkSession.sparkContext.parallelize(s.toList)
    }

    /** *
      * 把时间字符串变成ISO时间格式
      *
      * @param time
      * @return
      */
    def string2time(time: String) = {
      val timeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      timeFormatter.parse(time).getTime
    }

    /** *
      * 计算时间差，通过定义输出格式来输出时间差格式
      * second对应秒为单位 minute对应分钟为单位 hour对应小时为单位
      *
      * @param timeO
      * @param timeD
      * @param format
      * @return
      */
    def timediff(timeO: Long, timeD: Long, format: String): Int = {
      var mark = 0
      format match {
        case "second" => mark = 1000
        case "minute" => mark = 1000 * 60
        case "hour" => mark = 1000 * 60 * 60
      }
      ((timeD - timeO) / mark).toInt
    }

    /** *
      * 计算时间差，默认输出以秒为单位
      *
      * @param timeO
      * @param timeD
      * @return
      */
    def timediff(timeO: Long, timeD: Long): Int = {
      ((timeD - timeO) / 1000).toInt
    }

    def linkOD(O: sample, D: sample) = {
      val timecal = timediff(string2time(O.deal_time), string2time(D.deal_time))
      if (timecal >= 10800) {
        ODlink(O.card_id, O.deal_time, O.line, O.car_id, O.direction, O.station_id, O.station_name, O.station_lon, O.station_lat, D.station_id, D.station_name, D.station_lon, D.station_lat, timecal)
      }
    }

    def makeOD(s: (String, Iterable[sample])) = {
      val arr = s._2.toArray.sortWith((o, d) => o.deal_time < d.deal_time)
      for {
        i <- 0 until arr.size - 1;
        od = linkOD(arr(i), arr(i + 1))
      } yield od
    }

    def linkpair(x: sample, y: sample) = {
      val timecal = timediff(string2time(x.deal_time), string2time(y.deal_time))
      if (timecal >= 10800) {
        work(y.card_id, y.deal_time, y.car_id, y.line, y.direction, y.station_id, y.station_name, y.station_lon, y.station_lat, timecal)
      }
    }

    def makepairs(s: (String, Array[sample])) = {
      val arr = s._2.sortWith((o, d) => o.deal_time < d.deal_time)
      for {
        i <- 0 until arr.size - 1;
        pairs = linkpair(arr(i), arr(i + 1))
      } yield pairs
    }

    def getwork(s: sample) = {
      work(s.card_id, s.deal_time, s.line, s.car_id, s.direction, s.station_id, s.station_name, s.station_lon, s.station_lat, 0)

    }

    case class pairs(card_id1: String, deal_time1: String, line1: String, car_id1: String, direction1: String,
                     station_O_id: String, station_O_name: String, station_O_lon: Double, station_O_lat: Double,
                     card_id2: String, deal_time2: String, line2: String, car_id2: String, direction2: String,
                     station_D_id: String, station_D_name: String, station_D_lon: Double, station_D_lat: Double,
                     timediff: Int)
    case class ODlink(card_id: String, deal_time: String, line: String, car_id: String, direction: String,
                      station_O_id: String, station_O_name: String, station_O_lon: Double, station_O_lat: Double,
                      station_D_id: String, station_D_name: String, station_D_lon: Double, station_D_lat: Double,
                      timediff: Int)
    case class home(card_id: String, station_id: String, station_name: String, station_lon: Double, station_lat: Double, count: Int) {
      override def toString: String = card_id + "," + station_id + "," + station_name + "," + station_lon + "," + station_lat + "," + count
    }
    case class work(card_id: String, deal_time: String, line: String, car_id: String, direction: String, station_id: String, station_name: String, station_lon: Double, station_lat: Double, timediff: Int) {
      override def toString: String = card_id + "," + deal_time + "," + line + "," + car_id + "," + direction + "," + station_id + "," + station_name + "," + station_lon + "," + station_lat + "," + timediff
    }
    case class bus(card_id: String, deal_time: String, line: String, car_id: String, direction: String, devide: String, station_id: String, station_name: String, station_index: Int, station_lon: Double, station_lat: Double, station_time: String)
    case class sub(card_id: String, deal_time: String, line: String, car_id: String, direction: String, station_id: String, station_name: String, station_index: Int, station_lon: Double, station_lat: Double)
    case class sample(card_id: String, deal_time: String, line: String, car_id: String, direction: String, station_id: String, station_name: String, station_lon: Double, station_lat: Double) {
      override def toString: String = card_id + "," + deal_time + "," + line + "," + car_id + "," + direction + "," + station_id + "," + station_name + "," + station_lon + "," + station_lat
    }
    case class SampleWithDay(card_id: String, deal_time: String, day: String, line: String, car_id: String, direction: String, station_id: String, station_name: String, station_lon: Double, station_lat: Double) {
      override def toString: String = card_id + "," + deal_time + "," + line + "," + car_id + "," + direction + "," + station_id + "," + station_name + "," + station_lon + "," + station_lat
    }
}