package cn.sibat.junbo

import org.apache.spark.sql.functions.col
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import cn.sibat.util.timeformat.TimeFormat._
import scala.collection.mutable.ArrayBuffer

object CalTransFlow{
  private val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\data\\"
  private val outpath = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\换乘分析\\out\\"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext
    val dataBus = sc.textFile(path+"bus20181114\\*\\*\\*")

    val dataSzt = sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](path+"szt20181114\\*")
      .map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK"))
      .filter(s => s.split(","){0}!="卡号").filter(s => s.split(","){1}.substring(0,8)=="20181114")

    val bus = dataBus.map(line =>{
      val s = line.split(",")
      if(s.length==17) {
        BusD(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(11),s(12),s(13),s(14))
      }else{
        BusD(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),"","","","")
      }
    })

    dataBus.filter(s => s.split(",").length==11).take(20).foreach(println)

    val getLen = dataBus.map(s => {
      val l = s.split(",").length
      l
    }).groupBy(s => s).map(s =>{
      val len = s._1
      val cnt = s._2.size
      len+","+cnt
    }).foreach(println)

    def getZero(s:String): Double ={
      if(s==""||s.isEmpty) return 0.0
      return s.toDouble
    }

    println("BUS全量数据："+dataBus.count)
    println("BUS去除不完整数据："+bus.count())

    val szt = dataSzt.filter(s => s.split(",").length<=10).map(line =>{
      val s = line.split(",")
      if(s.length==10){
        SZT(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9))
      }else if(s.length==9){
        SZT(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),s(7),s(8),"2018-11-14")
      }else{
        SZT("","","","","","","","","","")
      }
    }).filter(!_.deal_time.isEmpty).filter(_.deal_time.substring(0,10)=="2018-11-14")

//    bus.filter(s => s.deal_time_in.substring(0,4)!="2018"||s.deal_time_out.substring(0,4)!="2018").foreach(println)
//    szt.filter(s => s.deal_time.substring(0,4)!="2018").foreach(println)

//    bus.take(10).foreach(println)
//    szt.take(10).foreach(println)

//    println("SZT全量数据："+dataSzt.count)
//    println("SZT去除不完整数据："+szt.count())



    val dataSzt2Bus = szt.filter(_.deal_type=="地铁出站").map(s => Sample(s.card_id,s.deal_time,s.deal_type,s.station_name))
      .union(bus.filter(!_.deal_time_in.isEmpty).filter(_.deal_time_in.substring(0,10)=="2018-11-14")
        .map(s => Sample(s.card_id,s.deal_time_in,"公交上车",s.station_in_name))).filter(!_.deal_time.isEmpty)
      .filter(_.deal_time.substring(0,10)=="2018-11-14")
      .groupBy(_.card_id)
      .map(s => {
        val id = s._1
        val group = s._2.toArray
        (id,group)
      }).flatMap(s => makepairSzt2Bus(s)).filter(_.card_id!="")

    dataSzt2Bus.groupBy(_.station_name_1).map(s =>{
      val station = s._1
      val count = s._2.size
      station+","+count
    }).coalesce(1).saveAsTextFile(outpath+"stationCnt\\szt")

    dataSzt2Bus.map(s => TransPairs(s.card_id,s.deal_time_1.substring(11,13),s.station_name_1,s.deal_time_2,s.station_name_2))
      .groupBy(_.deal_time_1).map(s =>{
      val hour = s._1
      val count = s._2.size
      hour+","+count
    }).coalesce(1).saveAsTextFile(outpath+"hour\\szt")

    val dataBus2Szt = szt.filter(_.deal_type=="地铁入站").map(s => Sample(s.card_id,s.deal_time,s.deal_type,s.station_name))
      .union(bus.filter(!_.deal_time_in.isEmpty).filter(_.deal_time_in.substring(0,10)=="2018-11-14")
        .map(s => Sample(s.card_id,s.deal_time_in,"公交上车",s.station_out_name))).filter(!_.deal_time.isEmpty).filter(_.deal_time.substring(0,10)=="2018-11-14")
      .groupBy(_.card_id).map(s =>{
      val id = s._1
      val group = s._2.toArray
      (id,group)
    }).flatMap(s => {
      try{
        makepairBus2Szt(s)
      } catch {
        case e:Exception =>
          println(s)
          makepairBus2Szt(s)
      }
    }).filter(_.card_id!="")

    val dataCount = new ArrayBuffer[String]()
    dataCount.append("公交转地铁：")
    dataCount.append(dataBus2Szt.count().toString)
    dataCount.append("地铁转公交：")
    dataCount.append(dataSzt2Bus.count().toString)
    val data = dataCount.foreach(println)

    dataBus2Szt.groupBy(_.station_name_2).map(s =>{
      val station = s._1
      val count = s._2.size
      station+","+count
    }).coalesce(1).saveAsTextFile(outpath+"stationCnt\\bus")

    calLine(dataSzt2Bus,"szt")
    calLine(dataBus2Szt.filter(!_.station_name_1.isEmpty),"bus")

    dataBus2Szt.map(s => TransPairs(s.card_id,s.deal_time_1.substring(11,13),s.station_name_1,s.deal_time_2,s.station_name_2))
      .groupBy(_.deal_time_1).map(s =>{
      val hour = s._1
      val count = s._2.size
      hour+","+count
    }).coalesce(1).saveAsTextFile(outpath+"hour\\bus")


  }

  def calLine(rdd: RDD[TransPairs],s:String): Unit ={
    rdd.groupBy(s => (s.station_name_1,s.station_name_2)).map(s =>{
      val station1 = s._1._1
      val station2 = s._1._2
      val cnt = s._2.size
      station1+","+station2+","+cnt
    }).coalesce(1).saveAsTextFile(outpath+"line\\"+s)
  }

  def makepairSzt2Bus(s:(String,Array[Sample])): IndexedSeq[TransPairs] ={
    val arr = s._2.sortWith((o, d) => o.deal_time < d.deal_time)
    for {
      i <- 0 until arr.size - 1;
      pairs = LinkSzt2Bus(arr(i), arr(i + 1))
    } yield pairs
  }

  def LinkSzt2Bus(o:Sample,d:Sample): TransPairs ={
    if(o.deal_type=="地铁出站"&&d.deal_type=="公交上车"){
      val time_diff = timediff(o.deal_time,d.deal_time,"minute")
      if(time_diff<=40) {
        TransPairs(d.card_id,o.deal_time,o.station_name,d.deal_time,d.station_name)
      }else{
        TransPairs("","","","","")
      }
    }else{
      TransPairs("","","","","")
    }
  }

  def makepairBus2Szt(s:(String,Array[Sample])): IndexedSeq[TransPairs] ={
    val arr = s._2.sortWith((o, d) => o.deal_time < d.deal_time)
    for {
      i <- 0 until arr.size - 1;
      pairs = LinkBus2Szt(arr(i), arr(i + 1))
    } yield pairs
  }

  def LinkBus2Szt(o:Sample,d:Sample): TransPairs ={
    val time_diff = timediff(o.deal_time,d.deal_time,"minute")
    if(d.deal_type=="地铁入站"&&o.deal_type=="公交上车"){
      if(time_diff<=40){
        TransPairs(d.card_id,o.deal_time,o.station_name,d.deal_time,d.station_name)
      }else{
        TransPairs("","","","","")
      }
    }else{
      TransPairs("","","","","")
    }
  }

  /**331472479,M2473,粤BAY472,up,2a6e239d66f04fe48b1369c2034a1444,2018-11-14T07:17:49.000Z,XBUS_00010559,
    * 华创达科技园南,3,113.835022,22.596894,,XBUS_00003556,固戍路口,31,113.863922,22.601376
    * 17
    */
  case class BusD(card_id:String, line:String, car_id:String, direction:String, code:String, deal_time_in:String,
                  xbus1:String, station_in_name:String, station_on_num:String, deal_time_out:String, xbus2:String,
                  station_out_name:String, station_out_num:String )

//661523463,20181112183123,地铁入站,0,    .00,268001139,地铁一号线,罗湖站,OGT-139,20181113
  //10
  case class SZT(card_id:String, deal_time:String, deal_type:String, deal_money:String, deal_value:String,
                 station_id:String, company:String, station_name:String, car_id:String, deal_date:String)

  case class Sample(card_id:String, deal_time:String, deal_type:String, station_name:String)

  case class TransPairs(card_id:String, deal_time_1:String, station_name_1:String, deal_time_2:String, station_name_2:String)
}