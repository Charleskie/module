package cn.sibat.junbo
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
object CalIOFlow{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    val TimeFormat = new SimpleDateFormat("yyyyMMdd")
    val path = args(0) // /user/wangsheng/SZT/DATE
    val outpath = args(1) // /user/wangsheng/zhangjun/CalStationIO/DATE
    CalStationIO(sparkSession,path).saveAsTextFile(outpath)
//    CalStationIO(sparkSession,"E:\\Portable\\sibat\\spark\\testdata").foreach(println)
//    sparkSession.sparkContext.textFile("E:\\Portable\\sibat\\spark\\testdata").take(30).foreach(println)

  }
  def CalStationIO(sparkSession: SparkSession,path:String):RDD[String]={
    sparkSession.sparkContext.textFile(path).map(_.split(",")).map(s => szt(s(0),s(1),s(2),s(3),s(4),s(5),s(6))).map(s => {
      val newtype =
      if(s.IOtype=="地铁入站"||s.IOtype=="地铁出站") {
        s.IOtype match {
          case "地铁入站" => "21"
          case "地铁出站" => "22"
          case _ => "ERROR"
        }
      }else{ s.IOtype}
      szt(s.code,s.id_card,s.station_BM,newtype,s.time,s.line,s.station_name)
    }).groupBy(_.station_name).map(s =>{
      val InFlow = s._2.count(_.IOtype.matches("21"))
      val OutFlow = s._2.count(_.IOtype.matches("22"))
      s._1+","+InFlow+","+OutFlow
    })
  }
  //000,698003599,241013122,地铁入站,2017-05-28T18:50:07.000Z,地铁十一号线,红树湾南,IGT-122
  case class szt(code:String,id_card:String,station_BM:String,IOtype:String,time:String,line:String,station_name:String)
}