package cn.sibat.gongan.warning

import java.text.{DateFormat, SimpleDateFormat}
import cn.sibat.wangsheng.timeformat.TimeFormat._
import cn.sibat.gongan.GetDataService.GetWarningData
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

object earlywarning{
  private val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\data\\"
//  private val path = "C:\\Users\\administer\\Desktop\\Kim1023\\"
  private val day = "1210"
  private val Months:Array[String] = Array("11")
  private val DATE:Array[String] = Array("11/13","11/12","11/14","11/15","11/16","11/17","11/18")
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
//    private val
//    val data =  sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](path+"early_warning1107.txt")
//      .map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK"))

//    val data = sparkSession.sparkContext.textFile(path+"early_warning"+day+".txt")
//      .filter(s => Months.contains(string2time(s.split(","){15}).substring(5,7)))
//      .filter(s => DATE.contains(string2time(s.split(","){15}).substring(5,10)))
////      .filter(s => DATE.contains(string2time(s.split(","){12}).substring(5,10)))
//      .map(s => s)

//    val date = sparkSession.read.textFile(path+"early_warning1029.xls").rdd.foreach(println)

    val beginday = getDate(-40,"yyyy/MM/dd HH:mm:ss")
    val endday = getDate(-11,"yyyy/MM/dd HH:mm:ss")
    println(beginday,endday)
    val earlywarning = sc.textFile(path+"early_warning"+day+".txt")
      .filter(s => string2time(s.split(","){12}).substring(0,10)>=beginday)
      .filter(s => string2time(s.split(","){12}).substring(0,10)<=endday)
//    earlywarning.take(10).foreach(println)

    val police_station = sparkSession.sparkContext.textFile(path+"police_station.csv")

    calOfficeCount(earlywarning,police_station)
//    calSum(earlywarning)
//    calDayCount(earlywarning).coalesce(1).saveAsTextFile(path+"\\out\\"+day+"day")
//    calSimilarityCount(earlywarning)
//    calHourCount(earlywarning).coalesce(1).saveAsTextFile(path+"\\out\\"+day+"hour")
//    calStationCount(earlywarning).coalesce(1).saveAsTextFile(path+"\\out\\"+day+"station")
//    calStationCountDist(earlywarning).coalesce(1).saveAsTextFile(path+"\\out\\"+day+"stationdist")
//    calTypeCount(earlywarning).coalesce(1).saveAsTextFile(path+"\\out\\"+day+"type")
//    calTrail(earlywarning).coalesce(1).saveAsTextFile(path+"\\out\\"+day+"trail")

  }

  def calSum(rdd: RDD[String])={
    println("预警总量："+rdd.count())
    println("立即处置预警总量："+rdd.filter(s => s.split(","){6}=="立即处置").count())
    println("预警总人数："+rdd.map(_.split(","){5}).distinct().count())
  }

  /***
    * 计算日预警量
    * @param rdd
    */
  def calDayCount(rdd: RDD[String]): RDD[String] ={
    rdd.map(s => {
      val line = s.split(",")
      (line(0),string2time(line(12)).substring(0,10))
    }).groupBy(s => s._2).map(s => s._1+","+s._2.size)
  }

  def calHourCount(rdd: RDD[String]):RDD[String] ={
    rdd.map(s=> {
      val line = s.split(",")
      (line(0),string2time(line(12)).substring(10,13))
    }).groupBy(s=> s._2).map(s=> s._1+","+s._2.size)
  }

  /***
    * 将字符串转成时间格式
    * @param time
    * @return
    */
  def string2time(time: String)=  {
    val timeFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    try{
      timeFormatter.format(timeFormatter.parse(time))
    }catch {
      case e: Exception =>{
        val date = "1980/01/01 00:00:00"
        timeFormatter.format(timeFormatter.parse(date))
      }
    }
  }

  /***
    * 计算站点预警量
    * @param rdd
    */
  def calStationCount(rdd:RDD[String]):RDD[String] ={
    rdd.map(s =>{
      val ss = s.split(",")
      (ss(0),ss(9))
    }).groupBy(s=>s._2).map(s=> s._1+","+s._2.size)
  }

  def calStationCountDist(rdd:RDD[String]):RDD[String] ={
    rdd.filter(s => s.split(","){6}=="立即处置").map(s =>{
      val line = s.split(",")
      (line(5),line(6),line(9))
    }).groupBy(s => s._3).map(s =>{
      val distinct = s._2.toArray.distinct.length
      s._1+","+distinct
    }).map(_.replaceAll("\\(","")).map(_.replaceAll("\\)",""))
  }

  /***
    * 计算各个预警类型的人数分布
    * @param rdd
    */
  def calTypeCount(rdd: RDD[String]): RDD[String] ={
    rdd.map(s =>{
      val line = s.split(",")
      (line(0),line(6),line(7),line.length)
    }).groupBy(s => (s._3,s._2)).map(s => s._1+","+s._2.size).map(s=>{
      val line = s.replaceAll("\\(","").replaceAll("\\)","").split(",")
      line(0)+","+line(1)+","+line(2)
    })
  }

  /***
    * 计算人脸准确度在各个百分比范围的占比
    * @param rdd
    */
  def calSimilarityCount(rdd: RDD[String]): Unit= {
    rdd.map(_.split(",")).filter(_.length>=20).map(s => (s(0),s(19))).filter(_._2!="")
      .groupBy(s => s._2.toDouble.toString.substring(0,5)).map(s => s._1.replaceAll("","").replaceAll("\\)","")+","+s._2.size)
      .foreach(println)
  }

  /***
    * 计算个人预警轨迹
    * @param rdd
    */
  def calTrail(rdd: RDD[String]):RDD[String] = {
    rdd.filter(s => s.split(","){6}=="立即处置").map(s =>{
      val line = s.split(",")
      (line(5),line(6),line(9))
    }).groupBy(s => (s._1,s._2)).map(s =>{
      s._1+","+s._2.map(_._3).toArray.distinct.mkString(";")+","+s._2.size
    }).map(_.replaceAll("\\(","")).map(_.replaceAll("\\)",""))
  }

  /***
    * 计算每个派出所的预警量
    * @param rdd
    * @param office
    */
  def calOfficeCount(rdd:RDD[String],office:RDD[String]): RDD[String] ={
    val office_station = office.map(s =>{
      val line = s.split(",")
      val police_station = line(1)
      val station_name = line(3)
      (station_name,police_station)
    })
    val data = rdd.map(s=>{
      val line = s.split(",")
      val person_id = line(5)
      val station_name = line(9)
      (station_name,person_id)
    }).join(office_station,2).map(s => {
      val station_name = s._1
      val police_station = s._2._2
      val persion_id = s._2._1
      (police_station,persion_id,station_name)
    }).groupBy(s => s._1+","+s._2).map(s =>{
      val sum = s._2.map(_._3).size
      val station_names = s._2.map(_._3).toArray.distinct.mkString(";")
      val ss = s._1.replaceAll("\\(","").replaceAll("\\)","")
      ss+","+station_names+","+sum
    })
    //派出所分组
    data.coalesce(1).saveAsTextFile(path+"out/"+day+"/office")
    //出现在多个派出所的人员标记
    data.map(s =>{
      val line = s.split(",")
      val police_station = line(0)
      val person_id = line(1)
      (person_id,police_station)
    }).groupBy(s => s._1).map(s => {
      val sum = s._2.map(_._2).toArray.distinct.length
      val police_stations = s._2.map(_._2).toArray.distinct.mkString(";")
      s._1+","+police_stations+","+sum
    })
  }

  /***
    * 计算客流数据
    * @param rdd
    */
  def calFlow(rdd:RDD[String]): RDD[String]={
    rdd.filter(s => s.split(","){12}.substring(0,4)=="2018").map(s =>{
      val line = s.split(",")
      val card_id = line(0)
      val day = line(1).substring(0,8)
      val hour = line(1).substring(10,12)
      (card_id,hour,day)
    }).groupBy(s => s._2+","+s._3).map(s =>{
      val hourCount = s._2.map(_._1).size
      val hour = s._1.split(","){0}
      (hour,hourCount)
    }).groupBy(_._1).map(s =>{
      val hourArray = s._2.map(_._2.toDouble).toArray
      val avg = hourArray.sum/hourArray.length
      val hour = s._1
      hour+","+avg.toInt
    })
  }

  case class earlywarning(id: String, device_id: String, device_type:String, device_address:String, data_sources:String ,
                          keyperson_id:String ,keyperson_state:String,keyperson_type:String ,event_address_id:String ,
                          event_address_name:String ,event_status:String ,compare_sources:String ,create_time:String ,
                          update_time:String ,convictions:String ,job_name:String ,name:String ,pid:String ,taskid:String ,similarity:String )

}