package cn.sibat.gongan.warning

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
//import org.apache.spark.ml.feature.LabeledPoint
//import org.apache.spark.ml.linalg.Vectors

object daily{
  private val POSTGRESUSER = "postgres"
  private val POSTGRESPASSWORD = "postgres"
  private val POSTGRESIP = "190.176.32.8"
  private val POSTGRESPORT = "5432"
  private val POSTGRESDATABASE = "police_traffic"
  private val POSTGRESDBTABLE = "early_warning"
  private val POSTGRESDRIVER = "org.postgresql.Driver"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext
//    val data = getEarlywarning(sparkSession)
//    val policeStationPath = args(0)
//    val date = args(1)

    val police_station = getPoliceStation(sparkSession,"C:\\Users\\小怪兽\\Desktop\\Kim1023\\police_station.csv")
    import sparkSession.sqlContext.implicits._
    val early_warning = sc.textFile("C:\\Users\\小怪兽\\Desktop\\Kim1023\\early_warning1112.txt").filter(s => s.split(",").length==22)
      .map(line =>{
      val s = line.split(",")
      early(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13),s(14),s(15),s(16)
        ,s(17),s(18),s(19),s(20),s(21))
    }).toDF().select("id","keyperson_type","keyperson_id", "event_address_name","create_time").filter(col("create_time").substr(0,4)==="2018")

    calDayDataCount(sparkSession,early_warning,"2018-11-10")
    calOfficeCount(police_station,early_warning)
  }

  def getPoliceStation(sparkSession: SparkSession,path:String): DataFrame ={
    sparkSession.sqlContext.read.csv(path).toDF("id","police_station","depart_id","station_name")
      .select("police_station","station_name").distinct()
  }


  def getEarlywarning(sparkSession: SparkSession): DataFrame ={
    val properties = new Properties()
    properties.put("user",POSTGRESUSER)
    properties.put("password",POSTGRESPASSWORD)
    properties.put("driver",POSTGRESDRIVER)
    sparkSession.sqlContext.read.jdbc("jdbc:postgresql://"+POSTGRESIP+":"+POSTGRESPORT+"/"+POSTGRESDATABASE,
      POSTGRESDBTABLE,properties)
  }

  def calDayDataCount(sparkSession: SparkSession,dataFrame: DataFrame,date:String){
    val data = dataFrame.withColumn("date",time(col("create_time")).substr(0,10)).filter(col("date")===date)

    val datacount = data.groupBy("keyperson_type","date").count().toDF("keyperson_type","date","count").show(10)

    val peoplecnt = data.select("keyperson_id","keyperson_type","date").distinct().groupBy("keyperson_type","date")
      .count().toDF("keyperson_type","date","count")

    val stationcnt = data.select("event_address_name","date").groupBy("event_address_name","date")
      .count().toDF("event_address_name","date","count")

    val hourcount = data.withColumn("hour",time(col("create_time")).substr(10,2))
      .select("date","hour").groupBy("date","hour").count().toDF("date","hour","count")
  }

  def calOfficeCount(office_data: DataFrame,warning_data: DataFrame): Unit ={
    warning_data.select("keyperson_id","event_address_name","create_time").toDF("keyperson_id","station_name","create_time")
      .withColumn("date",time(col("create_time")).substr(0,10))
      .join(office_data,"station_name").show(10)

  }

  /***
    * 编写SparkDataFrame的UDF
    * 将感知门的stime转成标准时间格式
    */
  val time = udf((s:String) => {
    val oldFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    newFormat.format(oldFormat.parse(s))
  })

  case class early(id:String,device_id:String,device_type:String,device_address:String,data_sources:String,
                           keyperson_id:String,keyperson_state:String,keyperson_type:String,event_address_id:String,
                           event_address_name:String,event_status:String,compare_sources:String,create_time:String,
                           update_time:String,convictions:String,job_name:String,name:String,pid:String,taskid:String,
                           similarity:String,position:String,data_device_type:String)
}