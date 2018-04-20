package cn.sibat.gongan

import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.expressions.Month
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object GongAn{
  def apply(): GongAn = new GongAn()
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sqlContext = new SQLContext(sc)
    val path = "E:\\Portable\\sibat\\2018\\公安多源数据周报\\GongAnData\\temp3.26-3.31—4.03-4.08"
    val outpath = "E:\\Portable\\sibat\\2018\\公安多源数据周报\\GongAnData\\temp3.26-3.31—4.03-4.08\\output"
    val month = "201804"
//    val date = "26,27,28,29,30,31".split(",")
    val date = "03,04,05,06,07,08".split(",")

//    val rzxd = sparkSession.read.parquet(path+"\\ap_point\\20180331\\*").show()
//    val sensor = sparkSession.read.parquet(path+"\\rzx_device\\20180331\\*").show()
//    val sensor1 = sparkSession.read.parquet(path+"\\rzx_feature\\20180331\\*").show()
//    val sensor2 = sparkSession.read.parquet(path+"\\rzx_location\\20180331\\*").show()
    val sensor3 = sparkSession.read.parquet(path+"\\sensordoor_face\\20180403\\*").count()
//    val sensor4 = sparkSession.read.parquet(path+"\\sensordoor_idcard\\20180328\\*").show(40)


    /***
      * 计算三种数据的采集量和去重量
      */
//    GongAn().CaldayCount(sparkSession,path+"\\ap_point\\",month,date).coalesce(1).saveAsTextFile(outpath+"\\daycount\\ap_point")
//    GongAn().CaldayCount(sparkSession,path+"\\rzx_device\\",month,date).coalesce(1).saveAsTextFile(outpath+"\\daycount\\rzx_device")
//    GongAn().CaldayCount(sparkSession,path+"\\rzx_feature\\",month,date).coalesce(1).saveAsTextFile(outpath+"\\daycount\\rzx_feature")
//    GongAn().CaldayCount(sparkSession,path+"\\rzx_location\\",month,date).coalesce(1).saveAsTextFile(outpath+"\\daycount\\rzx_location")
//    GongAn().CaldayCount(sparkSession,path+"\\sensordoor_face\\",month,date).coalesce(1).saveAsTextFile(outpath+"\\daycount\\sensordoor_face")
//    GongAn().CaldayCount(sparkSession,path+"\\sensordoor_idcard\\",month,date).coalesce(1).saveAsTextFile(outpath+"\\daycount\\sensordoor_idcard")

    /***
      *计算时间延迟
      */
//    GongAn().CalAPTimeDiff(sparkSession,sparkSession.read.parquet(path+"\\ap_point\\*\\*")).coalesce(1).write.csv(outpath+"\\timediff\\ap_point")
//    GongAn().CalSensorTimeDiff(sparkSession,sparkSession.read.parquet(path+"\\sensordoor_idcard\\*\\*").filter(col("recieveTime").contains(" ")))
//    GongAn().CalRZXfeatureTimeDiff(sparkSession,sparkSession.read.parquet(path+"\\rzx_feature\\*\\*")).coalesce(1).write.csv(outpath+"\\timediff\\rzx_feature")
    sparkSession.read.parquet(path+"\\sensordoor_idcard\\*\\*").filter(col("mac")==="000BABDA77DA").show()
    val day = udf((s:String)=>{
      s.substring(0,10)
    })
//    sparkSession.read.parquet(path+"\\rzx_feature\\*\\*").withColumn("day",day(col("starttime"))).groupBy("day").count().show()
//    println(sparkSession.read.parquet(path+"\\ap_point\\20180403\\*").withColumn("day",day(col("stime"))).filter(col("day")==="2018-04-03").count(),
//      sparkSession.read.parquet(path+"\\ap_point\\20180403\\*").withColumn("day",day(col("stime"))).filter(col("day")!=="2018-04-03").count())

    /***
      *计算ap数据的每个延时时间段的数据量
      */
      import sparkSession.implicits._
    val datatimediff = sc.textFile(outpath+"aptimediff").map(_.split(",")).map(s => (s(0),Integer.parseInt(new java.text.DecimalFormat("0").format(s(1).toDouble)))).toDF("mac","timediff").groupBy("timediff").count()

    /***
      * 计算人均采集次数
      */
//    GongAn().CalSensorValidRate(sparkSession,path+"sensor\\201803",outpath+"sensorgetRate","26".split(","))
  }
  case class ap_point(mac:String,bid:String,fid:String,aid:String,apid:String,stime:String,longtitude:String,latitude:String,recieveTime:String)

  case class rzx(account:String,apchannel:String,apencartype:String,bssid:String,companyid:String,consultxpoint:String,consultypoint:String,
                 devicenum:String,devmac:String,endtime:String,essid:String,historyessid:String,imei:String,imsi:String,mac:String,model:String,
                 osversion:String,phone:String,power:String,protocoltype:String,servicecode:String,starttime:String,station:String,stype:String,
                 url:String,xpoint:String,ypoint:String,recieveTime:String)

  case class sensor(mac:String,genId:String,timeStamp:String,idno:String,name:String,sexCode:String,nationCode:String,birth:String,addr:String,
                    authority:String,validBegin:String,validEnd:String,recieveTime:String)
}
class GongAn extends Serializable {

  /***
    * 计算人均采集次数
    * @param sparkSession
    * @param inpath
    * @param outpath
    * @param date
    */
  def CalGetRate(sparkSession: SparkSession,inpath:String,outpath:String,date:Array[String])={
    val out2 = for{
      i <- date;
      data = sparkSession.read.parquet(inpath+"\\rzx\\201803"+i).drop("recieveTime").distinct()
      grpdata = data.groupBy("mac").count().toDF("mac","sum")
    }yield grpdata
  }

  /***
    * 计算日采集量和去重后的量
    * @param sparkSession
    * @param inpath
    * @param month "201803"
    * @param date
    * @return
    */
  def CaldayCount(sparkSession: SparkSession, inpath:String, month: String, date:Array[String])={
    val out = for{
      i <- date;
      data = sparkSession.read.parquet(inpath+month+i)
      disc = data.drop("recieveTime").distinct().count()
      output = data.count()+","+disc+","+month+i
    }yield output
    sparkSession.sparkContext.parallelize(out)
  }


  /***
    * 计算感知门idcard的有效采集率
    * @param sparkSession
    * @param inpath
    * @param outpath
    * @param date
    */
  def CalSensorIDValidRate(sparkSession: SparkSession,inpath:String,outpath:String,month:String,date:Array[String])={
    val out = for{
      i <- date;
      data = sparkSession.read.parquet(inpath+i).drop("recieveTime").distinct()
      datavalid = data.filter(col("idno")!=="null").filter(col("idno") !== null).groupBy("mac").count().toDF("mac","validNum")
      day = udf((s:String)=>{
        val day = month
        day
      })
      datavalidrate = data.groupBy("mac").count().toDF("mac","all").join(datavalid,"mac").withColumn("day",day(col("mac"))+i)
      output = datavalidrate
    }yield output
    sparkSession.sparkContext.parallelize(out).coalesce(1).saveAsTextFile(outpath+"sensorRate")
  }



  /***
    * 计算感知门的时间延迟
    * @param sparkSession
    * @param dataFrame
    */
  def CalSensorTimeDiff(sparkSession: SparkSession,dataFrame: DataFrame)={
    /***
      * 编写SparkDataFrame的UDF
      * 将感知门的stime转成标准时间格式
      */
    val time = udf((s:String) => {
      val oldFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
      val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      newFormat.format(oldFormat.parse(s))
    })
    /***
      * 编写SparkDataFrame的UDF
      * 计算时间差，以分钟形式输出
      */
    val timediff = udf((s:String,t:String)=>{
      val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (timeFormat.parse(t).getTime-timeFormat.parse(s).getTime)/(1000*60)
    })
    dataFrame.filter(col("timeStamp")!==null).filter(col("timeStamp")!=="null").withColumn("time",time(col("timeStamp")))
      .withColumn("timediff",timediff(col("time"),col("recieveTime"))).groupBy("mac").avg("timediff")
  }

  /***
    * 计算AP数据时间延迟
    * @param sparkSession
    * @param dataFrame
    */
  def CalAPTimeDiff(sparkSession: SparkSession,dataFrame: DataFrame)={
    /***
      * 编写SparkDataFrame的UDF
      * 将ap时间转换成标准时间格式
      */
    val time = udf((s:String)=>{
      val oldFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      newFormat.format(oldFormat.parse(s))
    })
    /***
      * 编写SparkDataFrame的UDF
      * 计算时间差
      */
    val timediff = udf((s:String,t:String)=>{
      val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (timeFormat.parse(t).getTime-timeFormat.parse(s).getTime)/(1000*60)
    })
    val TimediffToString = udf((s:String)=>{
      Integer.parseInt(new java.text.DecimalFormat("0").format(s.toDouble))
    })
    dataFrame.filter(col("stime")!==null).filter(col("stime")!=="null").withColumn("time",time(col("stime")))
      .withColumn("timediff",timediff(col("stime"),col("recieveTime"))).groupBy("mac").avg("timediff").toDF("mac","timediff").withColumn("TimeDiff",TimediffToString(col("timediff")))
      .groupBy("TimeDiff").count().toDF("timediff","count")
  }

  /***
    * 计算任子行feature的时间差
    * @param sparkSession
    * @param dataFrame
    */
  def CalRZXfeatureTimeDiff(sparkSession: SparkSession, dataFrame: DataFrame)={
    /***
      * 编写SparkDataFrame的UDF
      * 计算时间差
      */
    val timediff = udf((s:String,t:String)=>{
      val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (timeFormat.parse(t).getTime-timeFormat.parse(s).getTime)/(1000*60)
    })
    val TimediffToString = udf((s:String)=>{
      Integer.parseInt(new java.text.DecimalFormat("0").format(s.toDouble))
    })
    dataFrame.filter(col("starttime")!==null).filter(col("starttime")!=="null")
      .withColumn("timediff",timediff(col("starttime"),col("recieveTime"))).groupBy("mac").avg("timediff").toDF("mac","timediff").withColumn("TimeDiff",TimediffToString(col("timediff")))
      .groupBy("TimeDiff").count()
  }
}