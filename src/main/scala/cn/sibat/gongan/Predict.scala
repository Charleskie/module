package cn.sibat.gongan

import java.text.{ParseException, SimpleDateFormat}
import java.util.Locale
import scala.collection.mutable.HashMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import cn.sibat.gongan.Constant.CaseConstant._
import org.apache.spark.rdd.RDD
import cn.sibat.util.timeformat.TimeFormat._

object Predict{
//  val datapath = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\客流预测\\data\\"
  private val datapath = "I:\\Kim1023\\客流预测\\data\\"
  private val sztpath = "I:\\Kim1023\\data\\SZT\\"
  private val outpath = "Kim/data/Szt/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val stationBMMap = new HashMap[String,String]{}
    val stationBM = sc.textFile("Kim/data/subway_zdbm_station").map(line => {
      val s = line.split(",")
      val station_id = s(0)
      val station_name = s(1)
      (station_id,station_name)
    }).collect().foreach(s => {
      stationBMMap.put(s._1,s._2)
    })

    val sztcsv = spark.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat]("Szt/SztCard_bak/" + "{20181202,20181203,20181204,20181205,20181211,20181212,20181209,20181210}")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
      .filter(!_.contains("交易")).filter(!_.contains("巴士")).filter(_.contains("地铁"))
      .filter(s => s.split(",").length>=9).map(line =>{
      val s = line.split(",")
      try{
        if(stationBMMap.contains(s(5).substring(0,6))){
          szt(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),stationBMMap(s(5).substring(0,6)),s(8),StringToISO(s(1),"yyyyMMddHHmmss").substring(0,10))
        }else{
          szt(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),s(7),s(8),StringToISO(s(1),"yyyyMMddHHmmss").substring(0,10))
        }
      }catch{
        case e:StringIndexOutOfBoundsException =>
          println(line)
          szt(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),s(7),s(8),StringToISO(s(1),"yyyyMMddHHmmss").substring(0,10))
      }
    })

    val sztdata = sc.textFile(sztpath+"{szt_20171203,szt_20171204,szt_20171210,szt_20171211,szt_20171217,szt_20171218,szt_20171224,szt_20171225,szt_20180101,szt_20171224}")
    val sztCase = sztdata.filter(!_.contains("交易")).filter(!_.contains("巴士")).map(line =>{
      val s = line.split(",")
      szt(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),stationBMMap(s(5).substring(0,6)),s(8),s(1).substring(0,8))
    }).union(sztcsv).filter(_.deal_type=="地铁出站").filter(_.deal_time.substring(11,13)>="06")

    val sundayszt = sztCase.filter(s => s.deal_time.substring(0, 10) == "2017-12-03" || s.deal_time.substring(0, 10) == "2018-01-01" || s.deal_time.substring(0, 10) == "2017-12-10" || s.deal_time.substring(0, 10) == "2017-12-17").map(s => calflow(s.card_id, s.station_name, s.deal_time))
    val mondayszt = sztCase.filter(s => s.deal_time.substring(0, 10) == "2017-12-04" || s.deal_time.substring(0, 10) == "2017-12-11" || s.deal_time.substring(0, 10) == "2017-12-18" || s.deal_time.substring(0, 10) == "2017-12-25").map(s => calflow(s.card_id, s.station_name, s.deal_time))

    val sundaypredict = sztCase.filter(s => s.deal_time.substring(0, 10) == "2017-12-03" || s.deal_time.substring(0, 10) == "2018-01-01" || s.deal_time.substring(0, 10) == "2017-12-10" || s.deal_time.substring(0, 10) == "2017-12-17" || s.deal_time.substring(0, 10) == "2018-12-02" || s.deal_time.substring(0, 10) == "2018-12-09" || s.deal_time.substring(0, 10) == "2018-12-16").map(s => calflow(s.card_id, s.station_name, s.deal_time))
    val mondaypredict = sztCase.filter(s => s.deal_time.substring(0, 10) == "2017-12-04" || s.deal_time.substring(0, 10) == "2017-12-11" || s.deal_time.substring(0, 10) == "2017-12-18" || s.deal_time.substring(0, 10) == "2017-12-25" || s.deal_time.substring(0, 10) == "2018-12-04" || s.deal_time.substring(0, 10) == "2018-12-11" || s.deal_time.substring(0, 10) == "2018-12-18").map(s => calflow(s.card_id, s.station_name, s.deal_time))

    /** *
      * 计算站点客流，对比圣诞和周一，元旦和周末
      */
    val Christmasdata = mondayszt.filter(s => s.deal_time.substring(0, 10) == "2017-12-25")
      .map(s => (s.station_name, s.deal_time.substring(0, 10))).toDF("station_name", "date")
      .groupBy(col("station_name")).count().toDF("station_name", "cnt")

    val mondaydata = mondayszt.filter(s => s.deal_time.substring(0, 10) != "2017-12-25")
      .map(s => (s.station_name, s.deal_time.substring(0, 10))).toDF("station_name", "date")
      .groupBy(col("station_name"), col("date")).count().toDF("station_name", "date", "cnt")
      .groupBy(col("station_name")).avg("cnt").toDF("station_name", "avg")
      .join(Christmasdata, Seq("station_name")).toDF("station_name", "avg", "Christmas").sort("Christmas")
    //      .coalesce(1).write.csv(datapath+"out\\ChristmasNew")

    val NewYeardata = sundayszt.filter(s => s.deal_time.substring(0, 10) == "2018-01-01")
      .map(s => (s.station_name, s.deal_time.substring(0, 10))).toDF("station_name", "date")
      .groupBy(col("station_name")).count().toDF("station_name", "cnt")


    val sundaydata = sundayszt.filter(s => s.deal_time.substring(0, 10) != "2018-01-01")
      .map(s => (s.station_name, s.deal_time.substring(0, 10))).toDF("station_name", "date")
      .groupBy(col("station_name"), col("date")).count().toDF("station_name", "date", "cnt")
      .groupBy(col("station_name")).avg("cnt").toDF("station_name", "avg")
      .join(NewYeardata, Seq("station_name")).toDF("station_name", "avg", "NewYear").sort("NewYear")
    //      .coalesce(1).write.csv(datapath+"out\\NewYearNew")

    val pinganye = sztCase.filter(s => s.deal_time.substring(0, 10) == "2017-12-24")
      .map(s => (s.station_name, s.deal_time.substring(0, 10))).toDF("station_name", "date")
      .groupBy(col("station_name")).count().toDF("station_name", "cnt")

    val pinganyeAndSunday = sundayszt.filter(s => s.deal_time.substring(0, 10) != "2018-01-01")
      .map(s => (s.station_name, s.deal_time.substring(0, 10))).toDF("station_name", "date")
      .groupBy(col("station_name"), col("date")).count().toDF("station_name", "date", "cnt")
      .groupBy(col("station_name")).avg("cnt").toDF("station_name", "avg")
      .join(pinganye, Seq("station_name")).toDF("station_name", "avg", "Christmas")
    //      .coalesce(1).write.csv(datapath+"out\\Pinganye")


    /** *
      * 计算线网的粒度客流
      */

    val sundaySizeflow = sundayszt.map(s => changetime(s.deal_time, 5)).toDF("deal_time").groupBy(col("deal_time"))
    //      .count().toDF().coalesce(1).write.csv(datapath+"out\\alldata\\sundaySizeflow")

    val mondaySizeflow = mondayszt.map(s => changetime(s.deal_time, 5)).toDF("deal_time").groupBy(col("deal_time"))
    //      .count().toDF().coalesce(1).write.csv(datapath+"out\\alldata\\mondaySizeflow")

    val pinganyeSizeflow = sztCase.filter(s => s.deal_time.substring(0, 10) == "2017-12-24").map(s => changetime(s.deal_time, 5)).toDF("deal_time").groupBy(col("deal_time"))
      .count().toDF("time", "cnt").sort("time").coalesce(1).write.csv(datapath + "out\\alldata\\pinganyeSizeflow")

    /** *
      * 计算线网每天的客流
      */
    val allSunday = sundayszt.map(s => (s.deal_time.substring(0, 10).replaceAll("T", " "))).toDF("date")
    //      .groupBy(col("date")).count().toDF().coalesce(1).write.csv(datapath+"out\\alldata\\allsundaydayflow")
    val allmonday = mondayszt.map(s => (s.deal_time.substring(0, 10).replaceAll("T", " "))).toDF("date")
    //      .groupBy(col("date")).count().toDF().coalesce(1).write.csv(datapath+"out\\alldata\\allmondaydayflow")

    val Christmas = sztCase.filter(s => s.deal_time.substring(0, 10) == "2017-12-24" || s.deal_time.substring(0, 10) == "2017-12-25")
      .map(s => (s.station_name, s.deal_time.substring(0, 10))).groupBy(s => (s._1, s._2)).map(s => {
      val station_name = s._1._1
      val date = s._1._2
      val cnt = s._2.size
      (station_name, date + "," + cnt)
    })


    // sztcsv.filter(_.station_id=="268001").count

    val pinganyeszt = sztCase.filter(s => s.deal_time.substring(0,10)=="2017-12-03"||s.deal_time.substring(0,10)=="2017-12-24"||s.deal_time.substring(0,10)=="2017-12-10"||s.deal_time.substring(0,10)=="2017-12-17").map(s => calflow(s.card_id,s.station_name,s.deal_time))


    val pinganyepredict = sztCase.filter(s => s.deal_time.substring(0,10)=="2017-12-04"||s.deal_time.substring(0,10)=="2017-12-11"||s.deal_time.substring(0,10)=="2017-12-18"||s.deal_time.substring(0,10)=="2017-12-25"||s.deal_time.substring(0,10)=="2018-12-03"||s.deal_time.substring(0,10)=="2018-12-10"||s.deal_time.substring(0,10)=="2018-12-18").map(s => calflow(s.card_id,s.station_name,s.deal_time))

    CalStationSizeFlow(spark,pinganyeszt,"西丽湖","2017-12-24","Pinganye")
    CalStationSizeFlow(spark,pinganyeszt,"深圳湾公园","2017-12-24","Pinganye")
    CalStationSizeFlow(spark,pinganyeszt,"世界之窗","2017-12-24","Pinganye")
    CalStationSizeFlow(spark,pinganyeszt,"福田口岸","2017-12-24","Pinganye")
    CalStationSizeFlow(spark,pinganyeszt,"购物公园","2017-12-24","Pinganye")


    CalStationSizeFlow(spark,mondayszt,"深圳湾公园","2017-12-25","Christmas")
    CalStationSizeFlow(spark,mondayszt,"宝安中心","2017-12-25","Christmas")
    CalStationSizeFlow(spark,mondayszt,"世界之窗","2017-12-25","Christmas")
    CalStationSizeFlow(spark,mondayszt,"后海","2017-12-25","Christmas")
    CalStationSizeFlow(spark,mondayszt,"购物公园","2017-12-25","Christmas")

    CalStationSizeFlow(spark,sundayszt,"深圳湾公园","2018-01-01","NewYear")
    CalStationSizeFlow(spark,sundayszt,"深圳北站","2018-01-01","NewYear")
    CalStationSizeFlow(spark,sundayszt,"罗湖","2018-01-01","NewYear")
    CalStationSizeFlow(spark,sundayszt,"双龙","2018-01-01","NewYear")
    CalStationSizeFlow(spark,sundayszt,"机场","2018-01-01","NewYear")

    PrePingStationSizeFlow(spark,"西丽湖",pinganyepredict)
    PrePingStationSizeFlow(spark,"深圳湾公园",pinganyepredict)
    PrePingStationSizeFlow(spark,"世界之窗",pinganyepredict)
    PrePingStationSizeFlow(spark,"福田口岸",pinganyepredict)
    PrePingStationSizeFlow(spark,"购物公园",pinganyepredict)

    PreChrisStationSizeFlow(spark,"深圳湾公园",mondaypredict)
    PreChrisStationSizeFlow(spark,"宝安中心",mondaypredict)
    PreChrisStationSizeFlow(spark,"世界之窗",mondaypredict)
    PreChrisStationSizeFlow(spark,"后海",mondaypredict)
    PreChrisStationSizeFlow(spark,"购物公园",mondaypredict)

    PreNewStationSizeFlow(spark,"深圳湾公园",sundaypredict)
    PreNewStationSizeFlow(spark,"深圳北站",sundaypredict)
    PreNewStationSizeFlow(spark,"罗湖",sundaypredict)
    PreNewStationSizeFlow(spark,"双龙",sundaypredict)
    PreNewStationSizeFlow(spark,"机场",sundaypredict)


  }

  case class calflow(card_id:String, station_name:String, deal_time:String)


  def StringToISO(s:String, format:String):String={
    val foremat = new SimpleDateFormat(format,Locale.ENGLISH)
    val newFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    try{
      newFormat.format(foremat.parse(s))
    }catch {
      case e:ParseException =>
        "1979-01-01T00:00:01.000Z"
    }

  }

  def CalStationSizeFlow(spark: SparkSession,data:RDD[calflow],station_name:String,time:String,festival:String)={
    import spark.implicits._
    val alldata = data.filter(_.station_name==station_name).map(s => changetime(s.deal_time,5)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","cnt")
    val avgdata = alldata.filter(col("deal_time").substr(0,10)!==time).withColumn("deal_time",col("deal_time").substr(12,8)).groupBy("deal_time").avg("cnt").toDF("deal_time","avg")
    val impodata = alldata.filter(col("deal_time").substr(0,10)===time).withColumn("deal_time",col("deal_time").substr(12,8)).join(avgdata,"deal_time").toDF("deal_time","cnt","avg").sort("deal_time").coalesce(1).write.csv(outpath+"stationSizeFlow_OUT/"+festival+station_name)
  }


  def PreChrisStationSizeFlow(spark: SparkSession,station_name:String,mondaysize:RDD[calflow]): Unit ={
    import spark.implicits._
    val monday = mondaysize.filter(_.station_name==station_name)
    val oldDec04 = monday.filter(_.deal_time.substring(0,10)=="2017-12-04").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec04")
    val oldDec11 = monday.filter(_.deal_time.substring(0,10)=="2017-12-11").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec18")
    val oldDec18 = monday.filter(_.deal_time.substring(0,10)=="2017-12-18").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec18")
    val oldDec25 = monday.filter(_.deal_time.substring(0,10)=="2017-12-25").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec25")
    val newDec04 = monday.filter(_.deal_time.substring(0,10)=="2018-12-04").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","newOct23")
    val newDec11 = monday.filter(_.deal_time.substring(0,10)=="2018-12-11").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","newOct23")
    val Charis = oldDec04.join(oldDec11,"deal_time").join(oldDec18,"deal_time").join(oldDec25,"deal_time").join(newDec04,"deal_time").join(newDec11,"deal_time").rdd.map(s =>{
      val time = s.getString(0)
      val oldDec04 = s.getLong(1).toInt
      val oldDec11 = s.getLong(2)
      val oldDec18 = s.getLong(3).toInt
      val oldDec25 = s.getLong(4).toInt
      val newDec04 = s.getLong(5).toInt
      val newDec11 = s.getLong(6)
      val oldavg = (oldDec04+oldDec11+oldDec18)/3
      val newavg = (newDec04+newDec11)/2
      val predict = newavg*oldDec25/oldavg
      (time,oldavg,newavg,predict.toInt)
    }).toDF("time","oldavg","newavg","predict").sort("time").coalesce(1).write.csv(outpath+"predictChris_OUT/"+station_name)
  }


  /***
    * 预测元旦粒度客流
    */
  def PreNewStationSizeFlow(spark:SparkSession,station_name:String,sundaysize:RDD[calflow]): Unit ={
    import spark.implicits._
    val sunday = sundaysize.filter(_.station_name==station_name)
    val oldDec03 = sunday.filter(_.deal_time.substring(0,10)=="2017-12-03").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec03")
    val oldDec10 = sunday.filter(_.deal_time.substring(0,10)=="2017-12-10").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec10")
    val oldDec17 = sunday.filter(_.deal_time.substring(0,10)=="2017-12-17").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec17")
    val oldNew01 = sunday.filter(_.deal_time.substring(0,10)=="2018-01-01").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldNew01")
    val newDec02 = sunday.filter(_.deal_time.substring(0,10)=="2018-12-02").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","newOct22")
    val newDec09 = sunday.filter(_.deal_time.substring(0,10)=="2018-12-09").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","newOct29")
    val NewYear = oldDec03.join(oldDec10,"deal_time").join(oldDec17,"deal_time").join(oldNew01,"deal_time").join(newDec02,"deal_time").join(newDec09,"deal_time").rdd.map(s =>{
      val time = s.getString(0)
      val oldDec03 = s.getLong(1).toInt
      val oldDec10 = s.getLong(2).toInt
      val oldDec17 = s.getLong(3).toInt
      val oldNew01 = s.getLong(4).toInt
      val newDec02 = s.getLong(5).toInt
      val newDec09 = s.getLong(6).toInt
      val oldavg = (oldDec03+oldDec10+oldDec17)/3
      val newavg = (newDec02+newDec09)/2
      val predict = newavg*oldNew01/oldavg
      (time,oldavg,newavg,predict.toInt)
    }).toDF("time","oldavg","newavg","predict").sort("time").coalesce(1).write.csv(outpath+"predictNew_OUT/"+station_name)
  }
  /***
预测平安夜客流
    ***/
  def PrePingStationSizeFlow(spark: SparkSession,station_name:String,mondaysize:RDD[calflow]): Unit ={
    import spark.implicits._
    val monday = mondaysize.filter(_.station_name==station_name)
    val oldDec04 = monday.filter(_.deal_time.substring(0,10)=="2017-12-04").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec04")
    val oldDec11 = monday.filter(_.deal_time.substring(0,10)=="2017-12-11").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec18")
    val oldDec18 = monday.filter(_.deal_time.substring(0,10)=="2017-12-18").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec18")
    val oldDec25 = monday.filter(_.deal_time.substring(0,10)=="2017-12-25").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","oldDec25")
    val newDec04 = monday.filter(_.deal_time.substring(0,10)=="2018-12-03").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","newOct23")
    val newDec11 = monday.filter(_.deal_time.substring(0,10)=="2018-12-10").map(s => changetime(s.deal_time,5).substring(11,19)).toDF("deal_time").groupBy("deal_time").count().toDF("deal_time","newOct23")
    val Charis = oldDec04.join(oldDec11,"deal_time").join(oldDec18,"deal_time").join(oldDec25,"deal_time").join(newDec04,"deal_time").join(newDec11,"deal_time").rdd.map(s =>{
      val time = s.getString(0)
      val oldDec04 = s.getLong(1).toInt
      val oldDec11 = s.getLong(2)
      val oldDec18 = s.getLong(3).toInt
      val oldDec25 = s.getLong(4).toInt
      val newDec04 = s.getLong(5).toInt
      val newDec11 = s.getLong(6)
      val oldavg = (oldDec04+oldDec11+oldDec18)/3
      val newavg = (newDec04+newDec11)/2
      val predict = newavg*oldDec25/oldavg
      (time,oldavg,newavg,predict.toInt)
    }).toDF("time","oldavg","newavg","predict").sort("time").coalesce(1).write.csv(outpath+"predictPinganye_OUT/"+station_name)
  }
}