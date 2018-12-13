package cn.sibat.gongan

import scala.collection.mutable.HashMap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import cn.sibat.gongan.UDF.TimeFormat._
import cn.sibat.wangsheng.timeformat.TimeFormat._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import cn.sibat.gongan.Constant.CaseConstant._
import org.apache.spark.rdd.RDD
import cn.sibat.gongan.SZT.CalSizeFlow._

object Predict{
//  val datapath = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\客流预测\\data\\"
  val datapath = "I:\\Kim1023\\客流预测\\data\\"
  val sztpath = "I:\\Kim1023\\data\\SZT\\"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    val data = sparkSession.sparkContext
      .hadoopFile[LongWritable, Text, TextInputFormat](datapath + "EX20181029.txt")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    val lendist = data
      .map(s =>{
        val ll = s.split("\t").length
        ll
      }).groupBy(_.toString).map(s => {
      val len = s._1
      val cnt = s._2.size
      len+","+cnt
    })

    data.map(line =>{
      val s = line.split("\t")
      gzdt(s(0).trim,s(1),s(2),s(3),s(4),s(5),StringToISO(s(6),"MMM dd yyyy K:mm:ss:000aa"),s(7),s(8),s(9))
    })


    val stationBMMap = new HashMap[String,String]{}
    val stationBM = sc.textFile(datapath+"subway_zdbm_station").map(line => {
      val s = line.split(",")
      val station_id = s(0)
      val station_name = s(1)
      (station_id,station_name)
    }).collect().foreach(s => {
      stationBMMap.put(s._1,s._2)
    })
    println(stationBMMap.size)
    for(i <- stationBMMap) println(i._1+","+i._2)
    val sztdata = sc.textFile(sztpath+"{szt_20171203,szt_20171204,szt_20171210,szt_20171211,szt_20171217,szt_20171218,szt_20171224,szt_20171225,szt_20180101}")
    val sztCase = sztdata.filter(!_.contains("交易")).filter(!_.contains("巴士")).map(line =>{
      val s = line.split(",")
      szt(s(0),StringToISO(s(1),"yyyyMMddHHmmss"),s(2),s(3),s(4),s(5),s(6),stationBMMap(s(5).substring(0,6)),s(8),isSundayorSaturday(s(1).substring(0,8),"yyyyMMdd"))
    })

    val sundayszt = sztCase.filter(s => s.deal_time.substring(0,10)=="2017-12-03"||s.deal_time.substring(0,10)=="2018-01-01"||s.deal_time.substring(0,10)=="2017-12-10"||s.deal_time.substring(0,10)=="2017-12-17")
    val mondayszt = sztCase.filter(s => s.deal_time.substring(0,10)=="2017-12-04"||s.deal_time.substring(0,10)=="2017-12-11"||s.deal_time.substring(0,10)=="2017-12-18"||s.deal_time.substring(0,10)=="2017-12-25")

    /***
      * 计算站点客流，对比圣诞和周一，元旦和周末
      */
    val Christmasdata = mondayszt.filter(s => s.deal_time.substring(0,10)=="2017-12-25")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).toDF("station_name","date")
      .groupBy(col("station_name")).count().toDF("station_name","cnt")

    val mondaydata = mondayszt.filter(s => s.deal_time.substring(0,10)!="2017-12-25")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).toDF("station_name","date")
      .groupBy(col("station_name"),col("date")).count().toDF("station_name","date","cnt")
      .groupBy(col("station_name")).avg("cnt").toDF("station_name","avg")
      .join(Christmasdata,Seq("station_name")).toDF("station_name","avg","Christmas")
      .coalesce(1).write.csv(datapath+"out\\Christmas")

    val NewYeardata = sundayszt.filter(s => s.deal_time.substring(0,10)=="2018-01-01")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).toDF("station_name","date")
      .groupBy(col("station_name")).count().toDF("station_name","cnt")


    val sundaydata = sundayszt.filter(s => s.deal_time.substring(0,10)!="2018-01-01")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).toDF("station_name","date")
      .groupBy(col("station_name"),col("date")).count().toDF("station_name","date","cnt")
      .groupBy(col("station_name")).avg("cnt").toDF("station_name","avg")
      .join(Christmasdata,Seq("station_name")).toDF("station_name","avg","Christmas")
      .coalesce(1).write.csv(datapath+"out\\NewYear")
//TODO 计算影响大的站点的粒度客流，进而进行预测
    /***
      * 计算线网的粒度客流
      */

    val sundaySizeflow = sundayszt.map(s => changetime(s.deal_time,5)).toDF("deal_time").groupBy(col("deal_time"))
//      .count().toDF().coalesce(1).write.csv(datapath+"out\\alldata\\sundaySizeflow")

    val mondaySizeflow = mondayszt.map(s => changetime(s.deal_time,5)).toDF("deal_time").groupBy(col("deal_time"))
//      .count().toDF().coalesce(1).write.csv(datapath+"out\\alldata\\mondaySizeflow")
    /***
      * 计算线网每天的客流
      */
    val allSunday = sundayszt.map(s => (s.deal_time.substring(0,10).replaceAll("T"," "))).toDF("date")
//      .groupBy(col("date")).count().toDF().coalesce(1).write.csv(datapath+"out\\alldata\\allsundaydayflow")
    val allmonday = mondayszt.map(s => (s.deal_time.substring(0,10).replaceAll("T"," "))).toDF("date")
//      .groupBy(col("date")).count().toDF().coalesce(1).write.csv(datapath+"out\\alldata\\allmondaydayflow")

    val Christmas = sztCase.filter(s => s.deal_time.substring(0,10)=="2017-12-24"||s.deal_time.substring(0,10)=="2017-12-25")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).groupBy(s => (s._1,s._2)).map(s => {
      val station_name = s._1._1
      val date = s._1._2
      val cnt = s._2.size
      (station_name,date+","+cnt)
    })

    /****
      * 计算各个站点两周的客流变化
      */

    val station_day_flow = sztCase.filter(_.deal_time.substring(0,10)>="2017-12-11")
      .map(s => (s.station_name,s.deal_time.substring(0,10))).toDF("station_name","date").groupBy(col("station_name"),col("date"))
      .count().toDF()
//      .coalesce(1).write.csv(datapath+"out\\stationdayflow")

    /***
      * 计算圣诞节和周末客流差值
      */
    val weekend = sztCase.filter(_.day=="weekend").filter(_.deal_time.substring(0,10)!="2017-12-24")
      .map(s => calflow(s.card_id,s.station_name,s.deal_time))
    CalAvgStationFlow(weekend).map(s =>{
      (s.split(","){0},s.split(","){1})
    }).join(Christmas)
    /***
      * 计算圣诞节和工作日的客流差值
      */
   val workday = sztCase.filter(_.day=="workday").filter(_.deal_time.substring(0,10)!="2017-12-25").map(s => calflow(s.card_id,s.station_name,s.deal_time))
    CalAvgStationFlow(workday).map(s => {
      val station_name = s.split(","){0}
      val avg = s.split(","){1}
      (station_name,avg)
    }).join(Christmas)

    /***
      * 计算深圳通一周的粒度客流，12月11日至12月25日
      */

    val sizeSztData = sztCase.filter(_.deal_time.substring(0,10)>="2017-12-11").map(s => (s.station_name,s.deal_type,s.deal_time))
    CalSizeFlow(sparkSession,sizeSztData,"老街","2017-12-11","2017-12-25")
//      .coalesce(1).write.csv(datapath+"out\\sizeFlow\\laojie")
    CalSizeFlow(sparkSession,sizeSztData,"深圳北站","2017-12-11","2017-12-25")
//      .coalesce(1).write.csv(datapath+"out\\sizeFlow\\shenzhenbei")
    CalSizeFlow(sparkSession,sizeSztData,"车公庙","2017-12-11","2017-12-25")
//      .coalesce(1).write.csv(datapath+"out\\sizeFlow\\chegongmiao")
    CalSizeFlow(sparkSession,sizeSztData,"福田口岸","2017-12-11","2017-12-25")
//      .coalesce(1).write.csv(datapath+"out\\sizeFlow\\futian")
    CalSizeFlow(sparkSession,sizeSztData,"大剧院","2017-12-11","2017-12-25")
//      .coalesce(1).write.csv(datapath+"out\\sizeFlow\\dajuyuan")
    CalSizeFlow(sparkSession,sizeSztData,"深大","2017-12-11","2017-12-25")
//      .coalesce(1).write.csv(datapath+"out\\sizeFlow\\shenda")
    CalSizeFlow(sparkSession,sizeSztData,"高新园","2017-12-11","2017-12-25")
//      .coalesce(1).write.csv(datapath+"out\\sizeFlow\\gaoxinyuan")
    CalSizeFlow(sparkSession,sizeSztData,"坪洲","2017-12-11","2017-12-25")
//      .coalesce(1).write.csv(datapath+"out\\sizeFlow\\pingzhou")
    CalSizeFlow(sparkSession,sizeSztData,"五和","2017-12-11","2017-12-25")
//      .coalesce(1).write.csv(datapath+"out\\sizeFlow\\wuhe")

    /***
      * 计算线网客流情况
      */

    val alldata = sztCase.filter(_.deal_time.substring(0,10)>="2017-12-11").map(s => (s.deal_time.substring(0,10)))
      .toDF("date").groupBy(col("date")).count().toDF()
//      .coalesce(1).write.csv(datapath+"out\\alldata\\dayflow")

    /****
      * 计算线网粒度客流
      */
    val alldatasize = sztCase.filter(_.deal_time.substring(0,10)>="2017-12-11").map(s => (s.station_name,s.deal_type,s.deal_time))
    alldatasize.map(s => (changetime(s._3,5))).toDF("deal_time").groupBy(col("deal_time")).count().toDF()
//      .coalesce(1).write.csv(datapath+"out\\alldata\\sizeflow")
//    CalSizeFlow(sparkSession,alldatasize,"2017-12-11","2017-12-25").coalesce(1).write.csv(datapath+"out\\alldata\\sizeflow")
  }

//  def CalStationDayFlow(data:RDD[calflow])={
//    data.map(s =>{
//      val date = s.deal_time.substring(0,10)
//      (s.station_name,date)
//    }).groupBy(_._2).map(s =>{
//      val station_name
//    })
//  }
  def CalAvgStationFlow(data: RDD[calflow]): RDD[String] ={
    val avgFlow = data.map(s =>{
      val date = s.deal_time.substring(0,10)
      val station_name = s.station_name
      (s.card_id,date,station_name)
    }).groupBy(s => (s._2,s._3)).map(s =>{
      val station_name = s._1._2
      val cnt = s._2.size
      (station_name,cnt)
    }).groupBy(_._1).map(s =>{
      val station_name = s._1
      val dayCnt = s._2.size
      val avg = s._2.map(s => s._2).toArray.sum/dayCnt
      station_name+","+avg.toInt
    })
    avgFlow
  }


  case class calflow(card_id:String, station_name:String, deal_time:String)
}