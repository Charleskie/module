package cn.sibat.gongan

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

import cn.sibat.gongan.GetDataService.GetWarningData._
import cn.sibat.gongan.Constant.CaseConstant._
import cn.sibat.gongan.Algorithm.DailyWarningAlgorithm._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import cn.sibat.gongan.UDF.TimeFormat._

import scala.collection.mutable.ArrayBuffer

object daily{
  private val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\data\\"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext

    val dateArr:Array[String] = Array("2018-11-01","2018-11-10")
    val police_station = getPoliceStation(sparkSession,path+"police_station.csv")
    import sparkSession.sqlContext.implicits._
    val early_warningAll = sc.textFile(path+"early_warning1119.txt")
      .filter(s => s.split(",").length==26&&s.split(","){0}!="id")
      .map(line =>{
      val s = line.split(",")
      warningdata(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13),s(14),s(15),s(16),s(17),
        s(18),s(19),s(20),s(21),s(22),s(23),s(24),s(25))
    }).filter(s => timeParse(s.create_time).substring(0,10)>="2018-07-29").toDF()
      .select("id","keyperson_type","keyperson_id", "event_address_name","create_time","pid","similarity")
      .withColumn("warning_date",timeParse(col("create_time")).substr(0,10))
//      .filter(col("create_time").substr(0,4)==="2018")

    val keypersonbase = sql.read.option("header",true).csv(path+"keyperson_base.csv")
      .select("NAME","id_number_18","id","create_time")

    val examinationData = sc.textFile(path+"sy_early_warning_examination_approval.txt")
      .filter(s => s.split(",").length==14)
      .map(line =>{
        val s = line.split(",")
        examination(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13))
      }).toDF().select("early_warning_id","examination_approval_type","avaliable","update_time")

    val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateNow = new Date()
    val calender = Calendar.getInstance()
    calender.setTime(dateNow)
    calender.add(Calendar.DAY_OF_MONTH,-5)
    val day = newFormat.format(calender.getTime).substring(0,10)
    val early_warning = early_warningAll.filter(col("warning_date")===day)
    val dataAll = new ArrayBuffer[String]()
    dataAll.append("#-----总量统计-------#")
    dataAll.append("预警总量："+early_warningAll.count()+","+"预警总人数："+early_warningAll.select("keyperson_id").distinct().count())
    println("##----总量计算完毕----##")
    dataAll.append("日期，预警量，预警人数")
    val dayCount = early_warning.select("warning_date","keyperson_id").rdd
      .map(s => (s.getString(0),s.getString(1))).groupBy(s => s._1)
      .map(s =>{
        val warning_date = s._1
        val cnt = s._2.size
        val cntdisc = s._2.map(_._2).toArray.distinct.length
        warning_date+","+cnt+","+cntdisc
      }).collect().foreach(s => dataAll.append(s))
    dataAll.append("#-----分派出所统计-------#")
    dataAll.append("日期，派出所，预警量，预警人数，累计预警量，累计预警人数")
    val officecnt = calOfficeCount(police_station,early_warningAll)
    val officecntdisc = officecnt.select("warning_date","police_station","keyperson_id","event_address_name")
      .filter(col("warning_date")===day)
      .rdd.map(s => (s.getString(0),s.getString(1),s.getString(2),s.getString(3)))
      .groupBy(s => (s._1,s._2)).map(s =>{
      val warning_date = s._1._1
      val police_station = s._1._2
      val cnt = s._2.size
      val cntdisc = s._2.toArray.map(s => s._3).distinct.length
      (police_station,warning_date+","+cnt+","+cntdisc)
    })
    officecnt.rdd.map(s => (s.getString(0),s.getString(1),s.getString(2),s.getString(3)))
      .groupBy(s => s._2).map(s =>{
      val police_station = s._1
      val cnt = s._2.size
      val cntdisc = s._2.toArray.map(s => s._3).distinct.length
      (police_station,cnt+","+cntdisc)
    }).join(officecntdisc).map( s =>{
      val warning_date = s._2._2.split(","){0}
      val police_station = s._1
      val todayCnt = s._2._2.split(","){1}
      val todayPerson = s._2._2.split(","){2}
      val allCnt = s._2._1.split(","){0}
      val allPerson = s._2._1.split(","){1}
      warning_date+","+police_station+","+todayCnt+","+todayPerson+","+allCnt+","+allPerson
    }).collect().foreach(s => dataAll.append(s))
    println("##----分派出所计算完毕----##")
    dataAll.append("#-----分类型统计-------#")
    dataAll.append("日期，人员类型，预警量，预警人数，累计预警量，累计预警人数")
    val allType = early_warningAll.select("warning_date","keyperson_id", "keyperson_type")
      .rdd.map(s => (s.getString(0),s.getString(1),s.getString(2))).groupBy(s => s._3)
      .map(s =>{
        val keyperson_type = s._1
        val cnt = s._2.size
        val cntdisc = s._2.map(s => s._2).toArray.distinct.length
        (keyperson_type,cnt+","+cntdisc)
      })
    early_warning.select("warning_date","keyperson_id", "keyperson_type" )
      .rdd.map(s => (s.getString(0),s.getString(1),s.getString(2))).groupBy(s => (s._1,s._3))
      .map(s => {
        val warning_date = s._1._1
        val keyperson_type = s._1._2
        val cnt = s._2.size
        val cntdisc = s._2.map(s => s._2).toArray.distinct.length
        (keyperson_type,warning_date+","+cnt+","+cntdisc)
      }).join(allType).map(s =>{
      val warning_date = s._2._1.split(","){0}
      val keyperson_type = s._1
      val todayCnt = s._2._1.split(","){1}
      val todayPerson = s._2._1.split(","){2}
      val allCnt = s._2._2.split(","){0}
      val allPerson = s._2._2.split(","){1}
      warning_date+","+keyperson_type+","+todayCnt+","+todayPerson+","+allCnt+","+allPerson
    }).collect().foreach(s => dataAll.append(s))
    println("##----分类型计算完毕----##")
    dataAll.append("#-----站点统计-------#")
    dataAll.append("日期，站点，预警量，预警人数,累计预警量，累计预警人数")
    val stationAll = early_warning.select("event_address_name","keyperson_id").rdd
      .map(s => (s.getString(0),s.getString(1))).groupBy(s => s._1).map(s =>{
      val event_address_name = s._1
      val cnt = s._2.size
      val cntdisc = s._2.map(s => s._2).toArray.distinct.size
      (event_address_name,cnt+","+cntdisc)
    })
    early_warning.select("event_address_name", "warning_date","keyperson_id").rdd
      .map(s => (s.getString(0),s.getString(1),s.getString(2))).groupBy(s => (s._1,s._2))
      .map(s =>{
        val warning_date = s._1._2
        val station_name = s._1._1
        val cnt = s._2.size
        val cntdist = s._2.map(s => s._3).toArray.distinct.length
        (station_name,warning_date+","+cnt+","+cntdist)
      }).join(stationAll).map( s=>{
      val warning_date = s._2._1.split(","){0}
      val event_address_name = s._1
      val todayCnt = s._2._1.split(","){1}
      val todayPerson = s._2._1.split(","){2}
      val allCnt = s._2._2.split(","){0}
      val allPerson = s._2._2.split(","){1}
      warning_date+","+event_address_name+","+todayCnt+","+todayPerson+","+allCnt+","+allPerson
    }).collect().foreach(s => dataAll.append(s))
    println("##----全部计算完毕----##")
    sc.parallelize(dataAll).coalesce(1).saveAsTextFile(path+"out\\"+day)



  }

}

//create_time.substr(2,2)==re