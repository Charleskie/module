package cn.sibat.gongan

import cn.sibat.gongan.GetDataService.GetWarningData._
import cn.sibat.gongan.Constant.CaseConstant._
import cn.sibat.gongan.Algorithm.DailyWarningAlgorithm._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import cn.sibat.gongan.UDF.TimeFormat._

object daily{
  private val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\data\\"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext

    val dateArr:Array[String] = Array("2018-11-01","2018-11-10")
    val police_station = getPoliceStation(sparkSession,path+"police_station.csv")
    import sparkSession.sqlContext.implicits._
    val early_warning = sc.textFile(path+"early_warning1116.txt")
      .filter(s => s.split(",").length==26&&s.split(","){0}!="id")
      .map(line =>{
      val s = line.split(",")
      warningdata(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13),s(14),s(15),s(16),s(17),
        s(18),s(19),s(20),s(21),s(22),s(23),s(24),s(25))
    }).filter(s => dateArr.contains(timeParse(s.create_time).substring(0,10))).toDF()
      .select("id","keyperson_type","keyperson_id", "event_address_name","create_time","pid","similarity")
      .withColumn("date",timeParse(col("create_time")).substr(1,10))
//      .filter(col("create_time").substr(0,4)==="2018")

    val keypersonbase = sql.read.option("header",true).csv(path+"keyperson_base.csv")
      .select("NAME","id_number_18","id","create_time")

    val examinationData = sc.textFile(path+"sy_early_warning_examination_approval.txt")
      .filter(s => s.split(",").length==14)
      .map(line =>{
        val s = line.split(",")
        examination(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13))
      }).toDF().select("early_warning_id","examination_approval_type","avaliable","update_time")
//    calDayDataCount(sparkSession,early_warning)
//    calOfficeCount(police_station,early_warning)

    calTimeDiffCatch(early_warning,keypersonbase,examinationData)
  }


}

//create_time.substr(2,2)==re