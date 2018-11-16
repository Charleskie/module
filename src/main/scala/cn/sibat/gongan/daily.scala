package cn.sibat.gongan

import java.text.SimpleDateFormat
import cn.sibat.gongan.GetDataService.GetWarningData._
import cn.sibat.gongan.Constant.CaseConstant.warning
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object daily{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext
//    val data = getEarlywarning(sparkSession)._1
//    val policeStationPath = args(0)
//    val date = args(1)

    val police_station = getPoliceStation(sparkSession,"C:\\Users\\小怪兽\\Desktop\\Kim1023\\police_station.csv")
    import sparkSession.sqlContext.implicits._
    val early_warning = sc.textFile("C:\\Users\\小怪兽\\Desktop\\Kim1023\\early_warning1112.txt").filter(s => s.split(",").length==22)
      .map(line =>{
      val s = line.split(",")
      warning(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13),s(14),s(15),s(16)
        ,s(17),s(18),s(19),s(20),s(21))
    }).toDF().select("id","keyperson_type","keyperson_id", "event_address_name","create_time").filter(col("create_time").substr(0,4)==="2018")


  }


}
