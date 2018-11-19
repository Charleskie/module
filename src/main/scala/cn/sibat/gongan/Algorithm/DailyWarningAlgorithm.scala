package cn.sibat.gongan.Algorithm

import cn.sibat.gongan.UDF.TimeFormat._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object DailyWarningAlgorithm{

  def calDayDataCount(sparkSession: SparkSession,data: DataFrame){
//    val data = dataFrame.withColumn("date",timeParse(col("create_time")).substr(0,10))
//      .filter(col("date")===date)

    val datacount = data.groupBy("keyperson_type","warning_date").count()
      .toDF("keyperson_type","warning_date","warning_count")

    val peoplecnt = data.select("keyperson_id","keyperson_type","warning_date").distinct()
      .groupBy("keyperson_type","warning_date")
      .count().toDF("keyperson_type","warning_date","warning_count")

    val stationcnt = data.select("event_address_name","warning_date")
      .groupBy("event_address_name","warning_date")
      .count().toDF("event_address_name","warning_date","warning_count")

    val hourcount = data.withColumn("warning_hour",timeParse(col("create_time"))
      .substr(12,2)).select("warning_date","warning_hour")
      .groupBy("warning_date","warning_hour").count()
      .toDF("warning_date","warning_hour","warning_count")
  }

  def calOfficeCount(office_data: DataFrame,warning_data: DataFrame): Unit ={
    warning_data.select("keyperson_id","event_address_name","create_time")
      .toDF("keyperson_id","station_name","create_time")
      .withColumn("warning_date",timeParse(col("create_time")).substr(0,10))
      .join(office_data,"station_name")
      .select("warning_date","police_station","keyperson_id","station_name")

  }


  def calTimeDiffCatch(warning_data:DataFrame,keyperson_base:DataFrame,examination:DataFrame): Unit ={
    warning_data.join(examination,col("id")===col("early_warning_id"))
      .filter(col("examination_approval_type")==="已撤控"&&col("avaliable")==="1")
      .select("keyperson_id","update_time","pid").distinct()
      .join(keyperson_base,col("pid")===col("id"))
      .select("keyperson_id","create_time","update_time")
      .withColumn("arrest_time_diff",timediff(timeToUnix(col("create_time"))
        ,col("update_time")))
      .withColumn("warning_date",UnixParse(col("update_time")).substr(1,10))
      .select("warning_date","keyperson_id","timediff")
  }
}