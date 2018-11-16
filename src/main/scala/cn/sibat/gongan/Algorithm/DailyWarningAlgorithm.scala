package cn.sibat.gongan.Algorithm

import cn.sibat.gongan.UDF.TimeFormat._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object DailyWarningAlgorithm{

  def calDayDataCount(sparkSession: SparkSession,dataFrame: DataFrame,date:String){
    val data = dataFrame.withColumn("date",time(col("create_time")).substr(0,10))
      .filter(col("date")===date)

    val datacount = data.groupBy("keyperson_type","date").count()
      .toDF("keyperson_type","date","count").show(10)

    val peoplecnt = data.select("keyperson_id","keyperson_type","date").distinct()
      .groupBy("keyperson_type","date")
      .count().toDF("keyperson_type","date","count")

    val stationcnt = data.select("event_address_name","date")
      .groupBy("event_address_name","date")
      .count().toDF("event_address_name","date","count")

    val hourcount = data.withColumn("hour",time(col("create_time")).substr(10,2))
      .select("date","hour").groupBy("date","hour").count()
      .toDF("date","hour","count")
  }

  def calOfficeCount(office_data: DataFrame,warning_data: DataFrame): Unit ={
    warning_data.select("keyperson_id","event_address_name","create_time")
      .toDF("keyperson_id","station_name","create_time")
      .withColumn("date",time(col("create_time")).substr(0,10))
      .join(office_data,"station_name").show(10)

  }
}