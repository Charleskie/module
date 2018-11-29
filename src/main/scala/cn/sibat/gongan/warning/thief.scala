package cn.sibat.gongan

import cn.sibat.gongan.GetDataService.GetWarningData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object thief{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext
    val data = GetWarningData.getWarningAndExam(sparkSession)._1
    val thief_data = data.filter(col("keyperson_type")==="扒窃")
      .select("keyperson_id","keyperson_type","create_time","event_address_name")
  }
}