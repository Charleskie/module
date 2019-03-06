package cn.sibat.gongan.SZT

import java.text.{ParseException, SimpleDateFormat}
import java.util.Locale
import scala.collection.mutable.HashMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import cn.sibat.gongan.Constant.CaseConstant._
import org.apache.spark.rdd.RDD
import cn.sibat.gongan.SZT.CalSizeFlow._

object DataDeal {
  private val datapath = "I:\\Kim1023\\客流预测\\outdata\\SZT\\"
  private val outpath = "I:\\Kim1023\\客流预测\\outdata\\SZT\\out\\"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    DealChris(spark,"深圳湾公园")
    DealChris(spark,"宝安中心")
    DealChris(spark,"世界之窗")
    DealChris(spark,"后海")
    DealChris(spark,"购物公园")

    DealNewYear(spark,"深圳湾公园")
    DealNewYear(spark,"深圳北站")
    DealNewYear(spark,"罗湖")
    DealNewYear(spark,"双龙")
    DealNewYear(spark,"机场")

    DealPinganye(spark,"西丽湖")
    DealPinganye(spark,"深圳湾公园")
    DealPinganye(spark,"世界之窗")
    DealPinganye(spark,"福田口岸")
    DealPinganye(spark,"购物公园")


  }
  def DealChris(spark:SparkSession,station_name:String): Unit ={
    /***
      * 处理站点粒度客流
      */
    val station_size_flow_in = spark.read.csv(datapath+"stationSizeFlow_IN\\Christmas"+station_name).toDF("time","in","normal_in").withColumn("normal_in",DoubleToInt(col("normal_in")))
    val station_size_flow_out = spark.read.csv(datapath+"stationSizeFlow_OUT\\Christmas"+station_name).toDF("time","out","normal_out").withColumn("normal_out",DoubleToInt(col("normal_out")))
    val sizeFlow = station_size_flow_in.join(station_size_flow_out,"time")

    /***
      * 处理圣诞预测客流
      */
    val christmas_in = spark.read.csv(datapath+"predictChris_IN\\"+station_name).toDF("time","old_in","new_in","predict_in")
    val christmas_out = spark.read.csv(datapath+"predictChris_OUT\\"+station_name).toDF("time","old_out","new_out","predict_out")
    sizeFlow.join(christmas_in.join(christmas_out,"time"),"time").sort("time").coalesce(1).write.csv(outpath+"Chris\\"+station_name)
  }

  def DealNewYear(spark:SparkSession,station_name:String): Unit ={
    /***
      * 处理站点粒度客流
      */
    val station_size_flow_in = spark.read.csv(datapath+"stationSizeFlow_IN\\NewYear"+station_name).toDF("time","in","normal_in").withColumn("normal_in",DoubleToInt(col("normal_in")))
    val station_size_flow_out = spark.read.csv(datapath+"stationSizeFlow_OUT\\NewYear"+station_name).toDF("time","out","normal_out").withColumn("normal_out",DoubleToInt(col("normal_out")))
    val sizeFlow = station_size_flow_in.join(station_size_flow_out,"time")

    /***
      * 处理圣诞预测客流
      */
    val newyear_in = spark.read.csv(datapath+"predictNew_IN\\"+station_name).toDF("time","old_in","new_in","predict_in")
    val newyear_out = spark.read.csv(datapath+"predictNew_OUT\\"+station_name).toDF("time","old_out","new_out","predict_out")
    sizeFlow.join(newyear_in.join(newyear_out,"time"),"time").sort("time").coalesce(1).write.csv(outpath+"New\\"+station_name)
  }
  def DealPinganye(spark:SparkSession,station_name:String): Unit ={
    /***
      * 处理站点粒度客流
      */
    val station_size_flow_in = spark.read.csv(datapath+"stationSizeFlow_IN\\Pinganye"+station_name).toDF("time","in","normal_in").withColumn("normal_in",DoubleToInt(col("normal_in")))
    val station_size_flow_out = spark.read.csv(datapath+"stationSizeFlow_OUT\\Pinganye"+station_name).toDF("time","out","normal_out").withColumn("normal_out",DoubleToInt(col("normal_out")))
    val sizeFlow = station_size_flow_in.join(station_size_flow_out,"time")

    /***
      * 处理圣诞预测客流
      */
    val pinganye_in = spark.read.csv(datapath+"predictPinganye_IN\\"+station_name).toDF("time","old_in","new_in","predict_in")
    val pinganye_out = spark.read.csv(datapath+"predictPinganye_OUT\\"+station_name).toDF("time","old_out","new_out","predict_out")
    sizeFlow.join(pinganye_in.join(pinganye_out,"time"),"time").sort("time").coalesce(1).write.csv(outpath+"Ping\\"+station_name)
  }

  val DoubleToInt = udf((s:Double)=>{
    s.toInt
  })
}