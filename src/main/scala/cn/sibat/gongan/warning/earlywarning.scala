package cn.sibat.gongan.warning

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

object earlywarning{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\"

    val data =   sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](path+"text.txt")
      .map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK"))

  }
  def caldaycount(rdd: RDD[String]): Unit ={
    rdd.map(s => {
      val line = s.split(" ")
      s()
    })
  }
}