package cn.sibat.gongan

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.SparkSession

object testGongAn{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\data\\"
    val data = sparkSession.sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path + "szt20181114\\200_2018111400.csv")
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))

    data.take(1).foreach(println)
    data.filter(s => s.contains("M476")).take(100).foreach(println)

  }
}