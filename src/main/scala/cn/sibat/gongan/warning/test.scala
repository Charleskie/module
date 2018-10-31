package cn.sibat.gongan.warning

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.{SQLContext, SparkSession}
import cn.sibat.gongan.warning.earlywarning.string2time

object test {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\"
    //计算8-1到10月27日个人站点轨迹情况
    sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](path+"text.txt")
      .map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK"))
      .map(s =>{
        val line = s.split("\\t")
        (line(5),line(6),line(9))
      }).groupBy(s => (s._1,s._2)).map(s =>{
      s._1+","+s._2.map(_._3).mkString(";")+","+s._2.size
    }).map(_.replaceAll("\\(","")).map(_.replaceAll("\\)",""))

    //计算站点预警人数（去重后的），1人多次算一人
    sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](path+"text.txt")
      .map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK"))
      .map(s =>{
        val line = s.split("\\t")
        (line(5),line(6),line(9))
      }).groupBy(s => s._3).map(s =>{
      s._1+","+s._2.size
    }).map(_.replaceAll("\\(","")).map(_.replaceAll("\\)",""))

    sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](path+"thistime.txt")
      .map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK"))
      .map(s =>{
        val line = s.split("\\t")
        (line(5),line(6),line(9))
      }).groupBy(s => (s._1,s._2)).map(s =>{
      s._1+","+s._2.map(_._3).mkString(";")+","+s._2.size
    }).map(_.replaceAll("\\(","")).map(_.replaceAll("\\)","")).foreach(println)

    sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](path+"thistime.txt")
      .map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK"))
      .map(s =>{
        val line = s.split("\\t")
        (line(5),line(6),line(9))
      }).groupBy(s => s._3).map(s =>{
      s._1+","+s._2.size
    }).map(_.replaceAll("\\(","")).map(_.replaceAll("\\)","")).foreach(println)
  }
}