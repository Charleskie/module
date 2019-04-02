package cn.sibat.paper

import org.apache.spark.sql.SparkSession

object dataprocess {
  private val path = "I:\\毕业论文\\subway5m\\subway5m\\"
  private val outpath = "I:\\毕业论文\\data\\out\\"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext

    val data = sc.textFile(path+"201805*").filter(s => s.split(","){0}.substring(11,19)>"07:00:00")
      .filter(s => s.split(","){0}.substring(11,19)<="23:00:00")
      .filter(s => s.split(","){1}=="世界之窗")
      .map(s =>{
        val line = s.split(",")
        val time = line(0)
        val station_name = line(1)
        val flow = line(2)
        (time,flow)
      }).sortBy(_._1).map(_._2).coalesce(1).saveAsTextFile(outpath+"monthdata")
    val commonday = sc.textFile(path+"{20180514,20180515,20180516,20180517,20180518}").filter(s => s.split(","){0}.substring(11,19)>"07:00:00")
      .filter(s => s.split(","){0}.substring(11,19)<="23:00:00")
      .filter(s => s.split(","){1}=="世界之窗")
      .map(s =>{
      val line = s.split(",")
      val time = line(0).substring(11,19)
      val station_name = line(1)
      val flow = line(2)
      (time,station_name,flow)
    }).groupBy(s => (s._1,s._2)).map(s =>{
      val time = s._1._1
      val flow = s._2.map(s => s._3.toInt).toArray
      (time,flow.sum/flow.length)
    }).sortBy(_._1).map(s => s._2)
//      .coalesce(1).saveAsTextFile(outpath+"commonday1")
    val festivalday = sc.textFile(path+"{20180501,20180618}").filter(s => s.split(","){0}.substring(11,19)>"07:00:00")
      .filter(s => s.split(","){0}.substring(11,19)<="23:00:00")
      .filter(s => s.split(","){1}=="世界之窗")
      .map(s =>{
        val line = s.split(",")
        val time = line(0).substring(11,19)
        val station_name = line(1)
        val flow = line(2)
        (time,station_name,flow)
      }).groupBy(s => (s._1,s._2)).map(s =>{
      val time = s._1._1
      val flow = s._2.map(s => s._3.toInt).toArray
      (time,flow.sum/flow.length)
    }).sortBy(_._1).map(s => s._2)
//      .coalesce(1).saveAsTextFile(outpath+"festivalday1")
  }
}