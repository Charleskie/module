package cn.sibat.homeAndwork

import org.apache.spark.sql.SparkSession
import cn.sibat.homeAndwork.MarkHomeWork.{home, sample, work}
import cn.sibat.wangsheng.StationCluster.StationCluster
import org.apache.spark.rdd.RDD

object OutHomeWork {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("yarn").getOrCreate()
    val sc = sparkSession.sparkContext
    val month = args(0)
    val date = args(1)
    val path = args(2)
    OutHome(sparkSession,sc.textFile(path + "HomeandWork/Homecal/" + date)).saveAsTextFile(path + "HomeandWork/OutHome/"+month)
    OutWork(sparkSession,sc.textFile(path + "HomeandWork/Workcal/" + date)).saveAsTextFile(path + "HomeandWork/OutWork/"+month)
  }

    /**
      * @param homecal
      * @return
      */
    def OutHome(sparkSession: SparkSession, homecal: RDD[String]): RDD[home] = {
      import sparkSession.implicits._
      val data_order = homecal.map(_.split(",")).map(s => sample(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7).toDouble, s(8).toDouble)).toDF()
        .groupBy("card_id", "station_id", "station_name", "station_lat", "station_lon").count()
        .toDF("card_id", "station_id", "station_name", "station_lat", "station_lon", "count").rdd
        .map(_.mkString(",").split(",")).map(s => home(s(0), s(1), s(2), s(3).toDouble, s(4).toDouble, s(5).toInt))
        .groupBy(_.card_id).map(s => StationCluster.Cluster(s)).map(s => s).flatMap(s => s)
        .groupBy(_.card_id).flatMap(s => {
        val top = s._2.toArray.sortWith(_.count > _.count).take(1)
        top
      }).filter(_.count > 10).map(_.toString.split(",")).map(s => home(s(0), s(1), s(2), s(3).toDouble, s(4).toDouble, s(5).toInt))
      data_order
    }

    /** *
      *
      * @param workcal
      * @return
      */
    def OutWork(sparkSession: SparkSession, workcal: RDD[String]): RDD[home] = {
      import sparkSession.implicits._
      val dataorder = workcal.map(_.split(",")).map(s => work(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7).toDouble, s(8).toDouble, s(9).toInt)).toDF()
        .groupBy("card_id", "station_id", "station_name", "station_lat", "station_lon").count()
        .toDF("card_id", "station_id", "station_name", "station_lat", "station_lon", "count").rdd
        .map(_.mkString(",").split(",")).map(s => home(s(0), s(1), s(2), s(3).toDouble, s(4).toDouble, s(5).toInt))
        .groupBy(_.card_id).map(s => StationCluster.Cluster(s)).flatMap(s => s).map(s => s)
        .groupBy(_.card_id).flatMap(s => {
        val top = s._2.toArray.sortWith(_.count > _.count).take(1)
        top
      }).filter(_.count > 10)
      dataorder
    }
}