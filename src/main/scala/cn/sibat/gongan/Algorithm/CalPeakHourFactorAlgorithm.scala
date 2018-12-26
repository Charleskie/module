package cn.sibat.gongan.Algorithm

import org.apache.spark.rdd.RDD
import cn.sibat.gongan.Constant.CaseConstant._
import org.apache.spark.sql.SparkSession

object CalPeakHourFactorAlgorithm {

  def CalPeakHourFactor(spark: SparkSession, rdd: RDD[sizeflow15min]): RDD[String] = {
    import spark.implicits._
    val data = rdd.toDF().rdd.map(s => {
      val station_name = s.getString(0)
      val deal_time = s.getString(1)
      val cnt = s.getLong(2)
      (station_name, deal_time, cnt)
    }).groupBy(_._1).map(s => {
      val station_name = s._1
      val flow_arr = s._2.toArray.sortBy(s => s._2 < s._2).map(s => (s._2,s._3))
      (station_name, flow_arr)
    }).map(s => {
      val station_name = s._1
      val timeAndFlow = s._2.sorted
      timeAndFlow.foreach(println)
      val getPeak = GetPeakHour(timeAndFlow)
      station_name+","+getPeak._1+","+getPeak._2+","+getPeak._3+","+getPeak._4+","+getPeak._4*1.0/(getPeak._2*4)
    })
    data
  }

  /***
    *
    * @param array  15分钟的粒度客流
    * @return 高峰小时的开始索引值，最高峰值，最高峰值时间索引值，高峰小时客流值
    */
  def GetPeakHour(array: Array[(String,Long)]): (String,Long,String,Long) = {
    try {
      var index = 0
      var PeakFlow = array(0)._2
      var PeakFlowTime = index
      var PeakHourFlow = array(0)._2 + array(1)._2 + array(2)._2 + array(3)._2
      for (j <- 1 until array.length - 4) {
        val tmp = array(j)._2 + array(j + 1)._2 + array(j + 2)._2 + array(j + 3)._2
        if (PeakHourFlow < tmp) {
          //如果tmp大于Peak，则将tmp的值传给Peak，并将j值记录下来
          PeakHourFlow = tmp //高峰小时客流
          index = j //高峰小时开始时刻的索引值
          PeakFlow = array(index)._2
          PeakFlowTime = index
          for (k <- j + 1 until j + 3) {
            val temp = array(k)._2
            if (array(PeakFlowTime)._2 < temp) {
              PeakFlowTime = k //将达到最高峰值的时间索引值记录下来
              PeakFlow = temp
            }
          }
        }
      }
      (array(index)._1, PeakFlow, array(PeakFlowTime)._1, PeakHourFlow) //高峰小时的开始索引值，最高峰值，最高峰值时间索引值，高峰小时客流值
    }catch {
      case e:ArrayIndexOutOfBoundsException =>
        array.foreach(println)
        (array(0)._1, array(0)._2, array(0)._1, array(0)._2)
    }
  }
}
