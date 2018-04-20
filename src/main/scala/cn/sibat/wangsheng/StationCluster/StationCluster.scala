package cn.sibat.wangsheng.StationCluster

import java.io.IOException
import cn.sibat.homeAndwork.MarkHomeWork.home

object StationCluster{
  def Cluster(data: (String,Iterable[home])): Array[home] = {
    val input = data._2
    if (input == null ) return null
    try {
      val t1 = input.iterator
      val finaldist = 2000.0
      val arr = scala.collection.mutable.ArrayBuffer[home]()
      while (t1.hasNext){
        val it1 = t1.next()
        val card_id = it1.card_id
        val station_id = it1.station_id
        val station_name = it1.station_name
        val lon = it1.station_lon
        val lat = it1.station_lat
        var count = it1.count
        val t2 = input.iterator
        while(t2.hasNext){
          val it2 = t2.next()
          val dis = GPSDistance.Distance(lon,lat,it2.station_lon,it2.station_lat)
          import java.math.BigDecimal
          val dis1 = new BigDecimal(dis)
          val result = dis1.setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue
          if ((result < finaldist) && (result != 0)) count += it2.count
        }
        arr.append(home(card_id,station_id,station_name,lon,lat,count))
      }
      arr.toArray
     } catch {
      case e: Exception =>
          throw new IOException(e.getMessage)
    }
  }
}
