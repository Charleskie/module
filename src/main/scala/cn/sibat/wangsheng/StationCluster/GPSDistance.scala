package cn.sibat.wangsheng.StationCluster


object GPSDistance{
  /**
    * 通过两个点的经纬度计算距离
    * @param lon1
    * @param lat1
    * @param lon2
    * @param lat2
    * @return
    */
  def Distance(lon1:Double, lat1:Double, lon2:Double, lat2:Double):Double = {
    if(lon1 == lon2 && lat1 == lat2 ) {
      return 0.0;
    } else {
      var a:Double = 0
      var b:Double = 0
      var R:Double = 0
      R = 6378137; // 地球半径
      val newlat1 = lat1 * Math.PI / 180.0;
      val newlat2 = lat2 * Math.PI / 180.0;
      a = newlat1 - newlat2;
      b = (lon1 - lon2) * Math.PI / 180.0;
      var d:Double = 0
      var sa2:Double = 0
      var sb2:Double = 0
      sa2 = Math.sin(a / 2.0);
      sb2 = Math.sin(b / 2.0);
      d = 2 * R * Math.asin(Math.sqrt(sa2 * sa2 + Math.cos(lat1)
        * Math.cos(lat2) * sb2 * sb2));
      return d;
    }
  }
}