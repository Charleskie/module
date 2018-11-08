package cn.sibat.traffic

import java.sql.{Connection, DriverManager, SQLException}
import java.text.SimpleDateFormat

import cn.sibat.traffic.BusClean.{BusDeal, BusO, BusStationGPS}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

/**
  * 公交到站预测分4种方法：
  *                      1.通过第二次坐车的上车地点，选择最近的与上一次搭乘线路和同方向的站点作为上次乘车的终点，限制最大距离为2km（适用于一天乘车次数大于1次且被预测乘车记录不是最后一条）
  *                      2.往返记录，最后两次乘车为公交且通线路，方向相反；若该用户为通勤用户，则默认其早高峰前往工作地，晚高峰前往住址地
  *                      3.默认上车人数多的站点下客人也多，把乘客乘车的同线路同方向下客量多的站点作为下车站点
  *                      4.随机选择同线路同方向的站点作为下车站点
  * Created by WJ on 2018/1/16.
  */

/**
  * BusO站点匹配
  */
class BusClean extends Serializable{
  /***
    *  把车牌号格式化
    */
  def CarID_Parse(card_id:String):String={
    val head = "粤B"
    var getCarID = ""
    if(card_id != null){
      val parse1 = card_id.replaceAll(" ","")
      val len = parse1.length
      if(len>=6 && parse1.substring(len-1,len).equals("D")){
        getCarID = head+parse1.substring(len-6,len)
      }else if(len>=5 && !parse1.substring(len-1,len).equals("D")){
        getCarID = head+parse1.substring(len-5,len)
      }else{
        getCarID = parse1
      }
    }else{
      getCarID = card_id
    }
    getCarID
  }

  /**
    * 把到站信息的时间戳转化成北京ISO格式时间
    */
  def timeChange(time:Long):String={
    new DateTime((time+8*60*60)*1000).toString("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  }

  /**
    * 秒
    */
  def timeDiff(time1:String,time2:String):Long={
    val SF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    (SF.parse(time1).getTime -  SF.parse(time2).getTime)/1000
  }

  /**
    * 通过公交线路(line)、方向(direction)、站点ID(station_id) 查询 station_name、station_index、lon、lat
    */
  def getStationInfo():Map[String,String]={
    //读取数据库中信息返回Map
    val getMap = scala.collection.mutable.Map[String,String]()
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://172.16.3.200/xbus_v2"
    val username = "xbpeng"
    val password = "xbpeng"
    var connection:Connection = null

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url,username,password)

      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select l.ref_id,l.directionCode,s.station_id,ss.name,s.stop_order,ss.lat,ss.lon from line l,line_stop s,station ss where l.id=s.line_id AND s.station_id=ss.id")
      while (resultSet.next()){
        val ref_id = resultSet.getString(1)
        val dir = resultSet.getInt(2)
        val s_id = resultSet.getString(3)
        val name = resultSet.getString(4)
        val index = resultSet.getString(5)
        val lat = resultSet.getString(6)
        val lon = resultSet.getString(7)
        val key = ref_id +","+ dir +","+ s_id
        val valued = name+","+index+","+lat+","+lon
        if(!getMap.contains(key)) getMap.put(key,valued)
      }
    }catch {
      case e:SQLException => e.printStackTrace()
    }
    getMap.toMap
  }
  /**
    * 获取公交交易数据
    *@param position card_id,car_id,time 的序列，“，”分隔：1,7,4
    */
  def CalBusDeal(data:RDD[String],position:String)={
    val positions = position.split(",")
    data.map(x=>{
      try{ val s = x.split(",")
      val card_id = s(positions(0).toInt)
      val car_id = s(positions(1).toInt)
      val new_car_id = CarID_Parse(car_id)
      val time = s(positions(2).toInt)
      BusDeal(card_id,new_car_id,time)
      }catch {
        case e:ArrayIndexOutOfBoundsException => BusDeal(x,"","")
      }
    }).filter(x=> !(x.car_id.isEmpty||x.deal_time.isEmpty||x.card_id.isEmpty))
  }

  /**
    *获取公交到站信息
    * @param position car_id,arrive_time,leave_time,line,direction,devide,station_id的序列，“，”分隔 ：1,2,3,5,4,10,8
    *
    */
  def CalBusStationGPS(data:RDD[String],position:String):RDD[BusStationGPS]={
    val Info = getStationInfo()
    val positions = position.split(",")
    data.map(x=>{
      val s = x.split("\t")
      val car_id = s(positions(0).toInt)
      val new_car_id = CarID_Parse(car_id.substring(car_id.size-6,car_id.size))
      val arrive_time:String = s(positions(1).toInt).trim
      val leave_time:String = s(positions(2).toInt).trim
      val line = s(positions(3).toInt)
      val direction = s(positions(4).toInt)
      val devide = s(positions(5).toInt)
      val station_id = s(positions(6).toInt)
      val new_deal_time = if(arrive_time != "null"){
        timeChange(arrive_time.toLong)
      }
      else if(leave_time != "null") {
        timeChange(leave_time.toLong)
      }else {
        "0"}
      val new_direction = direction match {
        case "up" => "1"
        case "down" => "2"
        case _ => direction
      }
      var stationName = ""
      var index =  -1
      var lat = 0.0
      var lon = 0.0
      if(Info.contains(line+","+new_direction+","+station_id)){val StationInfo = Info(line+","+new_direction+","+station_id).split(",")
        stationName = StationInfo(0)
        index =  StationInfo(1).toInt
        lat = StationInfo(2).toDouble
        lon = StationInfo(3).toDouble}
      BusStationGPS(new_car_id,new_deal_time,line,new_direction,devide,station_id,stationName,index,lon,lat)
    }).filter(gps => !(gps.car_id.isEmpty||gps.time=="0"||gps.direction.isEmpty||gps.devide.isEmpty||gps.station_id.isEmpty||gps.station_name.isEmpty||gps.index== -1||gps.lon == 0.0||gps.lat == 0.0))
  }


  /**
    *通过GPS信息和拍卡信息，确定拍卡记录的拍卡站点
    */
  def CalBusO(deal:RDD[String],position1:String,StationGPS:RDD[String],position2:String):RDD[BusO] ={
    val busDeal = CalBusDeal(deal,position1)
    val busGPS = CalBusStationGPS(StationGPS,position2)
    val grpDeal = busDeal.groupBy(_.car_id)
    val grpGPS = busGPS.groupBy(_.car_id)
    val joined = grpDeal.join(grpGPS)
    joined.flatMap(x=>{
      val outSet = scala.collection.mutable.HashSet[BusO]()
      val it_deal = x._2._1.iterator
      val it_GPS = x._2._2.iterator
      while (it_deal.hasNext){
        var varTimeDiff:Long = 30*60
        var get_stationInfo:BusStationGPS = null
        val temp = it_deal.next()
        while (it_GPS.hasNext){
          val tempGps = it_GPS.next()
          val timediff = Math.abs(timeDiff(temp.deal_time,tempGps.time))
          if(timediff < varTimeDiff){
            get_stationInfo = tempGps
            varTimeDiff = timediff
          }
        }
        if(get_stationInfo != null){
          val out = BusO(temp.card_id,temp.deal_time,get_stationInfo.line,get_stationInfo.car_id,get_stationInfo.direction,get_stationInfo.devide,get_stationInfo.station_id,
            get_stationInfo.station_name,get_stationInfo.index,get_stationInfo.lon,get_stationInfo.lat,get_stationInfo.time,varTimeDiff)
          outSet.add(out)}
      }
      outSet
    }).distinct()
  }
}


object BusClean{
  def apply(): BusClean = new BusClean()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val deal = sc.textFile("G:\\数据\\BusO\\20170612")
    val GPS = sc.textFile("G:\\数据\\BusO\\ARRLEA_Q_2017-06-12,G:\\数据\\BusO\\ARRLEA_Q_2017-06-13")
    //  val DealRDD =  BusClean().CalBusStationGPS(GPS,"1,2,3,5,4,10,8").foreach(println)//.coalesce(1).saveAsTextFile("G:\\数据\\BusO\\buyaode\\GPS")
    /*val orginDealRDD = deal.count()
    val DealRDD = BusClean().CalBusDeal(deal,"1,7,4").count()
    println("origin:"+orginDealRDD+","+"get:"+DealRDD)
    println(orginDealRDD-DealRDD)
    val GPSRDD =  BusClean().CalBusStationGPS(GPS,"1,2,3,5,4,10,8").count()
    val orginGPSRDD = GPS.count()
    println("origin:"+orginGPSRDD+","+"get:"+GPSRDD)
    println(orginGPSRDD-GPSRDD)*/
    //BusClean().CalBusO(deal,"1,7,4",GPS,"1,2,3,5,4,10,8").coalesce(1).saveAsTextFile("G:\\数据\\BusO\\output\\BusO20170612_3")
    //car_id,arrive_time,leave_time,line,direction,devide,station_id
    val DealRDD = BusClean().CalBusDeal(deal,"1,7,4")
    val grpDeal = DealRDD.groupBy(_.car_id)
    val DealRDD_o = DealRDD.count()
    val grpDeal_count = grpDeal.count()

    val GPSRDD =  BusClean().CalBusStationGPS(GPS,"1,2,3,5,4,10,8")
    val grpGPS = GPSRDD.groupBy(_.car_id)
    val GPSRDD_o = GPSRDD.count()
    val grpGPS_count = grpGPS.count()
    val joined = grpDeal.join(grpGPS).count()
    println("交易数据原始："+DealRDD_o+","+grpDeal_count+"GPS数据原始："+GPSRDD_o+","+grpGPS_count+"Join数量："+joined)
  }
  case class BusDeal(card_id:String,car_id:String,deal_time:String)
  case class BusStationGPS(car_id:String,time:String,line:String,direction:String,devide:String,station_id:String,station_name:String,index:Int,lon:Double,lat:Double)
  case class BusO(card_id:String,time:String,line:String,car_id:String,direction:String,devide:String,station_id:String,station_name:String,index:Int
                  ,lon:Double,lat:Double,station_time:String,timediff:Long){
    override def toString: String = Array(card_id,time,line,car_id,direction,devide,station_id,station_name,index.toString,
      lon.toString,lat.toString,station_time,timediff.toString).mkString(",")
  }
}
