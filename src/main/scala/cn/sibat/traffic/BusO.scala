package cn.sibat.traffic

import java.io.IOException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BusO{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext

  }
  def getBusO(sparkSession: SparkSession,cleanbus: RDD[String],arrivedata:RDD[String])={
    cleanbus.map(_.split(",")).map(s => (s(1),Carid_Parser(s(7)),s(4),1))
    arrivedata.map(_.split(",")).map(s =>{
      var time = ""
      if(s(2)==null){
        time = UnixToISO(s(3))
      }else if(s(2)!=null){
        time = UnixToISO(s(2))
      }else{
        null
      }
      s(1)+","+time+s(5)+","+s(4)+","+s(10)+","+s(8)+","+s(9)
    }).map(_.split(",")).map(s => arrive(s(0),s(1),s(2),DirecToNum(s(3)),s(4),s(5),s(6))).filter(s => s.line != null && s.station_id != null )
  }

  /***
    *
    * @param timestampString
    * @return
    */
  def UnixToISO(timestampString:String):String={
    val timestamp:Long = timestampString.toLong*1000+28800000;
    val date = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(new java.util.Date(timestamp));
    date
  }

  def DirecToNum(string: String):String={
    var direc = ""
    if(string=="up"){
      direc = "1"
    }else if(string == "down"){
      direc = "2"
    }else{
      null
    }
    direc
  }


  /***
    *
    * @param input
    * @return
    */
  def Carid_Parser(input: String): String = {
    if (input == null || input.size == 0) return null
    try {
      var val2 = ""
      var car_id = ""
      val real_car_id = "粤B"
      if (input != null) {
        val2 = input.replaceAll(" ", "")
        val len = val2.length
        if (len >= 6 && val2.substring(len - 1, len) == "D") {
          car_id = val2.substring(len - 6, len)
          real_car_id + car_id
        }
        else if (len >= 5 && !(val2.substring(len - 1, len) == "D")) {
          car_id = val2.substring(len - 5, len)
          real_car_id + car_id
        }
        else input
      }
      else input
    } catch {
      case e: Exception =>
        throw new IOException(e.getMessage)
    }
  }
  case class arrive(car_id:String,time:String,line:String,direction:String,devide:String,station_id:String,station_index:String){
    override def toString = car_id+","+time+","+line+","+direction+","+devide+","+station_id+","+station_index
  }
}