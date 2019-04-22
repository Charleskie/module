package cn.sibat.paper

import java.util

import org.apache.spark.sql.SparkSession
import cn.sibat.util.timeformat.TimeFormat.isWeekend

import scala.collection.mutable.ArrayBuffer

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
      }).sortBy(_._1).map(_._2).coalesce(1)
//      .saveAsTextFile(outpath+"monthdata")
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

    val months = new ArrayBuffer[String]()
    for(a <- 3 to 12
      if a != 4 ;if a !=6){
      months.append(int2String(a))
    }
    val days = new ArrayBuffer[String]()
    for(a <- months ){
      if(a.equals("03")){
        for(i <- 5 to 25){
          days.append("2018-"+a+"-"+int2String(i))
        }
      }else if(a.equals("05")){
        for(i <- 7 to 27){
          days.append("2018-"+a+"-"+int2String(i))
        }
      } else if(a.equals("07")){
        for(i <- 2 to 31){
          days.append("2018-"+a+"-"+int2String(i))
        }
      }else if(a.equals("08")){
        for(i <- 1 to 31){
          days.append("2018-"+a+"-"+int2String(i))
        }
      }else if(a.equals("09")){
        for(i <- 1 to 16){
          days.append("2018-"+a+"-"+int2String(i))
        }
      }else if(a.equals("10")){
        for(i <- 8 to 31){
          days.append("2018-"+a+"-"+int2String(i))
        }
      }else if(a.equals("11")){
        for(i <- 1 to 30){
          days.append("2018-"+a+"-"+int2String(i))
        }
      }else if(a.equals("12")){
        for(i <- 1 to 23){
          days.append("2018-"+a+"-"+int2String(i))
        }
      }
    }
    val workdaydata = sc.textFile(path+"2018*").filter(s => s.split(","){0}.substring(11,19)>"07:00:00")
      .filter(s => s.split(","){0}.substring(11,19)<="23:00:00")
      .filter(s => s.split(","){1}=="世界之窗")
      .map(s =>{
        val line = s.split(",")
        val time = line(0)
        val station_name = line(1)
        val flow = line(2)
        val weekField = isWeekend(time,"yyyy-MM-dd HH:mm:ss")
        val day = time.substring(0,10)
        (time,flow,weekField,day)
      }).filter(_._3=="weekend").filter(s => days.contains(s._4))
//      .sortBy(_._1).coalesce(1).saveAsTextFile(outpath+"festival")
    import sparkSession.implicits._
//    workdaydata.map(_._4).distinct().sortBy(s => s).toDF().show(200)

    val festivaldata = sc.textFile(path+"20190101").filter(s => s.split(","){0}.substring(11,19)>"07:00:00")
      .filter(s => s.split(","){0}.substring(11,19)<="23:00:00")
      .filter(s => s.split(","){0}.substring(0,10)=="2019-01-01")
      .filter(s => s.split(","){1}=="世界之窗")
      .map(s =>{
        val line = s.split(",")
        val time = line(0)
        val station_name = line(1)
        val flow = line(2)
        val weekField = isWeekend(time,"yyyy-MM-dd HH:mm:ss")
        val day = time.substring(0,10)
        (time,flow,weekField,day)
      })
      .sortBy(_._1).coalesce(1).saveAsTextFile(outpath+"festival")
  }

  /***
    * 将int类型整数转化成字符串，个位数前补0
    * @param num
    * @return
    */
  def int2String(num:Int): String ={
    var string = ""
    if(num<10) {
      string = "0"+num.toString
    }else{
      string = num.toString
    }
    string
  }
}