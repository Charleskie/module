import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer

object calTOSDiff{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    val inpath = "E:\\Portable\\sibat\\2018\\断面客流\\"
    //260011,赤湾,260,地铁二号线,22.47947413023911,113.89872986771447   --孖岭
    val stationBM = sc.textFile(inpath+"stationZDBM").map(_.split(",")).map(s => {
      val station_id = s(0)
      (station_id,s(1))
    }).toDF("station_id","station_name")
    //1241017000,1268028000,11.83614836,74,63,01-一月-2018 00:00:00,01-一月-2018 00:00:00,C4040020180101.dat,241,0
    val qingfendata = sc.textFile(inpath+"qingfen201801.txt").map(_.replaceAll("\"","").split(",")).filter(s => s(0).substring(0,1) == "1")
      .map(s => qingfen(s(0),s(1),s(2).toDouble,timeslice(s(4).toInt*15),"2018-01-"+s(6).substring(0,2)))
      .map(s => sample(s.station_fore,s.station_back,s.flow.toLong,CustomToISO(s.deal_time+" "+s.deal_timeslice))).map(s => {
      var flow = s.flow
      if(flow<0){
        flow = 0
      }
      sample(s.station_fore,s.station_back,flow,s.deal_time)
    }).sortBy(_.deal_time)
      .filter(s=> s.deal_time.substring(8,10).toInt<9&&s.deal_time.substring(8,10).toInt>1).groupBy(s => (s.station_fore,s.station_back,s.deal_time))
      .map(s => ClusterSameTime(s))
      .map(s => daybyday(s.station_fore,s.station_back,s.flow,s.deal_time.substring(0,10),s.deal_time.substring(11,19)))
      .filter(s => s.station_fore == "1268019000" && s.station_back == "1268008000")//过滤出老街到大剧院的断面，抽样
      .toDF("station_fore","station_back","flow","day","time")
      .groupBy("station_fore","station_back","time").avg("flow")
      .repartition(1).write.csv(inpath+"output\\sample\\北站-白石龙\\qingfentimeavg")


    //"爱联","吉祥","94","2018-01-01 00:00:12","261126","2611"   --子岭
    val tosdata = sc.textFile(inpath+"subway201801.txt").map(_.replaceAll("\"","").replaceAll("子岭","孖岭").split(",")).map(s => tos(s(0),s(1),s(2).toLong,s(3)))
        .toDF("station_name","station_back","flow","deal_time").join(stationBM,"station_name").toDF("station_fore","station_name","flow","deal_time","station_fore_id")
        .join(stationBM,"station_name").map(s => s(4)+","+s(5)+","+s(2)+","+s(3)).map(_.split(",")).map(s => sample(s(0),s(1),s(2).toLong,CustomToISO(s(3)))).rdd
        .groupBy(s => (s.station_fore,s.station_back)).map(s=> cleandata(s)).flatMap(s=>s).map(s => sample(s.station_fore,s.station_back,s.flow,changetime(s.deal_time,15)))
        .filter(s=> s.deal_time.substring(8,10).toInt<11&&s.deal_time.substring(8,10).toInt>1)//过滤一周
        .groupBy(s=>(s.station_fore,s.station_back,s.deal_time)).map(s => ClusterSameTime(s))
        .map(s => daybyday(s.station_fore,s.station_back,s.flow,s.deal_time.substring(0,10),s.deal_time.substring(11,19)))
        .filter(s => s.station_fore == "1268019000" && s.station_back == "1268008000")//过滤出老街到大剧院的断面，抽样
        .toDF("station_fore","station_back","flow","day","time")
        .groupBy("station_fore","station_back","time").avg("flow")//计算每个断面的日均客流
        .repartition(1).write.csv(inpath+"output\\sample\\北站-白石龙\\tostimeavg")

  }
  def timeslice(time:Int)={
    val HH = time/60 + 4
    val mm = time-(HH-4)*60
    val ss = "00"
    HH.toString+":"+mm.toString+":"+ss.toString
  }
  def CustomToISO(time:String)={
    val orignalFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = orignalFormat.parse(time)
    val ISOformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    ISOformat.format(date)
  }
  /***
    * @param time ISO时间格式
    * @param num  以num分钟间隔
    * @return 返回按num分钟间隔之后的时间
    */
  def changetime(time: String, num: Int): String = {
    val minToNum = time.substring(14, 16).toInt
    val dev: Int = minToNum / num
    val min: Int = dev * num
    var minToString = ""
    if (min < 10) {
      minToString = "0" + min.toString
    } else {
      minToString = min.toString
    }
    val changeTime = time.substring(0, 14) + minToString + ":00"
    changeTime
  }

  /***
    * 数据去重，时间连续但是客流量不变
    * @param data
    * @return
    */
  def cleandata(data:((String,String),Iterable[sample])):Array[sample]={
    val nt = ArrayBuffer[sample]()
    val t2 = data._2.toArray.sortBy(_.deal_time)
    var temp = t2(0)
    nt+=t2(0)
    for (i<- 0 until t2.length){
      if (temp.flow != t2(i).flow){
        temp = t2(i)
        nt+=temp
      }
    }
    nt.toArray
  }
  def ClusterSameTime(data:((String,String,String),Iterable[sample])) = {
    var count = data._2.map(s => s.flow).sum
    sample(data._1._1,data._1._2,count,data._1._3)
  }
  case class qingfen(station_fore:String,station_back:String,flow:Double, deal_timeslice:String,deal_time:String){
    override def toString = station_fore+","+station_back+","+flow+","+deal_timeslice+","+deal_time
  }
  case class tos(station_fore:String,station_back:String,flow:Long,deal_time:String){
    override def toString = station_fore+","+station_back+","+flow+","+deal_time
  }
  case class sample(station_fore:String,station_back:String,flow:Long,deal_time:String){
    override def toString = station_fore+","+station_back+","+flow+","+deal_time
  }
  case class daybyday(station_fore:String,station_back:String,flow:Long,day:String,time:String){
    override def toString = station_fore+","+station_back+","+flow+","+day+","+time
  }
}