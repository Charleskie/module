import java.text.SimpleDateFormat
import java.util.Calendar

import cn.sibat.homeAndwork.MarkHomeWork.sample
import org.apache.spark.sql.{SQLContext, SparkSession}
import cn.sibat.homeAndwork.OutHomeWork.{OutHome, OutWork}

object test{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sqlContext = new SQLContext(sc)
    import sparkSession.implicits._
    val path = "E:\\Portable\\sibat\\spark\\module\\testdata\\"
    val month = "20171103"
    import sparkSession.implicits._
    val homecal = sc.textFile(path + "outdata\\homecal\\"+month+"\\*")
    val workcal = sc.textFile(path + "outdata\\workcal\\"+month+"\\*")
//    OutHome(sparkSession,homecal).saveAsTextFile(path + "\\outdata\\outhome\\"+"201711")
////    OutWork(sparkSession,workcal).saveAsTextFile(path + "\\outdata\\outwork\\"+"201711")
//    val data = sc.textFile("E:\\Portable\\sibat\\spark\\module\\testdata\\SubwayOD\\20171101").map(_.split(",")).map(s => sample(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(8).toDouble, s(9).toDouble)).toDF().registerTempTable("dataDF")
//    val datadf = sqlContext.sql("select * from (select * from dataDF) dataDF  group by card_id").show(2)
    val str = homecal.map(_.split(",")).filter(s => s(3).isEmpty).map(_.mkString(","))
    print(BusO.UnixToISO("1465019731"))
    val datafor = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val time = "2018_03_26_18_08_37"
    val newfor = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S")
    val cal = Calendar.getInstance();
    cal.setTime(newfor.parse(newfor.format(datafor.parse(time))))
    print(newfor.format(datafor.parse(time)))
    println(Integer.parseInt(new java.text.DecimalFormat("0").format(0.69)))
//    println(datafor.format(time))

    val arr = Array(4,4,10)
    println(arr.sum/arr.size)
  }
  def fs(s:String,d:String)={
    if(s.length>d.length){
      s.length
    }
  }
}