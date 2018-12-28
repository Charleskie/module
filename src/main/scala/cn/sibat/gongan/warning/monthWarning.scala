
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import java.util.Properties
import java.util.Date
import scala.collection.mutable.ArrayBuffer

object monthWarning {

  val POSTGRESUSER = "postgres"
  val POSTGRESPASSWORD = "postgres"
  val POSTGRESPORT = "5432"
  val POSTGRESDRIVER = "org.postgresql.Driver"
  /** *
    * early_warning数据库的相关信息，IP地址，数据库，数据表
    */
  val EARLYWARNINGIP = "190.176.35.210" //early_warning数据库IP地址
  val POLICETRAFFICDB = "police_traffic" //police_traffic数据库
  val EARLYWRNINGTABLE = "early_warning" //early_warning表
  val EXAMINATIONDB = "sy_early_warning_examination_approval" //是否撤空数据表
  /** **
    * keyperson_base数据的相关信息，IP地址，数据库，数据表
    */
  val KEYPERSONBASEDB = "keyperson_base"
  val GJFJCOPYIP = "190.176.35.169" //gjfj_copy数据库IP地址
  val GJFJCOPYDB = "gjfj_copy" //gjfj_copy数据库
  /** *
    * 编写SparkDataFrame的UDF
    * 将感知门的stime转成标准时间格式
    */
  val timeParse = udf((s: String) => {
    val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    newFormat.format(newFormat.parse(s))
  })
  val UnixParse = udf((s: String) => {
    val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    newFormat.format(new Date(s.toLong * 1000))
  })
  /** *
    * 字符串时间转成Unix时间戳
    */
  val timeToUnix = udf((s: String) => {
    try {
      val oldFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      oldFormat.parse(s).getTime / 1000
    } catch {
      case e: Exception => {
        val oldFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.S")
        oldFormat.parse(s).getTime / 1000
      }
    }
  })
  /** *
    * 时间格式必须是Unix格式戳
    */
  val timediff = udf((s1: String, s2: String) => {
    s2.toLong - s1.toLong
  })

  def timeParseString(s: String) = {
    val oldFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    newFormat.format(oldFormat.parse(s))
  }

  /** *
    * 读取派出所数据
    *
    * @param sparkSession
    * @param path
    * @return
    */
  def getPoliceStation(sparkSession: SparkSession, path: String): DataFrame = {
    sparkSession.sqlContext.read.csv(path).toDF("id", "police_station", "depart_id", "station_name")
      .select("police_station", "station_name").distinct()
  }

  /** *
    * 读取early_warning预警和examination撤控数据
    *
    * @param sparkSession
    * @return
    */
  def getWarningAndExam(sparkSession: SparkSession): (DataFrame, DataFrame) = {
    val properties = new Properties()
    properties.put("user", POSTGRESUSER)
    properties.put("password", POSTGRESPASSWORD)
    properties.put("driver", POSTGRESDRIVER)
    val early_warning = sparkSession.sqlContext.read.jdbc("jdbc:postgresql://" + EARLYWARNINGIP + ":" + POSTGRESPORT + "/" + POLICETRAFFICDB,
      EARLYWRNINGTABLE, properties)
    val examination = sparkSession.sqlContext.read.jdbc("jdbc:postgresql://" + EARLYWARNINGIP + ":" + POSTGRESPORT + "/" + POLICETRAFFICDB,
      EXAMINATIONDB, properties)
    (early_warning, examination)
  }

  /** *
    * 读取挂网时间数据
    *
    * @param sparkSession
    * @return
    */
  def getPerson_base(sparkSession: SparkSession): DataFrame = {
    val properties = new Properties()
    properties.put("user", POSTGRESUSER)
    properties.put("password", POSTGRESPASSWORD)
    properties.put("driver", POSTGRESDRIVER)
    sparkSession.sqlContext.read.jdbc("jdbc:postgresql://" + GJFJCOPYIP + ":" + POSTGRESPORT + "/" + GJFJCOPYDB,
      KEYPERSONBASEDB, properties)
  }

  /** *
    * 计算派出所预警数据
    *
    * @param office_data
    * @param warning_data
    * @return
    */
  def calOfficeCount(office_data: DataFrame, warning_data: DataFrame): DataFrame = {
    val office = office_data.toDF("police_station", "event_address_name")
    warning_data.select("keyperson_id", "event_address_name", "create_time")
      .withColumn("warning_date", timeParse(col("create_time")).substr(0, 10))
      .join(office, "event_address_name")
      .select("warning_date", "police_station", "keyperson_id", "event_address_name")
  }

  /** *
    * 计算挂网时间
    *
    * @param warning_data   预警数据
    * @param keyperson_base 布控时间
    * @param examination    抓捕时间
    * @return
    */
  def calCatchTimeDiff(warning_data: DataFrame, keyperson_base: DataFrame, examination: DataFrame): DataFrame = {
    warning_data.join(examination, col("id") === col("early_warning_id"))
      .filter(col("examination_approval_type") === "已撤控" && col("avaliable") === "1")
      .select("keyperson_id", "update_time", "pid").distinct()
      .join(keyperson_base, col("pid") === col("id"))
      .select("keyperson_id", "create_time", "update_time")
      .withColumn("arrest_time_diff", timediff(timeToUnix(col("create_time"))
        , col("update_time")))
      .withColumn("warning_date", UnixParse(col("update_time")).substr(1, 10))
      .select("warning_date", "keyperson_id", "arrest_time_diff")
  }
  def main(args: Array[String]): Unit = {
    //TODO finish the month report code rewrite
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val beginDay = "2018-07-29"
    val path = "Kim/data/"
    val newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateNow = new Date()
    val calender = Calendar.getInstance()
    calender.setTime(dateNow)
    calender.add(Calendar.DAY_OF_MONTH, -1)
    val day = newFormat.format(calender.getTime).substring(0, 10)
    val early_warningAll = getWarningAndExam(sparkSession)._1.select("id", "keyperson_type", "keyperson_id", "event_address_name", "create_time", "pid", "similarity").withColumn("warning_date", timeParse(col("create_time")).substr(0, 10)).filter(col("warning_date") >= beginDay).filter(col("warning_date") <= day)
    val police_station = getPoliceStation(sparkSession, path)
    val early_warning = early_warningAll.filter(col("warning_date") === day)
    val dataAll = new ArrayBuffer[String]()
    dataAll.append("#-----总量统计-------#")
    dataAll.append("预警总量：" + early_warningAll.count() + "," + "预警总人数：" + early_warningAll.select("keyperson_id").distinct().count())
    println("##----总量计算完毕----##")
    dataAll.append("日期，预警量，预警人数")
    val dayCount = early_warning.select("warning_date", "keyperson_id").rdd.map(s => (s.getString(0), s.getString(1))).groupBy(s => s._1).map(s => {
      val warning_date = s._1
      val cnt = s._2.size
      val cntdisc = s._2.map(_._2).toArray.distinct.length
      warning_date + "," + cnt + "," + cntdisc
    }).collect().foreach(s => dataAll.append(s))
    dataAll.append("#-----分派出所统计-------#")
    dataAll.append("日期，派出所，预警量，预警人数，累计预警量，累计预警人数")
    val officecnt = calOfficeCount(police_station, early_warningAll)
    val officecntdisc = officecnt.select("warning_date", "police_station", "keyperson_id", "event_address_name").filter(col("warning_date") === day).rdd.map(s => (s.getString(0), s.getString(1), s.getString(2), s.getString(3))).groupBy(s => (s._1, s._2)).map(s => {
      val warning_date = s._1._1
      val police_station = s._1._2
      val cnt = s._2.size
      val cntdisc = s._2.toArray.map(s => s._3).distinct.length
      (police_station, warning_date + "," + cnt + "," + cntdisc)
    })
    officecnt.rdd.map(s => (s.getString(0), s.getString(1), s.getString(2), s.getString(3))).groupBy(s => s._2).map(s => {
      val police_station = s._1
      val cnt = s._2.size
      val cntdisc = s._2.toArray.map(s => s._3).distinct.length
      (police_station, cnt + "," + cntdisc)
    }).leftOuterJoin(officecntdisc).map(s => {
      val warning_date = s._2._2.map(s => s.split(","){0}).mkString("")
      val police_station = s._1
      val todayCnt = s._2._2.map(s => s.split(","){1}).mkString("")
      val todayPerson = s._2._2.map(s => s.split(","){2}).mkString("")
      val allCnt = s._2._1.split(","){0}
      val allPerson = s._2._1.split(","){1}
      warning_date + "," + police_station + "," + todayCnt + "," + todayPerson + "," + allCnt + "," + allPerson
    }).collect().foreach(s => dataAll.append(s))
    println("##----分派出所计算完毕----##")
    dataAll.append("#-----分类型统计-------#")
    dataAll.append("日期，人员类型，预警量，预警人数，累计预警量，累计预警人数")
    val allType = early_warningAll.select("warning_date", "keyperson_id", "keyperson_type").rdd.map(s => (s.getString(0), s.getString(1), s.getString(2))).groupBy(s => s._3).map(s => {
      val keyperson_type = s._1
      val cnt = s._2.size
      val cntdisc = s._2.map(s => s._2).toArray.distinct.length
      (keyperson_type, cnt + "," + cntdisc)
    })
    early_warning.select("warning_date", "keyperson_id", "keyperson_type").rdd.map(s => (s.getString(0), s.getString(1), s.getString(2))).groupBy(s => (s._1, s._3)).map(s => {
      val warning_date = s._1._1
      val keyperson_type = s._1._2
      val cnt = s._2.size
      val cntdisc = s._2.map(s => s._2).toArray.distinct.length
      (keyperson_type, warning_date + "," + cnt + "," + cntdisc)
    }).rightOuterJoin(allType).map(s => {
      val warning_date = s._2._1.map(s => s.split(","){0}).mkString("")
      val keyperson_type = s._1
      val todayCnt = s._2._1.map(s => s.split(","){1}).mkString("")
      val todayPerson = s._2._1.map(s => s.split(","){2}).mkString("")
      val allCnt = s._2._2.split(","){0}
      val allPerson = s._2._2.split(","){1}
      warning_date + "," + keyperson_type + "," + todayCnt + "," + todayPerson + "," + allCnt + "," + allPerson
    }).collect().foreach(s => dataAll.append(s))
    println("##----分类型计算完毕----##")
    dataAll.append("#-----分站点统计-------#")
    dataAll.append("日期，站点，预警量，预警人数,累计预警量，累计预警人数")
    val stationAll = early_warningAll.select("event_address_name", "keyperson_id").rdd.map(s => (s.getString(0), s.getString(1))).groupBy(s => s._1).map(s => {
      val event_address_name = s._1
      val cnt = s._2.size
      val cntdisc = s._2.map(s => s._2).toArray.distinct.size
      (event_address_name, cnt + "," + cntdisc)
    })
    early_warning.select("event_address_name", "warning_date", "keyperson_id").rdd.map(s => (s.getString(0), s.getString(1), s.getString(2))).groupBy(s => (s._1, s._2)).map(s => {
      val warning_date = s._1._2
      val station_name = s._1._1
      val cnt = s._2.size
      val cntdist = s._2.map(s => s._3).toArray.distinct.length
      (station_name, warning_date + "," + cnt + "," + cntdist)
    }).join(stationAll).map(s => {
      val warning_date = s._2._1.split(","){0}
      val event_address_name = s._1
      val todayCnt = s._2._1.split(","){1}
      val todayPerson = s._2._1.split(","){2}
      val allCnt = s._2._2.split(","){0}
      val allPerson = s._2._2.split(","){1}
      warning_date + "," + event_address_name + "," + todayCnt + "," + todayPerson + "," + allCnt + "," + allPerson
    }).collect().foreach(s => dataAll.append(s))
    println("##----全部计算完毕----##")
    sc.parallelize(dataAll).coalesce(1).saveAsTextFile(path + "out/" + day)
  }
}