package cn.sibat.gongan

import cn.sibat.gongan.GetDataService.GetWarningData._
import cn.sibat.gongan.Constant.CaseConstant._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object daily{
  private val path = "C:\\Users\\小怪兽\\Desktop\\Kim1023\\data\\"
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext

    val police_station = getPoliceStation(sparkSession,path+"police_station.csv")
    import sparkSession.sqlContext.implicits._
    val early_warning = sc.textFile(path+"early_warning1112.txt")
      .filter(s => s.split(",").length==22)
      .map(line =>{
      val s = line.split(",")
      warning(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13),s(14),s(15),s(16),s(17),
        s(18),s(19),s(20),s(21),s(22),s(23),s(24),s(25))
    }).toDF().select("id","keyperson_type","keyperson_id", "event_address_name","create_time","pid","similarity")
//      .filter(col("create_time").substr(0,4)==="2018")

    val keypersonbase = sc.textFile(path+"keyperson_base.txt").filter(s => s.split(",").length==66)
      .map(line =>{
        val s = line.split(",")
        keyperson_base(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13),s(14),s(15),s(16),
          s(17),s(18),s(19),s(20),s(21),s(22),s(23),s(24),s(25),s(26),s(27),s(28),s(29),s(30),s(31),s(32),s(33),s(34),
          s(35),s(36),s(37),s(38),s(39),s(40),s(41),s(42),s(43),s(44),s(45),s(46),s(47),s(48),s(49),s(50),s(51),s(52),
          s(53),s(54),s(55),s(56),s(57),s(58),s(59),s(60),s(61),s(62),s(63),s(64),s(65))
      }).toDF().select("NAME","id_number_18","id","create_time")

    val examinationData = sc.textFile(path+"sy_early_warning_examination_approval.txt")
      .filter(s => s.split(",").length==14)
      .map(line =>{
        val s = line.split(",")
        examination(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13))
      }).toDF().select("early_warning_id","examination_approval_type","avaliable")


  }


}
