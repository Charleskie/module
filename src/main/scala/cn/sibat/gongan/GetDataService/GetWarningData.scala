package cn.sibat.gongan.GetDataService

import java.util.Properties
import cn.sibat.gongan.Constant.DataBaseConstant._
import org.apache.spark.sql.{DataFrame, SparkSession}

object GetWarningData{
  /***
    * 读取派出所数据
    * @param sparkSession
    * @param path
    * @return
    */
  def getPoliceStation(sparkSession: SparkSession,path:String): DataFrame ={
    sparkSession.sqlContext.read.csv(path).toDF("id","police_station","depart_id","station_name")
      .select("police_station","station_name").distinct()
  }


  /***
    * 读取early_warning预警和examination撤控数据
    * @param sparkSession
    * @return
    */
  def getWarningAndExam(sparkSession: SparkSession): (DataFrame,DataFrame) ={
    val properties = new Properties()
    properties.put("user",POSTGRESUSER)
    properties.put("password",POSTGRESPASSWORD)
    properties.put("driver",POSTGRESDRIVER)
    val early_warning = sparkSession.sqlContext.read.jdbc("jdbc:postgresql://"+EARLYWARNINGIP+":"+POSTGRESPORT+"/"+POLICETRAFFICDB,
      EARLYWRNINGTABLE,properties)
    val examination = sparkSession.sqlContext.read.jdbc("jdbc:postgresql://"+EARLYWARNINGIP+":"+POSTGRESPORT+"/"+POLICETRAFFICDB,
      EXAMINATIONDB,properties)
    (early_warning,examination)
  }

  /***
    * 读取挂网时间数据
    * @param sparkSession
    * @return
    */
  def getPerson_base(sparkSession: SparkSession):DataFrame={
    val properties = new Properties()
    properties.put("user",POSTGRESUSER)
    properties.put("password",POSTGRESPASSWORD)
    properties.put("driver",POSTGRESDRIVER)
    sparkSession.sqlContext.read.jdbc("jdbc:postgresql://"+GJFJCOPYIP+":"+POSTGRESPORT+"/"+GJFJCOPYDB,
      KEYPERSONBASEDB,properties)
  }
}