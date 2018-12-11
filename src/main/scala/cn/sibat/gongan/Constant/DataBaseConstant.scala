package cn.sibat.gongan.Constant

object DataBaseConstant{

  val POSTGRESUSER = "postgres"
  val POSTGRESPASSWORD = "postgres"
  val POSTGRESPORT = "5432"
  val POSTGRESDRIVER = "org.postgresql.Driver"

  /***
    * early_warning数据库的相关信息，IP地址，数据库，数据表
    */
  val EARLYWARNINGIP = "190.176.35.210"           //early_warning数据库IP地址
  val POLICETRAFFICDB = "police_traffic"        //police_traffic数据库
  val EARLYWRNINGTABLE = "early_warning"        //early_warning表
  val EXAMINATIONDB = "sy_early_warning_examination_approval"   //是否撤空数据表


  /****
    * keyperson_base数据的相关信息，IP地址，数据库，数据表
    */
  val KEYPERSONBASEDB = "keyperson_base"
  val GJFJCOPYIP = "190.176.35.169"       //gjfj_copy数据库IP地址
  val GJFJCOPYDB = "gjfj_copy"            //gjfj_copy数据库

}