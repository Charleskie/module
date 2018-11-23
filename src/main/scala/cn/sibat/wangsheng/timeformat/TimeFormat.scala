package cn.sibat.wangsheng.timeformat

import java.text.SimpleDateFormat

object TimeFormat{
  private val TIME = "# TimeFormat Trans ERROR # "
  val ISOFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  /***
    *
    * @param time
    * @param format 原本的格式
    * @return
    */
  def string2timeString(time: String, format: String) :String=  {
    val foreFormat = new SimpleDateFormat(format)
    try{
      ISOFormat.format(foreFormat.parse(time))
    }catch {
      case e: Exception =>{
        val date = "1979-01-01T08:08:08.000Z"
//        ISOFormat.format(foreFormat.parse(date))
        println(TIME+e.getMessage)
        date
      }
    }
  }

  /***
    * @param time "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"时间格式
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
  /** *
    * 计算时间差，通过定义输出格式来输出时间差格式
    * second对应秒为单位 minute对应分钟为单位 hour对应小时为单位
    * @param timeO
    * @param timeD
    * @param format
    * @return
    */
  def timediff(timeO: String, timeD: String, format: String): Int = {
    if(timeO==""||timeO.isEmpty||timeD==""||timeD.isEmpty){
      return 1000000000
    }
    var mark = 0
    format match {
      case "second" => mark = 1000
      case "minute" => mark = 1000 * 60
      case "hour" => mark = 1000 * 60 * 60
    }
    try{
      ((ISOFormat.parse(timeD).getTime - ISOFormat.parse(timeO).getTime) / mark).toInt
    }catch {
      case e:Exception => return 1000000000
    }
  }
}