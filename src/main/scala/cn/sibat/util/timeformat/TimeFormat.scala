package cn.sibat.util.timeformat

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

object TimeFormat{
  private val TIME = "# TimeFormat Trans ERROR # "
  private val ISOFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  /***
    *
    * @param time
    * @param format 原本的格式
    * @return
    */
  def StringToISO(time: String,format: String) :String=  {
    val foreFormat = new SimpleDateFormat(format,java.util.Locale.ENGLISH)
    try{
      ISOFormat.format(foreFormat.parse(time))
    }catch {
      case e: ParseException =>{
        val date = "1979-01-01T08:08:08.000Z"
        println(TIME+time)
        e.printStackTrace()
        date
      }
    }
  }

  /***
    *
    * @param time
    * @param format 原本的格式
    * @return
    */
  def StringToISO(time: String, format: SimpleDateFormat) :String=  {
    val ts = format.parse("DEC 11 2018 12:12:21:000AM");
    try{
            ISOFormat.format(format.parse(time))
    }catch {
      case e: ParseException =>{
        val date = "1979-01-01T08:08:08.000Z"
        println(TIME+time)
        e.printStackTrace()
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

  /***
    * 获取当天后的第int天日期,int可以是正数，也可以是负数，返回日期格式由format定义
    * @param int
    */
  def getDate(int: Int,format: String): String ={
    val newFormat = new SimpleDateFormat(format)
    val dateNow = new Date()
    val calender = Calendar.getInstance()
    calender.setTime(dateNow)
    calender.add(Calendar.DAY_OF_MONTH,int)
    val day = newFormat.format(calender.getTime).substring(0,10)
    day
  }

  /***
    * 判断当前日期是否是周末，如果是就返回weekend，不是则返回workday，日期错误则返回DATE ERROR
    * @param date
    * @param format
    * @return
    */
  def isWeekend(date:String, format:String):String={
    val newFormat = new SimpleDateFormat(format)
    val dateNow = new Date()
    val calender = Calendar.getInstance()
    try{
      calender.setTime(newFormat.parse(date))
      if(calender.get(Calendar.DAY_OF_WEEK)==Calendar.SATURDAY||calender.get(Calendar.DAY_OF_WEEK)==Calendar.SUNDAY){
        return "weekend"
      }else{
        return "workday"
      }
    }catch {
      case e:Exception => return "DATE ERROR"
    }
  }

  /***
    * 判断当前日期是否是周末，如果是就返回weekend，不是则返回workday，日期错误则返回DATE ERROR
    * @param date
    * @param format
    * @return
    */
  def isSundayorSaturday(date:String, format:String):String={
    val newFormat = new SimpleDateFormat(format)
    val dateNow = new Date()
    val calender = Calendar.getInstance()
    try{
      calender.setTime(newFormat.parse(date))
      if(calender.get(Calendar.DAY_OF_WEEK)==Calendar.MONDAY)
        return "monday"
      if(calender.get(Calendar.DAY_OF_WEEK)==Calendar.SUNDAY){
        return "sunday"
      }else{
        return "workday"
      }
    }catch {
      case e:Exception =>
        println(date)
        return "DATE ERROR"
    }
  }
}