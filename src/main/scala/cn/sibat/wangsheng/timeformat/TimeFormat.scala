package cn.sibat.wangsheng.timeformat

import java.text.SimpleDateFormat

object TimeFormat{
  private val TIME = "# TimeFormat Trans ERROR # "
  /***
    * 将字符串format转成时间格式
    * @param time
    * @return
    */
  def string2timeString(time: String, format: String) :String=  {
    val foreFormat = new SimpleDateFormat(format)
    val ISOFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    try{
      ISOFormat.format(foreFormat.parse(time))
    }catch {
      case e: Exception =>{
        val date = "1979-01-01T08:08:08.000Z"
        ISOFormat.format(foreFormat.parse(date))
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

}