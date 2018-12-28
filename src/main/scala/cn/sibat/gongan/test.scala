package cn.sibat.gongan

import java.text.SimpleDateFormat
import java.util.Locale
import cn.sibat.util.timeformat.TimeFormat._

object test{
  def main(args: Array[String]): Unit = {
    println(StringToISO("DEC 11 2018 12:12:21:000PM","MMM d yyyy h:mm:ss:000aa"))
    val foreFormat = new SimpleDateFormat("MMM d yyyy h:mm:ss:000aa",Locale.ENGLISH)
    println(StringToISO("DEC 11 2018 12:12:21:000AM","MMM d yyyy h:mm:ss:000aa"))

//    println(StringToISO("DEC 11 2018 12:12:21:000AM",foreFormat))
    val ts = foreFormat.parse("DEC 11 2018 12:12:21:000AM");
    println(ts);
  }
}