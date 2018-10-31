package cn.sibat.gongan
import cn.sibat.gongan.warning.earlywarning.string2time

object testGongAn{
  def main(args: Array[String]): Unit = {
    val date = string2time("8/8/2018 10:00:00")
    println(date)
//    println(date.getDay,date.getDate,date.getHours,date.getMonth+1,date.getYear)

  }
}