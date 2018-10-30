package cn.sibat.gongan

import org.apache.spark.sql.SparkSession

object lookforszt{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("yarn").getOrCreate()
    val sc = sparkSession.sparkContext
    val path = ""
    val data = sparkSession.read.csv(path).map(s => )
  }
}

//690808429,20181017185456,地铁出站,3.8,   4.00,263030107,地铁五号线,下水径,OGT-107