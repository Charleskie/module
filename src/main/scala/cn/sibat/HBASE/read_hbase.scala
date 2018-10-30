package cn.sibat.HBASE

/**
  * Created by HuangAn on 2018/10/12.
  */

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object read_hbase{
  private def pointRuler(x: ZHX1, y: ZHX1)= {
    if (x.lat != 0 && x.lon!= 0 && y.lat != 0 && y.lon != 0)
    {
      val R =  6378.137
      val radLatx = x.lat * Math.PI / 180
      val radLaty = y.lat* Math.PI / 180
      val latDiff = radLaty - radLatx
      val lonDiff = y.lon* Math.PI / 180 - x.lon * Math.PI / 180
      val s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(latDiff / 2),2) + Math.cos(radLatx) * Math.cos(radLaty) * Math.pow(Math.sin(lonDiff / 2),2)))
      val mile = BigDecimal.decimal(s * R).setScale(3, BigDecimal.RoundingMode.HALF_UP)
      mile
    }else{
      val mile = BigDecimal.decimal(0).setScale(3, BigDecimal.RoundingMode.HALF_UP)
      mile
    }
  }

  private def getData(data: RDD[ZHX1]) = {
    val GPSmile = data.sortBy(x => (x.cardId, x.time)).groupBy(x => (x.cardId, x.date))
      .map(x => {
        val diff = {val arr = x._2.toArray
          for {
            i <- 0 until arr.size-1;
            point = pointRuler(arr(i),arr(i+1))
          } yield point }
        (x._1._1, x._1._2, diff.sum)
      }).distinct()
    GPSmile
  }

  def main(args:Array[String]): Unit = {

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("HBaseTest").set("spark.testing.memory", "512000000")
    val sc = new SparkContext(sparkConf)
    val outPath = "E:\\sibat\\code\\huangan0801"
    //------------------------------------------------------------

    // 创建hbase configuration
    val tablename = "ZHX_FILE_BUS_GPS_2018_08_01"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","10.20.7.129,10.20.7.131,10.20.7.132,10.20.7.133")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    // 将数据映射为表,也就是将 RDD转化为 dataframe schema
    val data = hbaseRDD.map(r => {
      val cardId = Bytes.toString(r._2.getRow).substring(0,9)
      val date = Bytes.toString(r._2.getValue(Bytes.toBytes("rt"),Bytes.toBytes("T"))).substring(0,10)
      val time = Bytes.toString(r._2.getValue(Bytes.toBytes("rt"),Bytes.toBytes("T"))).substring(11,19)
      val lat = Bytes.toString(r._2.getValue(Bytes.toBytes("rt"),Bytes.toBytes("LAT"))).toDouble
      val lon = Bytes.toString(r._2.getValue(Bytes.toBytes("rt"),Bytes.toBytes("LON"))).toDouble
      ZHX1(cardId, date, time, lat, lon)
    })
    //    data.foreach(println)
    //    hbaseRDD.coalesce(100).saveAsTextFile(outPath)
    data.coalesce(10).saveAsTextFile(outPath)
    //    getData(data).coalesce(1).saveAsTextFile(outPath)
  }
  case class ZHX1(cardId:String,date:String,time:String,lat:Double,lon:Double){
    override def toString: String = Array(cardId,date,time,lat,lon).mkString(",")
  }

}




