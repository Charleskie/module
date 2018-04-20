package cn.sibat.traffic

import java.io.IOException
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.util
import java.sql.Connection


object ReadStaticInfo{
  private val dbDriver = "com.mysql.jdbc.Driver"
  private val dbUrl = "jdbc:mysql://210.75.252.138:4522/xbus_v4"
  private val dbUser = "xbpeng"
  private val dbPass = "xbpeng"
  var staticinfo = new scala.collection.mutable.HashMap[String,String]
  var conn: Connection = null
  try {
    Class.forName(dbDriver)
  conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)
  val sql = "select l.ref_id,l.directionCode,s.station_id,ss.name,s.stop_order,ss.lat,ss.lon from line l,line_stop s,station ss where l.id=s.line_id AND s.station_id=ss.id"
  val stmt = conn.createStatement
  val rs = stmt.executeQuery(sql)
  while ( {
    rs.next
  }) {
    val m1 = rs.getString(1)
    val m2 = String.valueOf(rs.getInt(2))
    val m3 = rs.getString(3)
    val m4 = rs.getString(4)
    val m5 = rs.getString(5)
    val m6 = rs.getString(6)
    val m7 = rs.getString(7)
    val keyid = m1+","+m2 +","+ m3
    val value = m4+","+m5+","+m6+","+m7
    if(!staticinfo.contains(keyid)){
      staticinfo.put(keyid,value)
    }
  }
  }catch {
    case e:SQLException => e.printStackTrace()
  }
  staticinfo.toMap
}

	
	
	
