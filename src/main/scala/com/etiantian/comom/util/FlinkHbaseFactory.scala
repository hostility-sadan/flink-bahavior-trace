package com.etiantian.comom.util

import java.util.HashMap

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._

object FlinkHbaseFactory {
  var conn: Connection = null
  var tables: HashMap[String, Table] = new HashMap[String, Table]
  def initConn() {
    if (conn == null || conn.isClosed()) {
      println("----  Init Conn  -----")
      var hconf = HBaseConfiguration.create()
      hconf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER)
      hconf.set("hbase.zookeeper.property.clientPort", "2181")
      conn = ConnectionFactory.createConnection(hconf)
    }
  }
  def getConn() = {
    initConn
    conn
  }
  def getTable(tablename: String) = {
    tables.getOrElse(tablename, {
      initConn
      conn.getTable(TableName.valueOf(tablename))
    })
  }
  def put(tableName: String, p: Put) {
    getTable(tableName)
      .put(p)
  }

  def get(tableName: String, get: Get,cf:String, column: String) = {
    val r = getTable(tableName)
      .get(get)
    if (r!=null && !r.isEmpty()) {
      new String(r.getValue(cf.getBytes, column.getBytes))
    }else null
  }
}