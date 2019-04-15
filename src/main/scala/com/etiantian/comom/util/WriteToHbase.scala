package com.etiantian.comom.util
import org.apache.hadoop.hbase.client.Put
import org.json.JSONObject


class WriteToHbase extends Serializable{

  //过滤不能转换成json形式的数据
  def filterJson(message: String):Boolean={
    val x = new JSONObject(message)
    try {
      //      println("value===="+x.get("value").toString)
      val value = new JSONObject(x.get("value").toString.toLowerCase)
      value.getJSONObject("after")
      value.getJSONObject("before")
      //      println("value===="+x.get("value").toString)
      true
    } catch {
      case e: Exception => false
    }
  }

  /**
    *
    * @param message
    * @param topic
    * @param columns  输入的字段以逗号分开
    * @param keys      输入的多个key以|分开，一个key由多个字段组成，中间以逗号隔开
    */
  def writeToHbase(message:String,topic:String,columns:String,keys:String)={
    val value = new JSONObject(new JSONObject(message).get("value").toString.toLowerCase)
    val topicName = new JSONObject(message).get("topic").toString
    try {
      val after = value.getJSONObject("after")
      if (topic == topicName) {
        val dataBase = topic.split("\\.")
        var table = ""
        if (dataBase(0) == DATABASE){
          table = dataBase(2)+ "_mysql"
        }else{
          table = dataBase(2)+ "_oracle"
        }
        val props = columns.split(",").toList
        val keyList = keys.split("\\|").toList
        var alias = ""
        for(key <- keyList){
          val ke = key.split(",")
          alias = ke.mkString("_")
          var rowkey = ""
          for(i <- 0 to ke.length - 1){
            rowkey += after.get(ke(i)).toString +","
          }
//          println(rowkey)
//          println(rowkey.substring(0,rowkey.lastIndexOf(",")))
          val put = new Put(rowkey.substring(0,rowkey.lastIndexOf(",")).getBytes)
          props.map(prop => {
              if (after.has(prop) && !ke.contains(prop)) {
                put.addColumn("info".getBytes, prop.getBytes, after.get(prop).toString.getBytes)
              }
            })
          FlinkHbaseFactory.put(table + "_" + alias, put)
        }
      }
    }catch {
      case e:Exception => e.printStackTrace()
        println("====================== ('存储Hbase数据错误')======================")
    }

  }

}
