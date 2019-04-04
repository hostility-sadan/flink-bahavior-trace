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
        val props = columns.split(",").toList
        val keyList = keys.split("\\|").toList
        var alias = ""
        keyList.map(key => {
          if (key.length >= 3) {
            alias = key.substring(0, 3)
          } else {
            alias = key
          }
          val put = new Put(alias.getBytes)
          props.map(prop => {
            if (after.has(prop) && prop != key) {
              put.addColumn("info".getBytes, prop.substring(0, 3).getBytes, after.get(prop).toString.getBytes)
            }
          })
          FlinkHbaseFactory.put(topic + "_" + alias, put)
        })
      }
    }catch {
      case e:Exception => e.printStackTrace()
        println("====================== ('存储Hbase数据错误')======================")
    }

  }

}
