package com.etiantian.comom.util

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}
import org.json.JSONObject

class TopicMessage extends Serializable{
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

  //过滤task_type != 3的数据
  def filterType(message: String):Boolean={
    val value = new JSONObject(new JSONObject(message).get("value").toString.toLowerCase)
//    println("value:===================================="+value)
    val after = value.getJSONObject("after")
    if (!after.has("task_type")) {
      true
    }else{
      if (after.get("task_type").toString.toInt != 3)
        false
      else
        true
    }
  }

 def kafkaMessage(message: String)= {
   val time2 = System.currentTimeMillis()
   val value = new JSONObject(new JSONObject(message).get("value").toString.toLowerCase)
   val topic = new JSONObject(message).get("topic").toString
//   println("value: " + value)
     var user_id = ""
     var ques_id = ""
     var answer = ""
     var c_time = ""
     var task_id = ""
     var is_right = ""
     var task_type = "null"
     try {
       val after = value.getJSONObject("after")

       if (after.has("user_id"))
         user_id = after.get("user_id").toString
       if (after.has("ques_id")){
         ques_id = after.get("ques_id").toString
       }
       if (after.has("question_id")){
         ques_id = after.get("question_id").toString
       }
       if (after.has("ques_parent_id")){
         ques_id = after.get("ques_parent_id").toString
       }
       if (after.has("answer")){
         answer = after.get("answer").toString.toUpperCase
       }
       if (after.has("answer_content")) {
         answer = after.get("answer_content").toString.toUpperCase
       }
       if (after.has("c_time")) {
         val cTime = after.get("c_time").toString.toLong
         val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
         val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(cTime), ZoneOffset.of("+00:00"))
         c_time = dateTime.format(formatter)
       }
       if (after.has("task_id"))
         task_id = after.get("task_id").toString
       if (after.has("right_answer"))
         is_right = after.get("right_answer").toString
       if (after.has("result"))
         is_right = after.get("result").toString
       if (after.has("is_right")){
         if(after.get("is_right").toString == "2"){
           is_right = 0.toString
         }else{
           is_right = after.get("is_right").toString
         }
       }
       if (after.has("task_type"))
         task_type = after.get("task_type").toString
     } catch {
       case e: Exception => {
         println(e)
         println("====================== ('数据解析错误')======================")
//         (topic, partition.toString, offset.toString, new String(messageKey), new String(message), "")
       }
     }
   val time3 = System.currentTimeMillis()
   println("flink处理kafka数据的时间============"+(time3-time2).toString+"======================")
   (user_id, ques_id, answer, c_time, task_id, is_right,task_type,topic)
   }
}