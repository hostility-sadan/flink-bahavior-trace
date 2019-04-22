package com.etiantian.comom.util

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}

import org.apache.commons.lang3.StringUtils
import org.json
import org.json.JSONObject

class TopicMessage extends Serializable {

  //after没有值时，只进行before的为需要删除ES的数据
  //after有值时，是需要添加或者更改ES的数据
  def otherMessage(message: String): Boolean = {
    val x = new json.JSONObject(message)
    val value = new JSONObject(x.get("value").toString.toLowerCase)
        try {
          value.getJSONObject("after")
          false
        } catch {
          case e: Exception =>  true
        }
  }

  //过滤不能没有value的数据
  def messageJson(message: String) ={
    try {
      val x = new JSONObject(message)
      new JSONObject(x.get("value").toString.toLowerCase)
      true
    } catch {
      case e: Exception => false
    }
  }

  //过滤不能转换成json形式的after或before的数据
  def valueJson(message: String): Boolean = {
    try {
      val x = new JSONObject(message)
      val value = new JSONObject(x.get("value").toString.toLowerCase)
      //时间戳不能正确转换 以及 user_id为null
          var beforeFlag = false
          var afterFlag = false
          try {
            val data = value.getJSONObject("before")
            data.get("c_time").toString.toLong
            StringUtils.isNotBlank(data.get("user_id").toString)
            beforeFlag = true
          } catch {
            case e: Exception => beforeFlag = false
          }
          try {
            val data2 = value.getJSONObject("after")
            data2.get("c_time").toString.toLong
            StringUtils.isNotBlank(data2.get("user_id").toString)
            afterFlag = true
          } catch {
            case e: Exception => afterFlag = false
          }
          beforeFlag || afterFlag
        }
  }


  //过滤before和after里面task_type != 3的数据
  def taskType(message: String, t: Int): Boolean = {
    val value = new JSONObject(new JSONObject(message).get("value").toString.toLowerCase)
    var content = new json.JSONObject()
    t match {
      case 0 => {
        content = value.getJSONObject("before")
      }
      case 1 => {
        content = value.getJSONObject("after")
      }
    }
    if (!content.has("task_type")) {
      true
    } else {
      if (content.get("task_type").toString.toInt != 3)
        false
      else
        true
    }
  }


  def kafkaMessage(message: String, t: Int) = {
    //  def kafkaMessage(message: String) = {
    val time2 = System.currentTimeMillis()
    val value = new JSONObject(new JSONObject(message).get("value").toString.toLowerCase)
    val topic = new JSONObject(message).get("topic").toString
    var content = new json.JSONObject()
    t match {
      case 0 => {
        content = value.getJSONObject("before")
      }
      case 1 => {
        content = value.getJSONObject("after")
      }
    }

    //   println("value: " + value)
    val user_id = content.get("user_id").toString
    var ques_id = ""
    var answer = ""
    var c_time = ""
    var task_id = ""
    var is_right = ""
    var task_type = "null"
    //     try {
    //val after = value.getJSONObject("after")

    //       if (after.has("user_id")){
    //         if (after.get("user_id") != null) {
    //           user_id = after.get("user_id").toString
    //         }
    //       }
    if (content.has("ques_id")) {
      ques_id = content.get("ques_id").toString
    }
    if (content.has("question_id")) {
      ques_id = content.get("question_id").toString
    }
    if (content.has("ques_parent_id")) {
      ques_id = content.get("ques_parent_id").toString
    }
    if (content.has("answer")) {
      if (content.get("answer") != null) {
        answer = content.get("answer").toString.toUpperCase
      }
    }
    if (content.has("answer_content")) {
      if (content.get("answer_content") != null) {
        answer = content.get("answer_content").toString.toUpperCase
      }
    }
    if (content.has("c_time")) {
      if (!content.get("c_time").toString.isEmpty) {
        val cTime = content.get("c_time").toString.toLong
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(cTime), ZoneOffset.of("+00:00"))
        c_time = dateTime.format(formatter)
      }
    }
    if (content.has("task_id"))
      task_id = content.get("task_id").toString
    if (content.has("right_answer")) {
      if (content.get("right_answer") != null) {
        is_right = content.get("right_answer").toString
      }
    }
    if (content.has("result")) {
      if (content.get("result") != null) {
        is_right = content.get("result").toString
      }
    }
    if (content.has("is_right")) {
      if (content.get("is_right").toString == "2") {
        is_right = 0.toString
      } else {
        is_right = content.get("is_right").toString
      }
    }
    if (content.has("task_type"))
      task_type = content.get("task_type").toString
    //     } catch {
    //       case e: Exception => {
    //         println(e)
    //         println("====================== ('数据解析错误')======================")
    ////         (topic, partition.toString, offset.toString, new String(messageKey), new String(message), "")
    //       }
    //     }
    val time3 = System.currentTimeMillis()
    println("flink处理kafka数据的时间============" + (time3 - time2).toString + "======================")
    (user_id, ques_id, answer, c_time, task_id, is_right, task_type, topic)
  }
}