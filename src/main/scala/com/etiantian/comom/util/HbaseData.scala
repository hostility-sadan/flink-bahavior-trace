package com.etiantian.comom.util

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.flink.api.scala._

class HbaseData extends Serializable {
  //(user_id,ques_id,answer,c_time,task_id,is_right,task_type,topic)
  def readData(dataStream: DataStream[(String, String, String, String, String, String, String, String)]) = {
    dataStream.map(x => {
      val time1 = System.currentTimeMillis()
      val user_id = x._1
      val ques_id = x._2
      var answer = x._3
      val c_time = x._4
      val task_id = x._5
      val is_right = x._6
      val task_type = x._7
      val topic = x._8

      if (task_type == "3") {
        val get = new Get((user_id + "," + ques_id + "," + c_time).getBytes)
        answer = FlinkHbaseFactory.get("tp_ques_answer", get, "info", "ans")
      }
      var jid = ""
      var schoolId = ""
      val userId = new Get(user_id.getBytes)
      if (topic == STU_TOPIC) {
        val uid = FlinkHbaseFactory.get("user_info_mysql_user_id", userId, "info", "ett_user_id")
        val sid = FlinkHbaseFactory.get("user_info_mysql_user_id", userId, "info", "dc_school_id")
        if (uid != null && sid != null){
          jid = uid
          schoolId = sid
        }
      }
      if (topic == ANS_TOPIC) {
        val uid =  FlinkHbaseFactory.get("user_info_mysql_ref", userId, "info", "ett_user_id")
        val sid = FlinkHbaseFactory.get("user_info_mysql_ref", userId, "info", "dc_school_id")
        if (uid != null && sid != null){
          jid = uid
          schoolId = sid
        }
      }
      if (topic == BR_TOPIC || topic == TOLJ_TOPIC) {
        jid = user_id
        val sid = FlinkHbaseFactory.get("user_profile", userId, "up", "sid")
        if (sid != null){
          schoolId = sid
        }
      }
      val quesId = new Get(ques_id.getBytes)
      val subject = FlinkHbaseFactory.get("tol_ques_subject_info", quesId, "tol", "sub")
      val point_ids = FlinkHbaseFactory.get("ques_point_info", quesId, "tol", "ids")
      var list = ""
      var type_id = ""
      val object_type = 2
      val is_axp = 0
      if (jid != "" && jid != null && subject != null) {
        val newId = new Get((jid + "," + subject).getBytes)
        type_id = FlinkHbaseFactory.get("jid_sub_ver_info", newId, "tol", "ver")
        //        println("ques_id,type_id: " + ques_id + "," + type_id)
      }
      if (type_id != null) {
        val listGet = new Get((ques_id + "," + type_id).getBytes)
        list = FlinkHbaseFactory.get("ques_version_info", listGet, "tol", "info")
        //        println("list: " + list)
      }
      val time2 = System.currentTimeMillis()
      println("读取hbase的时间============" + (time2 - time1) + "======================")

      //        jid + "," + ques_id + "," + c_time + "&" + jid + "&" + ques_id + "&" + answer + "&" + c_time + "&" + task_id + "&" + is_right + "&" + subject + "&" + point_ids + "&" + list + "&" + object_type + "&" + is_axp

      ((jid + "," + ques_id + "," + c_time), jid, ques_id, answer, c_time, task_id, is_right, subject, point_ids, list, object_type, is_axp, schoolId)

    })
  }

  //处理同一用户同一时间有多个答案
  def writeData(dataStream: DataStream[(String, String, String, String, String, String, String, String)]) = {
    dataStream.map(x => {
      val user_id = x._1
      val ques_id = x._2
      val answer = x._3
      val c_time = x._4
      val task_type = x._7

      if (task_type == "3") {
        val time1 = System.currentTimeMillis()
        val put = new Put((user_id + "," + ques_id + "," + c_time).getBytes)
        val get = new Get((user_id + "," + ques_id + "," + c_time).getBytes)
        val answerGet = FlinkHbaseFactory.get("tp_ques_answer", get, "info", "ans")
        if (answerGet != null) {
          if (!answerGet.contains(answer)) {
            val ansNew = answer + "|" + answerGet
            put.addColumn("info".getBytes, "ans".getBytes, ansNew.getBytes)
            FlinkHbaseFactory.put("tp_ques_answer", put)
          }
        } else {
          put.addColumn("info".getBytes, "ans".getBytes, answer.getBytes)
          FlinkHbaseFactory.put("tp_ques_answer", put)
        }
        val time2 = System.currentTimeMillis()
        println("==数据写入hbase的时间============" + (time2 - time1) + "======================")
      }
    })
  }

  def errorToHBase(message: String) = {
    val rowkey = System.currentTimeMillis()
    val put = new Put(rowkey.toString.getBytes)
    put.addColumn("info".getBytes, "tol".getBytes, message.getBytes)
    FlinkHbaseFactory.put("flink_behavior_trace_error", put)
  }

  def errorToHBase(message: (String, String, String, String, String, String, String, String)) = {
    val rowkey = System.currentTimeMillis()
    val put = new Put(rowkey.toString.getBytes)
    put.addColumn("info".getBytes, "tol".getBytes, message.toString().getBytes)
    FlinkHbaseFactory.put("flink_behavior_trace_error", put)
  }

  def errorToHBase(message: (String, String, String, String, String, String, String, String, String, String, Int, Int, String)) = {
    val rowkey = System.currentTimeMillis()
    val put = new Put(rowkey.toString.getBytes)
    put.addColumn("info".getBytes, "tol".getBytes, message.toString().getBytes)
    FlinkHbaseFactory.put("flink_behavior_trace_error", put)
  }

}
