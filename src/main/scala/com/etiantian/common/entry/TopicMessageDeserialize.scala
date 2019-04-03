//package com.etiantian.common.entry
//
//import java.time.{Instant, LocalDateTime, ZoneOffset}
//import java.time.format.DateTimeFormatter
//
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo
//import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
//import org.json.JSONObject
//
//class TopicMessageDeserialize extends KeyedDeserializationSchema[(String,String,String,String,String,String)]{
// override def deserialize(messageKey:Array[Byte],message:Array[Byte], topic:String ,partition:Int , offset:Long )= {
//
//   try {
//     val key = new String(messageKey)
//     val valueStr = new String(message)
//     var user_id = ""
//     var ques_id = ""
//     var answer = ""
//     var c_time = ""
//     var task_id = ""
//     var is_right = ""
//     try {
//       val json = new JSONObject(valueStr)
//       val after = json.getJSONObject("after")
//
//       if (after.has("user_id"))
//         user_id = after.get("user_id").toString
//       if (after.has("ques_id"))
//         ques_id = after.get("ques_id").toString
//       if (after.has("answer"))
//         answer = after.get("answer").toString
//       if (after.has("c_time")) {
//         val cTime = after.get("c_time").toString.toLong
//         val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//         val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(cTime), ZoneOffset.of("+08:00"))
//         c_time = dateTime.format(formatter)
//       }
//       if (after.has("task_id"))
//         task_id = after.get("task_id").toString
//       if (after.has("is_right"))
//         is_right = after.get("is_right").toString
//     } catch {
//       case e: Exception => {
//         println(e)
//         println("====================== ('jsonObject解析错误')======================")
////         (topic, partition.toString, offset.toString, new String(messageKey), new String(message), "")
//       }
//     }
//     (user_id, ques_id, answer, c_time, task_id, is_right)
//   } catch {
//     case e: Exception => {
//       e.printStackTrace()
//       throw e
//     }
//   }
// }
// override def isEndOfStream(nextElement:(String,String,String,String,String,String))={
//   false
// }
// override def getProducedType()={
//  BasicTypeInfo.getInfoFor(classOf[Tuple6[String,String,String,String,String,String]])
// }
//}