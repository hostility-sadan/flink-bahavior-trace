package com.etiantian.develop

import java.util
import java.util.Properties

import com.etiantian.common.entry.JsonDeserializationSchema
import com.etiantian.comom.util.{FlinkHbaseFactory, WriteToHbase}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.hadoop.hbase.client.Put
import org.apache.flink.api.scala._
import org.json.JSONObject

object UserInfoToHbase {

  def main(args: Array[String]): Unit = {
    val pro = new Properties();
    pro.put("bootstrap.servers", BROKER);
    pro.put("group.id", GROUP_ID);
    pro.put("zookeeper.connect", KAFKA_ZOOKEEPER);
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(500)

    val topics = COMMON_TOPIC.split(",")
    val list = new util.ArrayList[String]()
    val topicMsgSchame = new JsonDeserializationSchema
    topics.foreach(list.add(_))
    val Consumer010 = new FlinkKafkaConsumer010[String](
      list,topicMsgSchame,pro
    ).setStartFromEarliest().setCommitOffsetsOnCheckpoints(false)
//    env.addSource(Consumer010).map(message => {
//      val value = new JSONObject(new JSONObject(message).get("value").toString.toLowerCase)
//      println(value)
//      try {
//        var user_id = ""
//        var ref = ""
//        var jid = ""
//        try {
//          val after = value.getJSONObject("after")
//          if (after.has("user_id"))
//            user_id = after.get("user_id").toString
//          if (after.has("ref"))
//            ref = after.get("ref").toString
//          if (after.has("ett_user_id"))
//            jid = after.get("ett_user_id").toString
//
//          println(user_id + "," + ref + "," + jid)
//          val put1 = new Put(user_id.getBytes)
//          put1.addColumn("info".getBytes, "ref".getBytes, ref.getBytes)
//          put1.addColumn("info".getBytes, "jid".getBytes, jid.getBytes)
//          FlinkHbaseFactory.put("tp_ques_test", put1)
//          val put2 = new Put(ref.getBytes)
//          put2.addColumn("info".getBytes, "uid".getBytes, jid.getBytes)
//          put2.addColumn("info".getBytes, "jid".getBytes, jid.getBytes)
//          FlinkHbaseFactory.put("tp_ques_test", put2)
////          val put3 = new Put(jid.getBytes)
////          put3.addColumn("info".getBytes, "jid".getBytes, jid.getBytes)
////          FlinkHbaseFactory.put("tp_ques_answer", put3)
//        }
//      }catch {
//        case e: Exception => {
//          println(e)
//          println("====================== ('数据解析错误')======================")
//          //         (topic, partition.toString, offset.toString, new String(messageKey), new String(message), "")
//        }
//      }
//    })

    val write = new WriteToHbase()
    env.addSource(Consumer010).map(write.writeToHbase(_,COMMON_TOPIC,"user_id,ref,ett_user_id","user_id|ref|user_id,ref"))
    env.execute()
  }
}
