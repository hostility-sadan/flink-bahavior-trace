package com.etiantian.develop

import java.net.{InetAddress, InetSocketAddress}
import java.util
import java.util.Properties
import com.etiantian.common.entry.JsonDeserializationSchema
import com.etiantian.comom.util._
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.index.IndexRequest

import scala.collection.JavaConversions._

object FlinkBehaviorTrace {
  val logger = LogManager.getLogger("FlinkBehaviorTrace")
  def main(args: Array[String]): Unit = {

    val ESConfig = new util.HashMap[String,String]()
    ESConfig.put("cluster.name",CLUSTER_NAME)
    ESConfig.put("bulk.flush.max.actions",MAX_ACTION)

//    val addressList = List(
//      new InetSocketAddress(InetAddress.getByName(ES_NAME),ES_PORT)
//    )
    val addressList = ES_NAME.split(",").map(host => new InetSocketAddress(InetAddress.getByName(host),ES_PORT)).toList

    val pro = new Properties();
    pro.put("bootstrap.servers", BROKER);
    pro.put("group.id", GROUP_ID);
    pro.put("zookeeper.connect", KAFKA_ZOOKEEPER);
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(500)

    val topics = TOPIC.split(",")
    val list = new util.ArrayList[String]()
    val topicMsgSchame = new JsonDeserializationSchema
    topics.foreach(list.add(_))
    val Consumer010 = new FlinkKafkaConsumer010[String](
      list,topicMsgSchame,pro
    ).setStartFromEarliest().setCommitOffsetsOnCheckpoints(false)
//    logger.warn("==kafka传输时间============"+(time2-time1)+"======================")

    val kafkaStream = env.addSource(Consumer010)
    kafkaStream.map(x => {
      val time1 = System.currentTimeMillis()
      println("==kafka传输时间============"+time1+"======================")
    })
    val kafkaMessage = new TopicMessage()
    val dataStream = kafkaStream.filter(kafkaMessage.filterJson(_))
      .filter(kafkaMessage.filterType(_))
      .map(kafkaMessage.kafkaMessage(_))

        dataStream.map(x => println(x))

    val hbaseReaderData = new HbaseData
    val a = hbaseReaderData.writeData(dataStream)
//    a.map(x => println(x))

    val stream = hbaseReaderData.readData(dataStream)
//    stream.map(x=>println(x))

    //        stream.map(x => println(x))

    stream.addSink(new ElasticsearchSink[String](ESConfig,addressList,new ElasticsearchSinkFunction[String] {
      override def process(data: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val time1 = System.currentTimeMillis()
        try {
          val writeData = (new WriteEsData).addEs(data)
          val id = writeData._1
          val content = writeData._2
          val indexRequest = new IndexRequest().index(
            ES_INDEX
          ).`type`(
            ES_TYPE
          ).id(id).source(content)
          requestIndexer.add(indexRequest)
        }
        catch {
          case e: Exception => e.printStackTrace()
            println("====================== ('存储es数据处理错误')======================")
        }

        val time2 = System.currentTimeMillis()
        println("==存储至es的时间============"+(time2-time1)+"======================")
      }

    }))


    env.execute()
  }

}
