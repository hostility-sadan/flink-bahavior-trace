package com.etiantian.develop

import java.net.{InetAddress, InetSocketAddress}
import java.util
import java.util.Properties

import com.etiantian.common.entry.JsonDeserializationSchema
import com.etiantian.comom.util._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink

import scala.collection.JavaConversions._

/**
  * 用户行为轨迹实时数据存储至es
  * flink从kafka里面读取数据，根据需求再读取hbase的数据，将结果数据存储至es
  */
object FlinkBehaviorTrace {
  def main(args: Array[String]): Unit = {

    val ESConfig = new util.HashMap[String, String]()
    ESConfig.put("cluster.name", CLUSTER_NAME)
    ESConfig.put("bulk.flush.max.actions", MAX_ACTION)

    val addressList = ES_NAME.split(",").map(host => new InetSocketAddress(InetAddress.getByName(host), ES_PORT)).toList

    val pro = new Properties();
    pro.put("bootstrap.servers", BROKER);
    pro.put("group.id", GROUP_ID);
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(500)

    val topics = TOPIC.split(",")
    topics.toList
    val list = new util.ArrayList[String]()
    val topicMsgSchame = new JsonDeserializationSchema
    topics.foreach(list.add(_))
    val Consumer010 = new FlinkKafkaConsumer010[String](
      list, topicMsgSchame, pro
    ).setStartFromGroupOffsets().setCommitOffsetsOnCheckpoints(false)
    //    logger.warn("==kafka传输时间============"+(time2-time1)+"======================")

    val kafkaMessage = new TopicMessage()
    val hbaseReaderData = new HbaseData()

    val kafkaStream = env.addSource(Consumer010)

    //对不同的数据分别进行处理
    val kafkaRightData = kafkaStream.filter(kafkaMessage.messageJson(_))
      .filter(kafkaMessage.valueJson(_))

    //将两种方式的数据分别存储至hbase
    kafkaStream.filter(!kafkaMessage.messageJson(_)).map(hbaseReaderData.errorToHBase(_))
    kafkaStream.filter(kafkaMessage.messageJson(_)).filter(!kafkaMessage.valueJson(_)).map(hbaseReaderData.errorToHBase(_))

    //对有before的task_type=3的进行过滤
    val beforeKafkaData = kafkaRightData.filter(kafkaMessage.otherMessage(_))
      .filter(kafkaMessage.taskType(_, 0))
    //    kafkaRightData.filter(kafkaMessage.filterMessage(_)).map(x => println("beforeKafkaData: "+x))

    //对有after的task_type = 3的进行过滤
    val kafkaData = kafkaRightData.filter(!kafkaMessage.otherMessage(_))
      .filter(kafkaMessage.taskType(_, 1))
    //    kafkaRightData.filter(!kafkaMessage.filterMessage(_)).map(x => println("kafkaData: "+x))

    //处理有before没有after的数据
    val dataDeleteStream = beforeKafkaData.map(kafkaMessage.kafkaMessage(_, 0))
    //    dataDeleteStream.map(x => println("处理后before的数据结果：" + x))

    //    处理有after的数据
    val dataStream = kafkaData.map(kafkaMessage.kafkaMessage(_, 1))
    //    dataStream.map(x => println("处理后after的数据结果：" + x))


    //有after的数据才会进行写hbase记录答案的操作
    hbaseReaderData.writeData(dataStream)


    val stream = hbaseReaderData.readData(dataStream).filter(x => x._2 != null && x._2 != "" && x._2 != "null")

    //    stream.map(x => println("stream: " + x))

    val deleteStream = hbaseReaderData.readData(dataDeleteStream)

    //    deleteStream.map(x => println("deleteStream" + x))

    val error2 = hbaseReaderData.readData(dataStream).filter(x => x._2 == null || x._2 == "" || x._2 == "null")
    //将错误数据存储至HBASE
    error2.map(hbaseReaderData.errorToHBase(_))


    //将有after的数据存储至ES
    stream.addSink(new ElasticsearchSink[(String, String, String, String, String, String, String, String, String, String, Int, Int)](ESConfig, addressList, new FlinkAddES))

    //只有before的数据，将ES中有的数据进行删除
    deleteStream.addSink(new ElasticsearchSink[(String, String, String, String, String, String, String, String, String, String, Int, Int)](ESConfig, addressList, new FlinkDeleteEs))


    env.execute()
  }


}
