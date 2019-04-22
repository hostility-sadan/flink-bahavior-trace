package com.etiantian.develop

import java.util.Properties

import com.etiantian.common.entry.JsonDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object Demo {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.enableCheckpointing(500)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh132:9092,cdh133:9092,cdh134:9092")
    properties.setProperty("group.id", "p1")
    properties.setProperty("auto.offset.reset", "latest")
    //    properties.setProperty("enable.auto.commit", "true")
    //    properties.setProperty("auto.commit.interval.ms", "1000")

    val consumer010 = new FlinkKafkaConsumer010[String](
      "ubuntu.school.user_info",
      new JsonDeserializationSchema,
      properties
    ).setStartFromGroupOffsets()

    val dataStream = senv.addSource(consumer010)
    dataStream.print()
    //    dataStream.map(x =>new JSONObject(x).get("value")).map(func)


    //    val parameter = new Configuration()
    //        parameter.setString("quorum", "t193,t194,t195")
    ////    parameter.setString("quorum", "cdh132,cdh133,cdh134")
    //    parameter.setString("port", "2181")
    //    parameter.setString("tableName", "test_kafka_city_info")
    //    val hBaseOutputFormat = new HBaseOutputFormat
    //    hBaseOutputFormat.setConfiguration(parameter)
    //    senv.addSource(consumer010).map(x => {
    //      val array = x.split(",")
    //
    //      val put = new Put(array(0).getBytes())
    //      put.addColumn("position".getBytes(), "city_name".getBytes(), array(0).getBytes())
    //      put.addColumn("position".getBytes(), "lng".getBytes(), array(1).getBytes())
    //      put.addColumn("position".getBytes(), "lat".getBytes(), array(2).getBytes())
    //      put
    //    }).writeUsingOutputFormat(hBaseOutputFormat)
    senv.execute()
  }
}
