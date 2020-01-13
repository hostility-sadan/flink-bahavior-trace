package com.etiantian.comom.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.index.get.GetResult

class FlinkAddES extends ElasticsearchSinkFunction[(String, String, String, String, String, String, String, String, String, String, Int, Int,String)]{
  override def process(data: (String, String, String, String, String, String, String, String, String, String, Int, Int,String), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
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
}
