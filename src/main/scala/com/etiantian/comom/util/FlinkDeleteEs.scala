package com.etiantian.comom.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest

class FlinkDeleteEs extends ElasticsearchSinkFunction[(String, String, String, String, String, String, String, String, String, String, Int, Int)]{
  override def process(data: (String, String, String, String, String, String, String, String, String, String, Int, Int), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    val time1 = System.currentTimeMillis()
    try {
      val writeData = (new WriteEsData).addEs(data)
      val id = writeData._1
      val hbaseData = new HbaseData

      val result = new GetRequest().index(ES_INDEX).`type`(ES_TYPE).id(id)
      println("获取es上的id数据：" + result)
      if (new GetRequest().index(ES_INDEX).`type`(ES_TYPE).id(id) != null){
        val deleteRequest = new DeleteRequest().index(ES_INDEX).`type`(ES_TYPE).id(id)
        requestIndexer.add(deleteRequest)
      }else{
        hbaseData.errorToHBase(data)
      }

    }
    catch {
      case e: Exception => e.printStackTrace()
        println("====================== ('删除es数据处理错误')======================")
    }

    val time2 = System.currentTimeMillis()
    println("==删除es数据的时间============"+(time2-time1)+"======================")
  }
}
